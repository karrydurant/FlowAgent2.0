# 多 Agent 协商引擎

## 舱壁隔离与熔断——PeerCircuitBreaker

舱壁隔离：把系统资源（线程池、数据库连接、HTTP 连接池、并发额度）按调用方分开隔离，每个调用方有自己的配额，一个慢调用方最多耗尽自己的配额，不会拖垮整个系统。

熔断：当一个下游服务连续失败，就主动 "断开" 对它的调用

```java
public class PeerCircuitBreaker {
    // 放行失败的原因
    public enum Skip {
        // 本地投递池满了：这次没投出去，但下一轮还有机会
        SKIPPED_BULKHEAD,
        // 这个 Agent 熔断了：本轮直接跳过，不再给它发消息
        SKIPPED_CIRCUIT_OPEN,
        // 一轮内重试次数用完
        SKIPPED_BULKHEAD_EXHAUSTED
    }

    // 一次投递、答复的最终结论
    public enum Outcome {
        SUCCESS,              // 成功：Agent 正常回话了
        // 根据返回的错误类型来判断是瞬时失败还是永久失败
        TRANSIENT_FAILURE,    // 瞬时失败：Agent 跑炸了
        PERMANENT_FAILURE,    // 永久失败：对端明确拒绝/不可用
        SILENT,               // 沉默：本轮截止时间到了，但一条答复都没收到
        LOCAL_ERROR           // 本地错误：Redis/执行器故障，不是对端 Agent 的问题
    }

    // 一次放行的结果
    // skip       没放行的原因，null 表示放行了
    // probeToken 半开探针的 token，普通投递是 null
    public record Permit(Skip skip, String probeToken) {
        public boolean acquired() {
            return skip==null;
        }
        public static Permit normal() {
            return new Permit(null, null);
        }
        public static Permit skipped(Skip skip) {
            return new Permit(skip, null);
        }
    }

    // 熔断器三态
    public enum State {
        CLOSED,     // 闭合：正常投递
        OPEN,       // 打开：跳过不投
        HALF_OPEN  // 半开：放一个探针试试
    }

    private final NegotiationProperties properties;
    private final LongSupplier clock;

    // 每个 Agent 一个信号量：控制它同时在飞的投递数
    private final Map<String, Semaphore> permits=new ConcurrentHashMap<>();

    // 每个 Agent 一份健康状态：熔断状态机跑在这上面
    private final Map<String, Health> health=new ConcurrentHashMap<>();

    @Autowired
    public PeerCircuitBreaker(NegotiationProperties properties) {
        this(properties, System::currentTimeMillis);
    }

    /** 测试用：注入假时钟，不用真 sleep */
    PeerCircuitBreaker(NegotiationProperties properties, LongSupplier clock) {
        this.properties = properties;
        this.clock = clock;
    }

    // 尝试放行一次投递
    public Permit acquire(String agentId) {
        if (agentId == null) {
            return Permit.normal();   // 没指定 Agent 就不限流
        }

        // 拿这个 Agent 的健康状态，没有就新建一个
        Health h = health.computeIfAbsent(agentId, ignored -> new Health());
        long now = clock.getAsLong();
        String probeToken = null;

        // 熔断
        synchronized (h) {
            // 熔断打开中：如果还在熔断窗口内，直接跳过
            if (h.state == State.OPEN) {
                if (now < h.openUntil) {
                    // 还没到恢复时间，这次不投
                    return Permit.skipped(Skip.SKIPPED_CIRCUIT_OPEN);
                }

                // 熔断窗口到了，进半开状态，准备放探针
                transition(h, State.HALF_OPEN, now, "open-window-elapsed");
                h.probeToken = null;
                log.info("[PeerCircuit] OPEN -> HALF_OPEN | agent={} | attempts={}",
                        agentId, h.attempts);
            }

            // 半开状态：控制同时只有一个探针在飞
            if (h.state == State.HALF_OPEN) {
                if (h.probeToken != null) {
                    // 已经有一个探针在跑了
                    long elapsed = now - h.probeStartedAt;
                    if (elapsed > Math.max(1L, properties.getCircuitProbeTimeoutMs())) {
                        // 探针超时了，说明 Agent 还是没好，重新熔断
                        reopen(agentId, h, now, "probe-timeout", false);
                        return Permit.skipped(Skip.SKIPPED_CIRCUIT_OPEN);
                    }
                    // 探针还在跑，这次不投
                    return Permit.skipped(Skip.SKIPPED_CIRCUIT_OPEN);
                }
                // 没有探针在跑，放一个新探针
                h.probeToken = UUID.randomUUID().toString();
                h.probeStartedAt = now;
                probeToken = h.probeToken;
            }
        }

        // 舱壁限流
        Semaphore semaphore = permits.computeIfAbsent(agentId, ignored ->
                new Semaphore(Math.max(1, properties.getPerPeerMaxInflight())));

        if (!semaphore.tryAcquire()) {
            // 这个 Agent 同时在飞的投递已经到上限了
            synchronized (h) {
                // 如果刚才放了探针但没真投出去（因为舱壁满了）
                if (probeToken != null && probeToken.equals(h.probeToken)) {
                    h.probeToken = null;
                }
            }
            return Permit.skipped(Skip.SKIPPED_BULKHEAD);
        }

        // 两道闸都过了，放行
        // 如果是半开探针，带上 token，结算时要原样带回来
        return new Permit(null, probeToken);
    }

    // 投递完了（成功失败都算），归还舱壁许可
    public void release(String agentId) {
        if (agentId == null) {
            return;
        }
        Semaphore semaphore = permits.get(agentId);
        if (semaphore != null) {
            semaphore.release();
        }
    }

    // 给一次投递/答复下结论。
    public void complete(String agentId, String probeToken, Outcome outcome) {
        if (agentId == null || outcome == null) {
            return;
        }
        Health h = health.computeIfAbsent(agentId, ignored -> new Health());
        long now = clock.getAsLong();

        synchronized (h) {
            // ── 本地错误：不算 Agent 的 ──
            if (outcome == Outcome.LOCAL_ERROR) {
                if (probeToken != null && probeToken.equals(h.probeToken)) {
                    // 把探针位还回去，下次重新探，别算 Agent 的失败
                    h.probeToken = null;
                    h.probeStartedAt = 0L;
                }
                return;
            }

            // ── 探针结算 ──
            if (probeToken != null) {
                if (!probeToken.equals(h.probeToken)) {
                    // 迟到的旧探针结果：当前已经换了新探针，这个旧的不算数
                    return;
                }
                h.probeToken = null;
                h.probeStartedAt = 0L;

                if (outcome == Outcome.SUCCESS) {
                    // 探针成功了 → Agent 恢复正常，关闭熔断
                    close(agentId, h, now, "probe-success");
                } else {
                    // 探针失败了 → 还是不行，重新熔断
                    reopen(agentId, h, now, "probe-" + outcome.name().toLowerCase(),
                            outcome == Outcome.PERMANENT_FAILURE);
                }
                return;
            }

            // ── 半开期间的普通投递：只记样本，不参与开关 ──
            if (h.state == State.HALF_OPEN) {
                record(h, outcome, now);
                return;
            }

            // 熔断打开期间：啥都不记
            if (h.state == State.OPEN) {
                return;
            }

            // ── 正常闭合状态：记样本，判断要不要熔断 ──
            record(h, outcome, now);

            // 永久失败直接熔断，不等阈值
            if (outcome == Outcome.PERMANENT_FAILURE) {
                reopen(agentId, h, now, "permanent-failure", true);
                return;
            }

            // 检查三个熔断条件
            String cause = tripCause(h);
            if (cause != null) {
                reopen(agentId, h, now, cause, false);
            }
        }
    }

    // 判断是否触发熔断
    private String tripCause(Health h) {
        // 条件 1：连续失败次数够了
        if (h.consecutiveFailures >= Math.max(1, properties.getCircuitFailureThreshold())) {
            return "failure-threshold";
        }
        // 条件 2：连续沉默次数够了
        if (h.silentStreak >= Math.max(1, properties.getCircuitSilentThreshold())) {
            return "silent-threshold";
        }
        // 条件 3：时间窗口内失败率够了
        int samples = h.window.size();
        int minimum = Math.max(1, properties.getCircuitMinSamples());
        if (samples < minimum) {
            return null;   // 样本太少，不判
        }
        int failures = 0;
        for (Sample sample : h.window) {
            if (sample.failure()) {
                failures++;
            }
        }
        double rate = (double) failures / samples;
        return rate >= properties.getCircuitFailureRateThreshold() ? "failure-rate" : null;
    }

    // 打开熔断：指数退避
    private void reopen(String agentId, Health h, long now, String cause, boolean permanent) {
        h.attempts = Math.min(h.attempts + 1, 10);   // 最多退避 10 次
        long base = Math.max(1L, permanent
                ? properties.getCircuitPermanentOpenMs()   // 永久失败熔断更久
                : properties.getCircuitOpenMs());
        long max = Math.max(base, properties.getCircuitOpenMaxMs());

        long duration = base;
        // 指数退避：每次翻倍，直到上限
        for (int i = 1; i < h.attempts && duration < max; i++) {
            duration = duration > max / 2 ? max : duration * 2;
        }
        duration = Math.min(max, Math.max(1L, duration));
        duration = jitter(duration);   // 加随机抖动，别所有 Agent 同时恢复

        transition(h, State.OPEN, now, cause);
        h.openUntil = now + duration;
        h.probeToken = null;
        h.probeStartedAt = 0L;
        log.warn("[PeerCircuit] -> OPEN | agent={} | cause={} | attempts={} | openFor={}ms",
                agentId, cause, h.attempts, duration);
    }

    // 给熔断时长加随机抖动，避免所有 Agent 同时恢复导致流量突增
    private long jitter(long duration) {
        double ratio = Math.max(0D, Math.min(0.9D, properties.getCircuitOpenJitterRatio()));
        if (ratio <= 0D) {
            return duration;
        }
        long delta = Math.max(1L, (long) (duration * ratio));
        long low = Math.max(1L, duration - delta);
        long high = duration + delta;
        return ThreadLocalRandom.current().nextLong(low, high + 1);
    }
}
```

## 问题

### 并发隔离具体怎么做的？是每个 Agent 一个线程池吗？还是信号量？

每个 Agent 一个 `Semaphore`，控制它同时在飞的投递数

### 熔断是什么状态机？

三态：CLOSED → OPEN → HALF_OPEN

CLOSED：正常投递

OPEN：跳过不投，等熔断窗口到期

HALF_OPEN：窗口到了，放一个探针试试 Agent 恢复了没

### 熔断触发条件是什么

1.连续失败达到一定次数(3 次)

2.连续沉默达到一定次数(3 次)

3.时间窗口内失败率达标(50 %)

### 为什么把熔断和限流分为两层

两层解决的是不同问题：

舱壁(信号量)：一个慢 Agent 占满整个投递池 —— 它同时在飞的投递数被限制了，不会把全局池子占满

熔断(三态状态机)：一个已经挂了的 Agent，每轮还照样给它发消息干等超时 —— 熔断了直接跳过，不发了

### 了解 Resilience4j 吗，简单介绍一下？为什么这里不用 Resilience4j？

Resilience4j 是 Java 生态里的一个容错库，它提供了熔断、限流、舱壁、重试、超时等功能。之所以不用是因为此处对熔断和舱壁隔离的要求比较简单，完全可以自己实现

### 熔断打开后怎么恢复

指数退避 + 探针：

1. 熔断打开，等一段时间（第一次几秒，第二次翻倍，指数退避，有上限）

2. 时间到了进 HALF_OPEN，放一个探针请求试试

3. 探针成功 → 关闭熔断，恢复正常

4. 探针失败 → 重新打开熔断，退避时间再翻倍

5. 加随机抖动，避免所有 Agent 同时恢复导致流量突增

### 一个 Agent 一直不回话，算失败吗？

算

### 本地 Redis 挂了导致投递失败

### 探针超时了怎么办？

