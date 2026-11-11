# 多 Agent 会话与消息传输

## 数据结构

### 会话里的一条消息——A2aMessage

| 字段 | 类型 | 说明 |
|---|---|---|
| messageId | String | 消息唯一标识，服务端生成（a2a-msg-xxx） |
| sessionId | String | 所属会话 |
| seq | long | 会话内单调序号，Redis 分配，当增量拉取游标用 |
| fromAgentId | String | 发送方 |
| toAgentId | String | 接收方，null = 广播 |
| kind | MessageKind | 消息种类（6种） |
| payload | Map | 消息体，结构由 kind 决定 |
| replyTo | String | 回复的是哪条消息（messageId），配对用 |
| importance | Importance | NORMAL / CRITICAL，历史裁剪时 CRITICAL 不丢 |
| createdAt | Instant | 投递时间 |

### 消息类型——MessageKind

| kind | 谁发的 | 干什么 | 会不会触发执行 |
|---|---|---|---|
| REQUEST | 委派方 | "帮我做件事" | ✅ 会，跑一遍 Agent |
| REPLY | 被委派方 | 对 REQUEST 的答复 | ❌ 只落盘 |
| SOLICIT | 召集人 | "你对这事什么立场"（协商用） | ✅ 会，跑一遍 Agent |
| PROPOSE | 参与者 | 对 SOLICIT 的答复，带置信度 | ❌ 只落盘 |
| VOTE | 参与者 | 对某个方案表态 | ❌ 只落盘 |
| INFORM | 任何人 | 纯告知，不等回复 | ❌ 只落盘 |

### 一次协作的容器——A2aSession

| 字段 | 类型 | 说明 |
|---|---|---|
| sessionId | String | 会话唯一标识（a2a-session-xxx） |
| topic | String | 这次协作要解决的事 |
| initiator | String | 发起方 agentId |
| members | List<Member> | 成员表（含发起方） |
| status | SessionStatus | OPEN / CLOSED / EXPIRED |
| messageCount | int | 已投递消息条数（消息预算判定用） |
| fromRunId | String | 哪个工作流 run 发起的（可空） |
| fromNodeId | String | 哪个节点发起的（可空） |
| negotiationId | String | 对应的协商 ID（可空） |
| createdAt / updatedAt | Instant | 时间戳 |

### 协作成员——Member

| 字段 | 类型 | 说明 |
|---|---|---|
| agentId | String | 成员 ID |
| endpoint | String | 开会话时快照下来的投递地址 |
| role | MemberRole | 本次协作的角色 |
| joinedAt | Instant | 加入时间 |

### 协作成员角色——MemberRole

| 角色 | 干什么 |
|---|---|
| PARTICIPANT | 普通参与者，投一票 |
| VETO | 否决者，投 reject 直接终止讨论，不看票数 |
| SUPERVISOR | 主管，各方意见齐了之后被单独征询裁决 |
| SYNTHESIZER | 综合方，轮次跑完后合成一份结论 |
| CONVENER | 召集人，不表态，负责投出每轮议题 |

## 会话管理器——SessionManager

负责开会话、投递消息、读取、收尾

### persist

把会话和消息写进 Redis。

工作流程：校验、去重、预占序号、落盘、合并元数据。

```java
private A2aSession persist(String sessionId, A2aMessage message) {
    // 消息体不能为空
    if (message == null) {
        throw FlowAgentException.invalidParam("消息体不能为空");
    }
    A2aSession session = requireOpenSession(sessionId);

    String from = message.getFromAgentId();
    // 发送方不能为空
    if (from == null || from.isBlank()) {
        throw FlowAgentException.invalidParam("发送方 fromAgentId 不能为空");
    }
    // 发送方必须在会话成员里
    if (!session.hasMember(from)) {
        throw FlowAgentException.of("NOT_A_MEMBER",
                "发送方不在会话成员里: " + from, 403);
    }
    String to = message.getToAgentId();
    if (to != null) {
        // 自环检查
        // 正常链路不会自环，这个检查是防御性的 —— 配置错了或者代码有 bug 时，及时拦下
        if (to.equals(from)) {
            throw FlowAgentException.of("SESSION_LOOP",
                    "接收方就是发送方自身，会形成自环: " + from, 409);
        }
        // 接收方必须在会话成员里
        if (!session.hasMember(to)) {
            throw FlowAgentException.of("NOT_A_MEMBER",
                    "接收方不在会话成员里: " + to, 403);
        }
    }

    fillDefaults(message, sessionId);

    // replyTo 与 fromAgentId 都是请求体自填的
    // 没有这道检查，B 可以提交一条 replyTo 指向「发给 A 的请求」的回复，在会话日志里冒充 A 的表态。
    if (message.getReplyTo() != null && !message.getReplyTo().isBlank()) {
        try {
            store.findMessage(sessionId, message.getReplyTo()).ifPresent(original -> {
                String expected = original.getToAgentId();
                if (expected != null && !expected.isBlank() && !expected.equals(from)) {
                    throw FlowAgentException.of("REPLY_ATTRIBUTION_MISMATCH",
                            "回复方 " + from + " 不是原消息的接收方 " + expected, 403);
                }
            });
        } catch (FlowAgentException e) {
            throw e;
        } catch (RuntimeException e) {
            // fail-open：读不到原消息不能变成「回复写不进去」
            log.warn("[A2A] 读取被回复消息失败，跳过归属校验 | session={} | replyTo={} | error={}",
                    sessionId, message.getReplyTo(), e.toString());
        }
    }

    // 这条消息要不要做去重保护
    // 只有 REPLY 和 PROPOSE 类型的消息要做去重保护（因为同一个问题应该只有一个答案）
    boolean guardable = message.getReplyTo() != null && !message.getReplyTo().isBlank()
            && REPLY_DEDUPE_KINDS.contains(message.getKind());
    // 有没有抢到幂等槽
    boolean claimed = false;
    if (guardable) {
        try {
            // 认领槽
            A2aMessage winner = store.claimReply(message);
            // 失败返回赢家信息
            if (winner != null) {
                // 不落盘、不投递，把先到的那条原样返回
                // 调用方拿到的仍然是一个带 messageId 与 seq 的真实消息，只是它不是它刚提交的那一份
                long n = replyDuplicatesSkipped.incrementAndGet();
                log.warn("[A2A] 重复回复已拦下，返回先到的那条 | session={} | from={} | replyTo={} | 累计 {} 条",
                        sessionId, message.getFromAgentId(), message.getReplyTo(), n);
                throw new DuplicateReply(winner);
            }
            claimed = true;
        } catch (DuplicateReply duplicate) {
            // 重复回复异常
            throw duplicate;
        } catch (Exception e) {
            // fail-open 降级——Redis 抖动时，锁抢不到，系统不阻塞写入，直接放行
            long n = replyGuardFailures.incrementAndGet();
            // 日志留痕
            if (n == 1 || n % 100 == 0) {
                // 计数逐次，日志限流
                log.warn("[A2A] 回复去重闸读不到，本次放行不做去重 | 累计 {} 次 | session={} | replyTo={} | error={}",
                        n, sessionId, message.getReplyTo(), e.toString());
            }
        }
    }

    long reserved = -1L;
    // seq>0 说明已经预置过序号了不过 append 还没有执行进程就崩了
    if (message.getSeq() <= 0) {
        // 预占
        reserved = store.reserveSeq(sessionId, messageBudget);
        // 返回 -1，说明超过消息预算
        if (reserved < 0) {
            if (claimed) {
                // 释放槽位
                releaseQuietly(message);
            }
            throw FlowAgentException.of("MESSAGE_BUDGET_EXCEEDED",
                    "会话 " + sessionId + " 已达消息预算 " + messageBudget + " 条", 429);
        }
        // 给内存里的java对象 setSeq
        message.setSeq(reserved);
    }

    long seq;
    try {
        seq = store.append(message);
    } catch (RuntimeException e) {
        // 为什么 try 只包住 append
        // 消息一旦写进 Redis，就算后续唤醒对端失败，也不能回滚序号和锁
        // 因为消息已经在日志里了，回滚了就等于允许再写一遍，会造成重复
        if (reserved > 0) {
            // 预占了序号要回滚
            rollbackQuietly(sessionId, reserved);
        }
        if (claimed) {
            releaseQuietly(message);
        }
        throw e;
    }
    if (claimed) {
        // 给 Redis 上存的信息补上 seq
        refreshQuietly(message);
    }
    
    signalReply(sessionId, message);
    session.setMessageCount((int) seq);
    session.setUpdatedAt(Instant.now());
    store.updateAfterAppend(session);
    return session;
}
```

#### 回复去重槽——claimReply

```java
public A2aMessage claimReply(A2aMessage message) {
    RBucket<String> slot=redissonClient.getBucket(replyKey(message));
    String mine=JSON.toJSONString(message);
    if (trySet(slot, mine)) {
        return null;
    }
    String winner=slot.get();
    if (winner == null) {
        return trySet(slot, mine) ? null : parse(slot.get());
    }
    return parse(winner);
}
```

## 投递闸——NegotiationTurnGuard

## 执行闸——PeerDispatcher

## 问题

### 自环检查

### replyTo 和 fromAgentId 检查

### 为什么只有回复类消息要做去重，其它消息不去重不会有问题吗？



### 去重过程中 Redis 抖动导致没有抢到锁怎么办？

Redis 挂了就放行不做去重，但单独计数。如果因为 Redis 挂了就不投递消息，那一次 Redis 抖动会变成一堆永远不执行的委派 —— 这比重复执行严重得多。

### seq 编号是怎么分配的？为什么要预占？

先通过 CAS 循环而不是 INCR 实现 seq 自增，预占位置，如果预占成功但是落盘失败就回滚序号(回滚失败的话对应 seq 编号就是空消息)。

之所以不用 INCR 是因为：要先判断 "够不够" 再自增。INCR 是原子的但没法先看再增；
CAS 循环让 "比较" 和 "自增" 成为一次操作。

之所以要预占是因为：有消息预算，如果不预占，直接 append 然后到第 201 条才发现预算超了

### 简历上描述 "共同会话 + 单独收件箱实现了各 Agent 唤醒互不打扰" —— "互不打扰" 具体指什么？

1.每个 Agent 有自己独立的收件箱队列(Redis a2a:inbox:{agentId})，取消息只会从自己的队列中取，不会碰别的 Agent 的收件箱

2.执行是后台线程异步的，wake 不会打断正在跑的推理

### 会话日志和收件箱是两份数据，为什么要两份？会话日志里不是已经有所有消息了吗，为什么不直接让 Agent 去会话日志里读取？

会话日志记录的是会话开始至今的所有消息，而收件箱记录的是某个 Agent 需要知道的消息的 seq 编号，对于一个 Agent 
并不是会话里所有消息都要了解。

### 收件箱是怎么发挥作用的？

每次 Agent 执行之前通过收件箱中记录的 seq 编号从会话中读取还未读过但是需要它知道的消息，将其拼接进 prompt

### 收件箱是 push 还是 pull？Agent 怎么知道 "我有新消息了"？如果它正在跑一个 30 秒的推理循环，这条消息要等多久才能被看到？

收件箱不知道有新消息，它就是每轮 ReAct 循环开头，主动去拉一次自己的收件箱。如果它正在跑一轮推理循环那要等这一轮跑完，
下一轮开头才会看到这条消息。

### 为什么不做主动唤醒？

因为要叫醒一个正在跑的 Agent，你得把它的 ReAct 循环改成可挂起的 —— 推理跑到一半停下来，插一条新消息进去，再接着跑。

而收件箱 pull 的代价就是 "消息最多晚一轮被看到"—— 但对 Agent 协作来说，晚几秒看到一条 INFORM 消息，不是什么致命问题。

### 简历上描述 "消息幂等认领，避免重复执行" —— 重复执行的触发场景是什么？谁会重复发？

1.wake 通知超时后重试：A 给 B 发消息，消息落盘了，A 发 wake HTTP 给 B，这个 wake 请求超时了 —— A 以为没送到，重试了一次。
(这里的 wake 是主动唤醒，但是这不是收件箱对 Agent 的主动唤醒，这个唤醒只针对 REQUEST/SOLICIT 类型的消息，因为这两类消息需要接收方主动干活)

2.rewake 主动重发：

### REQUEST/SOLICIT 类型消息唤醒对应 Agent 失败了怎么办？

接收错误，将错误分为两类抛给上游：

408 / 429 / 5xx / 网络错误 —— 暂时失败，可以重试

其它 4xx —— 地址 / 配置就是错的，重试没用

由上游决定是否重试

### 认领是怎样用 Redis 实现的？键是什么？TTL 设置了多久，为什么？

用的是 Redisson 的 RBucket.trySet(value, ttl, unit)，即 SETNX。trySet 在 key 不存在时才写入，而且值和 TTL 一起写入，
不拆成 set + expire 两条命令。键是 `A2A_SESSION_HANDLED:{messageId}`，TTL 设置为 600 s，大于一次执行的
最长耗时。如果 TTL 比执行耗时短，标记会在 Agent 还没跑完时就过期，重放的 wake 又能挤进来跑第二遍，这道闸就白设了。

### 消息认领成功了，Agent 开始跑，跑到一半进程被终止了。这条消息现在是什么状态？会不会再被执行？

认领成功后标记是 RUNNING。线程内异常会被 catch 标成 FAILED，允许重投但不自动重跑；进程被强杀则卡在 
RUNNING，TTL 内重放被拦下不会重复执行，TTL 到期或被协商侧陈旧判定回收后，才允许重新认领执行。
