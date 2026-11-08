# 工作流执行引擎

这一模块主要负责：

1.接收工作流定义(WorkflowDefinition)+用户输入，创建 WorkflowRun 实例

2.按拓扑顺序逐层执行节点（同层并行）

3.每执行完一个节点，保存 CheckPoint（断点续跑）

4.处理超时、重试、降级、条件分支

5.将最终结果写入 WorkflowRun.output

## 单节点执行——executeNodeWithRetries

```java
private void executeNodeWithRetries(WorkflowDefinition def, WorkflowRun run,
                                    String nodeId, WorkflowNode node,
                                    Deadline deadline, NodeTrace trace) {
    // 先判断工作流状态
    if (isTerminal(run.getStatus())) {
        log.warn("[Executor] node not started: run already terminal | runId={} | node={} | status={}",
                run.getRunId(), nodeId, run.getStatus());
        return;
    }

    // 构建一条节点执行记录，初始状态 RUNNING。
    // 这个对象贯穿整个方法：成功补 output/duration，失败补 error/retryCount
    // 最后写进 run.nodeResults。
    WorkflowRun.NodeExecutionResult result = WorkflowRun.NodeExecutionResult.builder()
            .nodeId(nodeId)
            .nodeName(node.getName())
            .status(WorkflowRun.NodeExecutionResult.NodeStatus.RUNNING)
            .startedAt(Instant.now())
            .build();

    // 把自己标记为"正在跑"（CopyOnWriteArrayList，同层多节点并发写）
    run.getActiveNodeIds().add(nodeId);
    run.addTimelineEvent("NODE_START", nodeId, "开始执行: " + node.getName());
    log.info("[Executor] node start | runId={} | node={} | type={}",
            run.getRunId(), nodeId, node.getNodeType());

    int attempt = 0;            // 当前第几次尝试，0 = 第一次（不是重试）
    Exception lastError = null; // 保留最后一次失败原因，全部重试失败后交给降级策略

    while (attempt <= node.getMaxRetries()) {
        // 检查工作流状态
        if (isTerminal(run.getStatus())) {
            log.warn("[Executor] node retry aborted: run already terminal | runId={} | node={} | status={}",
                    run.getRunId(), nodeId, run.getStatus());
            return;
        }
        try {
            if (attempt > 0) {
                log.info("[Executor] node retry {}/{} | runId={} | node={}",
                        attempt, node.getMaxRetries(), run.getRunId(), nodeId);
                trace.retry(attempt, node.getMaxRetries());
                // 每次节点重试前等待一下：失败后立即重试大概率会撞上同一个问题
                Thread.sleep(node.getRetryDelaySeconds() * 1000L);
            }

            // 按节点类型从工厂拿到对应的执行器
            NodeExecutor executor = nodeExecutorFactory.get(node.getNodeType());

            // 组装本节点的输入：全局变量 + 前驱节点输出 + 元信息（runId/nodeId 等）
            Map<String, Object> nodeInput = buildNodeInput(def, run, node);

            // 把当前线程的 "上下文" 抄一份，交给后台工作线程用
            Future<Map<String, Object>> future=nodeCallExecutor.submit(FlowAgentContext.wrap(() -> executor.execute(node, nodeInput, run)));

            Map<String, Object> nodeOutput;
            try {
                // 阻塞等待节点结果，最多等 nodeTimeout 秒
                nodeOutput = future.get(nodeTimeout, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // 等待线程被中断
                // 先取消在途任务，然后原样抛出，交给外层专用 catch 处理
                future.cancel(true);
                throw e;
            } catch (TimeoutException e) {
                // 节点超时
                future.cancel(true);
                // 包装成 NODE_TIMEOUT 抛出去
                throw new FlowAgentException("NODE_TIMEOUT",
                        "节点 " + node.getName() + " 执行超时 (" + nodeTimeout + "s)");
            } catch (ExecutionException e) {
                // 节点执行本身抛异常了
                Throwable cause = e.getCause();
                if (cause instanceof FlowAgentException) {
                    // 业务异常，原样抛
                    throw (FlowAgentException) cause;
                }
                if (cause instanceof Exception) {
                    // 普通异常，原样抛给外层分类。
                    throw (Exception) cause;
                }
                // 兜底包装成节点错误
                throw new FlowAgentException("NODE_ERROR",
                        "节点执行异常: " + cause.getMessage());
            }

            // 运行成功
            result.setStatus(WorkflowRun.NodeExecutionResult.NodeStatus.COMPLETED);
            result.setOutput(nodeOutput);
            result.setInput(nodeInput);
            result.setCompletedAt(Instant.now());
            result.setDurationMs(Duration.between(result.getStartedAt(), result.getCompletedAt()).toMillis());
            result.setRetryCount(attempt);

            // 写变量、记完成、存检查点——一把锁内原子提交。
            commitNodeSuccess(def, run, nodeId, result, nodeOutput);

            log.info("[Executor] node complete | runId={} | node={} | duration={}ms",
                    run.getRunId(), nodeId, result.getDurationMs());
            trace.success(attempt);
            return;
        } catch (InterruptedException e) {
            // 被中断不重试（线程池在关闭，重试没有意义）
            // 恢复中断位
            Thread.currentThread().interrupt();
            trace.failed(e, attempt);
            throw new FlowAgentException("NODE_INTERRUPTED",
                    "节点 " + node.getName() + " 执行被中断", 500, e);
        } catch (Exception e) {
            // 挂起异常，不重试
            if (e instanceof WorkflowSuspendedException suspended) {
                trace.suspended(suspended);
                throw suspended;
            }

            // 普通失败：记录原因，attempt 自增重试
            lastError = e;
            attempt++;
            log.warn("[Executor] node error (attempt {}/{}) | runId={} | node={} | error={}",
                    attempt, node.getMaxRetries() + 1, run.getRunId(), nodeId, e.getMessage());
        }
    }

    // 所有重试都耗尽
    trace.failed(lastError, Math.max(0, node.getMaxRetries()));
    // 交给降级策略
    settleNodeFailure(def, run, nodeId, node, result, lastError);
}
```

节点在同一层内由一个 Semaphore 控制并发，拿到许可才开始执行。拿到许可后先看一眼 run 是不是已经进入终态 —— 兄弟节点可能已经把整个工作流推到失败、完成或取消，这时候再启动就是白跑还产生副作用，直接返回。

确认能跑，就把 nodeId 登记到 activeNodeIds，进入重试循环。每次重试开头再查一次终态—— 因为第一次失败和第二次重试之间，工作流状态可能被置为终止。

每次重试把节点任务提交到独立的 nodeCallExecutor 线程池，用 Future 拿结果。
这里有个关键选择：用 `ExecutorService.submit()` 返回的 `FutureTask`，而不是 `CompletableFuture`。因为 JDK 里 `CompletableFuture.cancel(true)` 只是把自己标记为已取消，并不会中断正在执行的工作线程；只有 `FutureTask.cancel(true)` 才会真正对工作线程发出中断信号 —— 节点超时必须能立刻止血，不能让任务继续占用线程池。

等待过程中三种异常三种出口：

1. 等待线程自己被中断（比如线程池 shutdownNow）：先 cancel 掉节点工作线程，再把异常抛到方法级 catch，那里用 `Thread.currentThread().interrupt()` 恢复中断位—— 因为 `Thread.sleep` 和 `FutureTask.get` 抛中断时都会清掉中断标志，不恢复的话上层就感知不到关闭信号了。出现这个异常不重试，run 直接落失败。

2. 节点执行超时：cancel 工作线程，包成 NODE_TIMEOUT 走通用失败分支进重试（等待超时不是直接读节点配的 timeout，而是取它和工作流全局剩余预算的较小值）

3. 节点抛挂起异常（人工审批）：这个在通用 catch 里被特意挑出来直接重抛，不进重试 —— 否则重试会再建一个审批任务，一次挂起变成 N 个待审批。

重试用尽后按节点配的降级策略处理(SKIP/FALLBACK_VALUE/PAUSE/FAIL)

### 层间执行——executeLayer

```java
private void executeLayer(WorkflowDefinition def, WorkflowRun run,
                          List<String> nodeIds, Deadline deadline) {
    // 先查全局超时
    if (deadline.expired()) {
        markGlobalTimeout(def, run);
        return;
    }

    if (nodeIds.size() == 1) {
        // 单个节点直接执行（避免线程池开销）
        executeSingleNode(def, run, nodeIds.get(0), deadline);
        return;
    }

    // 并发闸门：同一时刻最多 maxConcurrency 个节点在执行。
    int maxConcurrency = Math.max(1, def.getConfig().getMaxConcurrency());
    Semaphore permits = new Semaphore(maxConcurrency);
    log.debug("[Executor] layer concurrency gate | runId={} | nodes={} | maxConcurrency={}",
            run.getRunId(), nodeIds.size(), maxConcurrency);

    // 多节点并行执行
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (String nodeId : nodeIds) {
        futures.add(CompletableFuture.runAsync(
                // FlowAgentContext.wrap() 把当前线程的上下文传到工作线程，否则子线程拿不到
                FlowAgentContext.wrap(() -> {
                    try {
                        // 层内节点数可能大于 maxConcurrency，超出的需要排队
                        // 阻塞方法——拿不到许可就一直等，等到有人释放为止
                        permits.acquire();
                    // 线程池被关闭会让其中断
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(ie);
                    }
                    try {
                        executeSingleNode(def, run, nodeId, deadline);
                    } finally {
                        // 释放信号量
                        permits.release();
                    }
                }),
                layerExecutor));
    }

    // 等待所有节点完成，但不超过工作流剩余预算
    CompletableFuture<Void> all =
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    try {
        if (deadline.unlimited()) {
            // 不限时：一直等到底
            all.join();
        } else {
            // 有全局预算：最多等剩余预算秒数。Math.max(1, ...) 保证至少等 1 秒。
            all.get(Math.max(1, deadline.remainingSeconds()), TimeUnit.SECONDS);
        }
    } catch (TimeoutException te) {
        // 全局超时：放弃等待本层，把工作流置为失败。
        // 这里 cancel 的是编排层的 CompletableFuture，只能阻止「尚未开始」的节点，无法中断已在跑的节点（CompletableFuture 不支持 interrupt）
        // 真正的止血靠 executeSingleNode 里把节点超时钳制到剩余预算，节点会自行超时退出。
        futures.forEach(f -> f.cancel(true));
        markGlobalTimeout(def, run);
    } catch (InterruptedException ie) {
        futures.forEach(f -> f.cancel(true));
        Thread.currentThread().interrupt();
        throw new CompletionException(ie);
    } catch (ExecutionException ee) {
        // 单节点的异常已在 executeSingleNode 内部消化（重试/降级），能冒到这里的是编排层自身的异常，不应被吞掉
        throw new CompletionException(ee.getCause() == null ? ee : ee.getCause());
    }
}
```

这段代码中涉及到的并发模型

1.信号量——Semaphore

2.异步编排——并行提交+统一等待

把 N 个节点提交成 N 个 Future，然后 `allOf` 等它们全部完成。

3.ThreadLocal 上下文传播——`FlowAgentContext.wrap(() -> { ... })`

### 核心状态机——executeInternal

```java
private WorkflowRun prepareResume(WorkflowDefinition definition, WorkflowRun run) {
    String runId = run.getRunId();
    if (definition == null) {
        throw FlowAgentException.businessError("INVALID_DEFINITION", "工作流定义不能为空");
    }
    // 先校验 DAG 本身合法
    DagValidator.validate(definition);

    String currentFingerprint = definition.fingerprint();
    String snapshotFingerprint = run.getDefinitionFingerprint();
    // 结构指纹（拓扑有没有变）
    if (snapshotFingerprint != null && !snapshotFingerprint.equals(currentFingerprint)) {
        throw FlowAgentException.businessError("DEFINITION_CHANGED",
                "工作流定义已变更，无法从旧 Checkpoint 恢复（快照=" + snapshotFingerprint
                        + ", 当前=" + currentFingerprint + "）。请重新发起一次执行。");
    }
    if (snapshotFingerprint == null) {
        // 兼容本次改动之前保存的老快照：无指纹可比，放行但留痕
        log.warn("[Executor] checkpoint 无定义指纹（旧版本快照），跳过一致性校验 | runId={}", runId);
        run.setDefinitionFingerprint(currentFingerprint);
    }

    // 执行配置也必须一致。结构指纹只覆盖拓扑，改 prompt / 工具参数 / 超时 / 降级策略 / 节点顺序都不会被上面那道门发现；
    // 旧快照没有这个字段（null），放行并就地升级，避免这次改动把存量快照一次性判死。
    String currentExecutionFingerprint = definition.executionFingerprint();
    String snapshotExecutionFingerprint = run.getExecutionFingerprint();
    if (snapshotExecutionFingerprint != null
            && !snapshotExecutionFingerprint.equals(currentExecutionFingerprint)) {
        throw FlowAgentException.businessError("DEFINITION_CHANGED",
                "工作流执行配置已变更，无法从旧 Checkpoint 恢复（快照="
                        + snapshotExecutionFingerprint + ", 当前=" + currentExecutionFingerprint
                        + "）。请重新发起一次执行。");
    }
    if (snapshotExecutionFingerprint == null) {
        log.info("[Executor] checkpoint 无执行配置指纹（旧版本快照），跳过校验并升级 | runId={}",
                runId);
        run.setExecutionFingerprint(currentExecutionFingerprint);
    }

    // 状态必须可恢复，已经跑完的、被取消的，不能恢复
    WorkflowRun.RunStatus status = run.getStatus();
    if (status == WorkflowRun.RunStatus.COMPLETED) {
        throw FlowAgentException.businessError("ALREADY_COMPLETED",
                "工作流已执行完成，无需恢复: " + runId);
    }
    if (status == WorkflowRun.RunStatus.CANCELLED) {
        throw FlowAgentException.businessError("RUN_CANCELLED",
                "工作流已被取消，不允许恢复: " + runId);
    }

    // 并发检查 —— 是不是已经有人在跑了
    // containsKey：同 JVM 内部二次调用
    // isHeld：多副本
    if (runningWorkflows.containsKey(runId) || leaseManager.isHeld(runId)) {
        throw FlowAgentException.conflict("工作流正在运行中，不能重复恢复: " + runId
                + "（若对端实例已崩溃，租约将在 " + leaseManager.watchdogMillis() / 1000
                + "s 内自动释放，请稍后重试）");
    }

    // 清理中断残留：崩溃时 activeNodeIds 里的节点并未完成，需重跑；
    // failedNodeIds 同理清空，否则重跑成功后失败记录仍在，状态自相矛盾。
    // blockedNodeIds 与 failedNodeIds 同去同留 —— 它记的是「上游断了所以没跑」，失败节点要重跑，下游就该解冻。
    // 不清的后果：上游重跑成功了，下游却永远不跑，而且连日志都没有
    // skippedNodeIds 刻意保留 —— 和 completedNodeIds 一样，跳过是「已经定下来的结论」
    List<String> interrupted = new ArrayList<>(run.getActiveNodeIds());
    List<String> previouslyFailed = new ArrayList<>(run.getFailedNodeIds());
    List<String> previouslyBlocked = new ArrayList<>(run.getBlockedNodeIds());
    run.getActiveNodeIds().clear();
    run.getFailedNodeIds().clear();
    run.getBlockedNodeIds().clear();
    run.setErrorMessage(null);
    run.setCompletedAt(null);
    run.setResumeCount(run.getResumeCount() + 1);

    run.addTimelineEvent("WORKFLOW_RESUME", run.getLastCheckpointNodeId(),
            String.format("从 Checkpoint 恢复（第 %d 次）| 已完成 %d 节点 | 待重跑: %s",
                    run.getResumeCount(), run.getCompletedNodeIds().size(),
                    interrupted.isEmpty() && previouslyFailed.isEmpty()
                            ? "无" : union(union(interrupted, previouslyFailed), previouslyBlocked)));

    // 占用「本 JVM 正在跑它」的槽位。
    // 这里必须用 putIfAbsent，不能像上面那道检查一样「先 containsKey 再 put」：
    // 先检查再操作，不是原子的，两个线程完全可能同时通过第一步的检查，然后都执行第二步
    // 可能会出现真正在跑的线程在记录里消失了
    WorkflowRun holder = runningWorkflows.putIfAbsent(runId, run);
    if (holder != null) {
        throw FlowAgentException.conflict("工作流正在运行中，不能重复恢复: " + runId);
    }
    finishedWorkflows.remove(runId);

    log.info("[Executor] resuming | runId={} | resumeCount={} | completed={} | skipped={} | interrupted={} | failed={} | unblocked={}",
            runId, run.getResumeCount(), run.getCompletedNodeIds().size(),
            run.getSkippedNodeIds().size(), interrupted, previouslyFailed, previouslyBlocked);
    return run;
}

```

### 断点恢复——prepareResume

读 Redis checkpoint → 校验双指纹 → 重建内存状态

```java

```

## 问题

### WorkflowDefinition 长什么样？

工作流定义就是一个 JSON 对象，核心是 nodes + edges，表达成一张 DAG：

```json
{
  "id": "wf-fault-diagnosis-001",
  "name": "智能故障诊断工作流",
  "version": "1.0.0",
  "description": "当用户报告服务异常时，自动检索知识图谱、分析根因、生成诊断报告",
  "triggerCondition": "用户报告服务故障或性能问题",

  "nodes": [ ... ],   // 节点列表
  "edges": [ ... ],   // 边列表（连接节点）

  "config": { ... },  // 全局配置
  "tags": ["fault-diagnosis", "sre", "demo"]
}
```

#### 节点定义

通用字段

```json
{
  "id": "node-1",
  "name": "理解用户报告",
  "nodeType": "AGENT",          // 节点类型，决定用哪个执行器
  "description": "解析用户报告的故障现象",
  "timeoutSeconds": 90,          // 单次执行超时
  "maxRetries": 1,               // 失败后重试次数（默认1）
  "retryDelaySeconds": 2,        // 重试间隔
  "fallbackStrategy": "SKIP"     // 降级策略：SKIP / FALLBACK_VALUE / PAUSE / FAIL
}
```

各类型还有专属配置

#### 边定义

```json
"edges": [
  { "source": "node-1", "target": "node-2" },           // 无条件边
  { "source": "node-2", "target": "node-3" },
  { "source": "node-5", "target": "node-6", "condition": "approval_required" },  // 条件边
  { "source": "node-5", "target": "node-7", "condition": "auto_fix" },           // 条件边
  { "source": "node-6", "target": "node-7" }
]
```

#### 全局配置

```json
"config": {
  "globalTimeoutSeconds": 600,    // 整个工作流最多跑多久
  "maxConcurrency": 10,           // 同层节点最大并行数
  "checkpointEnabled": true,      // 是否开启断点续跑
  "detailedTracing": true         // 是否开启全链路追踪
}
```

### WorkflowRun 的用途是什么？

它是一次工作流执行的 "运行时账本"—— 引擎跑的时候往里面记状态，崩了靠它恢复，跑完了靠它给查询。

具体用途：

1.执行引擎的状态中枢：引擎每跑一个节点，都在读写这个对象

节点开始 → 加进 `activeNodeIds`
  
节点跑完 → 移到 `completedNodeIds`，写进 `nodeResults`

节点失败 → 移到 `failedNodeIds`，走降级

条件不匹配 → 移到 `skippedNodeIds`

上游失败被拦住 → 移到 `blockedNodeIds`

2.Checkpoint 快照的载体：整个 `WorkflowRun` 被序列化成 JSON 存到 Redis。进程崩了，从 Redis 反序列化回来，就能从断点继续跑

3.异步轮询的数据来源：用户提交工作流是异步的，提交后拿到一个 `runId`，然后轮询查询状态。查的就是这个对象 —— 状态是 RUNNING 还是 COMPLETED、哪些节点跑完了、最终输出是什么。

#### checkpoint 存在 Redis 中，数据结构是什么？一个复杂的 DAG 快照多大？

```plaintext
Redis Key:   fa:workflow:ckpt:{runId}
Redis 类型:  String（RBucket）
Value:       整个 WorkflowRun 序列化成的 JSON 字符串
TTL:         7 天
```

写入时机：每个节点完成后全量写入

写入互斥：兄弟节点并发提交时，"写变量" 和 "序列化快照" 必须互斥，否则会拍到 "变量里已有输出、completedNodeIds 里还没记" 的中间状态，恢复时节点被重跑、副作用发生第二次

快照里装了什么：`WorkflowRun` 对象序列化后的 JSON

主要字段：

固定字段（小）：runId、workflowId、status、时间戳、两个指纹、resumeCount、suspendedApprovalNodeId/taskId—— 约 1-2KB
  
五个节点集合：activeNodeIds /completedNodeIds/failedNodeIds /skippedNodeIds/blockedNodeIds—— 每个就是节点 ID 字符串列表，几十到几百字节
  
nodeResults：`Map<nodeId, NodeExecutionResult>`—— 每个节点的输入、输出、耗时、重试次数、状态
  
variables：节点间传递的数据 ——这是快照大小的大头
  
timeline：时间线事件列表，每个节点几条事件

快照多大，取决于节点输出：

| 场景 | 大小估算 |
| ---- | ---- |
| 5 个节点，TOOL 节点返回小结果（几 KB）|	~15-30KB JSON |
| 10 个节点，含 AGENT 节点 LLM 回复 + TOOL 节点数据	| ~30-100KB JSON |
| 大输出场景（TOOL 节点返回大段 SQL 结果 / HTTP 响应）| 几百 KB |

#### 为什么存储值用 String 而不是 Hash

使用 String 存储时通过序列化与反序列化实现 Object->String->Object 的转化，如果用 Hash 就要每个字段存一个 field（status、completedNodeIds 等分开存），改一个节点就要改多个 field，中间如果崩了数据就不一致了

### java中线程的中断机制？

别的线程调 t.interrupt() → 给线程 t 设一个"中断标志 = true" → 就完了，线程 t 继续跑，什么都不会发生

`interrupt()` 不会真的打断线程。它只是发了个信号："你有空了看看这个标志，自己决定要不要停。"

#### 什么时候线程才会 "感知" 到中断？

1.在可中断方法上阻塞

在下述方法上等待时，被 interrupt 会立刻抛 InterruptedException：

Thread.sleep(ms), object.wait(), thread.join(), future.get(timeout), queue.poll(timeout), httpClient.send()

这些方法内部会定期检查中断标志，发现标志 = true，就抛出 `InterruptedException` 并清掉标志。

2.主动检查标志

代码里自己写循环，每轮检查一次

#### 忘记中断复位会怎样？

中断标志是给上层看的信号，catch 了不恢复，就是把信号摘了 —— 上层以为没被中断过，该停的不停

### 为什么要设置三个不同线程池分别负责工作流、层、节点三类任务？各个线程池的配置又是怎么样的？

主要是为了避免互相阻塞导致死锁，假设 `layerExecutor` 和 `nodeCallExecutor` 共用一个池：

第 1 层有 4 个节点并行 → 4 个编排线程启动，提交 4 个节点任务 → 编排线程阻塞在 future.get() 上，等节点结果
→ 节点任务要执行，但池里 4 个线程都被编排线程占了 → 节点任务拿不到线程，永远不执行 → 编排线程永远等不到结果，继续占着线程 → 死锁

还能隔离故障，节点执行可能很慢，如果共用一个池，慢节点会把所有线程都占住，编排任务也跟着卡，拆开之后，节点池慢了，编排池还能正常调度

| 配置项 | `workflowExecutor`<br>工作流提交池 | `layerExecutor`<br>层内编排池 | `nodeCallExecutor`<br>节点执行池 |
|---|---|---|---|
| corePoolSize | 2 | 4 | 8 |
| maxPoolSize | 8 | 64 | 64 |
| keepAliveTime | 60s | 60s | 60s |
| 工作队列 | `LinkedBlockingQueue(256)` | `SynchronousQueue` | `SynchronousQueue` |
| 拒绝策略 | `CallerRunsPolicy` | `CallerRunsPolicy` | `CallerRunsPolicy` |
| 职责 | 接收新 run 的异步提交入口 | 提交同层节点任务后阻塞在 `future.get()` 等结果，不跑节点业务 | 真正执行 `NodeExecutor.execute`，跑节点业务逻辑 |
| 扩容行为 | 256 容量内排队，满了才扩到 max=8 | 无队列缓冲，无空闲线程立即扩容到 max=64 | 无队列缓冲，无空闲线程立即扩容到 max=64 |

### 线程池的拒绝策略为什么全是 CallerRunsPolicy？还有什么拒绝策略，介绍一下？会不会带来什么问题？

ThreadPoolExecutor 的四种拒绝策略：

| 策略 | 行为 | 适用场景 |
|---|---|---|
| AbortPolicy（默认） | 抛 RejectedExecutionException，任务不执行 | 任务不能丢，宁可报错 |
| CallerRunsPolicy | 提交任务的那个线程自己跑这个任务 | 任务不能丢，希望自然反压 |
| DiscardPolicy | 静默丢弃新任务，什么都不做 | 任务可以丢（如日志采集） |
| DiscardOldestPolicy | 丢弃队列里最老的任务，然后重试提交新任务 | 只关心最新任务（如消息队列） |

为什么：

1.任务不能丢，工作流的节点执行、工作流提交都是核心业务 —— 不能因为池满了就静默丢弃。丢了一个节点，整个工作流的 DAG 就断了。

2.自然反压：layerExecutor 满了 → 调用者（编排线程）被拉去跑节点任务 → 它本来就在 future.get() 上等结果 → 现在自己跑一个节点，跑完继续等 → 提交速度自然慢下来，不会继续往池里塞任务

会带来什么问题：

节点编排，工作流执行会被阻塞，且 layerExecutor 和 nodeExecutor 两池的 max 都是 64，大概率不会出现这种问题

### 为什么 layer 和 node 池用 SynchronousQueue 而不是有界队列

因为 ThreadPoolExecutor 只在队列满时才扩容到 maxPoolSize。

如果 layer 池用 LinkedBlockingQueue，线程额度被占满后，新的任务进来不会马上扩容（由于队列太长排不满线程池几乎不会扩容），并发度锁死在 core=4。SynchronousQueue 不存任务，直接交给线程或扩容，所以能按需扩到 max=64。

另外前面提到的死锁的前提条件就是不使用 SynchronousQueue 而是有界队列

### 任务在三个线程池之间的流转过程

首先请求进来后，整个工作流的执行先交给 workflow 池——core=2、有界队列 256，负责异步拉起一个工作流实例。它在这个线程里抢到 Redis 分布式锁、对 DAG 做 Kahn 拓扑排序分出 L1/L2/L3 各层，然后逐层推进。

每一层的调度交给 layer 池。layer 池的线程只做一件事：把这一层的节点任务全部派发到 node 池，然后阻塞在 future.get() 上等结果。等这一层全部节点回来，判断哪些成功、哪些走了降级、哪些被上游拦住，再决定下一层怎么路由。

节点真正的业务执行在 node 池。

### 提交节点任务用的是 ExecutorService.submit() 还是 CompletableFuture.supplyAsync()？为什么？

ExecutorService.submit()

`submit()` 返回 FutureTask，`supplyAsync()` 返回 CompletableFuture。执行 cancel(true) ，两者行为不同，前者真的调工作线程的 `interrupt()`，后者只是对 future 自身标记为取消。

在本项目中节点绝大部分是等 HTTP 响应的任务，`HttpClient.send()` 响应中断，所以 `FutureTask.cancel(true)` 真的能打断它们，不仅能及时释放线程资源还能节省 token

### 节点执行失败了怎么办，有哪几种失败的情况？介绍一下它的降级策略？

执行失败就直接结束节点执行(应用直接关闭)或重试，重试耗尽走降级策略。

失败情况：

1.InterruptedException——被中断(应用关闭)，不重试

2.TimeoutException——重试

3.ExecutionException——只有挂起异常不重试

降级策略：

| 策略 | 行为 | 对下游的影响 | 对工作流状态的影响 |
| ---- | ---- | ---- | ---- |
| SKIP | 跳过这个节点，不记 completedNodeIds | 下游被拦（边不成立） | RUNNING，继续下一层 |
| FALLBACK_VALUE | 写个兜底值，记完成 |	下游拿到兜底值照跑	| RUNNING，继续下一层 |
| PAUSE	暂停工作流 |	不进下一层	| PAUSED，等人工恢复 |
| FAIL | 终止工作流 | 不进下一层 | FAILED，整个失败 |

审批节点的 SKIP 和 FALLBACK_VALUE 被强制升级为 FAIL——人工审批不能跳过、不能给兜底值，必须走完整审批流程，否则等于绕过了人工把关。

### 节点超时怎么处理？

节点执行使用 future.get(timeout) 等待，超时后 `cancel(true)` 发中断信号让工作线程别再占着线程池，然后包装成 `NODE_TIMEOUT` 进入重试循环；重试耗尽后按降级策略处理。超时值钳制在工作流剩余预算内，防止单节点拖垮全局预算。

### 提交层任务用的是什么？为什么？

`CompletableFuture.runAsync(task, layerExecutor)`，因为后续要用 allOf 聚合，而 allOf 只接收 CompletableFuture，编排层的 `cancel(true)` 只是 "阻止还没开始的任务"—— 真正的止血靠节点自己的超时钳制。

编排层在两种情况下会调用 `cancel(true)` ：全局超时、应用中断，这两种情况下节点都能及时中断，因此即使用 `CompletableFuture.runAsync(task, layerExecutor)` 也不用担心不能及时中断

###  一层有 20 个节点并发，但 maxConcurrency=5，剩下 15 个还在等并发槽位。这时候整个工作流到了全局超时时间，这 15 个还没开始跑的节点怎么办?（全局超时怎么处理）

全局超时后，编排线程立刻把整个运行标记成失败状态。正在跑的节点还有约 1 秒的节点超时窗口(平滑过渡)，之后被 interrupt 退出并释放并发槽。排队等槽位的节点不会被 `cancel(true)` 唤醒，它们要等正在跑的节点释放槽位后一个接一个被唤醒 —— 但被唤醒后走进节点执行，第一行就检查运行状态，发现已经是失败了，直接返回，不发请求，不烧 token。

### 下一层执行的前提条件是什么？怎么等上一层全部执行完？上一层报错怎么办？为什么采用 allOf 的等待策略？介绍一下 allOf 的工作原理？还有什么等待策略，介绍一下？

1.条件是：这一层所有节点都跑完了，且工作流状态还是 RUNNING

2.通过 `CompletableFuture.allOf()`+`.get()` 这对组合等待上一层节点全部执行完：

```java
// 这一层 N 个节点，每个提交一个任务，各自返回一个 future
List<CompletableFuture<Void>> futures=new ArrayList<>();
for (String nodeId : nodeIds) {
    futures.add(CompletableFuture.runAsync(() -> {
        executeSingleNode(...);
    }, layerExecutor));
}

// 把 N 个 future 聚合成一个"全部完成"的 future
CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

// 阻塞等待这个聚合 future —— 它会在 N 个节点全部跑完时才放行
all.get(deadline.remainingSeconds(), TimeUnit.SECONDS);
```

3.报错分两种情况

单个节点报错，不影响同层其他节点，报错的节点自己走重试、降级，降级结果决定下一层是否执行

层粒度的报错，全局超时、等待线程被中断、编排层异常，层级报错直接终止整个工作流(除非是挂起异常，等待人工审批)

4.因为这是 DAG 分层执行—— 下一层节点的输入依赖上一层节点的输出。上一层还有节点没跑完，下一层的输入就不完整，必须等全部完成才能往下走。

5.`allOf` 接收 N 个 future，返回一个 `CompletableFuture<Void>`。它内部给每个原始 future 注册了完成回调，计数器减到 0 时自己完成；任何一个原始 future 异常完成，它立即短路异常完成。它不携带结果值，只负责 "等全部结束" 这件事。

6.其它等待策略

| 策略 | 语义 | 适用场景 | 为什么这里不用 |
|---|---|---|---|
| **allOf** | N 个都完成才放行 | DAG 分层、汇总所有结果 |  |
| **anyOf** | N 个里任意一个完成就放行 | 竞速场景：同时发三个请求，谁先回来用谁的 | 下一层需要所有上游结果，不能只要一个 |
| **invokeAll** | 提交 + 等待打包成一个阻塞调用 | 简单并行任务，不需要中途操作 | 不能加超时控制和逐任务取消 |
| **invokeAny** | 谁先成功用谁的，其余取消 | 冗余调用、多源查询 | 同上，需要全部完成 |
| **for 循环逐个 get()** | 按顺序等一个完一个 | 需要按顺序收集结果 | 没有聚合 future，不能统一加超时 |
| **CountDownLatch** | 手动 countDown，等归零 | 需要手动控制"什么时候算完成" | 代码里 N 个任务自动完成，不需要手动计数 |

### 如何控制层内并发？如果不控制会怎样？为什么用 semaphore ，有其它选择吗？

用 Semaphore 信号量，在共享线程池之上加一层轻量级的并发闸门 —— 线程池负责全局资源，信号量负责每个工作流自己的并发度。

不控制会怎么样：最直接的就是 LLM 和外部 API 被 429 限流，然后触发重试风暴——重试又发请求，又被限流，整层集体失败（多个节点进入重试，发起更多请求，请求又全部 429，所有节点重试耗尽，标记为失败，节点进入 failedNodeIds）

不限制 layerExecutor 的线程数，因为 layerExecutor 是全局共享的线程池；也不每层新建一个固定大小的线程池，创建和销毁线程池也有成本；也不用 CountDownLatch，因为它是一个一次性的屏障

### 双指纹（拓扑结构 + 执行配置）具体 hash 了哪些字段（一个节点改了提示词算不算指纹变化）

#### 结构指纹：

| 纳入字段 | 说明 |
|---|---|
| `node.id` | 节点唯一标识 |
| `node.nodeType` | 节点类型（AGENT/TOOL/CONDITION 等） |
| `edge.source` | 边起点 |
| `edge.target` | 边终点 |
| `edge.condition` | 边的条件表达式 |

#### 配置指纹：

1.WorkflowNode 上的字段

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | String | 节点 ID |
| `nodeType` | Enum | 节点类型 |
| `timeoutSeconds` | int | 节点超时 |
| `maxRetries` | int | 最大重试次数 |
| `retryDelaySeconds` | int | 重试间隔 |
| `fallbackStrategy` | Enum | 降级策略（SKIP/FALLBACK_VALUE/PAUSE/FAIL） |
| `agentConfig` | AgentConfig |  |
| `toolConfig` | ToolConfig |  |
| `conditionConfig` | ConditionConfig |  |
| `approvalConfig` | ApprovalConfig |  |
| `aggregateConfig` | AggregateConfig |  |
| `delegateConfig` | DelegateConfig |  |
| `negotiateConfig` | NegotiateConfig |  |
| `retrievalConfig` | RetrievalConfig |  |
| `judgeConfig` | JudgeConfig |  |

2.WorkflowConfig

| 字段 | 说明 |
|---|---|
| `globalTimeoutSeconds` | 全局超时 |
| `maxConcurrency` | 最大并发 |
| `autoRetry` | 是否自动重试 |
| `maxWorkflowRetries` | 工作流最大重试 |
| `checkpointEnabled` | 是否开启 Checkpoint |
| `checkpointInterval` | Checkpoint 间隔 |
| `detailedTracing` | 是否开启详细追踪 |

#### 节点列表的顺序改变也会影响指纹

因为 `updateFinalAnswerNode` 用 `layer * 10000 + ordinal` 选最终答案，顺序变了答案可能变（可能有多个 AGENT 节点都输出了 `finalAnswer`，要根据拓扑结构的先后顺序决定选择哪个作为最终答案）

#### 哪些变化不影响指纹

节点层面：name（节点名字）、description（节点描述）、labels（节点标签）

工作流层面：name（工作流名字）、description（工作流描述）、version（版本号）、triggerCondition（触发条件描述）、createdBy /createdAt/updatedAt、tags（标签列表）

这些是展示元数据 —— 改个名字、加个标签，不影响执行结果，所以不该让已有 checkpoint 失效。  

### 恢复时怎么知道上一次执行哪些节点完成了、哪些节点在中断时还在跑

恢复时靠五个集合区分节点状态。

completedNodeIds 保留不重跑，输出已经在 variables 里了；

skippedNodeIds 也保留 —— 条件跳过是定论，重算结果一样，清掉会重复判断。

activeNodeIds 清空 —— 崩溃时正在跑的节点结果不可信，要重跑。

failedNodeIds 和 blockedNodeIds 一起清空 —— 失败节点重试，被拦住的下游解冻。

恢复后引擎从拓扑重新走一遍，已完成的跳过，中断和失败的重跑，被拦的等上游跑完再执行。
