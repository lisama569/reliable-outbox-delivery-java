# 可靠异步事件投递（Transactional Outbox + 本地模拟消息队列）

在业务状态变更后，先把待投递事件与业务变更在**同一个本地事务**中落库，再由后台投递器
把事件发送到本地模拟的消息队列，由消费端按事件标识幂等处理。不依赖任何需要独立部署的
外部服务、真实凭据或付费服务：数据库使用嵌入式 H2（文件库，进程重启数据不丢），消息队列
是进程内实现，但按真实消息系统的语义对待（发布可失败、消费至少一次、失败重投）。

## 1. 技术栈

- Spring Boot 3.4 / Java 21、Spring Web、Spring Security
- Spring JDBC + H2 2.x（默认文件库 `./target/h2data/app`，无需外部数据库）
- 无第三方消息中间件：`LocalMessageQueue` 为进程内模拟队列

## 2. 整体流程

```
业务事务                         后台投递器(N个并发)                 消费端
┌────────────────────┐        ┌───────────────────────┐       ┌────────────────────┐
│ UPDATE biz_order   │        │ 扫描 PENDING 事件      │       │ 收到消息           │
│ INSERT outbox_event│  提交  │ 行锁原子领取           │ 发布  │ 幂等检查 eventId    │
│  (同一事务)         │──────▶│ 发布到本地队列         │──────▶│ 执行业务副作用       │
└────────────────────┘        │ 成功→DELIVERED         │       │ 成功 ack / 失败 nack │
                              │ 可重试→退避重试         │       └────────────────────┘
                              │ 超限/不可重试→DEAD      │
                              └───────────────────────┘
```

## 3. 投递语义

- **原子性（事件不丢）**：业务数据与 outbox 事件在同一事务写入，事务提交成功后事件必然
  存在于 `outbox_event` 表，随后的进程崩溃不影响其被投递。
- **至少一次投递（at-least-once）**：投递器在发布成功后才把事件标记为 DELIVERED；
  任何崩溃/异常都会使事件保持可领取状态并被再次投递。
- **消费端幂等**：消费端以**事件标识（eventId）**为幂等键，在独立事务中先向
  `processed_event` 插入记录，再执行业务副作用（`fulfillment`），两者同一事务。
  重复投递时插入冲突，副作用整体跳过，因此同一业务副作用最多执行一次。
- **消息队列也遵循至少一次**：消费返回可重试失败（nack）时，队列延迟重投同一条消息；
  消费端的幂等逻辑保证重投安全。

## 4. 事件状态流转

```
PENDING ──领取(行锁)──▶ IN_PROGRESS ──发布成功──────────────▶ DELIVERED（终态）
   ▲                        │
   │                        ├─ 可重试失败且未超限：回到 PENDING，设置 next_retry_at 退避
   │                        ├─ 可重试失败达到上限：────▶ DEAD
   │                        └─ 不可重试失败：──────────▶ DEAD
   │
   ├─ 人工重投 DEAD：DEAD ──redrive──▶ PENDING（尝试计数归零、redelivery_count+1）
   └─ 崩溃残留：IN_PROGRESS 超过 lock-timeout ──回收扫描──▶ PENDING

DEAD ──人工标记已处理(resolve)──▶ RESOLVED（终态，不再投递）
```

字段含义（`outbox_event`）：`attempt_count` 为当前这一轮已尝试次数（人工重投后归零）；
`max_attempts` 为本轮最大尝试次数；`redelivery_count` 为人工重投次数；`dead_reason` /
`last_error` 分别保留进入死信的原因与最近一次错误；`locked_by`/`locked_at` 记录当前领取者
与领取时间。

## 5. 重试、退避与死信

- 投递结果分三类（`DeliveryResult`）：`SUCCESS`、`RETRYABLE_FAILURE`、
  `NON_RETRYABLE_FAILURE`。
- **可重试失败**：按指数退避重试，退避时长 `min(backoff-max, backoff-base * 2^(attempt-1))`，
  事件在退避窗口到期前不会被领取（`next_retry_at`）。
- **尝试上限**：达到 `outbox.default-max-attempts`（默认 5）后进入 DEAD，
  记录失败原因与尝试次数。
- **不可重试失败**（如毒消息被 broker 永久拒绝）：第一次即进入 DEAD。
- **死信重投**：`redrive` 将 DEAD 置回 PENDING，尝试计数归零、重投次数加一，
  重新走同一套退避/上限/幂等规则；若仍持续失败会再次进入 DEAD。
- **标记已处理**：`resolve` 用于已在旁路人工处理完毕的死信，置为终态 RESOLVED。
- **崩溃恢复**：回收线程与投递线程相互独立（避免被阻塞的投递饿死），周期性把
  `locked_at` 早于 `now - lock-timeout` 的 IN_PROGRESS 事件重置为 PENDING。

## 6. 并发与顺序

- 可配置多个投递器并发运行（`outbox.dispatchers`，默认 2）。领取在**单个事务内**先用
  `SELECT ... FOR UPDATE` 对候选行加行锁、再以条件 UPDATE（要求当前仍为 PENDING）置为
  IN_PROGRESS。后到的事务会阻塞等待，待前者提交后基于最新数据重新求值，因此
  **同一条事件不会被两个执行者同时领取/处理**。
- 每次处理都会向 `dispatch_audit` 写入（eventId、dispatcherId、attempt），用于审计与
  并发测试断言。
- **同一业务聚合有序**：候选查询带聚合顺序保护——若同一 `(aggregate_type, aggregate_id)`
  已存在未终态（PENDING/IN_PROGRESS/DEAD）的更早事件，则当前事件不可领取；
  DEAD/RESOLVED/DELIVERED 才会放行后续事件，避免并发重试造成的聚合内乱序。

## 7. HTTP 接口与权限

采用 HTTP Basic（仅本地演示，用户保存在内存中，非真实凭据）：

| 用户 | 角色 |
| --- | --- |
| `admin` / `admin123` | `OPS_ADMIN`（查询 + 重投 + 标记已处理） |
| `viewer` / `viewer123` | `OPS_VIEWER`（只读查询） |

业务接口（匿名可用）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/orders` | 创建订单（body: `{"customer":"..."}`），原子落库 OrderCreated 事件 |
| POST | `/api/orders/{orderId}/ship` | 发货，原子落库 OrderShipped 事件 |

运维接口（`/api/admin/events`）：

| 方法 | 路径 | 角色 | 说明 |
| --- | --- | --- | --- |
| GET | `/api/admin/events?limit=N` | VIEWER/ADMIN | 最近事件 |
| GET | `/api/admin/events/{eventId}` | VIEWER/ADMIN | 查询单个事件 |
| GET | `/api/admin/events/aggregate/{type}/{id}` | VIEWER/ADMIN | 某聚合下的事件（含顺序） |
| GET | `/api/admin/events/dead` | VIEWER/ADMIN | 死信列表（含原因与尝试次数） |
| POST | `/api/admin/events/{eventId}/redrive` | ADMIN | 死信人工重投 |
| POST | `/api/admin/events/{eventId}/resolve` | ADMIN | 死信标记已处理 |

错误以统一 JSON 返回，可明确区分：

- 404 `EVENT_NOT_FOUND`：事件不存在；
- 409 `INVALID_STATE`：非法状态流转（如对 DELIVERED 事件重投）；
- 400 `BAD_REQUEST`：参数不合法；
- 401 `UNAUTHORIZED`：未认证；
- 403 `FORBIDDEN`：已认证但角色不足（如 viewer 执行 redrive）。

## 8. 可观测日志

投递与消费的关键判定均打印事件标识、尝试次数与判定结果，例如：

```
eventId=... attempt=1/5 verdict=RETRYABLE_FAILURE status=PENDING nextRetryAt=+100ms dispatcher=dispatcher-2
eventId=... attempt=5/5 verdict=RETRYABLE_FAILURE status=DEAD reason=retry limit (5) exhausted ...
eventId=... attempt=1 verdict=SUCCESS status=DELIVERED dispatcher=dispatcher-1
eventId=... consume attempt 2 verdict=SUCCESS action=SKIPPED_DUPLICATE
dispatcher dispatcher-2 recovered 3 stale IN_PROGRESS events after crash/lock timeout
```

## 9. 本地验证

需要 JDK 21+、Maven，无需 Docker 或任何外部服务：

```bash
mvn test
```

测试覆盖以下场景（全部基于 H2 内存库/文件库，无需外部组件）：

1. 正常投递 + 消费副作用恰好一次（`HappyPathTest`）；
2. 同一事件被重复消费（nack 重投 + broker 主动重投）均不重复副作用
   （`DuplicateConsumptionTest`）；
3. 投递过程中进程重启：PENDING 未投递、IN_PROGRESS 锁超时残留均被新进程恢复，
   事件不丢且副作用一次（`RestartRecoveryTest`，使用独立 H2 文件库与 restart profile）；
4. 可重试失败达到上限进入死信、毒消息一次进入死信，以及人工 redrive 成功、
   resolve 终态（`RetryDeadLetterTest`）；
5. 两个投递器并发：发布门闩确定性地让两个执行者同时处理，审计断言每条事件仅被处理一次
   （`ConcurrentDispatchTest`）；
6. 权限与错误码：viewer 可查不可写、匿名 401、非法状态 409、事件不存在 404、空参数 400
   （`AuthorizationAndErrorsTest`）。

手动体验（启动后端口见日志，默认随机端口；可在 application.properties 固定）：

```bash
mvn spring-boot:run

# 创建订单（匿名）
curl -s -u admin:admin123 -X POST localhost:8080/api/orders \
  -H 'Content-Type: application/json' -d '{"customer":"alice"}'

# 查询事件（viewer 只读）
curl -s -u viewer:viewer123 localhost:8080/api/admin/events/dead

# 死信重投（仅 admin）
curl -s -u admin:admin123 -X POST localhost:8080/api/admin/events/{eventId}/redrive
```

## 10. 主要配置项

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| `outbox.dispatchers` | 2 | 并发投递器数量 |
| `outbox.poll-batch-size` | 16 | 单次领取上限 |
| `outbox.poll-interval-millis` | 50 | 轮询间隔 |
| `outbox.default-max-attempts` | 5 | 单轮最大尝试次数 |
| `outbox.backoff-base-millis` | 100 | 指数退避基准 |
| `outbox.backoff-max-millis` | 5000 | 退避上限 |
| `outbox.lock-timeout-millis` | 30000 | 领取锁超时，超时视为崩溃残留 |
| `outbox.recovery-interval-millis` | 1000 | 崩溃残留回收扫描间隔 |
| `localqueue.queue-name` | order.events | 本地队列名 |
| `localqueue.consumer-workers` | 2 | 队列消费线程数 |
| `localqueue.redelivery-delay-millis` | 200 | nack 后的重投延迟 |

## 11. 关键类

- `outbox/OutboxPublisher`：在业务事务内写入事件；
- `outbox/OutboxRepository`：事件落库、行锁领取、状态流转、崩溃回收；
- `outbox/OutboxDispatcher` / `OutboxSchedulerConfig`：并发投递器与其生命周期；
- `queue/LocalMessageQueue` / `MessageQueue`：至少一次语义的本地队列；
- `queue/FailureSimulator`：测试用故障注入（可重试/不可重试、发布/消费、并发门闩）；
- `biz/OrderService`：示例业务（事务内写业务表 + outbox）；
- `biz/OrderEventConsumer` / `FulfillmentService`：幂等消费与业务副作用；
- `web/AdminController` / `OrderController`：运维与业务 HTTP 入口；
- `config/SecurityConfig`：角色与 401/403 处理。
