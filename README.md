# Reliable Outbox Delivery（可靠异步事件投递）

在 Spring Boot 工程上实现的一套**事务性发件箱（Transactional Outbox）**可靠事件投递能力：
业务状态变更与待投递事件在同一个本地事务中落库，后台投递器把事件发送到一个
**进程内本地消息队列**（按真实消息系统的语义对待），消费端按事件标识幂等处理。
全程不依赖任何需要独立部署的外部服务、真实凭据或付费服务，H2 文件库即可运行。

---

## 1. 架构与组件

| 组件 | 位置 | 职责 |
| --- | --- | --- |
| 订单写模型 | `service/OrderService`、`repo/JdbcOrderRepository` | 业务变更与 outbox 事件**同一事务**写入 |
| outbox 存储 | `domain/OutboxEvent`、`repo/JdbcOutboxRepository` | 事件状态机、原子领取、退避调度、死信、孤儿回收 |
| 后台投递器 | `dispatch/OutboxDispatcher`、`dispatch/DispatcherManager` | 多实例并发轮询、发布到队列、等待消费确认、按结果流转 |
| 退避策略 | `dispatch/BackoffPolicy` | 指数退避，可配置初值、倍率、上限 |
| 本地消息队列 | `broker/LocalMessageBroker` | FIFO 单消费者队列，发布后等待消费者 ack/nack |
| 消费端 | `consumer/OrderEventHandler`、`repo/JdbcConsumerRepository` | 按 `eventId` 幂等，副作用与去重记录同一事务 |
| 运维 API | `service/EventAdminService`、`web/EventAdminController` | 查询事件/死信、死信重投、标记已处理 |
| 鉴权 | `security/SecurityConfig` | HTTP Basic + 基于角色的访问控制 |

数据库使用嵌入式 **H2**（默认文件库 `./data/outboxdb`），共四张表：

- `purchase_order`：订单写模型；
- `outbox_event`：事件发件箱（状态、尝试次数、下次尝试时间、锁、失败原因）；
- `consumed_event`：消费去重表，主键就是 `event_id`；
- `order_projection`：消费端读模型，代表“业务副作用”（订单状态投影）。

表结构在启动时由 `config/SchemaInitializer` 以 `CREATE TABLE IF NOT EXISTS` 幂等创建，
因此进程重启后数据与表结构都保留。

---

## 2. 投递语义

### 2.1 原子性：业务与事件一起提交

`JdbcOrderRepository#createWithEvent` / `#markPaidWithEvent` 在单个数据库事务内
依次执行业务表更新与 `outbox_event` 插入：

- 事务提交成功 ⇒ 事件一定可查，不会丢；
- 业务或事件写入失败 ⇒ 整个事务回滚，两者都不生效。

后台投递器只读取**已提交**的事件，因此不会读到未提交数据。

### 2.2 至少一次投递（at-least-once）

投递流程：

```
claimDue（原子领取，PENDING/RETRY → PROCESSING，attempts+1）
   └─ 发布到本地队列并同步等待消费确认
        ├─ ack            → markDelivered（PROCESSING → DELIVERED）
        ├─ 可重试异常/超时 → 未达上限：scheduleRetry（→ RETRY，退避后到期）
        │                  达到上限：markDead（→ DEAD，记录原因与尝试次数）
        └─ 不可重试异常    → markDead（第一次尝试即 DEAD）
```

至少一次意味着**重复投递是正常情况**（例如：消费副作用已提交，但在回写
DELIVERED 前进程被杀）。重复消息由消费端幂等去重，因此业务效果是**恰好一次**。

### 2.3 消费端幂等

`JdbcConsumerRepository#consumeIfFirstTime` 在同一事务内：

1. 向 `consumed_event(event_id PRIMARY KEY, ...)` 插入去重记录；
2. 插入成功才执行业务副作用（MERGE `order_projection`）；
3. 重复消息插入主键冲突（`DuplicateKeyException`）⇒ 返回 `false`，副作用**不再执行**。

消费日志每次都会打印 `eventId`、第几次投递（`deliveryIndex`）以及判定结果
（`SIDE_EFFECT_APPLIED` / `DUPLICATE_SKIPPED`）。

### 2.4 本地消息队列语义

`LocalMessageBroker` 是一个进程内 FIFO 队列（队列名 `order.events`）：

- `publish` 入队后以 `CompletableFuture` 等待消费者确认，ack/nack/超时都会回传给投递器；
- 每个队列单线程串行消费，保证队列内顺序；
- 队列是**内存**的：进程崩溃时未确认的消息会丢失，但 outbox 事件仍在数据库中，
  重启后通过孤儿回收重新发布，因此不破坏“不丢事件”的承诺；
- 未分类的 `RuntimeException` 按可重试错误处理；确认超时（默认 10s）也是可重试。

---

## 3. 事件状态流转

```
                 业务事务提交
                       │
                       ▼
                  ┌────────┐  投递成功(ack)  ┌───────────┐
                  │ PENDING│───────────────▶│ DELIVERED │（终态）
                  └────────┘                 └───────────┘
                     │  ▲                         ▲
        原子领取      │  │ 退避到期                │ 孤儿回收后重投
                     ▼  │                         │
                  ┌────────────┐  可重试且未超限  ┌───────┐
                  │ PROCESSING │───────────────▶│ RETRY │
                  └────────────┘                  └───────┘
                     │           │
       进程崩溃/被杀  │           ├─ 可重试且达到上限
       (停留在此，    │           └─ 不可重试（第一次即进入）
        等待孤儿回收) ▼
                  ┌──────┐  人工标记已处理   ┌──────────┐
                  │ DEAD │───────────────▶│ RESOLVED │（终态）
                  └──────┘                  └──────────┘
                     │
                     │ 人工重投（仅 DEAD 可执行）
                     └──────────────▶ PENDING（attempts 清零，失败信息清空）
```

- `DEAD` 在被 `resolve` 之前属于**非终态**：同一聚合中排在它后面的事件会被
  “队头阻塞”，不会越过死信先投递（避免乱序与基于陈旧状态的处理）；
- `DELIVERED`、`RESOLVED` 为终态，任何流转操作都会被拒绝并返回 409。

### 3.1 并发互斥（同一条事件不会被重复领取）

`claimDue` 是**单条** `UPDATE ... WHERE id = (SELECT ...)`：

- 子查询只选 `PENDING/RETRY` 且已到期的事件，并且要求同聚合不存在更早的非终态事件；
- 外层 `WHERE status IN ('PENDING','RETRY')` 在写锁上二次校验；
- 多个投递器并发执行时，只有一个 UPDATE 能命中同一行，行锁保证互斥，
  因此同一条事件不可能被两个执行者同时处理。

### 3.2 同聚合顺序

子查询中的 `NOT EXISTS` 条件实现**每聚合队头阻塞（per-aggregate head-of-line）**：
只有同一聚合内 id 最小的非终态事件可以被领取。不同聚合之间互不阻塞，
多个投递器仍可并行处理不同聚合，吞吐不受单线程限制。

### 3.3 崩溃恢复（孤儿回收）

投递器领取事件后会写入 `locked_by` 与 `locked_at`。`reclaimStale` 周期性地把
`PROCESSING` 且锁定时间超过 `outbox.visibility-timeout` 的事件重置为 `RETRY`
并立即到期，由存活的投递器重新投递。

模拟“进程被杀”时，对应 worker 线程会因致命错误终止，`DispatcherManager`
在可见性超时后用同一 `workerId` 重新拉起一个 worker 完成接管；
真实多实例部署中，则由其它存活实例的孤儿回收接管。

---

## 4. 重试与退避规则

- 错误分类：
  - `NonRetryableDeliveryException` ⇒ **不可重试**，第一次尝试直接进死信；
  - `RetryableDeliveryException`、未分类运行时异常、消费确认超时 ⇒ **可重试**。
- 尝试上限：`outbox.max-attempts`（默认 **5**）。第 5 次尝试仍失败时进入死信，
  不再安排下一次退避。
- 退避公式（`BackoffPolicy`，`failedAttempts` 从 1 开始）：

  ```
  delay(n) = min(backoffInitial × backoffMultiplier^(n-1), backoffMax)
  ```

  默认值：初值 `100ms`、倍率 `2.0`、上限 `5s`。
  即 100ms → 200ms → 400ms → 800ms（第 5 次失败后直接 DEAD，不再等待）。
- 每次判定都会记录 `attempts`、`last_error`、`next_attempt_at`；
  死信额外记录 `dead_reason`（`MAX_ATTEMPTS_EXCEEDED ...` 或 `NON_RETRYABLE: ...`）
  与 `dead_at`。

---

## 5. 死信处理

死信只能通过运维 API 处理，且**仅 `DEAD` 状态**可执行：

1. **人工重投（requeue）**：`DEAD → PENDING`，`attempts` 清零、错误信息清空，
   之后仍走同一套领取 / 退避 / 上限 / 幂等规则——重投不会绕过幂等，也不会重置语义；
2. **标记已处理（resolve）**：`DEAD → RESOLVED`（终态），表示人工确认放弃投递；
3. 对非 DEAD 事件执行这两个操作 ⇒ `409 ILLEGAL_STATE`；
   事件不存在 ⇒ `404 RESOURCE_NOT_FOUND`。

---

## 6. HTTP API 与权限

使用 HTTP Basic 认证。内置两个本地用户（仅用于本地演示/测试）：

| 用户 | 密码 | 角色 |
| --- | --- | --- |
| `ops` | `ops-pass` | `ROLE_OPS`（运维，可写） |
| `viewer` | `viewer-pass` | `ROLE_VIEWER`（只读） |

| 方法与路径 | 说明 | OPS | VIEWER | 未认证 |
| --- | --- | --- | --- | --- |
| `POST /api/orders` | 创建订单（同事务写事件） | ✅ | 403 | 401 |
| `POST /api/orders/payments` | 支付订单（同事务写事件） | ✅ | 403 | 401 |
| `GET  /api/orders/{orderNo}` | 查询订单 | ✅ | ✅ | 401 |
| `GET  /api/admin/events?status=&limit=` | 查询事件（可按状态过滤） | ✅ | ✅ | 401 |
| `GET  /api/admin/events/{id}` | 查询单个事件 | ✅ | ✅ | 401 |
| `GET  /api/admin/events/dead-letters` | 死信列表 | ✅ | ✅ | 401 |
| `POST /api/admin/events/{id}/requeue` | 死信重投 | ✅ | 403 | 401 |
| `POST /api/admin/events/{id}/resolve` | 死信标记已处理 | ✅ | 403 | 401 |

可区分的错误响应（统一 JSON 体，含 `code` 字段）：

| HTTP 状态 | code | 触发条件 |
| --- | --- | --- |
| 401 | `UNAUTHENTICATED` | 未提供凭据 |
| 403 | `ACCESS_DENIED` | 角色不足（如 VIEWER 执行重投） |
| 404 | `RESOURCE_NOT_FOUND` | 事件/订单不存在 |
| 409 | `ILLEGAL_STATE` | 非法状态流转（如对 DELIVERED 重投）、并发状态冲突 |
| 400 | `VALIDATION_FAILED` / `BAD_REQUEST` | 参数校验失败、`status` 查询参数不是合法的事件状态 |

---

## 7. 配置项（`application.properties`）

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| `outbox.enabled` | `true` | 是否启动后台投递器 |
| `outbox.dispatcher-instances` | `2` | 并发投递器实例数 |
| `outbox.poll-interval` | `200ms` | 无可领事件时的轮询间隔 |
| `outbox.max-attempts` | `5` | 最大尝试次数（达到后进死信） |
| `outbox.backoff-initial` | `100ms` | 首次退避时长 |
| `outbox.backoff-multiplier` | `2.0` | 退避倍率 |
| `outbox.backoff-max` | `5s` | 退避上限 |
| `outbox.visibility-timeout` | `2s` | PROCESSING 锁的可见性超时（孤儿回收阈值） |
| `outbox.reclaim-interval` | `1s` | 孤儿回收扫描间隔 |
| `outbox.claim-batch-size` | `10` | 预留批量配置（当前每轮领取 1 条） |

---

## 8. 本地验证方法

### 8.1 运行测试（无需 Docker、无外部服务）

```bash
mvn test
```

测试使用各自独立的内存 H2（重启场景使用临时目录下的文件 H2），
包含以下 8 个场景，均在 `src/test/.../tests` 下：

| 测试类 | 覆盖场景 |
| --- | --- |
| `ApplicationTests` | Spring 上下文冒烟 |
| `NormalDeliveryTest` | 正常投递：创建/支付事件最终 DELIVERED，副作用各一次 |
| `DuplicateConsumptionTest` | 副作用已落库但 DELIVERED 前“进程被杀”，孤儿回收重投后副作用仍只执行一次 |
| `ProcessRestartTest` | **完整进程重启**：关闭并重建整个 Spring 上下文 + 同一文件 H2，事件不丢、副作用不重复 |
| `RetryToDeadLetterTest` | 持续可重试失败按指数退避重试，5 次后进入死信，保留原因与尝试次数 |
| `NonRetryableAndResolveTest` | 不可重试第一次即死信；死信可标记 RESOLVED；重复操作返回 409 |
| `DeadLetterRequeueTest` | 死信人工重投后成功；越权 403、不存在 404、未认证 401、非法流转 409 |
| `ConcurrentDispatchTest` | 4 个投递器并发处理 40 个聚合不重复领取；同聚合事件严格有序、最终状态正确 |

测试日志中可直接观察到事件标识、尝试次数与每次判定，例如：

```
[dispatch] eventId=... claimed by worker=worker-2 attempt=3/5
[dispatch] eventId=... attempt=3 verdict=RETRY nextAttemptAt=... reason=...
[dispatch] eventId=... attempt=5 verdict=DEAD lastError=...
[consumer] eventId=... deliveryIndex=2 verdict=DUPLICATE_SKIPPED
[dispatch] worker worker-1 reclaimed 1 stale PROCESSING event(s) ...
```

查看完整日志报告：`target/surefire-reports/`。

### 8.2 手工冒烟（启动应用）

```bash
mvn spring-boot:run
```

```bash
# 创建订单
curl -u ops:ops-pass -H 'Content-Type: application/json' \
  -d '{"orderNo":"ORD-1","amountCents":1200}' \
  http://localhost:8080/api/orders

# 支付订单
curl -u ops:ops-pass -H 'Content-Type: application/json' \
  -d '{"orderNo":"ORD-1"}' \
  http://localhost:8080/api/orders/payments

# 查看全部事件 / 死信
curl -u viewer:viewer-pass http://localhost:8080/api/admin/events
curl -u viewer:viewer-pass http://localhost:8080/api/admin/events/dead-letters

# 死信重投 / 标记已处理（仅 ops）
curl -u ops:ops-pass -X POST http://localhost:8080/api/admin/events/1/requeue
curl -u ops:ops-pass -X POST http://localhost:8080/api/admin/events/1/resolve
```

应用默认使用文件库 `./data/outboxdb`（已在 `.gitignore` 中忽略）；
删除该目录即可从空状态重新开始。

---

## 9. 设计边界与说明

- 本地消息队列是**进程内实现**，不具备跨进程持久化；事件的可靠性事实来源是
  数据库中的 outbox 表。替换为 Kafka/RabbitMQ 等真实中间件时，
  只需替换 `MessagePublisher`/消费订阅部分，状态机、幂等与重试规则保持不变。
- 示例业务副作用是更新读模型 `order_projection`；真实系统中可替换为任意
  可与 `consumed_event` 同事务提交（或可补偿）的副作用。
- 鉴权使用内存用户与 HTTP Basic，仅用于本地演示；接入企业 SSO/IAM 时替换
  `SecurityConfig` 即可，`@Secured` 角色规则无需改动。
