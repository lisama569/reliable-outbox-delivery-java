-- outbox 事件表：与业务数据同库同事务，保证业务变更与事件落库的原子性
CREATE TABLE IF NOT EXISTS outbox_event (
    id               VARCHAR(64)  PRIMARY KEY,
    aggregate_type   VARCHAR(64)  NOT NULL,
    aggregate_id     VARCHAR(64)  NOT NULL,
    event_type       VARCHAR(64)  NOT NULL,
    payload          CLOB         NOT NULL,
    status           VARCHAR(32)  NOT NULL,
    attempt_count    INT          NOT NULL DEFAULT 0,
    max_attempts     INT          NOT NULL,
    next_retry_at    TIMESTAMP WITH TIME ZONE,
    locked_by        VARCHAR(128),
    locked_at        TIMESTAMP WITH TIME ZONE,
    dead_reason      CLOB,
    last_error       CLOB,
    redelivery_count INT          NOT NULL DEFAULT 0,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL
);

-- 投递器领取候选扫描：只取 PENDING 且退避窗口已过的事件
CREATE INDEX IF NOT EXISTS idx_outbox_claim
    ON outbox_event (status, next_retry_at, aggregate_id, id);

-- 运维查询：按聚合查看事件
CREATE INDEX IF NOT EXISTS idx_outbox_aggregate
    ON outbox_event (aggregate_type, aggregate_id, created_at);

-- 业务订单表
CREATE TABLE IF NOT EXISTS biz_order (
    id          VARCHAR(64) PRIMARY KEY,
    status      VARCHAR(32) NOT NULL,
    customer    VARCHAR(128) NOT NULL,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL
);

-- 消费端幂等表：事件标识只允许出现一次，重复投递不再执行业务副作用
CREATE TABLE IF NOT EXISTS processed_event (
    event_id      VARCHAR(64) PRIMARY KEY,
    consumed_at    TIMESTAMP WITH TIME ZONE NOT NULL,
    consumer       VARCHAR(128) NOT NULL
);

-- 业务副作用表：记录"已履约"的订单，用于断言副作用只执行一次
CREATE TABLE IF NOT EXISTS fulfillment (
    order_id        VARCHAR(64) PRIMARY KEY,
    event_id        VARCHAR(64) NOT NULL,
    fulfilled_at    TIMESTAMP WITH TIME ZONE NOT NULL
);

-- 投递审计：投递器每次"处理一条事件"（领取后）插一行，用于并发断言
CREATE TABLE IF NOT EXISTS dispatch_audit (
    event_id       VARCHAR(64) NOT NULL,
    dispatcher_id  VARCHAR(128) NOT NULL,
    attempt_no     INT NOT NULL,
    processed_at   TIMESTAMP WITH TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dispatch_audit_event ON dispatch_audit (event_id);
