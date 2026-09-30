package com.github.lisama569.reliableoutboxdeliveryjava.web;

/**
 * 可被调用方区分的业务错误码。
 */
public enum ErrorCode {
    /** 事件不存在。 */
    EVENT_NOT_FOUND,
    /** 非法状态流转（如对非 DEAD 事件执行重投）。 */
    INVALID_STATE,
    /** 越权操作。 */
    FORBIDDEN,
    /** 未认证。 */
    UNAUTHORIZED,
    /** 请求参数不合法。 */
    BAD_REQUEST
}
