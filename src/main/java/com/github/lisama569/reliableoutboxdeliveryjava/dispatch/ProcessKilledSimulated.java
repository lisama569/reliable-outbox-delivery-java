package com.github.lisama569.reliableoutboxdeliveryjava.dispatch;

/**
 * 模拟投递器所在进程在关键点被强杀：不是普通业务异常，
 * 投递线程不会捕获它（Error 会直接终止 worker 的 run 循环）。
 * 事件会停留在 PROCESSING，由存活投递器经孤儿回收重新投递。
 */
public class ProcessKilledSimulated extends Error {

    public ProcessKilledSimulated(String message) {
        super(message);
    }
}
