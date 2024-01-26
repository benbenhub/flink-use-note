package com.flinkuse.cases.common;

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @author learn
 * @date 2023/6/13 14:52
 */
public class CountAndTimeTrigger<T> extends Trigger<T, GlobalWindow> {
    private static final long serialVersionUID = 1L;
    private final long countThreshold;
    private final long timeThreshold;
    private long lastFiredTime;

    public CountAndTimeTrigger(long countThreshold, Time timeThreshold) {
        this.countThreshold = countThreshold;
        this.timeThreshold = timeThreshold.toMilliseconds();
    }

    @Override
    public TriggerResult onElement(T element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
        long currentCount;
        if (ctx.getPartitionedState(getCountDescriptor()).value() == null) {
            currentCount = 1L;
        } else {
            currentCount = ctx.getPartitionedState(getCountDescriptor()).value() + 1;
        }
        ctx.getPartitionedState(getCountDescriptor()).update(currentCount);
        if (currentCount >= countThreshold || (timestamp - lastFiredTime) >= timeThreshold) {
            // 当元素数量达到阈值或时间间隔达到阈值时，触发窗口计算
            lastFiredTime = timestamp;
            ctx.getPartitionedState(getCountDescriptor()).clear();
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        if ((time - lastFiredTime) >= timeThreshold) {
            // 当时间间隔达到阈值时，触发窗口计算
            lastFiredTime = time;
            ctx.getPartitionedState(getCountDescriptor()).clear();
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(getCountDescriptor()).clear();
    }

    private ValueStateDescriptor<Long> getCountDescriptor() {
        return new ValueStateDescriptor<>("count", Long.class);
    }

}
