package driving.function;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import util.DateUtils;

@Slf4j
public abstract class EarlyResultEventTimeTrigger<T> extends Trigger<T, TimeWindow> {
  ListStateDescriptor<Long> endMessageTimerDesc = new ListStateDescriptor<>("timers", Long.class);
  ReducingStateDescriptor<Long> countDesc = new ReducingStateDescriptor<>("count", new LongAdder(), Long.class);
  ReducingStateDescriptor<Long> lastCountWhenFiringDesc = new ReducingStateDescriptor<>("lastCount", new LongAdder(), Long.class);

  public abstract boolean eval(T element);

  @Override
  public TriggerResult onElement(T element, long timestamp, TimeWindow timeWindow, TriggerContext triggerContext)
      throws Exception {
    log.debug("onElement(ts: {}, mTs: {}, currWm: {})", timestamp, timeWindow.maxTimestamp(), triggerContext.getCurrentWatermark());
    triggerContext.getPartitionedState(countDesc).add(1L);
    log.debug("count: {}", triggerContext.getPartitionedState(countDesc).get());

    if (timeWindow.maxTimestamp() <= triggerContext.getCurrentWatermark()) {
      return fireOrContinue(triggerContext);
    } else {
      if (eval(element)) { // 마지막이면 현재시간을 eventtime으로 넣어서 다음 onElement나 onEventTime에 무조건 fire되도록 만듦.
        log.debug("registerEventTimeTimer(last one) {}", DateUtils.isoDateTime(timestamp));
        triggerContext.registerEventTimeTimer(timestamp);
        triggerContext.getPartitionedState(endMessageTimerDesc).add(timestamp);
      } else {
        log.debug("registerEventTimeTimer {}", DateUtils.isoDateTime(timestamp));
        triggerContext.registerEventTimeTimer(timeWindow.maxTimestamp());
      }
      return TriggerResult.CONTINUE;
    }
  }

  TriggerResult fireOrContinue(TriggerContext triggerContext) throws Exception {
    log.debug("fireOrContinue");
    Long count = triggerContext.getPartitionedState(countDesc).get();
    ReducingState<Long> lastCountState = triggerContext.getPartitionedState(lastCountWhenFiringDesc);
    Long lastCount = lastCountState.get();

    if (lastCount == null) {
      lastCount = 0L;
    }
    Long diff = count - lastCount;
    lastCountState.add(diff);

    if (diff > 0) {
      log.info("FIRE! diff: {}, count: {}, lastCount: {}", diff, count, lastCount);
      return TriggerResult.FIRE;
    } else {
      return TriggerResult.CONTINUE;
    }
  }

  @Override
  public TriggerResult onProcessingTime(long timestamp, TimeWindow timeWindow,
      TriggerContext triggerContext) throws Exception {
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onEventTime(long timestamp, TimeWindow timeWindow, TriggerContext triggerContext)
      throws Exception {
    log.debug("onEventTime(timestamp: {}, mTs: {}, currWm: {})", timestamp, timeWindow.maxTimestamp(), triggerContext.getCurrentWatermark());
    if (timestamp < timeWindow.maxTimestamp()) {
      triggerContext.deleteEventTimeTimer(timestamp);
      ListState<Long> timerState = triggerContext.getPartitionedState(endMessageTimerDesc);
      List<Long> filteredTimestamp = StreamSupport
          .stream(timerState.get().spliterator(), false)
          .filter(x -> x != timestamp)
          .collect(Collectors.toList());

      timerState.update(filteredTimestamp);
      return fireOrContinue(triggerContext);
    } else if (timestamp == timeWindow.maxTimestamp()) {
      return fireOrContinue(triggerContext);
    } else {
      return TriggerResult.CONTINUE;
    }
  }


  @Override
  public boolean canMerge() {
    log.debug("canMerge");
    return true;
  }

  @Override
  public void onMerge(TimeWindow window,
      OnMergeContext ctx) throws Exception {
    log.debug("onMerge");
    ctx.mergePartitionedState(countDesc);
    ctx.mergePartitionedState(lastCountWhenFiringDesc);
    ctx.mergePartitionedState(endMessageTimerDesc);

    ListState<Long> timer = ctx.getPartitionedState(endMessageTimerDesc);

    if (timer.get() != null) {
      log.debug("timer: " + timer.get());
      timer.get().forEach(timestamp -> ctx.registerEventTimeTimer(timestamp));
    }
    ctx.registerEventTimeTimer(window.maxTimestamp());
    // only register a timer if the watermark is not yet past the end of the merged window
    // this is in line with the logic in onElement(). If the watermark is past the end of
    // the window onElement() will fire and setting a timer here would fire the window twice.
//    long windowMaxTimestamp = window.maxTimestamp();
//    if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
//      ctx.registerEventTimeTimer(window.maxTimestamp());
//    }
  }

  @Override
  public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
    log.debug("Clear");
    triggerContext.deleteEventTimeTimer(timeWindow.maxTimestamp());
  }


  private static class LongAdder implements ReduceFunction<Long> {

    @Override
    public Long reduce(Long a, Long b) {
      log.debug("a: {} b: {}", a, b);
      return a + b;
    }
  }
}
