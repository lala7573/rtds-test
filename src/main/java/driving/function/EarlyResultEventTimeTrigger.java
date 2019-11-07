package driving.function;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

@Slf4j
public abstract class EarlyResultEventTimeTrigger<T> extends Trigger<T, TimeWindow> {
  ListStateDescriptor<Long> endMessageTimerDesc = new ListStateDescriptor<>("timers", Long.class);
  ValueStateDescriptor<Long> expectedCountDesc = new ValueStateDescriptor<Long>("expectCount", Long.class);
  ReducingStateDescriptor<Long> countDesc = new ReducingStateDescriptor<>("count", new LongAdder(), Long.class);
  ReducingStateDescriptor<Long> lastCountDesc = new ReducingStateDescriptor<>("lastCount", new LongAdder(), Long.class);

  public abstract Optional<Long> eval(T element);

  @Override
  public TriggerResult onElement(T element, long timestamp, TimeWindow timeWindow, TriggerContext triggerContext)
      throws Exception {
    triggerContext.getPartitionedState(countDesc).add(1L);
    log.debug("onElement(ts: {}, mTs: {}, currWm: {}), count({})", timestamp, timeWindow.maxTimestamp(), triggerContext.getCurrentWatermark(), triggerContext.getPartitionedState(countDesc).get());

    if (timeWindow.maxTimestamp() <= triggerContext.getCurrentWatermark()) {
      return fireOrContinue(triggerContext);
    } else {
      Optional<Long> lastCountOpt = eval(element);
      if (lastCountOpt.isPresent()) {
        triggerContext.registerEventTimeTimer(timestamp);
        triggerContext.getPartitionedState(expectedCountDesc).update(lastCountOpt.get());
        triggerContext.getPartitionedState(endMessageTimerDesc).add(timestamp);
      } else {
        triggerContext.registerEventTimeTimer(timeWindow.maxTimestamp());
      }
      return TriggerResult.CONTINUE;
    }
  }

  boolean isCountAsExpected(TriggerContext triggerContext) throws Exception {
    Long lastCount = triggerContext.getPartitionedState(expectedCountDesc).value();
    Long count = triggerContext.getPartitionedState(countDesc).get();

    return count != null && count.equals(lastCount);
  }

  TriggerResult fireOrContinue(TriggerContext triggerContext) throws Exception {
    log.debug("fireOrContinue");

    Long lastCount = triggerContext.getPartitionedState(lastCountDesc).get();
    Long count = triggerContext.getPartitionedState(countDesc).get();
    if (lastCount == null) {
      lastCount = 0L;
    }

    Long diff = count - lastCount;
    if (isCountAsExpected(triggerContext) && diff > 0) {
      // prevent same data triggered
      triggerContext.getPartitionedState(lastCountDesc).add(diff);
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
    return true;
  }

  @Override
  public void onMerge(TimeWindow window,
      OnMergeContext ctx) throws Exception {
    log.debug("onMerge");
    ctx.mergePartitionedState(countDesc);
    ctx.mergePartitionedState(lastCountDesc);
    ctx.mergePartitionedState(endMessageTimerDesc);

    ListState<Long> timer = ctx.getPartitionedState(endMessageTimerDesc);

    if (timer.get() != null) {
      log.debug("timer: " + timer.get());
      timer.get().forEach(timestamp -> ctx.registerEventTimeTimer(timestamp));
    }
    ctx.registerEventTimeTimer(window.maxTimestamp());
  }

  @Override
  public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
    log.debug("Clear");
    triggerContext.deleteEventTimeTimer(timeWindow.maxTimestamp());
  }


  private static class LongAdder implements ReduceFunction<Long> {

    @Override
    public Long reduce(Long a, Long b) {
      return a + b;
    }
  }
}
