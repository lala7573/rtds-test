package driving.job;

import static junit.framework.TestCase.assertTrue;

import driving.function.EarlyResultEventTimeTrigger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

public class EarlyResultEventTriggerTest {

  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberSlotsPerTaskManager(2)
              .setNumberTaskManagers(1)
              .build());

  @Test
  public void testIncrementPipeline() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // configure your test environment
    env.setParallelism(2);
    env.getConfig().disableSysoutLogging();

    // values are collected in a static variable
    CollectSink.values.clear();

    final long current = System.currentTimeMillis();
    // create a stream of custom elements and apply transformations
    env.fromElements(0, 1, 2, 3)
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Integer>(Time.minutes(1)) {
          @Override
          public long extractTimestamp(Integer element) {
            return current + element;
          }
        })
        .keyBy((KeySelector<Integer, Integer>) integer -> 1)
        .window(EventTimeSessionWindows.withGap(Time.minutes(1)))
        .trigger(new EarlyResultEventTimeTrigger<Integer>() {
          @Override
          public boolean eval(Integer element) {
            return true;
          }
        })
        .aggregate(new IntegerAgg())
        .flatMap(
        (FlatMapFunction<List<Integer>, Integer>) (integers, collector) -> {
          for(Integer i: integers) {
            collector.collect(i);
          }
        })
        .addSink(new CollectSink());

    // execute
    env.execute("x");

    // verify your results
    System.out.println(CollectSink.values);
//    assertTrue(CollectSink.values.containsAll(list(2L, 22L, 23L)));
  }

  @AllArgsConstructor
  public static class TestModel {
    int sequenceNum;
    String key;
  }

  // create a testing sink
  private static class CollectSink implements SinkFunction<Integer> {

    // must be static
    public static final List<Integer> values = new ArrayList<>();

    @Override
    public synchronized void invoke(Integer value) throws Exception {
      values.add(value);
    }
  }

  static class IntegerAgg implements AggregateFunction<Integer, List<Integer>, List<Integer>>{
    @Override
    public List<Integer> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<Integer> add(Integer testModel, List<Integer> testModels) {
      testModels.add(testModel);
      return testModels;
    }

    @Override
    public List<Integer> getResult(List<Integer> testModels) {
      return testModels;

    }

    @Override
    public List<Integer> merge(List<Integer> testModels, List<Integer> acc1) {
      testModels.addAll(acc1);
      return testModels;
    }
  }

  public <T> List list(T ... elems) {
    return Arrays.asList(elems);
  }
}
