package driving.job;

import static junit.framework.TestCase.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

public class FlinkSampleTest {

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

    // values are collected in a static variable
    CollectSink.values.clear();

    // create a stream of custom elements and apply transformations
    env.fromElements(1L, 21L, 22L)
        .map(new IncrementMapFunction())
        .addSink(new CollectSink());

    // execute
    env.execute();

    // verify your results
    assertTrue(CollectSink.values.containsAll(list(2L, 22L, 23L)));
  }


  public static class IncrementMapFunction implements MapFunction<Long, Long> {

    @Override
    public Long map(Long record) throws Exception {
      return record + 1;
    }
  }


  // create a testing sink
  private static class CollectSink implements SinkFunction<Long> {

    // must be static
    public static final List<Long> values = new ArrayList<>();

    @Override
    public synchronized void invoke(Long value) throws Exception {
      values.add(value);
    }
  }

  public <T> List list(T ... elems) {
    return Arrays.asList(elems);
  }
}