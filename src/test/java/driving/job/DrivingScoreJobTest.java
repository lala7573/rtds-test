package driving.job;

import static junit.framework.TestCase.assertTrue;

import driving.DrivingScoreJob;
import driving.model.DriveEvent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;
import util.JsonUtils;

@Slf4j
public class DrivingScoreJobTest {

  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberSlotsPerTaskManager(2)
              .setNumberTaskManagers(1)
              .build());


  @Test
  public void testDrivingScoreJob() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // configure your test environment
    env.setParallelism(2);

    // values are collected in a static variable
    CollectSink.values.clear();

    // create a stream of custom elements and apply transformations
    DataStream dataStream = env.addSource(new SourceFunction<String>() {

      @Override
      public void run(SourceContext<String> ctx) throws Exception {
        List<DriveEvent> joanne = DriverTest.suddenStopDriver("joanne", "H스퀘어");
        collectAsJson(ctx, joanne.subList(0, joanne.size()-1));
        collectAsJson(ctx, DriverTest.normalDriver("dana", "동은유치원"));

        while (CollectSink.values.stream().filter(x-> x.contains("joanne")).count() != 1) {
          collectAsJson(ctx, DriverTest.randomDriver());
        }

        Thread.sleep(5000);
      }

      public void collectAsJson(SourceContext<String> ctx, List<DriveEvent> events)
          throws InterruptedException {
        log.info("generated size: {} tid: {}", events.size(), events.get(0).transactionId());
        for (DriveEvent event: events) {
          ctx.collect(JsonUtils.writeAsString(event));
          Thread.sleep(1000);
//          break;
        }
      }

      @Override
      public void cancel() {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    DrivingScoreJob.process(dataStream).addSink(new CollectSink()).name("TestSink");

    System.out.println(env.getStreamTimeCharacteristic());
    env.execute("Hello, World!");

    // verify your results
    System.out.println("SINK Values-------");
    System.out.println(CollectSink.values);
    assertTrue(CollectSink.values.size() > 0);
  }

  // create a testing sink
  private static class CollectSink implements SinkFunction<String> {

    // must be static
    public static final ArrayList<String> values = new ArrayList<>();

    @Override
    public synchronized void invoke(String value) throws Exception {
      System.out.println(value);
      values.add(value);
    }
  }

  public <T> List list(T ... elems) {
    return Arrays.asList(elems);
  }
}