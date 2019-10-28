package driving.job;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import driving.model.DriveEvent;
import driving.model.DriveSummary;
import driving.DrivingScoreJob;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
        for (DriveEvent event: DriverTest.suddenStopDriver("joanne", "H스퀘어")) {
          System.out.println(JsonUtils.writeAsString(event));
          ctx.collect(JsonUtils.writeAsString(event));
//          Thread.sleep(1000 * 1);
        }

//        for (DriveEvent event: DriverTest.normalDriver("dana", "동은유치원")) {
//          System.out.println(JsonUtils.writeAsString(event));
//          ctx.collect(JsonUtils.writeAsString(event));
//        }

        while(CollectSink.values.size() == 0) {
          Thread.sleep(1000 * 5);
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

//        .addSink(new CollectDriveEvent());
    DrivingScoreJob.process(dataStream).addSink(new CollectSink()).name("TestSink");
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // execute
    env.execute("Hello, World!");

    // verify your results
    System.out.println(CollectSink.values);
    assertEquals(1, CollectSink.values.size());
//    assertTrue(CollectSink.values.containsAll(list(2L, 22L, 23L)));
  }

  // create a testing sink
  private static class CollectSink implements SinkFunction<DriveSummary> {

    // must be static
    public static final ArrayList<DriveSummary> values = new ArrayList<>();

    @Override
    public synchronized void invoke(DriveSummary value) throws Exception {
      System.out.println(JsonUtils.writeAsString(value));
      values.add(value);
    }
  }

  public <T> List list(T ... elems) {
    return Arrays.asList(elems);
  }
}