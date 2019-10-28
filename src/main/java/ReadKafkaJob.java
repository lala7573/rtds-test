import java.util.Properties;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class ReadKafkaJob {

  public static void main(String[] args) throws Exception {
    final ParameterTool parameterTool = ParameterTool.fromArgs(args);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    env.getConfig().disableSysoutLogging();
    env.getConfig().setLatencyTrackingInterval(1000);
    env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 10000));
    env.enableCheckpointing(1000); // create a checkpoint every 1 seconds
    env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    Properties props = parameterTool.getProperties();
    props.setProperty("auto.offset.reset", "latest");
    env
        .addSource(
            new FlinkKafkaConsumer<>(
                parameterTool.getRequired("input-topic"),
                new SimpleStringSchema(),
                props))
            .name("KafkaConsumer").setParallelism(1)
        .print();

    env.execute("ReadKafka");
  }
}
