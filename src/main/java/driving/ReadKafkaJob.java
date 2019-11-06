package driving;

import driving.KafkaStreamingJob.Config;
import java.util.Properties;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;

public class ReadKafkaJob {

  public static void main(String[] args) throws Exception {
    final ParameterTool parameterTool = ParameterTool.fromArgs(args);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    env.getConfig().disableSysoutLogging();
    env.getConfig().setLatencyTrackingInterval(1000);
    env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 10000));
    env.enableCheckpointing(1000 * 10); // create a checkpoint every 1 seconds
    env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    DataStream dataStream = env.addSource(getConsumer(parameterTool)).name("KafkaConsumer");
    DrivingScoreJob.process(dataStream).addSink(getKafkaProducer(parameterTool));

    env.execute("ReadKafka");
  }

  public static FlinkKafkaConsumer getConsumer(ParameterTool parameterTool) {
    Properties props = parameterTool.getProperties();
    props.setProperty("auto.offset.reset", "latest");
    return new FlinkKafkaConsumer<>(
        parameterTool.getRequired("input-topic"),
        new SimpleStringSchema(),
        props);
  }

  public static FlinkKafkaProducer getKafkaProducer(ParameterTool parameterTool) {
    Properties props = parameterTool.getProperties();
//    producerProps.put("bootstrap.servers", config.producerConfig.bootstrapServers);
//    producerProps.put("group.id", config.producerConfig.groupId);
    return new FlinkKafkaProducer<>(
        parameterTool.getRequired("output-topic"),
        new SimpleStringSchema(),
        props);
  }
}
