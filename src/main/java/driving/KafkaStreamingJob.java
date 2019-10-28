package driving;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.representer.Representer;

@Slf4j
public class KafkaStreamingJob {

  public static void main(String[] args) throws Exception {
    if (args.length == 0 || "--help".equals(args[0]) || "-h".equals(args[0])) {
      printUsage();
      return;
    }

    Config config = Config.readFromYaml(args[0]);
    log.info(config.toString());

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    FlinkKafkaConsumer<String> consumer = getKafkaConsumer(config);
    FlinkKafkaProducer producer = getKafkaProducer(config);

    setFlinkSettings(env, config.flattenMap());
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    DataStream dataStream = env.addSource(consumer);
    DrivingScoreJob.process(dataStream).addSink(producer);

    env.execute("aa");
  }

  private static FlinkKafkaProducer getKafkaProducer(Config config) {
    Properties producerProps = new Properties();
    producerProps.putAll(config.producerConfig.config);
    producerProps.put("bootstrap.servers", config.producerConfig.bootstrapServers);
    producerProps.put("group.id", config.producerConfig.groupId);
    return new FlinkKafkaProducer<>(
        config.producerConfig.topic,
        new SimpleStringSchema(),
        producerProps);
  }

  private static FlinkKafkaConsumer<String> getKafkaConsumer(Config config) {
    Properties consumerProps = new Properties();
    consumerProps.putAll(config.consumerConfig.config);
    consumerProps.put("bootstrap.servers", config.consumerConfig.bootstrapServers);
    consumerProps.put("group.id", config.consumerConfig.groupId);
    return new FlinkKafkaConsumer<>(
        Pattern.compile(config.consumerConfig.topic),
        new SimpleStringSchema(),
        consumerProps);
  }

  static void printUsage() {
    System.out.println("Usage: [config.yaml]");
  }

  static void setFlinkSettings(StreamExecutionEnvironment env, Map<String, String> config) {
    ParameterTool parameter = ParameterTool.fromMap(config);
    env.getConfig().setGlobalJobParameters(parameter); // make parameters available in the web interface

//    env.getConfig().disableSysoutLogging();
//    env.getConfig().setLatencyTrackingInterval(1000);
    env.enableCheckpointing(1000 * 60 * 2); // create a checkpoint every 2 minutes
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
    env.getCheckpointConfig().setCheckpointTimeout(1000 * 60);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
  }


  public static class Config {
    KafkaConfig consumerConfig;
    KafkaConfig producerConfig;

    @Data
    public static class KafkaConfig {
      String bootstrapServers;
      String topic;
      String groupId;
      Integer parallelism;
      Map<String, String> config;
    }

    public static Config readFromYaml(String filename) throws FileNotFoundException {
      Representer representer = new Representer();
      representer.getPropertyUtils().setSkipMissingProperties(true);

      Yaml yaml = new Yaml(new Constructor(Config.class), representer);
      File file = new File(filename);
      InputStream inputStream = new FileInputStream(file);
      return yaml.load(inputStream);
    }

    public Map<String, String> flattenMap() throws IllegalAccessException {
      Map<String, String> map = new HashMap<>();

      for (Field field: consumerConfig.getClass().getDeclaredFields()) {
        map.put("consumerConfig." + field.getName(), field.get(consumerConfig).toString());
      }

      for (Field field: producerConfig.getClass().getDeclaredFields()) {
        map.put("producerConfig." + field.getName(), field.get(producerConfig).toString());
      }

      return map;
    }
  }
}
