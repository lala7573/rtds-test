package driving.job;

import driving.job.Driver.DrivingBuilder;
import driving.model.DriveEvent;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import util.JsonUtils;

public class DriverTest {
  @Test
  public void produceKafka() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<>(props);

    for (DriveEvent i: fastFinishDriver("joanne", "집")) {
      String message = JsonUtils.writeAsString(i);
      producer.send(new ProducerRecord<>("input-topic", message));
    }


    producer.close();
  }
  @Test
  public void test() {
    for(DriveEvent i: fastFinishDriver("joanne", "H스퀘어")) {
      System.out.println(JsonUtils.writeAsString(i));
//      for (DriveCoordinate coordinate: i.getCoordinates()) {
//        System.out.println(coordinate);
//      }
    }
  }


  public static List<DriveEvent> normalDriver(String userId, String destination) {
    Driver driver = new DrivingBuilder(userId, destination)
        .drive(DriveCoordinateGenerator.normalAccDrive())
        .drive(DriveCoordinateGenerator.normalDrive(10))
        .drive(DriveCoordinateGenerator.normalDecelDrive())
        .arrived();

    return driver.driveEvents;
  }

  public static List<DriveEvent> fastStartDriver(String userId, String destination) {
    Driver driver = new DrivingBuilder(userId, destination)
        .drive(DriveCoordinateGenerator.rapidAccDrive())
        .drive(DriveCoordinateGenerator.normalDrive(10))
        .drive(DriveCoordinateGenerator.normalDecelDrive())
        .arrived();

    return driver.driveEvents;
  }

  public static List<DriveEvent> fastFinishDriver(String userId, String destination) {
    Driver driver = new DrivingBuilder(userId, destination)
        .drive(DriveCoordinateGenerator.rapidAccDrive())
        .drive(DriveCoordinateGenerator.normalDrive(10))
        .drive(DriveCoordinateGenerator.rapidDecelDrive())
        .arrived();

    return driver.driveEvents;
  }

  public static List<DriveEvent> suddenStopDriver(String userId, String destination) {
    Driver driver = new DrivingBuilder(userId, destination)
        .drive(DriveCoordinateGenerator.normalAccDrive())
        .drive(DriveCoordinateGenerator.normalDrive(5))
        .drive(DriveCoordinateGenerator.rapidDecelDrive())
        .drive(DriveCoordinateGenerator.normalAccDrive())
        .drive(DriveCoordinateGenerator.normalDrive(5))
        .drive(DriveCoordinateGenerator.normalDecelDrive())
        .arrived();

    return driver.driveEvents;
  }
}
