package driving.job;

import driving.job.Driver.DrivingBuilder;
import driving.model.DriveEvent;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import util.JsonUtils;

public class DriverTest {
  @Test
  public void produceKafka() throws InterruptedException {
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

    for (DriveEvent i: fastFinishDriver("joanne", "집" + UUID.randomUUID())) {
      String message = JsonUtils.writeAsString(i);
      System.out.println(message);
      producer.send(new ProducerRecord<>("input-topic", message));

      Thread.sleep(100);
    }

    for (int i = 0; i < 100; i++) {
      for (DriveEvent event: randomDriver()) {
        producer.send(new ProducerRecord<>("input-topic", JsonUtils.writeAsString(event)));
      }
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

  private static Random rand = new Random();
  public static List<DriveEvent> randomDriver() {
    int i = rand.nextInt();
    if (i % 4 == 0) {
      return DriverTest.normalDriver(UUID.randomUUID().toString(), "normal");
    } else if (i % 4 == 1) {
      return DriverTest.suddenStopDriver(UUID.randomUUID().toString(), "suddenStop");
    } else if (i % 4 == 2) {
      return DriverTest.fastStartDriver(UUID.randomUUID().toString(), "fastStart");
    } else {
      return DriverTest.fastFinishDriver(UUID.randomUUID().toString(), "fastFinish");
    }
  }

  public static List<DriveEvent> normalDriver(String userId, String destination) {
    Driver driver = new DrivingBuilder(userId, destination)
        .drive(DriveCoordinateGenerator.normalAccDrive())
        .drive(DriveCoordinateGenerator.normalDrive(rand.nextInt() % 10))
        .drive(DriveCoordinateGenerator.normalDecelDrive())
        .arrived();

    return driver.driveEvents;
  }

  public static List<DriveEvent> fastStartDriver(String userId, String destination) {
    Driver driver = new DrivingBuilder(userId, destination)
        .drive(DriveCoordinateGenerator.rapidAccDrive())
        .drive(DriveCoordinateGenerator.normalDrive(rand.nextInt() % 20))
        .drive(DriveCoordinateGenerator.normalDecelDrive())
        .arrived();

    return driver.driveEvents;
  }

  public static List<DriveEvent> fastFinishDriver(String userId, String destination) {
    Driver driver = new DrivingBuilder(userId, destination)
        .drive(DriveCoordinateGenerator.rapidAccDrive())
        .drive(DriveCoordinateGenerator.normalDrive(rand.nextInt() % 20))
        .drive(DriveCoordinateGenerator.rapidDecelDrive())
        .arrived();

    return driver.driveEvents;
  }

  public static List<DriveEvent> suddenStopDriver(String userId, String destination) {
    Driver driver = new DrivingBuilder(userId, destination)
        .drive(DriveCoordinateGenerator.normalAccDrive())
        .drive(DriveCoordinateGenerator.normalDrive(rand.nextInt() % 10))
        .drive(DriveCoordinateGenerator.rapidDecelDrive())
        .drive(DriveCoordinateGenerator.normalAccDrive())
        .drive(DriveCoordinateGenerator.normalDrive(rand.nextInt() % 10))
        .drive(DriveCoordinateGenerator.normalDecelDrive())
        .arrived();

    return driver.driveEvents;
  }
}
