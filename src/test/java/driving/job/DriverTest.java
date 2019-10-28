package driving.job;

import driving.job.Driver.DrivingBuilder;
import driving.model.DriveEvent;
import java.util.List;
import org.junit.Test;

public class DriverTest {
  @Test
  public void test() {
    for(DriveEvent i: fastFinishDriver("joanne", "H스퀘어")) {
      System.out.println(i);
//      for (DriveCoordinate coordinate: i.getCoordinates()) {
//        System.out.println(coordinate);
//      }
    }
  }

  @Test
  public void test2() {
    System.out.println(1 ^ 2 ^ 3);
    System.out.println(1 ^ 3);

    System.out.println((1 ^ 2 ^ 3) ^ (1 ^ 3));
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
