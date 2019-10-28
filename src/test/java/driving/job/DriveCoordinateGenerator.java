package driving.job;

import driving.model.DriveEvent.DriveCoordinate;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class DriveCoordinateGenerator {
  static int MINUTES = 60;
  static double speedGuide = 60;

  public static Stream<DriveCoordinate> normalDrive(int durationInMin) {
    return IntStream.range(0, durationInMin * 60)
        .mapToObj(x -> new DriveCoordinate(speedGuide, speedGuide));
  }

  public static Stream<DriveCoordinate> fastDrive(int durationInMin) {
    return IntStream.range(0, durationInMin * MINUTES)
        .mapToObj(x -> new DriveCoordinate(speedGuide + 20, speedGuide));

  }

  public static Stream<DriveCoordinate> randomDrive(int durationInMin) {
    Random random = new Random();
    return random.doubles(durationInMin * MINUTES, speedGuide - 5, speedGuide)
        .mapToObj(distance -> new DriveCoordinate(distance, speedGuide));
  }

  public static Stream<DriveCoordinate> normalAccDrive() {
    return IntStream.range(0, 12)
        .mapToObj(i -> new DriveCoordinate(Math.min(i * 5, 60), speedGuide));
  }

  public static Stream<DriveCoordinate> rapidAccDrive() {
    return IntStream.range(0, 3)
        .mapToObj(i -> new DriveCoordinate(Math.min(i * 20, 60), speedGuide));
  }

  public static Stream<DriveCoordinate> normalDecelDrive() {
    return IntStream.range(0, 13)
        .mapToObj(i -> new DriveCoordinate(Math.max(60 - i * 5, 0), speedGuide));
  }

  public static Stream<DriveCoordinate> rapidDecelDrive() {
    return IntStream.range(0, 4)
        .mapToObj(i -> new DriveCoordinate(Math.max(60 - i * 20, 0), speedGuide));
  }
}
