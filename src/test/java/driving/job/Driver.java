package driving.job;

import com.google.common.collect.Lists;
import driving.model.DriveEvent;
import driving.model.DriveEvent.DriveCoordinate;
import driving.model.DriveEvent.MessageType;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Driver {
  final int chunkSize = 300;

  String userId;
  String destination;
  Counter counter;
  List<DriveEvent> driveEvents;

  private Driver(DrivingBuilder driverBuilder) {
    this.userId = driverBuilder.userId;
    this.destination = driverBuilder.destination;
    this.counter = driverBuilder.counter;

    setDriveEvents(driverBuilder.coordinates);
  }

  private void setDriveEvents(List<DriveCoordinate> coordinates) {
    LocalDateTime dateTime = LocalDateTime.now().minusSeconds(coordinates.size());
    this.driveEvents = new ArrayList<>();
    this.driveEvents.add(initDriveEvent(dateTime));

    List<List<DriveCoordinate>> chunkedList = Lists.partition(coordinates, chunkSize);
    for(List<DriveCoordinate> chunked: chunkedList.subList(0, chunkedList.size() - 1)) {
      dateTime = dateTime.plusSeconds(chunked.size());

      driveEvents.add(new DriveEvent(
          userId,
          destination,
          dateTime,
          MessageType.PERIODIC,
          counter.getCounter(),
          chunked
      ));
    }

    dateTime = dateTime.plusSeconds(chunkedList.get(chunkedList.size()-1).size());
    driveEvents.add(new DriveEvent(
        userId,
        destination,
        dateTime,
        MessageType.END,
        counter.getCounter(),
        chunkedList.get(chunkedList.size()-1)
    ));
  }

  private DriveEvent initDriveEvent(LocalDateTime lastDateTime) {
    return new DriveEvent(
        userId,
        destination,
        lastDateTime,
        MessageType.INIT,
        counter.getCounter(),
        new ArrayList<>()
    );
  }

  public static class DrivingBuilder {
    String userId;
    String destination;
    Counter counter;
    List<DriveCoordinate> coordinates;

    public DrivingBuilder(String userId, String destination) {
      this.userId = userId;
      this.destination = destination;
      this.counter = new Counter();
      this.coordinates = new ArrayList<>();
    }

    public DrivingBuilder drive(Stream<DriveCoordinate> driving) {
      coordinates.addAll(driving.collect(Collectors.toList()));
      return this;
    }

    public void drive(List<DriveCoordinate> driving) {
      coordinates.addAll(driving);
    }

    public Driver arrived() {
      return new Driver(this);
    }
  }

  static class Counter {
    private int counter = 1;

    int getCounter() {
      return counter ++;
    }
  }
}
