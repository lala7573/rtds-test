package driving.model;

import driving.model.DriveEvent.DriveCoordinate;
import driving.score.DrivingScore;
import driving.score.DrivingScoreCalculator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@NoArgsConstructor
public class DriveSummary {
  String userId;
  String destination;
  int size;
  List<DrivingScore> scores;

  public static DriveSummary create(List<DriveEvent> driveEvents) {
    log.info("DriveSummary DriveEvent.size {}", driveEvents.size());
//    for(DriveEvent x: driveEvents) {
//      log.info("" + x);
//    }
    DriveSummary drivingScore = new DriveSummary();

    DriveEvent first = driveEvents.get(0);
    drivingScore.userId = first.userId;
    drivingScore.destination = first.destination;
    drivingScore.size = driveEvents.size();
    drivingScore.scores = new ArrayList<>();

    List<DriveCoordinate> coordinates = new ArrayList<>();
    Collections.sort(driveEvents,
        Comparator.comparingInt(DriveEvent::getMessageSequenceNumber));

    for(DriveEvent driveEvent: driveEvents) {
      coordinates.addAll(driveEvent.coordinates);
    }

    for (DrivingScoreCalculator calculator: DrivingScoreCalculator.values()) {
      drivingScore.scores.add(calculator.getScore(coordinates));
    }

    return drivingScore;
  }
}
