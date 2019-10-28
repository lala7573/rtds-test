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
  List<DriveCoordinate> coordinates;
  List<DrivingScore> scores;

  public static DriveSummary create(List<DriveEvent> driveEvents) {
    log.info("size {}",driveEvents.size());
    for(DriveEvent x: driveEvents) {
      log.info("" + x);
    }
    DriveSummary drivingScore = new DriveSummary();

    DriveEvent first = driveEvents.get(0);
    drivingScore.userId = first.userId;
    drivingScore.destination = first.destination;
    drivingScore.coordinates = new ArrayList<>();
    drivingScore.scores = new ArrayList<>();

    Collections.sort(driveEvents,
        Comparator.comparingInt(DriveEvent::getMessageSequenceNumber));

    for (DriveEvent driveEvent: driveEvents) {
      drivingScore.coordinates.addAll(driveEvent.coordinates);
    }

    // TODO 왜 ???
    if (drivingScore.coordinates.isEmpty()) {
      return null;
    }

    for (DrivingScoreCalculator calculator: DrivingScoreCalculator.values()) {
      drivingScore.scores.add(calculator.getScore(drivingScore.coordinates));
    }

    return drivingScore;
  }
}
