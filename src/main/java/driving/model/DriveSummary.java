package driving.model;

import driving.model.DriveEvent.DriveCoordinate;
import driving.model.DriveEvent.MessageType;
import driving.score.DrivingScore;
import driving.score.DrivingScoreCalculator;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@NoArgsConstructor
public class DriveSummary {
  String userId;
  String destination;
  LocalDateTime initDateTime;
  List<DrivingScore> scores;

  // for debug
  int size;
  public static boolean isPerfect(List<DriveEvent> driveEvents) {
    Optional<Long> endSeqNumOpt = driveEvents.stream().filter(x-> x.getMessageType()== MessageType.END).map(
        DriveEvent::getMessageSequenceNumber).findFirst();
    return endSeqNumOpt.filter(v -> driveEvents.size() == v).isPresent();
  }
  public static Optional<DriveSummary> createIfPerfect(List<DriveEvent> driveEvents) {
    log.info("DriveSummary isPerfect:{} tid:{}", isPerfect(driveEvents), driveEvents.get(0).transactionId());

    if (!isPerfect(driveEvents)) {
      return Optional.empty();
    }

    DriveSummary drivingScore = new DriveSummary();

    DriveEvent first = driveEvents.get(0);
    drivingScore.userId = first.userId;
    drivingScore.destination = first.destination;
    drivingScore.initDateTime = first.timestamp;
    drivingScore.size = driveEvents.size();
    drivingScore.scores = new ArrayList<>();

    List<DriveCoordinate> coordinates = new ArrayList<>();
    Collections.sort(driveEvents,
        Comparator.comparingLong(DriveEvent::getMessageSequenceNumber));

    for(DriveEvent driveEvent: driveEvents) {
      coordinates.addAll(driveEvent.coordinates);
    }

    for (DrivingScoreCalculator calculator: DrivingScoreCalculator.values()) {
      drivingScore.scores.add(calculator.getScore(coordinates));
    }

    return Optional.of(drivingScore);
  }
}
