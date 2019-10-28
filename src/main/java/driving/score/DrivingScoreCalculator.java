package driving.score;

import driving.model.DriveEvent.DriveCoordinate;
import java.util.List;

public enum DrivingScoreCalculator {
  OVER_SPEED {
    @Override
    public DrivingScore getScore(List<DriveCoordinate> coordinates) {
      double speeding = 0;
      for (int i = 0; i < coordinates.size(); i ++) {
        DriveCoordinate curr = coordinates.get(i);

        if (curr.getDistancePerSec() < curr.getSpeedGuideInKMPerHour()) {
          speeding += 1;
        }
      }

      return new DrivingScore("overSpeed", speeding/(double)coordinates.size() * 100);
    }
  },
  RAPID_ACC {
    @Override
    public DrivingScore getScore(List<DriveCoordinate> coordinates) {
      double rapidAcc = 0;
      for (int i = 0; i < coordinates.size() - 1; i ++) {
        DriveCoordinate curr = coordinates.get(i);
        DriveCoordinate next = coordinates.get(i + 1);

        double acc = curr.getDistancePerSec() - next.getDistancePerSec();
        if (acc >= 10) {
          rapidAcc += 1;
        }
      }

      return new DrivingScore("rapidAcc", rapidAcc);
    }
  },
  RAPID_DECEL {
    @Override
    public DrivingScore getScore(List<DriveCoordinate> coordinates) {
      double rapidDecel = 0;
      for (int i = 0; i < coordinates.size() - 1; i ++) {
        DriveCoordinate curr = coordinates.get(i);
        DriveCoordinate next = coordinates.get(i + 1);

        double acc = curr.getDistancePerSec() - next.getDistancePerSec();
        if (acc < -10) {
          rapidDecel += 1;
        }
      }

      return new DrivingScore("rapidDecel", rapidDecel);
    }
  };


  public abstract DrivingScore getScore(List<DriveCoordinate> coordinates);
}
