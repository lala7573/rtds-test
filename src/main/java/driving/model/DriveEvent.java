package driving.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@JsonSerialize
@NoArgsConstructor
@AllArgsConstructor
public class DriveEvent implements Serializable {
  String userId;
  String destination;

  @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
  LocalDateTime timestamp;
  MessageType messageType;
  int messageSequenceNumber;
  List<DriveCoordinate> coordinates;

  public String transactionId() {
    return this.userId + "/" + destination;
  }
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class DriveCoordinate implements Serializable {
    double distancePerSec;
    double speedGuideInKMPerHour;
  }

  public enum MessageType {
    INIT, PERIODIC, END;

    @JsonValue
    final String value() {
      return this.name();
    }
  }
}
