package util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

public class DateUtils {
  public static LocalDateTime fromTimestamp(long timestamp) {
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), TimeZone.getDefault().toZoneId());
  }

  public static String isoDateTime(long timestamp) {
    return fromTimestamp(timestamp).format(DateTimeFormatter.ISO_DATE_TIME);
  }

}
