package driving;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

import driving.function.EarlyResultEventTimeTrigger;
import driving.function.StreamAggregator;
import driving.model.DriveEvent;
import driving.model.DriveEvent.MessageType;
import driving.model.DriveSummary;
import java.sql.Timestamp;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import util.DateUtils;
import util.JsonUtils;

@Slf4j
public class DrivingScoreJob {

  public static SingleOutputStreamOperator process(DataStream<String> stream) {
    return stream
        .map(json -> JsonUtils.objectMapper.readValue(json, DriveEvent.class)).name("jsonParser")
        .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor<DriveEvent>(seconds(1)) {
          @Override
          public long extractTimestamp(DriveEvent driveEvent) {
            long currentTimestamp = Timestamp.valueOf(driveEvent.getTimestamp()).getTime();
            log.info("seq: {}, time: {}", driveEvent.getMessageSequenceNumber(), DateUtils.isoDateTime(currentTimestamp));
            return currentTimestamp;
          }
        }).name("BOOTimeExtractor")
        .keyBy(new KeySelector<DriveEvent, String>() {
          @Override
          public String getKey(DriveEvent driveEvent) throws Exception {
            return driveEvent.getUserId()+ "/" + driveEvent.getDestination();
          }
        })
        .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
        .trigger(
            new EarlyResultEventTimeTrigger<DriveEvent>() {
              @Override
              public boolean eval(DriveEvent element) {
                return element.getMessageType() == MessageType.END;
              }
            }
        )
        .aggregate(new StreamAggregator<>()).name("StreamAggregator")
        .map(driveEvents ->
            DriveSummary.create(driveEvents.collect(Collectors.toList()))).name("DriveSummary")
        .map(JsonUtils::writeAsString);
  }
}
