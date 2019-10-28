package driving;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

import driving.model.DriveEvent;
import driving.model.DriveEvent.MessageType;
import driving.model.DriveSummary;
import driving.function.EarlyResultEventTimeTrigger;
import driving.function.StreamAggregator;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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
            log.info("seq: {}, time: {}", driveEvent.getMessageSequenceNumber(), currentTimestamp);
            return currentTimestamp;
          }
        }).name("BOOTimeExtractor")
        .keyBy(DriveEvent::transactionId)
        .window(EventTimeSessionWindows.withGap(Time.milliseconds(1000 * 60)))
        .trigger(new EarlyResultEventTimeTrigger<DriveEvent>() {
          @Override
          public boolean eval(DriveEvent element) {
            return element.getMessageType() == MessageType.END;
          }
        })
        .aggregate(new StreamAggregator<>()).name("StreamAggregator")
        .map(driveEvents ->
            DriveSummary.create(driveEvents.collect(Collectors.toList()))).name("DriveSummary")
        .filter(Objects::nonNull);
  }
}
