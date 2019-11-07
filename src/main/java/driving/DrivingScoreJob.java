package driving;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

import driving.function.EarlyResultEventTimeTrigger;
import driving.function.StreamAggregator;
import driving.model.DriveEvent;
import driving.model.DriveEvent.MessageType;
import driving.model.DriveSummary;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
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
            return Timestamp.valueOf(driveEvent.getTimestamp()).getTime();
          }
        }).name("BOOTimeExtractor")
        .keyBy((KeySelector<DriveEvent, String>) DriveEvent::transactionId)
        .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
        .trigger(
            new EarlyResultEventTimeTrigger<DriveEvent>() {
              @Override
              public Optional<Long> eval(DriveEvent element) {
                return element.getMessageType() == MessageType.END? Optional.of(element.getMessageSequenceNumber()): Optional.empty();
              }
            }
        )
        .aggregate(new StreamAggregator<>()).name("StreamAggregator")
        .map(new MapFunction<Stream<DriveEvent>, Optional<DriveSummary>>() {
          @Override
          public Optional<DriveSummary> map(Stream<DriveEvent> driveEvents) throws Exception {
                return DriveSummary.createIfPerfect(driveEvents.collect(Collectors.toList()));
          }
        }).name("DriveSummary")
        .filter(Optional::isPresent)
        .map(x-> JsonUtils.writeAsString(x.get()));
  }
}
