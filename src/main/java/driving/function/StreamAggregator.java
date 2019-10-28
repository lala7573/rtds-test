package driving.function;

import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;

@Slf4j
public class StreamAggregator<T> implements AggregateFunction<T, Stream<T>, Stream<T>> {

  @Override
  public Stream<T> createAccumulator() {
    return Stream.empty();
  }

  @Override
  public Stream<T> add(T t, Stream<T> tStream) {
    return Stream.concat(tStream, Stream.of(t));
  }

  @Override
  public Stream<T> getResult(Stream<T> tStream) {
    return tStream;
  }

  @Override
  public Stream<T> merge(Stream<T> tStream, Stream<T> acc1) {
    return Stream.concat(tStream, acc1);
  }
}