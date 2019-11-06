# Driving Score

## Build
```
mvn clean package -DskipTests 
```

## Execute
```
flink run target/job.jar --bootstrap.servers localhost:9092 --group.id test --input-topic input-topic --output-topic output-topic
```

## Test set
[produce driver testset](./src/test/java/driving/job/DriverTest.java#L15)'