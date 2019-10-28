################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
FROM maven:3.6.1-jdk-8-slim as build

ENV         APP_HOME        /app
WORKDIR     $APP_HOME

## Flink docker image를 만들고 거기서 땡겨온다?
## 개발 하는 동안은 naver mirror를 쓰자 (최신 release만 저장해두고 있음 -> 장점: 업데이트 소식을 알 수 있다. 단점: build 실패 자주 뜬다)
## https://www.apache.org/dyn/closer.lua/flink/flink-1.8.1/flink-1.8.1-bin-scala_2.11.tgz
RUN         curl --output /flink-1.8.1.tgz http://mirror.navercorp.com/apache/flink/flink-1.8.2/flink-1.8.2-bin-scala_2.12.tgz
RUN         tar xvfp /flink-1.8.1.tgz

ADD         pom.xml			$APP_HOME/
RUN         mvn install

ADD         .               $APP_HOME/
RUN         mvn package

FROM openjdk:8-jre-alpine

# Install requirements
RUN         apk update && apk add --no-cache bash snappy gcompat libc6-compat

# Flink environment variables
ENV         FLINK_INSTALL_PATH      /opt
ENV         FLINK_HOME              $FLINK_INSTALL_PATH/flink
ENV         FLINK_LIB_DIR           $FLINK_HOME/lib
ENV         PATH                    $PATH:$FLINK_HOME/bin

ENV         APP_HOME        /app
COPY        --from=build    $APP_HOME/flink-*         $FLINK_INSTALL_PATH/flink-dist/
COPY        --from=build    $APP_HOME/target/job.jar  $FLINK_INSTALL_PATH

RUN set -x && \
  ln -s $FLINK_INSTALL_PATH/flink-* $FLINK_HOME && \
  ln -s $FLINK_INSTALL_PATH/job.jar $FLINK_LIB_DIR && \
  addgroup -S flink && adduser -D -S -H -G flink -h $FLINK_HOME flink && \
  chown -R flink:flink $FLINK_INSTALL_PATH/flink-* && \
  chown -h flink:flink $FLINK_HOME && \
  cp $FLINK_HOME/opt/flink-metrics-*   $FLINK_LIB_DIR

COPY        docker-entrypoint.sh         /

USER flink
EXPOSE 8081 6123 9250
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["--help"]
