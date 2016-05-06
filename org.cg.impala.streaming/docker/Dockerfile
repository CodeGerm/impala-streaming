FROM java:openjdk-8-jre

RUN mkdir -p /opt/compactionServer/config
RUN mkdir -p /opt/compactionServer/bin


ADD ./server-start.sh /opt/compactionServer/
ADD ./compaction-server /etc/init.d/
ADD ./org.cg.impala.streaming-0.0.1-SNAPSHOT.jar  /opt/compactionServer/bin/
ADD ./ImpalaJDBC41-2.5.28.jar  /opt/compactionServer/bin/
ADD ./compaction.conf /opt/compactionServer/config/
ADD ./properties.conf /opt/compactionServer/config/


