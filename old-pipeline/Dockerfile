FROM flink:1.18-scala_2.12

RUN apt-get update && apt-get install -y python3 python3-pip python3-dev \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install apache-flink==1.18.0 redis

# Đúng tên JAR cho Flink 1.18
RUN wget -q https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar \
    -P /opt/flink/lib/

COPY jobs/ /opt/flink/jobs/
COPY cameras_with_zones_merged.json /opt/flink/jobs/cameras_with_zones_merged.json
# cd /opt/flink
# bin/flink run -py jobs/edge_count_aggregator.py