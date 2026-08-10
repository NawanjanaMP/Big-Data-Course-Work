# Flink 1.18 image extended with Python 3 + PyFlink + Kafka connector.
# The stock flink image is Java-only; PyFlink jobs need a Python runtime
# inside BOTH the JobManager and TaskManager containers.

FROM flink:1.18.1-scala_2.12-java11

# Python runtime for PyFlink
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir apache-flink==1.18.1

# Kafka connector (the "fat" sql-connector jar also serves DataStream jobs)
RUN wget -q -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar
