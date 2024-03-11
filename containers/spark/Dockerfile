FROM deltaio/delta-docker:latest
# latest is at 1.0.0_3.0.0, 

# FROM deltaio/delta-docker:0.8.1_2.3.0
# https://github.com/delta-io/delta-docs/blob/main/static/quickstart_docker/0.8.1_2.3.0/Dockerfile

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    rsync && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ./containers/spark/requirements.txt ./
RUN pip install -r requirements.txt

COPY ./containers/spark/conf/spark-defaults.conf "$SPARK_HOME/conf/spark-defaults.conf"
COPY ./containers/spark/conf/log4j2.properties "$SPARK_HOME/conf/log4j2.properties"
COPY ./containers/spark/conf/metrics.properties "$SPARK_HOME/conf/metrics.properties"

# ENV SPARK_MASTER="spark://local-spark:7077"
# ENV SPARK_MASTER_HOST local-spark
# ENV SPARK_MASTER_PORT 7077
ENV SPARK_CONF_DIR="/opt/spark/conf"

# Create and event logging directory to store job logs
RUN mkdir /tmp/spark-events

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

ENTRYPOINT ["tail", "-f", "/dev/null"]