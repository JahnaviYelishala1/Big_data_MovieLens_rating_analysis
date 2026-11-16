FROM python:3.12-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    HADOOP_VERSION=3.4.2 \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    SPARK_VERSION=4.3.0 \
    SPARK_HOME=/opt/spark

ENV PATH="${JAVA_HOME}/bin:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        gnupg \
        openjdk-17-jdk \
        netcat-openbsd \
        procps \
        openssh-client \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt \
    && curl --fail --location --retry 3 "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o /tmp/hadoop.tar.gz \
    && tar -xzf /tmp/hadoop.tar.gz -C /opt \
    && test -d /opt/hadoop-${HADOOP_VERSION} \
    && mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} \
    && curl --fail --location --retry 3 "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && test -d /opt/spark-${SPARK_VERSION}-bin-hadoop3 \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} \
    && rm -f /tmp/hadoop.tar.gz /tmp/spark.tgz \
    && mkdir -p /opt/hadoop/logs /opt/spark/logs /hadoop/dfs/name /hadoop/dfs/data \
    && test -x "${JAVA_HOME}/bin/java" \
    && test -x "${HADOOP_HOME}/bin/hdfs" \
    && test -x "${SPARK_HOME}/bin/spark-submit" \
    && ln -sf ${SPARK_HOME}/conf /etc/spark

WORKDIR /opt
