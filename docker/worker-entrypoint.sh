#!/bin/bash
set -Eeuo pipefail

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/opt/spark
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

log() {
  printf '[%s] %s %s\n' "${HOSTNAME:-worker}" "$(date --iso-8601=seconds)" "$*"
}

on_error() {
  local status=$?
  log "ERROR: command failed at line ${BASH_LINENO[0]} (exit ${status})."
  cleanup
  exit "$status"
}

cleanup() {
  log "Stopping Spark Worker and DataNode."
  stop-worker.sh || true
  hdfs --daemon stop datanode || true
}

shutdown() {
  cleanup
  exit 0
}

trap on_error ERR
trap shutdown TERM INT

wait_for_port() {
  local host=$1 port=$2 label=$3
  for attempt in $(seq 1 60); do
    if nc -z "$host" "$port"; then
      log "$label is reachable at ${host}:${port}."
      return 0
    fi
    log "Waiting for $label at ${host}:${port} (${attempt}/60)."
    sleep 5
  done
  log "ERROR: timed out waiting for $label at ${host}:${port}."
  return 1
}

mkdir -p /hadoop/dfs/data
wait_for_port master 9000 "HDFS NameNode RPC"
wait_for_port master 7077 "Spark Master RPC"

log "Starting Hadoop DataNode."
hdfs --daemon start datanode

datanode_registered=false
for attempt in $(seq 1 60); do
  if hdfs dfsadmin -report 2>/dev/null | grep -Fq "Hostname: ${HOSTNAME}"; then
    datanode_registered=true
    break
  fi
  log "Waiting for DataNode registration with HDFS (${attempt}/60)."
  sleep 5
done
if [ "$datanode_registered" != true ]; then
  log "ERROR: DataNode ${HOSTNAME} did not register within 5 minutes."
  exit 1
fi
log "DataNode ${HOSTNAME} registered with HDFS."

log "Starting Spark Worker at spark://master:7077."
start-worker.sh spark://master:7077
wait_for_port localhost 8081 "Spark Worker UI"

log "DataNode and Spark Worker started successfully."
while true; do
  if ! jps -l | grep -q 'org.apache.hadoop.hdfs.server.datanode.DataNode'; then
    log "ERROR: Hadoop DataNode process exited."
    exit 1
  fi
  if ! jps -l | grep -q 'org.apache.spark.deploy.worker.Worker'; then
    log "ERROR: Spark Worker process exited."
    exit 1
  fi
  sleep 10
done
