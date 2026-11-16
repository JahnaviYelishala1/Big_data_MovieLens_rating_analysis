#!/bin/bash
set -Eeuo pipefail

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/opt/spark
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

log() {
  printf '[master] %s %s\n' "$(date --iso-8601=seconds)" "$*"
}

on_error() {
  local status=$?
  log "ERROR: command failed at line ${BASH_LINENO[0]} (exit ${status})."
  cleanup
  exit "$status"
}

cleanup() {
  log "Stopping Spark Master and NameNode."
  stop-master.sh || true
  hdfs --daemon stop namenode || true
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

name_dir=/hadoop/dfs/name
mkdir -p "$name_dir"

if [ -f "$name_dir/current/VERSION" ]; then
  log "Existing NameNode metadata found; leaving it unchanged."
elif find "$name_dir" -mindepth 1 -maxdepth 1 -print -quit | grep -q .; then
  log "ERROR: NameNode directory is not empty but has no current/VERSION metadata; refusing to format."
  exit 1
else
  log "Fresh NameNode volume detected; formatting it once."
  hdfs namenode -format -nonInteractive
fi

log "Starting Hadoop NameNode."
hdfs --daemon start namenode

name_node_ready=false
for attempt in $(seq 1 60); do
  if hdfs dfsadmin -report >/dev/null 2>&1 && nc -z master 9000; then
    name_node_ready=true
    break
  fi
  log "Waiting for HDFS NameNode readiness (${attempt}/60)."
  sleep 5
done
if [ "$name_node_ready" != true ]; then
  log "ERROR: HDFS NameNode did not become ready within 5 minutes."
  exit 1
fi
log "HDFS NameNode is ready."

log "Starting Spark Master."
start-master.sh
wait_for_port master 7077 "Spark Master RPC"
wait_for_port localhost 8080 "Spark Master UI"

log "NameNode and Spark Master started successfully."
while true; do
  if ! jps -l | grep -q 'org.apache.hadoop.hdfs.server.namenode.NameNode'; then
    log "ERROR: Hadoop NameNode process exited."
    exit 1
  fi
  if ! jps -l | grep -q 'org.apache.spark.deploy.master.Master'; then
    log "ERROR: Spark Master process exited."
    exit 1
  fi
  sleep 10
done
