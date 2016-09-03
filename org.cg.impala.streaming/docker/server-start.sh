#!/bin/bash

set -e

MAIN_CLASS=org.cg.impala.streaming.server.ImpalaCompactionServer
BIN_DIR=/opt/compactionServer/bin
CONFIG=/opt/compactionServer/config/properties.conf
COMPACTION_CONF=/opt/compactionServer/config/compaction.conf

: ${service_port:?"Environment variables: service_port can't be empty"}
: ${impala_url:?"Environment variables: impala_url can't be empty"}
: ${tmp_table_dir:?"Environment variables: tmp_table_dir can't be empty"}
: ${hdfs_url:?"Environment variables: hdfs_url can't be empty"}

sed -i -- "s|{{service_port}}|$service_port|g" $CONFIG
sed -i -- "s|{{impala_url}}|$impala_url|g" $COMPACTION_CONF
sed -i -- "s|{{tmp_table_dir}}|$tmp_table_dir|g" $COMPACTION_CONF
sed -i -- "s|{{hdfs_url}}|$hdfs_url|g" $COMPACTION_CONF
sed -i -- "s|{{stateFilesDir}}|$stateFilesDir|g" $COMPACTION_CONF



ARGS=""

exec java $ARGS -cp "$BIN_DIR/*" $MAIN_CLASS $CONFIG