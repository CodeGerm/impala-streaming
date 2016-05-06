#!/bin/bash

MAIN_CLASS=org.cg.impala.streaming.server.ImpalaCompactionServer
BIN_DIR=/opt/compactionServer/bin
CONFIG=/opt/compactionServer/config/properties.conf

ARGS=""

exec java $ARGS -cp "$BIN_DIR/*" $MAIN_CLASS $CONFIG