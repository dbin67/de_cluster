#!/bin/bash

# start ssh
/usr/sbin/sshd

# start hbase
$HBASE_HOME/bin/hbase-daemon.sh --config "${HBASE_CONF_DIR}" foreground_start thrift