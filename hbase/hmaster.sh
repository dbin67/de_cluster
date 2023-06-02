#!/bin/bash

# start ssh
/usr/sbin/sshd

# start hbase
$HBASE_HOME/bin/hbase-daemons.sh --config "${HBASE_CONF_DIR}" start zookeeper
$HBASE_HOME/bin/hbase-daemons.sh --config "${HBASE_CONF_DIR}" --hosts "${HBASE_REGIONSERVERS}" start regionserver
$HBASE_HOME/bin/hbase-daemons.sh --config "${HBASE_CONF_DIR}" --hosts "${HBASE_BACKUP_MASTERS}" start master-backup
$HBASE_HOME/bin/hbase-daemon.sh --config "${HBASE_CONF_DIR}" foreground_start master