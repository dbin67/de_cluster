#!/bin/bash

# start ssh
/usr/sbin/sshd

# start nodemanagers
$HADOOP_HOME/bin/yarn --workers --config $HADOOP_CONF_DIR --daemon start nodemanager

# start resourcemanager
$HADOOP_HOME/bin/yarn --config $HADOOP_CONF_DIR resourcemanager