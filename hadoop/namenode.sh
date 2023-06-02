#!/bin/bash

# start ssh
/usr/sbin/sshd

# start namenode
if [ "$(ls -A ${HADOOP_HOME}/dfs/name)" ]; then
  echo "NameNode is already formatted."
else
  echo "Format NameNode."
  $HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR namenode -format -y
fi

# start datanodes
$HADOOP_HOME/bin/hdfs --workers --config $HADOOP_CONF_DIR --daemon start datanode

# start namenode
$HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR namenode