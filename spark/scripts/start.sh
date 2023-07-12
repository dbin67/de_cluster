#!/bin/sh

# run zepplin server
/zeppelin-0.10.1-bin-all/bin/zeppelin-daemon.sh start

# run jupyter server
nohup jupyter lab --port 7777 --ip=0.0.0.0 --no-browser --allow-root &

# run spark history server
/opt/spark-3.4.1-bin-hadoop3/bin/spark-class org.apache.spark.deploy.history.HistoryServer

/usr/sbin/sshd -D