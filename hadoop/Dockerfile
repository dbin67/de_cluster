FROM base

ENV HADOOP_VERSION=3.3.5
ENV HADOOP_URL=https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz

RUN wget --quiet ${HADOOP_URL}
RUN tar -xvzf hadoop-${HADOOP_VERSION}.tar.gz && rm -rf hadoop-${HADOOP_VERSION}.tar.gz

RUN mv hadoop-${HADOOP_VERSION} /opt
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV PATH="${HADOOP_HOME}/bin:$PATH"

ENV HADOOP_PID_DIR=${HADOOP_HOME}/
ENV HDFS_NAMENODE_USER="root"
ENV HDFS_DATANODE_USER="root"
ENV HDFS_SECONDARYNAMENODE_USER="root"
ENV YARN_RESOURCEMANAGER_USER="root"
ENV YARN_NODEMANAGER_USER="root"

ADD core-site.xml ${HADOOP_CONF_DIR}
ADD yarn-site.xml ${HADOOP_CONF_DIR}
ADD mapred-site.xml ${HADOOP_CONF_DIR}
ADD hdfs-site.xml ${HADOOP_CONF_DIR}
ADD workers ${HADOOP_CONF_DIR}

RUN mkdir -p $HADOOP_HOME/dfs/data
RUN mkdir -p $HADOOP_HOME/yarn/data

RUN echo 'export JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk' >> ${HADOOP_CONF_DIR}/hadoop-env.sh
RUN echo 'export HDFS_NAMENODE_USER="root"' >> ${HADOOP_CONF_DIR}/hadoop-env.sh
RUN echo 'export HDFS_DATANODE_USER="root"' >> ${HADOOP_CONF_DIR}/hadoop-env.sh
RUN echo 'export HDFS_SECONDARYNAMENODE_USER="root"' >> ${HADOOP_CONF_DIR}/hadoop-env.sh
RUN echo 'export YARN_RESOURCEMANAGER_USER="root"' >> ${HADOOP_CONF_DIR}/hadoop-env.sh
RUN echo 'export YARN_NODEMANAGER_USER="root"' >> ${HADOOP_CONF_DIR}/hadoop-env.sh

ADD namenode.sh /namenode.sh
RUN chmod a+x /namenode.sh

ADD resourcemanager.sh /resourcemanager.sh
RUN chmod a+x /resourcemanager.sh

# 9864 : datanode web UI port
# 9866 : port for data transfer
# 8042 : yarn nodemanager webapp

# 9000 : IPC port (namenode <> datanode 통신을 위한 포트)
# 9870 : NameNode HTTP web ui

# 8088 : yarn resource manager web ui
# 8030 : yarn scheduler interface
# 8032 : yarn resource manager IPC port
EXPOSE 9864 9866 8042 9000 9870 8088 8030 8031 8032

CMD ["/usr/sbin/sshd", "-D"]