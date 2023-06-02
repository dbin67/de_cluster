FROM base

ENV HBASE_VERSION=2.5.4
ENV HBASE_URL=http://apache.mirror.cdnetworks.com/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz

RUN wget ${HBASE_URL}
RUN tar -xvzf hbase-${HBASE_VERSION}-bin.tar.gz && rm -rf hbase-${HBASE_VERSION}-bin.tar.gz

RUN mv hbase-${HBASE_VERSION} /opt
ENV HBASE_HOME=/opt/hbase-${HBASE_VERSION}
ENV HBASE_CONF_DIR=${HBASE_HOME}/conf
ENV PATH="${HBASE_HOME}/bin:$PATH"

ADD hbase-site.xml ${HBASE_CONF_DIR}
ADD regionservers ${HBASE_CONF_DIR}
ADD backup-masters ${HBASE_CONF_DIR}

RUN echo 'export JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk' >> ${HBASE_CONF_DIR}/hbase-env.sh
RUN echo 'StrictHostKeyChecking no' >> /etc/ssh/ssh_config

ADD hmaster.sh /hmaster.sh
RUN chmod a+x /hmaster.sh

ADD thrift.sh /thrift.sh
RUN chmod a+x /thrift.sh

# 16000 : The port the HBase Master should bind to.
# 16010 : The port for the HBase Master web UI. Set to -1 if you do not want a UI instance run.

# 16020 : The port the HBase RegionServer binds to.
# 16030 : The port for the HBase RegionServer web UI Set to -1 if you do not want the RegionServer UI to run.

# 9090  : Thrift server
EXPOSE 16000 16010 16020 16030 9090

CMD ["/usr/sbin/sshd", "-D"]