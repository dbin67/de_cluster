FROM base

# Hbase 버전 및 설치 파일
ENV HBASE_VERSION=2.5.5
ENV HBASE_URL=http://apache.mirror.cdnetworks.com/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz

# Hbase 다운로드 및 압축 해제
RUN wget ${HBASE_URL}
RUN tar -xvzf hbase-${HBASE_VERSION}-bin.tar.gz && rm -rf hbase-${HBASE_VERSION}-bin.tar.gz

# Hbase 디렉토리 /opt  이동 및 환경 변수 설정
RUN mv hbase-${HBASE_VERSION} /opt
ENV HBASE_HOME=/opt/hbase-${HBASE_VERSION}
ENV HBASE_CONF_DIR=${HBASE_HOME}/conf
ENV PATH="${HBASE_HOME}/bin:$PATH"

ADD conf ${HBASE_CONF_DIR}

# scripts 복사 및 권한 설정
ADD scripts /scripts
RUN chmod -R a+x /scripts

# Hbase 환경 설정 파일 env.sh 에 java_home 경로 추가 
RUN echo 'export JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk' >> ${HBASE_CONF_DIR}/hbase-env.sh

# ssh 호스트 키 확인 비활성화
RUN echo 'StrictHostKeyChecking no' >> /etc/ssh/ssh_config


# 16000 : The port the HBase Master should bind to.
# 16010 : The port for the HBase Master web UI. Set to -1 if you do not want a UI instance run.

# 16020 : The port the HBase RegionServer binds to.
# 16030 : The port for the HBase RegionServer web UI Set to -1 if you do not want the RegionServer UI to run.

# 9090  : Thrift server
EXPOSE 16000 16010 16020 16030 9090

CMD ["/usr/sbin/sshd", "-D"]