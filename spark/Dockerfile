FROM hadoop

# spark 버전 및 설치파일
ENV SPARK_VERSION=3.4.1
ENV SPARK_URL=https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz

# spark 다운로드 및 압축해제
RUN wget --quiet -O spark.tgz ${SPARK_URL}
RUN tar -xf spark.tgz && rm -rf spark.tgz

# spark 디렉토리 이동 및 환경 변수 설정
RUN mv spark-${SPARK_VERSION}-bin-hadoop3 /opt
ENV SPARK_HOME=/opt/spark-${SPARK_VERSION}-bin-hadoop3
ENV PATH="${SPARK_HOME}/bin:$PATH"

# spark 설정 파일 추가
ADD conf/spark ${SPARK_HOME}/conf

# spark 로그 데이터 디렉토리 생성
RUN mkdir /opt/spark-${SPARK_VERSION}-bin-hadoop3/logs

# zeppelin 설치
ENV ZEPPELIN_VERSION=0.10.1
ENV ZEPPELIN_FILE_NAME=zeppelin-$ZEPPELIN_VERSION-bin-all

RUN wget https://dlcdn.apache.org/zeppelin/zeppelin-$ZEPPELIN_VERSION/$ZEPPELIN_FILE_NAME.tgz

RUN tar -xvzf $ZEPPELIN_FILE_NAME.tgz && rm $ZEPPELIN_FILE_NAME.tgz

ADD conf/zeppelin /$ZEPPELIN_FILE_NAME/conf

# scripts 복사 및 권한 설정
ADD scripts /scripts
RUN chmod -R a+x /scripts


# 8080 : zeppelin web UI port
# 18080 : spark history web UI port

EXPOSE 8080 18080

CMD ["/scripts/start.sh"]