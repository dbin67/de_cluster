FROM base

# airflow 버전
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=2.6.1
ENV CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.9.txt

# airflow 설치
RUN pip3 install "apache-airflow[celery]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
RUN pip3 install psycopg2-binary redis

# third-party
RUN wget https://bitbucket.org/eunjeon/mecab-ko/downloads/mecab-0.996-ko-0.9.2.tar.gz

ENV MECAB_KO_FILENAME "mecab-0.996-ko-0.9.2"
ENV MECAB_KO_URL "https://bitbucket.org/eunjeon/mecab-ko/downloads/$MECAB_KO_FILENAME.tar.gz"

ENV MECAB_KO_DIC_FILENAME "mecab-ko-dic-2.1.1-20180720"
ENV MECAB_KO_DIC_URL "https://bitbucket.org/eunjeon/mecab-ko-dic/downloads/$MECAB_KO_DIC_FILENAME.tar.gz"

RUN wget $MECAB_KO_URL
RUN tar xvfz "$MECAB_KO_FILENAME.tar.gz"
WORKDIR $MECAB_KO_FILENAME
RUN ./configure --build=aarch64-unknown-linux-gnu
RUN make
RUN make check
RUN make install

WORKDIR /
RUN wget $MECAB_KO_DIC_URL
RUN tar xvfz "$MECAB_KO_DIC_FILENAME.tar.gz"
WORKDIR $MECAB_KO_DIC_FILENAME
RUN ./configure
RUN make
RUN make install

WORKDIR /
RUN pip3 install requests bs4 happybase python-mecab-ko
RUN rm -rf mecab-*

# [core]
# executor = CeleryExecutor
# load_examples = False

# [database]
# sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres/airflow

# [celery]
# broker_url = redis://:@redis:6379/0
# result_backend = db+postgresql://airflow:airflow@postgres/airflow

# airflow 설정 파일 추가
ADD conf/ ${AIRFLOW_HOME}

# airflow 데이터 디렉토리 생성
RUN mkdir -p ${AIRFLOW_HOME}/dags
RUN mkdir -p ${AIRFLOW_HOME}/logs
RUN mkdir -p ${AIRFLOW_HOME}/config
RUN mkdir -p ${AIRFLOW_HOME}/plugins

# scripts 복사 및 권한 설정
ADD scripts /scripts
RUN chmod -R a+x /scripts

# 8080 : airflow web ui
EXPOSE 8080
