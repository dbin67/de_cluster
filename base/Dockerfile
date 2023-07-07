FROM alpine

# Alpine 패키지 업데이트 및 업그레이드
RUN apk update && apk upgrade --no-cache

# 필요 패키지 설치
RUN apk add --no-cache bash vim openjdk8 wget openssh-server procps findutils \
	openssh-client openssh-askpass build-base libffi-dev openssl-dev zlib-dev sqlite-dev


# JAVA_HOME 환경 변수 설정
ENV JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk
ENV PATH="$PATH:$JAVA_HOME/bin"

# python 버전 및 설치 파일
ENV PYTHON_VERSION=3.9.16
ENV PYTHON_URL=https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz

# python 다운로드 및 압축 해제
RUN wget $PYTHON_URL
RUN tar -xvzf Python-$PYTHON_VERSION.tgz && rm Python-$PYTHON_VERSION.tgz

# python 설치
RUN /Python-$PYTHON_VERSION/configure --enable-optimizations

# py3와 python사이의 심볼릭 링크 생성
RUN make install
RUN ln -s /usr/local/bin/python3.9 /usr/bin/python3
RUN ln -s /usr/local/bin/python3.9 /usr/bin/python
# -> 하는 이유 : /usr/bin/python 명령어로 python 실행하게 하려고 했음.
# 주피터 사용할꺼면 안해도 됨

# SSH 키 생성 및 설치
RUN ssh-keygen -f /etc/ssh/ssh_host_rsa_key -t rsa -N "" \
	&& ssh-keygen -f /etc/ssh/ssh_host_ecdsa_key -t ecdsa -N "" \
	&& ssh-keygen -f /etc/ssh/ssh_host_ed25519_key -t ed25519 -N ""

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_dsa
RUN cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys


# Alpine Linux 기반의 Docker 이미지에서 SSH키를 생성 및 사용