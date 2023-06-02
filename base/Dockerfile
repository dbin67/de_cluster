FROM alpine

RUN apk update && apk upgrade --no-cache
RUN apk add --no-cache bash vim openjdk8 wget openssh-server openssh-client openssh-askpass

ENV JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk
ENV PATH="$PATH:$JAVA_HOME/bin"

RUN ssh-keygen -f /etc/ssh/ssh_host_rsa_key -t rsa -N "" \
	&& ssh-keygen -f /etc/ssh/ssh_host_ecdsa_key -t ecdsa -N "" \
	&& ssh-keygen -f /etc/ssh/ssh_host_ed25519_key -t ed25519 -N ""

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_dsa
RUN cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
