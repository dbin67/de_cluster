DOCKER_NETWORK =	my_cluster
IMAGES :=			base hadoop hbase
CONTAINERS :=		namenode datanode1 datanode2 datanode3 resourcemanager \
					hmaster hregion1 hregion2 thrift

all: build run

create-network:
	@if [ -z "$(shell docker network ls | grep $(DOCKER_NETWORK))" ]; then \
		docker network create $(DOCKER_NETWORK); \
	fi

build: create-network
	@for image in $(IMAGES); do \
		docker inspect $$image >/dev/null 2>&1 || docker build -t $$image ./$$image; \
	done

run: run_hadoop run_hbase

run_hadoop:
	docker run -d -p 9862:9864 -p 8040:8042 -h datanode1 --name datanode1 --network ${DOCKER_NETWORK} hadoop
	docker run -d -p 9863:9864 -p 8041:8042 -h datanode2 --name datanode2 --network ${DOCKER_NETWORK} hadoop
	docker run -d -p 9864:9864 -p 8042:8042 -h datanode3 --name datanode3 --network ${DOCKER_NETWORK} hadoop
	docker run -d -p 9870:9870 -h namenode --name namenode --network ${DOCKER_NETWORK} hadoop /namenode.sh
	docker run -d -p 8088:8088 -h resourcemanager --name resourcemanager --network ${DOCKER_NETWORK} hadoop /resourcemanager.sh

run_hbase:
	docker run -d -p 16030:16030 -p 16011:16010 -h hregion1 --name hregion1 --network ${DOCKER_NETWORK} hbase
	docker run -d -p 16031:16030 -h hregion2 --name hregion2 --network ${DOCKER_NETWORK} hbase
	docker run -d -p 16010:16010 -h hmaster --name hmaster --network ${DOCKER_NETWORK} hbase /hmaster.sh
	docker run -d -p 9090:9090 -h thrift --name thrift --network ${DOCKER_NETWORK} hbase /thrift.sh

stop:
	docker stop ${CONTAINERS}

clean: stop
	docker rm ${CONTAINERS}

fclean: clean
	docker rmi $(IMAGES)

# HADOOP
# need to fix workers core-site.xml, yarn-site.xml
build_hadoop:
	docker inspect base >/dev/null 2>&1 || docker build -t base ./base
	docker inspect hadoop >/dev/null 2>&1 || docker build -t hadoop ./hadoop

datanode: build_hadoop
	docker run -d -p 9864:9864 -p 8042:8042 --name datanode hadoop

namenode: build_hadoop
	docker run -d -p 9870:9870 -p 9000:9000 --name namenode hadoop /namenode.sh

resourcemanager: build_hadoop
	docker run -d -p 8088:8088 --name resourcemanager hadoop /resourcemanager.sh

# HBASE
# need to fix backup-masters, regionservers, hbase-site.xml
build_hbase:
	docker inspect base >/dev/null 2>&1 || docker build -t base ./base
	docker inspect hbase >/dev/null 2>&1 || docker build -t hbase ./hbase

hmaster: build_hbase
	docker run -d -p 16010:16010 --name hmaster hbase /hmaster.sh

hregion: build_hbase
	docker run -d -p 16030:16030 --name hregion hbase

hregion_with_backup: build_hbase
	docker run -d -p 16030:16030 -p 16010:16010 --name hregion hbase

thrift:
	docker run -d -p 9090:9090 --name thrift hbase /thrift.sh