DOCKER_NETWORK =	my_cluster
IMAGES :=			base hadoop hbase airflow spark
CONTAINERS :=		datanode1 datanode2 datanode3 namenode resourcemanager \
					hregion1 hregion2 hmaster thrift \
					postgres redis \
					spark

all: build run

# 도커 네트워크 생성
create-network:
	@if [ -z "$(shell docker network ls | grep $(DOCKER_NETWORK))" ]; then \
		docker network create $(DOCKER_NETWORK); \
	fi

# 빌드
build: create-network
	@for image in $(IMAGES); do \
		docker inspect $$image >/dev/null 2>&1 || docker build -t $$image ./$$image; \
	done

# hadoop, hbase 컨테이너 실행
run: run_hadoop run_hbase run_airflow run_spark

# 각 컨테이너를 네트워크에 연결해 실행 -> 각 포트 매핑 및 호스트 이름 설정
run_hadoop:
	docker run -d -p 9862:9864 -p 8040:8042 -h datanode1 \
	--name datanode1 --network ${DOCKER_NETWORK} hadoop

	docker run -d -p 9863:9864 -p 8041:8042 -h datanode2 \
	--name datanode2 --network ${DOCKER_NETWORK} hadoop

	docker run -d -p 9864:9864 -p 8042:8042 -h datanode3 \
	--name datanode3 --network ${DOCKER_NETWORK} hadoop
	
	sleep 1

	docker run -d -p 9870:9870 -h namenode --name namenode \
	--network ${DOCKER_NETWORK} hadoop /scripts/namenode.sh

	docker run -d -p 8088:8088 -h resourcemanager --name resourcemanager \
	--network ${DOCKER_NETWORK} hadoop /scripts/resourcemanager.sh

# hbase 컨테이너를 네트워크에 연결해 실행 -> 각 포트 매핑 및 호스트 이름 설정
run_hbase:
	docker run -d -p 16030:16030 -p 16011:16010 -h hregion1\
	 --name hregion1 --network ${DOCKER_NETWORK} hbase

	docker run -d -p 16031:16030 -h hregion2 \
	--name hregion2 --network ${DOCKER_NETWORK} hbase

	sleep 1

	docker run -d -p 16010:16010 -h hmaster \
	--name hmaster --network ${DOCKER_NETWORK} hbase /scripts/hmaster.sh

	docker run -d -p 9090:9090 -h thrift \
	--name thrift --network ${DOCKER_NETWORK} hbase /scripts/thrift.sh

run_airflow:
	docker run -h postgres --name postgres -e POSTGRES_DB=airflow \
	-e POSTGRES_USER=airflow -e POSTGRES_PASSWORD=airflow -d --network ${DOCKER_NETWORK} postgres:13

	docker run -h redis --name redis -d --network ${DOCKER_NETWORK} redis

	docker run -d --rm --network ${DOCKER_NETWORK} airflow /scripts/init.sh

	mkdir -p airflow/source/config
	mkdir -p airflow/source/dags
	mkdir -p airflow/source/logs
	mkdir -p airflow/source/plugins
	
	sleep 1
	
	docker run -h scheduler --name scheduler -e TZ=UTC \
	-v $(shell pwd)/airflow/source/dags:/opt/airflow/config \
	-v $(shell pwd)/airflow/source/dags:/opt/airflow/dags \
	-v $(shell pwd)/airflow/source/dags:/opt/airflow/logs \
	-v $(shell pwd)/airflow/source/dags:/opt/airflow/plugins \
	-d --network ${DOCKER_NETWORK} airflow airflow scheduler

	docker run -h celery_worker1 --name celery_worker1 -e TZ=UTC \
	-v $(shell pwd)/airflow/source/dags:/opt/airflow/config \
	-v $(shell pwd)/airflow/source/dags:/opt/airflow/dags \
	-v $(shell pwd)/airflow/source/dags:/opt/airflow/logs \
	-v $(shell pwd)/airflow/source/dags:/opt/airflow/plugins \
	-d --network ${DOCKER_NETWORK} airflow airflow celery worker

	docker run -h celery_worker2 --name celery_worker2 -e TZ=UTC \
	-v $(shell pwd)/airflow/source/dags:/opt/airflow/config \
	-v $(shell pwd)/airflow/source/dags:/opt/airflow/dags \
	-v $(shell pwd)/airflow/source/dags:/opt/airflow/logs \
	-v $(shell pwd)/airflow/source/dags:/opt/airflow/plugins \
	-d --network ${DOCKER_NETWORK} airflow airflow celery worker
	
	docker run -p 8080:8080 -h webserver --name webserver -e TZ=UTC \
	-v $(shell pwd)/airflow/source/dags:/opt/airflow/config \
	-v $(shell pwd)/airflow/source/dags:/opt/airflow/dags \
	-v $(shell pwd)/airflow/source/dags:/opt/airflow/logs \
	-v $(shell pwd)/airflow/source/dags:/opt/airflow/plugins \
	-d --network ${DOCKER_NETWORK} airflow airflow webserver

run_spark:
	docker run -d -p 8081:8080 -p 18080:18080 -h spark \
	--name spark --network ${DOCKER_NETWORK} spark


# 시작
start:
	docker start ${CONTAINERS}

# 정지
stop:
	docker stop ${CONTAINERS}

# 컨테이너 삭제
clean: stop
	docker rm ${CONTAINERS}

# 이미지 삭제
fclean: clean
	docker rmi $(IMAGES)
