#!/bin/bash

# user create
airflow users create \
	--username admin \
	--password admin \
	--firstname admin \
	--lastname admin \
	--role Admin \
	--email email

# run webserver
airflow webserver