#!/bin/bash

# initialize database
airflow db init

# user create
airflow users create \
	--username admin \
	--password admin \
	--firstname admin \
	--lastname admin \
	--role Admin \
	--email email