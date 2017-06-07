#!/bin/bash

job_name=map-imsi-msisdn
executors=8
cores=2
executor_mem=3G
delete_checkpoint=1

jar_name=map-imsi-msisdn-1.0-SNAPSHOT.jar

export SPARK_1_6=spark-submit

MAIN_CLASS=vn.com.viettel.vtcc.ImsiMsisdnMapperApp


${SPARK_1_6} \
--jars $(echo lib/*.jar |tr ' ' ',') \
--class ${MAIN_CLASS} \
--name ${job_name} \
--master yarn-cluster \
--num-executors ${executors} \
--executor-memory ${executor_mem} \
--executor-cores ${cores} \
--driver-memory 3G \
--driver-cores 4 \
-queue default ${jar_name}