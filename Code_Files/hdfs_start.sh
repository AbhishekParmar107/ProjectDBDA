#!/bin/bash

hdfs dfs -ls /

if [ $? -eq 1 ]
then
start-dfs.sh
fi

hdfs dfs -ls /SparkERIngest
if [ $? -eq 1 ]
then
hadoop dfsadmin -safemode leave
hdfs dfs -mkdir -p /SparkERIngest
fi



