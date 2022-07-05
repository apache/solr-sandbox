#!/bin/bash

pid="$$"

echo "pid=${pid}"

base="${PWD}/cluster"

kafkaBase="https://archive.apache.org/dist/kafka/2.8.1"
solrBase="https://dlcdn.apache.org/lucene/solr/8.11.1"

kafka="kafka_2.12-2.8.1"
solr="solr-8.11.1"

trap 'echo exittrap;cd ${base}/${kafka};bin/kafka-server-stop.sh config/server.properties;bin/zookeeper-server-stop.sh config/zookeeper.properties;cd ${base}/${solr};bin/solr stop -all;pkill -TERM -P ${pid}' EXIT

bash cluster.sh

echo "send update"
curl -X POST -d '{"add":{"doc":{"id":"1","text":"datalicious"},"commitWithin":10}}' -H "Content-Type: application/json" http://127.0.0.1:8983/solr/collection1/update

echo "stop cluster"
bash cluster-stop.sh