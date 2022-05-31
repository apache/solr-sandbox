#!/bin/bash

kafkaBase="https://archive.apache.org/dist/kafka/2.8.1"
solrBase="https://dlcdn.apache.org/lucene/solr/8.11.1"

kafka="kafka_2.12-2.8.1"
solr="solr-8.11.1"


cd cluster

(
  cd "${kafka}" || exit

  bin/zookeeper-server-stop.sh config/zookeeper.properties
  bin/kafka-server-stop.sh config/server.properties
)

(
  cd "${solr}" || exit

  bin/solr stop -all
)