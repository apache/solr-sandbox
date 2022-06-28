#!/bin/bash

pid="$$"

echo "pid=${pid}"

kafkaBase="https://archive.apache.org/dist/kafka/2.8.1"
solrBase="https://dlcdn.apache.org/lucene/solr/8.11.1"

kafka="kafka_2.12-2.8.1"
solr="solr-8.11.1"

base="${PWD}/cluster"

#trap 'echo exittrap;cd ${base}/${kafka};bin/zookeeper-server-stop.sh config/zookeeper.properties;bin/kafka-server-stop.sh config/server.properties;cd ${base}/${solr};bin/solr stop -all;pkill -TERM -P ${pid}' EXIT



if [ ! -d cluster ]
then
  mkdir cluster
fi

cd cluster || exit

if [ ! -f ${kafka}.tgz ]
then
  wget "${kafkaBase}/${kafka}.tgz"
fi

if [ ! -d ${kafka} ]
then
  tar -xvzf ${kafka}.tgz
fi

if [ ! -f ${solr}.tgz ]
then
  wget "${solrBase}/${solr}.tgz"
fi

if [ ! -d ${solr} ]
then
  tar -xvzf ${solr}.tgz
fi

(
  cd "${kafka}" || exit



sed -i "s|/tmp/kafka-logs|${PWD}/kafka_data/|" config/server.properties
sed -i "s|/tmp/zookeeper|${PWD}/zk_data|" config/zookeeper.properties


bin/zookeeper-server-start.sh config/zookeeper.properties > ../kafka_zk.log &

bin/kafka-server-start.sh config/server.properties > ../kafka_server.log &

# The following commented out  section is just for helpful reference

# for kafka 2.x zk port of 2181, for 3.x broker of 9093

bin/kafka-topics.sh --create --topic crossdc --bootstrap-server 127.0.0.1:9092 --partitions 1 --replication-factor 1

# bin/kafka-topics.sh --list --bootstrap-server 127.0.0.1:9093

# bin/kafka-console-producer.sh --broker-list 127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095 --topic my-kafka-topic

# bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9093 --topic my-kafka-topic --from-beginning

# bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9093 --topic my-kafka-topic --from-beginning --group group2
)

# need to go to lib folder - I can't believe there is no shared lib folder by default - crazy
mkdir "${solr}/server/solr/lib"

cp ../crossdc-producer/build/libs/crossdc-producer-*.jar "${solr}"/server/solr/lib

(
  cd "${solr}" || exit

  echo -e "SOLR_OPTS=\"$SOLR_OPTS -Dsolr.sharedLib=lib -DbootstrapServers=127.0.0.1:9092 -DtopicName=crossdc\"" >>  bin/solr.in.sh

  chmod +x  bin/solr

  bin/solr start -cloud > ../solr.log

  # for kafka 2.x ZK is on 2181, for Solr ZK is on 9983
  # for the moment we upload the config set used in crossdc-producer tests

  if [ ! -d "../../crossdc-producer/src/test/resources/configs/cloud-minimal/conf" ]
  then
    echo "Could not find configset folder to upload"
    exit 1
  fi
  bin/solr zk upconfig -z 127.0.0.1:9983 -n crossdc -d ../../crossdc-producer/src/test/resources/configs/cloud-minimal/conf

  bin/solr create -c collection1 -n crossdc

  bin/solr status
)

cp ../crossdc-consumer/build/distributions/crossdc-consumer-*.tar .

tar -xvf crossdc-consumer-*.tar
rm crossdc-consumer-*.tar

(
  cd crossdc-consumer* || exit
  CROSSDC_CONSUMER_OPTS="-Dlog4j2.configurationFile=../log4j2.xml -DbootstrapServers=127.0.0.1:9092 -DzkConnectString=127.0.0.1:9983 -DtopicName=crossdc" bin/crossdc-consumer > ../crossdc_consumer.log &
)
