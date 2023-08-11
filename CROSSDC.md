# Apache Solr CrossDC Module Documentation

Solr Cross DC is a simple cross-data-center fail-over solution for Apache Solr. It has three key components: the CrossDC Producer, the CrossDC Consumer, and Apache Kafka. The Producer is a Solr UpdateProcessor plugin that forwards updates from the primary data center, while the Consumer is an update request consumer application that receives updates in the backup data center. Kafka is the distributed queue that connects the two.

## Overview

Apache Solr CrossDC is a robust fail-over solution for Apache Solr, facilitating seamless replication of Solr updates across multiple data centers. Consisting of three integral components - the CrossDC Producer, the CrossDC Consumer, and Apache Kafka, it provides high availability and disaster recovery for your Solr clusters.

 * CrossDC Producer: An UpdateProcessor plugin for Solr, it intercepts updates in the primary data center and dispatches them to a distributed queue.
 * CrossDC Consumer: An update request consumer application that pulls updates from the distributed queue and forwards them to a Solr cluster in the backup data center.
 * Apache Kafka: A distributed queue system that links the Producer and Consumer.

## Setup Procedure

Implementing the Solr CrossDC involves the following steps:

1. Apache Kafka Cluster: Ensure the availability of an Apache Kafka cluster. This acts as the distributed queue interconnecting data centers.
2. CrossDC Solr Plugin: Install this plugin on each node in your Solr cluster (in both primary and backup data centers). Configure solrconfig.xml to reference the new UpdateProcessor and set it up for the Kafka cluster.
3. CrossDC Consumer Application: Install this application in the backup data center, then configure it for both the Kafka and Solr clusters.

### Detailed Configuration & Startup

#### CrossDC Producer Solr Plug-In

1. Define the sharedLib directory in solr.xml and place the CrossDC producer plug-in jar file in this directory. 
    **solr.xml**

   ```xml
   <solr>
     <str name="sharedLib">${solr.sharedLib:}</str>
   ```
3. Add the new UpdateProcessor in solrconfig.xml.
    ```xml
       <updateRequestProcessorChain  name="mirrorUpdateChain" default="true">
       
         <processor class="org.apache.solr.update.processor.MirroringUpdateRequestProcessorFactory">
           <str name="bootstrapServers">${bootstrapServers:}</str>
           <str name="topicName">${topicName:}</str>
         </processor>
       
         <processor class="solr.LogUpdateProcessorFactory" />
         <processor class="solr.RunUpdateProcessorFactory" />
       </updateRequestProcessorChain>
       
4. Add an external version constraint UpdateProcessor to the update chain added to solrconfig.xml to accept user-provided update versions.
   See https://solr.apache.org/guide/8_11/update-request-processors.html#general-use-updateprocessorfactories and https://solr.apache.org/docs/8_1_1/solr-core/org/apache/solr/update/processor/DocBasedVersionConstraintsProcessor.html
5. Start or restart your Solr clusters.

##### Configuration Properties for the CrossDC Producer:

The required configuration properties are:
- `bootstrapServers`: list of servers used to connect to the Kafka cluster
- `topicName`: Kafka topicName used to indicate which Kafka queue the Solr updates will be pushed on

Optional configuration properties:
- `batchSizeBytes`: maximum batch size in bytes for the Kafka queue
- `bufferMemoryBytes`: memory allocated by the Producer in total for buffering 
- `lingerMs`: amount of time that the Producer will wait to add to a batch
- `requestTimeout`: request timeout for the Producer 
- `indexUnmirrorableDocs`: if set to True, updates that are too large for the Kafka queue will still be indexed on the primary.
- `enableDataCompression`: whether to use compression for data sent over the Kafka queue - can be none (default), gzip, snappy, lz4, or zstd
- `numRetries`: Setting a value greater than zero will cause the Producer to resend any record whose send fails with a potentially transient error.
- `retryBackoffMs`: The amount of time to wait before attempting to retry a failed request to a given topic partition.
- `deliveryTimeoutMS`: Updates sent to the Kafka queue will be failed before the number of retries has been exhausted if the timeout configured by delivery.timeout.ms expires first
- `maxRequestSizeBytes`: The maximum size of a Kafka queue request in bytes - limits the number of requests that will be sent over the queue in a single batch.

#### CrossDC Consumer Application

1. Extract the CrossDC Consumer distribution file into an appropriate location in the backup data center.
2. Start the Consumer process using the included start script at bin/crossdc-consumer.
3. Configure the CrossDC Consumer with Java system properties using the CROSSDC_CONSUMER_OPTS environment variable.

##### Configuration Properties for the CrossDC Consumer:

The required configuration properties are: 
- `bootstrapServers`: list of Kafka bootstrap servers.
- `topicName`: Kafka topicName used to indicate which Kafka queue the Solr updates will be pushed to. This can be a comma separated list for the Consumer if you would like to consume multiple topics.
- `zkConnectString`: Zookeeper connection string used to connect to Solr.

Optional configuration properties:
- `consumerProcessingThreads`: The number of threads used by the consumer to concurrently process updates from the Kafka queue.

Optional configuration properties used when the consumer must retry by putting updates back on the Kafka queue:
- `batchSizeBytes`: maximum batch size in bytes for the Kafka queue
- `bufferMemoryBytes`: memory allocated by the Producer in total for buffering 
- `lingerMs`: amount of time that the Producer will wait to add to a batch
- `requestTimeout`: request timeout for the Producer 
- `maxPollIntervalMs`: the maximum delay between invocations of poll() when using consumer group management.

#### Central Configuration Option

Manage configuration centrally in Solr's Zookeeper cluster by placing a properties file called crossdc.properties in the root Solr
Zookeeper znode, eg, */solr/crossdc.properties*. The bootstrapServers and topicName properties can be included in this file. For
the Producer plugin, all of the crossdc configuration properties can be used here. For the CrossDC Consumer application you can also
configure all of the crossdc properies here, however you will need to set the zkConnectString as a system property to allow retrieving
the rest of the configuration from Zookeeper.

#### Disabling CrossDC via Configuration

To make the Cross DC UpdateProcessor optional in a common solrconfig.xml, use the enabled attribute. Setting it to false turns the processor into a NOOP in the chain.

## Limitations

- Delete-By-Query converts to DeleteById, which can be much less efficient for queries matching large numbers of documents.
  Forwarding a real Delete-By-Query could also be a reasonable option to add if it is not strictly reliant on not being reordered with other requests.

