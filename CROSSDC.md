# Solr Cross DC: Getting Started

Solr Cross DC is a simple cross-data-center fail-over solution for Apache Solr. It has three key components: the CrossDC Producer, the CrossDC Consumer, and Apache Kafka. The Producer is a Solr UpdateProcessor plugin that forwards updates from the primary data center, while the Consumer is an update request consumer application that receives updates in the backup data center. Kafka is the distributed queue that connects the two.

## Overview

Solr Cross DC is designed to provide a simple and reliable way to replicate Solr updates across multiple data centers. It is particularly useful for organizations that need to ensure high availability and disaster recovery for their Solr clusters.

The CrossDC Producer intercepts updates when the node acts as the leader and puts those updates onto the distributed queue. The CrossDC Consumer polls the distributed queue and forwards updates to the configured Solr cluster upon receiving the update requests.

## Getting Started

To use Solr Cross DC, follow these steps:

1. Startup or obtain access to an Apache Kafka cluster to provide the distributed queue between data centers.
2. Install the CrossDC Solr plugin on each node in your Solr cluster (in both primary and backup data centers). Place the jar in the sharedLib directory specified in solr.xml and configure solrconfig.xml to reference the new UpdateProcessor and configure it for the Kafka cluster.
3. Install the CrossDC consumer application in the backup data center and configure it for the Kafka cluster and the Solr cluster it will send consumed updates to.

### Configuration and Startup

#### Installing and Configuring the Cross DC Producer Solr Plug-In

1. Configure the sharedLib directory in solr.xml (e.g., sharedLIb=lib) and place the CrossDC producer plug-in jar file into the specified folder. 
    **solr.xml**

   ```xml
   <solr>
     <str name="sharedLib">${solr.sharedLib:}</str>
   ```
3. Configure the new UpdateProcessor in solrconfig.xml.
    ```xml
       <updateRequestProcessorChain  name="mirrorUpdateChain" default="true">
       
         <processor class="org.apache.solr.update.processor.MirroringUpdateRequestProcessorFactory">
           <str name="bootstrapServers">${bootstrapServers:}</str>
           <str name="topicName">${topicName:}</str>
         </processor>
       
         <processor class="solr.LogUpdateProcessorFactory" />
         <processor class="solr.RunUpdateProcessorFactory" />
       </updateRequestProcessorChain>
       ```
4. Add an external version constraint UpdateProcessor to the update chain added to solrconfig.xml to allow user-provided update versions.
   See https://solr.apache.org/guide/8_11/update-request-processors.html#general-use-updateprocessorfactories and https://solr.apache.org/docs/8_1_1/solr-core/org/apache/solr/update/processor/DocBasedVersionConstraintsProcessor.html
4. Start or restart the Solr cluster(s).

##### Configuration Properties

There are two configuration properties: 
- `bootstrapServers`: list of servers used to connect to the Kafka cluster
- `topicName`: Kafka topicName used to indicate which Kafka queue the Solr updates will be pushed on 

#### Installing and Configuring the CrossDC Consumer Application

1. Uncompress the distribution tar or zip file for the CrossDC Consumer into an appropriate install location on a node in the receiving data center.
2. Start the Consumer process via the included shell start script at bin/crossdc-consumer.
3. Configure the CrossDC Consumer via Java system properties pass in the CROSSDC_CONSUMER_OPTS environment variable.

The required configuration properties are: 
- `bootstrapServers`: list of servers used to connect to the Kafka cluster 
- `topicName`: Kafka topicName used to indicate which Kafka queue the Solr updates will be pushed to. This can be a comma separated list for the Consumer if you would like to consume multiple topics.
- `zkConnectString`: Zookeeper connection string used by Solr to connect to its Zookeeper cluster in the backup data center

The following additional configuration properties should either be specified for both the producer and the consumer or in the shared Zookeeper central config properties file:

- `batchSizeBytes`: maximum batch size in bytes for the queue
- `bufferMemoryBytes`: memory allocated by the Producer in total for buffering 
- `lingerMs`: amount of time that the Producer will wait to add to a batch
- `requestTimeout`: request timeout for the Producer 

#### Central Configuration Option

You can manage the configuration centrally in Solr's Zookeeper cluster by placing a properties file called *crossdc.properties* in the root Solr Zookeeper znode, eg, */solr/crossdc.properties*. Both *bootstrapServers* and *topicName* properties can be put in this file. For the CrossDC Consumer application, you would only have to set *zkConnectString* for the local Solr cluster.

#### Making the Cross DC UpdateProcessor Optional in a Common solrconfig.xml

Use the *enabled* attribute, false turns the processor into a NOOP in the chain.

## Limitations

- Delete-By-Query converts to DeleteById, which can be much less efficient for queries matching large numbers of documents.
  Forwarding a real Delete-By-Query could also be reasonable if it is not strictly reliant on not being reordered with other requests.

cluster.sh* script located in the root of the CrossDC repository. This script is a helpful developer tool for manual testing and it will download Solr and Kafka and then configure both for Cross DC.