# Solr Cross DC: Getting Started

**A simple cross-data-center fail-over solution for Apache Solr.**



[TOC]

## Overview

The design for this feature involves three key components:

- A UpdateProccessor plugin for Solr to forward updates from the primary data center.
- An update request consumer application to receive updates in the backup data center.
- A distributed queue to connect the above two.

The UpdateProcessor plugin is called the CrossDC Producer, the consumer application is called the CrossDC Consumer, and the supported distributed queue application is Apache Kafka.

## Getting Started

To use Solr Cross DC, you must complete the following steps:

- Startup or obtain access to an Apache Kafka cluster to provide the distributed queue between data centers.
- Install the CrossDC Solr plugin on each of the nodes in your Solr cluster (in your primary and backup data centers) by placing the jar in the correct location and configuring solrconfig.xml to reference the new UpdateProcessor and then configure it for the Kafka cluster.
- Install the CrossDC consumer application in the backup data center and configure it for the Kafka cluster and the Solr cluster it will send consumed updates to.

The Solr UpdateProccessor plugin will intercept updates when the node acts as the leader and then put those updates onto the distributed queue. The CrossDC Consumer application will poll the distributed queue and forward updates on to the configured Solr cluster upon receiving the update requests.

### Configuration and Startup

The current configuration options are entirely minimal. Further configuration options will be added over time. At this early stage, some may also change.

#### Installing and Configuring the Cross DC Producer Solr Plug-In

1. Configure the sharedLib directory in solr.xml (eg sharedLIb=lib) and place the CrossDC producer plug-in jar file into the specified folder. It's not advisable to attempt to use the per SolrCore instance directory lib folder as you would have to duplicate the plug-in many times and manage it when creating new collections or adding replicas or shards.


**solr.xml**

   ```xml
   <solr>
     <str name="sharedLib">${solr.sharedLib:}</str>
   ```



2. Configure the new UpdateProcessor in solrconfig.xml

   **NOTE:** `The following is not the recommended configuration approach in production, see the information on central configuration below!`



**solrconfig.xml**

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

Notice that this update chain has been declared to be the default chain used.



##### Configuration Properties

There are two configuration properties. You can specify them directly, or use the above notation to allow them to specified via system property (generally configured for Solr in the bin/solr.in.sh file).

   ```
   bootstrapServers
   ```

The list of servers used to connect to the Kafka cluster, see https://kafka.apache.org/28/documentation.html#producerconfigs_bootstrap.servers

   ```
   topicName 
   ```

The Kafka topicName used to indicate which Kafka queue the Solr updates will be pushed on to.



3. Add an external version constraint UpdateProcessor to the update chain added to solrconfig.xml to allow user-provided update versions (as opposed to the two Solr clusters using the independently managed built-in versioning).

   https://solr.apache.org/guide/8_11/update-request-processors.html#general-use-updateprocessorfactories

   https://solr.apache.org/docs/8_1_1/solr-core/org/apache/solr/update/processor/DocBasedVersionConstraintsProcessor.html


4. Start or restart the Solr cluster(s).



#### Installing and Configuring the CrossDC Consumer Application

1. Uncompress the distribution tar or zip file for the CrossDC Consumer into an appropriate install location on a node in the receiving data center.
2. You can start the Consumer process via the included shell start script at bin/crossdc-consumer.
3. You can configure the CrossDC Consumer via Java system properties pass in the CROSSDC_CONSUMER_OPTS environment variable, i.e. CROSSDC_CONSUMER_OPTS="-DbootstrapServers=127.0.0.1:2181 -DzkConnectString=127.0.0.1:2181 -DtopicName=crossdc" bin/crossdc-consumer

The required configuration properties are:


   *bootstrapServers* - the list of servers used to connect to the Kafka cluster https://kafka.apache.org/28/documentation.html#producerconfigs_bootstrap.servers

   *topicName* - the Kafka topicName used to indicate which Kafka queue the Solr updates will be pushed to.

   *zkConnectString* - the Zookeeper connection string used by Solr to connect to its Zookeeper cluster in the backup data center

Additional configuration properties:

   *groupId* - the group id to give Kafka for the consumer, default to the empty string if not specified.

The following additional configuration properties should either be specified for both the producer and the consumer or in the shared Zookeeper
central config properties file. This is because the Consumer will use a Producer for retries.

   *batchSizeBytes* - the maximum batch size in bytes for the queue
   *bufferMemoryBytes* - the amount of memory in bytes allocated by the Producer in total for buffering 
   *lingerMs* - the amount of time that the Producer will wait to add to a batch
   *requestTimeout* - request timeout for the Producer - when used for the Consumers retry Producer, this should be less than the timeout that will cause the Consumer to be removed from the group for taking too long.
   *maxPollIntervalMs* - The maximum delay between invocations of poll() when using consumer group management.
#### Central Configuration Option

You can optionally manage the configuration centrally in Solr's Zookeeper cluster by placing a properties file called *crossdc.properties* in the root Solr Zookeeper znode, eg, */solr/crossdc.properties*.  This allows you to update the configuration in a central location rather than at each solrconfig.xml in each Solr node and also automatically deals with new Solr nodes or Consumers to come up without requiring additional configuration.



Both *bootstrapServers* and *topicName* properties can be put in this file, in which case you would not have to specify any Kafka configuration in the solrconfig.xml for the CrossDC Producer Solr plugin. Likewise, for the CrossDC Consumer application, you would only have to set *zkConnectString* for the local Solr cluster. Note that the two components will be looking in the Zookeeper clusters in their respective data center locations.

You can override the properties file location and znode name in Zookeeper using the system property *zkCrossDcPropsPath=/path/to/props_file_name.properties*

#### Making the Cross DC UpdateProcessor Optional in a Common solrconfig.xml

The simplest and least invasive way to control whether the Cross DC UpdateProcessor is on or off for a node is to configure the update chain it's used in to be the default chain or not via Solr's system property configuration syntax.  This syntax takes the form of ${*system_property_name*} and will be substituted with the value of that system property when the configuration is parsed. You can specify a default value using the following syntax: ${*system_property_name*:*default_value*}. You can use the same syntax and property name to specify whether or not the Cross DC UpdateRequestProcessor is enabled or not.

*Having a separate updateRequestProcessorChain avoids a lot of additional constraints you have to deal with or consider, now or in the future, when compared to forcing all Cross DC and non-Cross DC use down a single, required, common updateRequestProcessorChain.*

Further, any application consuming the configuration with no concern for enabling Cross DC will not be artificially limited in its ability to define, manage and use updateRequestProcessorChain's.

The following would enable a system property to safely and non invasively enable or disable Cross DC for a node:


```xml
<updateRequestProcessorChain  name="crossdcUpdateChain" default="${crossdcEnabled:false}">
  <processor class="org.apache.solr.update.processor.MirroringUpdateRequestProcessorFactory">
    <bool name="enabled">${enabled:false}</bool>
  </processor>
  <processor class="solr.LogUpdateProcessorFactory" />
  <processor class="solr.RunUpdateProcessorFactory" />
</updateRequestProcessorChain>
```



The above configuration would default to Cross DC being disabled with minimal impact to any non-Cross DC use, and Cross DC could be enabled by starting Solr with the system property crossdcEnabled=true.

The last chain to declare it's the default wins, so you can put this at the bottom of almost any existing solrconfig.xml to create an optional Cross DC path without having to audit, understand, adapt, or test existing non-Cross DC paths as other options call for.

The above is the simplest and least obtrusive way to manage an on/off switch for Cross DC.

**Note:** If your configuration already makes use of update handlers and/or updates independently specifying different updateRequestProcessorChains, your solution may end up a bit more sophisticated.



For situations where you do want to control and enforce a single updateRequestProcessorChain path for every consumer of the solrconfig.xml, it's enough to simply use the *enabled* attribute, turning the processor into a NOOP in the chain.



```xml
<updateRequestProcessorChain  name="crossdcUpdateChain">
  <processor class="org.apache.solr.update.processor.MirroringUpdateRequestProcessorFactory">
    <bool name="enabled">${enabled:false}</bool>
  </processor>
  <processor class="solr.LogUpdateProcessorFactory" />
  <processor class="solr.RunUpdateProcessorFactory" />
</updateRequestProcessorChain>
```



## Limitations

- Delete-By-Query is not officially supported, however, there are a number of options you can attempt to use if needed.
  Which option to use can be controlled with the update parameter "deleteMethod". A different default can be configured using CrossDC configuration parameter defaultDBQMethod. Just set it to the desired default value.
1. default: This is the method used if no deleteMethod parameter is provided, or if it's provided with the value "default". This method will use pagination to fetch documents matching the delete query. It fetches a subset of documents (currently 1000), and deletes them iteratively until all matching documents have been deleted.

2. convert_no_paging: If the deleteMethod parameter is provided with the value "convert_no_paging", the delete by query operation will convert the delete query into individual delete-by-id commands. This method fetches all documents that match the delete query at once (up to 10000), and then deletes each document one by one.

3. delete_by_query: If the deleteMethod parameter is provided with the value "delete_by_query", the delete by query operation will be performed as a single operation. It will delete all documents that match the delete query at once, both locally and in the mirror cluster (via Kafka).
   With this method, if the system goes down at a bad time, there are no gaurantees around the replaying of the dbq or the order that it happens in relative to other document updates or additions if replayed upon recovery.

4. delete_by_query_local: If the deleteMethod parameter is provided with the value "delete_by_query_local", the delete by query operation will be performed as a single operation, but only in the local cluster. No deletion will be mirrored in the other clusters.



## Additional Notes

In these early days, it may help to reference the *cluster.sh* script located in the root of the CrossDC repository. This script is a helpful developer tool for manual testing and it will download Solr and Kafka and then configure both for Cross DC.