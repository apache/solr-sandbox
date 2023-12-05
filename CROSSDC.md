# Apache Solr CrossDC Module Documentation

Solr Cross DC is a simple cross-data-center fail-over solution for Apache Solr. It has three key components: the CrossDC Producer, the CrossDC Consumer, and Apache Kafka. The Producer is a Solr UpdateProcessor plugin that forwards updates from the primary data center, while the Consumer is an update request consumer application that receives updates in the backup data center. Kafka is the distributed queue that connects the two.

## Overview

Apache Solr CrossDC is a robust fail-over solution for Apache Solr, facilitating seamless replication of Solr updates across multiple data centers. Consisting of three integral components - the CrossDC Producer, the CrossDC Consumer, and Apache Kafka, it provides high availability and disaster recovery for your Solr clusters.

 * CrossDC Producer: a suite of Solr components responsible for intercepting Solr requests in the source Solr instance, and then sending them to a distributed queue:
   * `MirroringUpdateProcessor` plugin intercepts indexing updates,
   * `MirroringCollectionsHandler` plugin intercepts collection admin requests,
   * `MirroringConfigSetsHandler` plugin intercepts ConfigSet management requests.
 * CrossDC Consumer: a request consumer application that pulls mirrored requests from the distributed queue and forwards them to a Solr cluster in the backup data center.
 * Apache Kafka: A distributed queue system that links the Producer and Consumer.

## Setup Procedure

Implementing the Solr CrossDC involves the following steps:

1. Apache Kafka Cluster: Ensure the availability of an Apache Kafka cluster. This acts as the distributed queue interconnecting data centers.
2. CrossDC Solr Plugin: Install this plugin on each node in your Solr cluster (in both primary and backup data centers).
   1. Configure `solrconfig.xml` to reference the new `MirroringUpdateProcessor` and set it up for the Kafka cluster.
   2. Optionally configure `solr.xml` to use `MirroringCollectionsHandler` and `MirroringConfigSetsHandler` if necessary.
3. CrossDC Consumer Application: Install this application in the backup data center, then configure it for both the Kafka and Solr clusters.

### Detailed Configuration & Startup

#### CrossDC Producer Solr Plug-In

##### Mirroring Updates

1. Define the sharedLib directory in solr.xml and place the CrossDC producer plug-in jar file in this directory. 
    **solr.xml**

   ```xml
   <solr>
     <str name="sharedLib">${solr.sharedLib:}</str>
   ```
2. Add the new UpdateProcessor in solrconfig.xml.
    ```xml
       <updateRequestProcessorChain  name="mirrorUpdateChain" default="true">
       
         <processor class="org.apache.solr.update.processor.MirroringUpdateRequestProcessorFactory">
           <str name="bootstrapServers">${bootstrapServers:}</str>
           <str name="topicName">${topicName:}</str>
         </processor>
       
         <processor class="solr.LogUpdateProcessorFactory" />
         <processor class="solr.RunUpdateProcessorFactory" />
       </updateRequestProcessorChain>
       
3. Add an external version constraint UpdateProcessor to the update chain added to solrconfig.xml to accept user-provided update versions.
   See https://solr.apache.org/guide/8_11/update-request-processors.html#general-use-updateprocessorfactories and https://solr.apache.org/docs/8_1_1/solr-core/org/apache/solr/update/processor/DocBasedVersionConstraintsProcessor.html
4. Start or restart your Solr clusters.

##### Mirroring Collection Admin Requests
Add the following line to `solr.xml`:
```xml
<solr>
    <str name="collectionsHandler">org.apache.solr.handler.admin.MirroringCollectionsHandler</str>
...
</solr>
```

In addition to the general properties that determine distributed queue parameters, this handler supports the following properties:
* `mirror.collections` - comma-separated list of collections for which the admin commands will be mirrored. If this list is empty or the property is not set then admin commands for all collections will be mirrored.

##### Mirroring ConfigSet Admin Requests
Add the following line to `solr.xml`:
```xml
<solr>
    <str name="configSetsHandler">org.apache.solr.handler.admin.MirroringConfigSetsHandler</str>
...
</solr>
```

##### Configuration Properties for the CrossDC Producer:

The required configuration properties are:
- `bootstrapServers`: list of servers used to connect to the Kafka cluster
- `topicName`: Kafka topicName used to indicate which Kafka queue the Solr updates will be pushed on. This topic must already exist.

Optional configuration properties:
- `batchSizeBytes`: (integer) maximum batch size in bytes for the Kafka queue
- `bufferMemoryBytes`: (integer) memory allocated by the Producer in total for buffering 
- `lingerMs`: (integer) amount of time that the Producer will wait to add to a batch
- `requestTimeout`: (integer) request timeout for the Producer 
- `indexUnmirrorableDocs`: (boolean) if set to True, updates that are too large for the Kafka queue will still be indexed on the primary.
- `enableDataCompression`: (boolean) whether to use compression for data sent over the Kafka queue - can be none (default), gzip, snappy, lz4, or zstd
- `numRetries`: (integer) Setting a value greater than zero will cause the Producer to resend any record whose send fails with a potentially transient error.
- `retryBackoffMs`: (integer) The amount of time to wait before attempting to retry a failed request to a given topic partition.
- `deliveryTimeoutMS`: (integer) Updates sent to the Kafka queue will be failed before the number of retries has been exhausted if the timeout configured by delivery.timeout.ms expires first
- `maxRequestSizeBytes`: (integer) The maximum size of a Kafka queue request in bytes - limits the number of requests that will be sent over the queue in a single batch.
- `dqlTopicName`: (string) if not empty then requests that failed processing `maxAttempts` times will be sent to a "dead letter queue" topic in Kafka (must exist if configured).
- `mirrorCommits`: (boolean) if "true" then standalone commit requests will be mirrored, otherwise they will be processed only locally.
- `expandDbq`: (enum) if set to "expand" (default) then Delete-By-Query will be expanded before mirroring into series of Delete-By-Id, which may help with correct processing of out-of-order requests on the consumer side. If set to "none" then Delete-By-Query requests will be mirrored as-is.

#### CrossDC Consumer Application

1. Extract the CrossDC Consumer distribution file into an appropriate location in the backup data center.
2. Start the Consumer process using the included start script at bin/crossdc-consumer.
3. Configure the CrossDC Consumer with Java system properties using the CROSSDC_CONSUMER_OPTS environment variable.

##### API Endpoints
Currently the following endpoints are exposed (on local port configured using `port` property, default is 8090):
* `/metrics` - (GET) this endpoint returns JSON-formatted metrics describing various aspects of document processing in Consumer.
* `/threads` - (GET) returns a plain-text thread dump of the JVM running the Consumer application.

##### Configuration Properties for the CrossDC Consumer:

The required configuration properties are: 
- `bootstrapServers`: list of Kafka bootstrap servers.
- `topicName`: Kafka topicName used to indicate which Kafka queue the Solr updates will be pushed to. This can be a comma separated list for the Consumer if you would like to consume multiple topics.
- `zkConnectString`: Zookeeper connection string used to connect to Solr.

Optional configuration properties:
- `consumerProcessingThreads`: The number of threads used by the consumer to concurrently process updates from the Kafka queue.
- `port`: local port for the API endpoints. Default is 8090.

Optional configuration properties used when the consumer must retry by putting updates back on the Kafka queue:
- `batchSizeBytes`: maximum batch size in bytes for the Kafka queue
- `bufferMemoryBytes`: memory allocated by the Producer in total for buffering 
- `lingerMs`: amount of time that the Producer will wait to add to a batch
- `requestTimeout`: request timeout for the Producer 
- `maxPollIntervalMs`: the maximum delay between invocations of poll() when using consumer group management.

#### Central Configuration Option

Manage configuration centrally in Solr's Zookeeper cluster by placing a properties file called `crossdc.properties` in the root Solr
Zookeeper znode, eg, */solr/crossdc.properties*. The `bootstrapServers` and `topicName` properties can be included in this file. For
the Producer plugin, all of the crossdc configuration properties can be used here. For the CrossDC Consumer application you can also
configure all of the crossdc properties here, however you will need to set the `zkConnectString` as a system property to allow retrieving
the rest of the configuration from Zookeeper.

#### Disabling CrossDC via Configuration

To make the Cross DC UpdateProcessor optional in a common `solrconfig.xml`, use the enabled attribute. Setting it to false turns the processor into a NOOP in the chain.

## Limitations

- When `expandDbq` property is set to `expand` (default) then Delete-By-Query converts to a series of Delete-By-Id, which can be much less efficient for queries matching large numbers of documents. Setting this property to `none` results in forwarding a real Delete-By-Query - this reduces the amount of data to mirror but may cause different results due to the potential re-ordering of failed & re-submitted requests between Consumer and the target Solr.