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

- Delete-By-Query is not officially supported.

    - Work-In-Progress: A non-efficient option to issue multiple delete by id queries using the results of a given standard query.

    - Simply forwarding a real Delete-By-Query could also be reasonable if it is not strictly reliant on not being reordered with other requests.



## Additional Notes

In these early days, it may help to reference the *cluster.sh* script located in the root of the CrossDC repository. This script is a helpful developer tool for manual testing and it will download Solr and Kafka and then configure both for Cross DC.