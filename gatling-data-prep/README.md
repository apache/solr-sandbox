# Gatling Data Preparation

Performance benchmarks, whether query or index based, rely on input data.
This module offers utilities to help fetch and prepare that data.
Currently the only supported data source is wikipedia, but other sources may be added over time.

# Wikipedia Data
## Download

Wikipedia offers "raw" dumps from https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2

Since Wikipedia content changes daily, some users may prefer to use an older, more static dump.
Interested users can explore the options available at https://dumps.wikimedia.org/

As of writing, the compressed dump is roughly ~25GB, and expands to ~75GB when extracted.

```
bunzip2 enwiki-latest-pages-articles.xml.bz2
```

## Solr-Documentification

Of course, the Wikipedia dumps must be converted into Solr documents before they can be ingested.

To generate data suitable for indexing into Solr, we will process the raw wikipedia XML into solr documents.
Currently we support outputting documents as either XML or JSON, though other formats (e.g. javabin) may be added over time.

First, compile the code we use to read the raw wikipedia archives.
```
./gradlew :gatling-data-prep:jar
```

Next, we will use this tool to convert the data:

```
java -cp gatling-data-prep/build/libs/data-prep.jar WikipediaXmlToSolr source-file output-dir data-type batch-size text-size
```

With the following arguments:

* `source-file` is the path to the previously unzipped Wikipedia dump
* `output-dir` is the path to a directory intended to contain the batch files (e.g. ".gatling/batches")
* `data-type` is one of `xml` or `json`, and determines the output format
* `batch-size` is an integer, and will control how many documents are saved per file. These files will later be uploaded to Solr as individual batches.
* `text-size` is an integer, and will truncate article text longer beyond a certain length. Lucene benchmarks run with both 1k and 4k truncations.

An example invocation might look like:

```
mkdir -p .gatling/batches
java -cp gatling-data-prep/build/libs/data-prep.jar WikipediaXmlToSolr ~/Downloads/wiki .gatling/batches enwiki-latest-pages-articles.xml json 5000 1000
```

This will generate json files with 5000 articles per file, and each article's text will be truncated to 1kb of data.
The files will be placed in the `.gatling/batches` directory (a "default" location used by the wikipedia simulations)
The wikipedia dump contains approximately 20M pages, so this output directory should contain a little over 4000 output files.
