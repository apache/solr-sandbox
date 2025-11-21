# Gatling Simulations

This module supports running performance benchmarks packaged and run as "Gatling simulations".

## Quickstart using Pre-Processed Data

There is preprocessed data available at https://nightlies.apache.org/solr/benchmark-data/wiki/solr-wiki-batches-5k-1k.tar.gz.  This tgz contains ~5k Solr-ready JSON files, each containing a batch of 5k wikipedia articles truncated at 1k each.

1. Download pre-processed data:
  `mkdir -p .gatling/batches && cd .gatling/batches && wget https://nightlies.apache.org/solr/benchmark-data/wiki/solr-wiki-batches-5k-1k.tar.gz && tar -xvf solr-wiki-batches-5k-1k.tar.gz`
1. Start a local Solr - any Solr can be used: local or remote, Docker or baremetal, release or SNAPSHOT, etc. Benchmarking will assume `http://localhost:8983/solr` unless told otherwise.
1. Install wiki configset to Solr:
  `./scripts/gatling/setup_wikipedia_tests.sh`.
1. Run benchmark:
  `NUM_SHARDS=2 ./gradlew gatlingRun --simulation index.IndexWikipediaBatchesSimulation`.

## Supported Simulations

Currently only a single simulation is supported, "IndexWikipediaBatchesSimulation", which provides a benchmark for indexing truncated wikipedia pages.
(This benchmark relies on having processed data available to index.
See [the data-prep README](../gatling-data-prep/README.md) for details on creating the necessary data.)
This benchmark can be can be configured using the environment-variable knobs below:

- `TESTS_WORK_DIR` - used to locate Wikipedia data (defaults to: `$REPOSITORY_ROOT/.gatling`)
- `BATCH_CONTENT_TYPE` - the format of the preprocessed Wikipedia data. Options are `application/json` or `application/xml` (defaults to: `application/json`)
- `CONCURRENT_USERS` - the number of threads used to index data to Solr (defaults to: 10)
- `endpoint` - a Solr URL to send documents to (defaults to: `"http://localhost:8983/solr"`)
- `COLLECTION_NAME` - the collection name to index data into, created by the simulation (defaults to: "wikipedia")
- `NUM_SHARDS` - the number of shards for the created collection (defaults to: 1)
- `NUM_REPLICAS` - the number of replicas for each shard of the created collection (defaults to: 1)

## Running Built-In Scenarios

Built-in indexing scenarios will create a collection and delete it after the test completes.  You still need to load your own configset first.

Indexing benchmarks may be run using the command below from the repository root

```
    NUM_SHARDS=2 ./gradlew gatlingRun  --simulation index.IndexWikipediaBatchesSimulation
```

Gatling will print basic statistics on `stdout`, but a more comprehensive (and human-friendly) HTML report is also available in `gatling-simulations/build/reports`
