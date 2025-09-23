#!/bin/bash

BENCHMARK_DATA_BASE_URL="https://nightlies.apache.org/solr/benchmark-data"
WIKI_SOLR_FILE="solr-wiki-batches-5k-1k.tar.gz"

# Assumes running from the root of Solr-sandbox
function gatling_download_wiki_data() {
  local batches_path=".gatling/batches"
  mkdir -p $batches_path

  if [ "$(ls -A $batches_path | wc -l)" -eq "0" ]; then
    pushd $batches_path
      wget ${BENCHMARK_DATA_BASE_URL}/wiki/${WIKI_SOLR_FILE}
      tar -xvf ${WIKI_SOLR_FILE}
      rm $WIKI_SOLR_FILE
    popd
  else
    >&2 echo "Wiki data already downloaded; skipping..."
  fi
}
