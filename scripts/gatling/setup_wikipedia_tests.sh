#!/usr/bin/env bash

function print_help {
    echo "Prepares a Solr environment to run Gatling simulations based on wikipedia data"
    echo "Options:"
    echo "  h: Print this help"
    echo "  s: The Solr endpoint to use. 'localhost:8983' is the default"
    echo "  m: The major version of Solr in use. i.e. 'main' or '9' or '8'..."
    echo "Example:"
    echo "scripts/gatling/setup_wikipedia_tests.sh -s http://localhost:8983"
}

SOLR_ENDPOINT="localhost:8983"
# This should be either "main" or a released major version ("9", "8", etc.)
SOLR_MAJOR_VERSION=${SOLR_MAJOR_VERSION:-main}

TESTS_WORK_DIR=${TESTS_WORK_DIR:-".gatling"}


while getopts ":s:m:h" opt; do
    case $opt in
        s)
            SOLR_ENDPOINT=$OPTARG
            ;;
        m)
            SOLR_MAJOR_VERSION=$OPTARG
            ;;
        h)
            print_help
            exit 1
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            exit 1
            ;;
        :)
            echo "Option -$OPTARG requires an argument." >&2
            exit 1
            ;;
    esac
done

echo "setting up sample wikipedia queries"
rm -f $TESTS_WORK_DIR/wikipedia-queries.txt
cp gatling-simulations/src/gatling/resources/wikipedia-queries.txt $TESTS_WORK_DIR/wikipedia-queries.txt

CONF="wikipedia-${SOLR_MAJOR_VERSION}"


echo "Pinging $SOLR_ENDPOINT"
curl -s "$SOLR_ENDPOINT/solr/admin/cores" > /dev/null

if [ $? -eq 0 ]; then
    echo "Solr endpoint OK"
else
    echo "Couldn't talk to $SOLR_ENDPOINT"
    exit 1
fi

set -e

if [ ! -d ./scripts ]; then
    echo "Run this script from the root of the project"
    exit 1
fi

mkdir -p $TESTS_WORK_DIR/results

if [ -e "$TESTS_WORK_DIR/batches" ]; then
    echo "Found uncompressed wikipedia batches locally."
else
  # TODO It's probably beyond the purposes of this script to download a wiki dump and preprocess it into
  #  batch-files for the caller, particularly since that involves some decision-making on their part
  #  (batch-size, max-text-size, data-format), but maybe we could let them specify a tarball URL of pre-
  #  prepared data that we can download and unpack?
    echo "Data-batches not found in ${TESTS_WORK_DIR}/batches; create batch files using the instructions in gatling-data-prep/README.md"
    exit 1
fi

echo "Uploading latest configset"
rm -f $TESTS_WORK_DIR/wikipedia-conf.zip
( cd gatling-simulations/src/gatling/resources/configSets/$CONF ; zip -r wikipedia-conf.zip * )
mv gatling-simulations/src/gatling/resources/configSets/$CONF/wikipedia-conf.zip $TESTS_WORK_DIR/wikipedia-conf.zip

curl -s -XPOST -T "$TESTS_WORK_DIR/wikipedia-conf.zip" -H "Content-Type: application/octet-stream" "$SOLR_ENDPOINT/solr/admin/configs?action=UPLOAD&name=wikipedia&overwrite=true&cleanup=true" && echo "...Configset uploaded"

echo "Done! This script doesn't create the Collection, that's expected to be done inside the simulation"
echo "  Example: ./gradlew gatlingRun --simulation index.IndexWikipediaBatchesSimulation"
