#!/bin/bash

set -eu

# Usage: ./convert-mailing-lists-to-solr-docs.sh <mbox-data-directory> <solr-doc-output-dir>

if [[ -z ${1:-} ]]; then
  echo "'mbox-directory' argument is required but was not provided; exiting"
  exit 1
fi
if [[ -z ${2:-} ]]; then
  echo "'solr-doc-output-dir' argument is required but was not provided; exiting"
  exit 1
fi

MBOX_DIRECTORY=$1
SOLR_DOC_OUTPUT_DIRECTORY=$2

if [[ -d $SOLR_DOC_OUTPUT_DIRECTORY ]]; then
  echo "Output directory [$SOLR_DOC_OUTPUT_DIRECTORY] already exists; clearing it out and continuing..."
  rm -rf $SOLR_DOC_OUTPUT_DIRECTORY
fi
mkdir -p $SOLR_DOC_OUTPUT_DIRECTORY

for filepath in $(find $MBOX_DIRECTORY -name "*.mbox")
do
  python3 convert-mbox-to-solr-docs.py $filepath $SOLR_DOC_OUTPUT_DIRECTORY
done

echo "Solr documents now available in $SOLR_DOC_OUTPUT_DIRECTORY; use Solr's 'bin/post' to upload as desired!"
