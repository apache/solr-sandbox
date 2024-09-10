#!/bin/bash

set -eu

# Usage: ./convert-mailing-lists-to-solr-docs.sh [<mbox-data-directory>] [<solr-doc-output-dir>]
#   <mbox-data-directory> and <solr-doc-output-dir> are both optional, but
#   <mbox-data-directory> must be specified if <solr-doc-output-dir> is.

# Determine mbox dir (must already exist and contain mbox data)
DEFAULT_MBOX_LOCATION="output/mbox-data"
MBOX_DIRECTORY="${1:-}"
if [[ -z "$MBOX_DIRECTORY" ]]; then
  MBOX_DIRECTORY="$DEFAULT_MBOX_LOCATION"
fi

# Determine doc output dir (may not exist)
DEFAULT_DOC_OUTPUT_DIR="output/solr-data"
DOC_OUTPUT_DIR="${2:-}"
if [[ -z ${DOC_OUTPUT_DIR} ]]; then
  DOC_OUTPUT_DIR=$DEFAULT_DOC_OUTPUT_DIR
fi

# Ensure doc output dir exists
if [[ -d $DOC_OUTPUT_DIR ]]; then
  echo "Output directory [$DOC_OUTPUT_DIR] already exists; clearing it out and continuing..."
  rm -rf $DOC_OUTPUT_DIR
fi
mkdir -p $DOC_OUTPUT_DIR

# Iterate over mbox files and convert
for filepath in $(find $MBOX_DIRECTORY -name "*.mbox")
do
  python3 convert-mbox-to-solr-docs.py $filepath $DOC_OUTPUT_DIR
done

echo "Solr documents now available in $DOC_OUTPUT_DIR; use 'bin/post' (or 'bin/solr post' depending on your Solr version) to upload!"
