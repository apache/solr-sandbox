#!/bin/bash

set -eu

# Usage: ./convert-git-repositories-to-solr-docs.sh <git-repo-directory> <solr-doc-output-dir>

# TODO - pretty lazy, make more resilient and remove
current_dir=$(basename `pwd`)
if [[ "solr-datasets" != $current_dir ]]; then
  echo "Script intended to be run from the repo root dir; exiting"
  exit 1
fi

if [[ -z ${1:-} ]]; then
  echo "'git-repo-directory' argument is required but was not provided; exiting"
  exit 1
fi
if [[ -z ${2:-} ]]; then
  echo "'solr-doc-output-dir' argument is required but was not provided; exiting"
  exit 1
fi

GIT_REPO_DIRECTORY=$1
SOLR_DOC_OUTPUT_DIRECTORY=$2

if [[ -d $SOLR_DOC_OUTPUT_DIRECTORY ]]; then
  echo "Output directory [$SOLR_DOC_OUTPUT_DIRECTORY] already exists; clearing it out and continuing..."
  rm -rf $SOLR_DOC_OUTPUT_DIRECTORY
fi
mkdir -p $SOLR_DOC_OUTPUT_DIRECTORY

# This repo list should always remain in sync with the value in 'download-git-repositories.sh'
GIT_REPOS=("solr" "solr-site" "solr-sandbox" "solr-operator")

for repo in ${GIT_REPOS[@]}; do
  ./export-git-data.sh ${GIT_REPO_DIRECTORY}/$repo $SOLR_DOC_OUTPUT_DIRECTORY
done

