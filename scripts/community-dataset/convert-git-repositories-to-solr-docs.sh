#!/bin/bash

set -eu

# Usage: ./convert-git-repositories-to-solr-docs.sh [<git-repo-directory>] [<solr-doc-output-dir>]

# Determine git dir (must already exist and contain git checkouts)
DEFAULT_GIT_LOCATION="output/git-data"
GIT_DIRECTORY="${1:-}"
if [[ -z "$GIT_DIRECTORY" ]]; then
  GIT_DIRECTORY="$DEFAULT_GIT_LOCATION"
fi

# Determine output dir (may not exist)
DEFAULT_DOC_OUTPUT_DIR="output/solr-data"
DOC_OUTPUT_DIR="${2:-}"
if [[ -z ${DOC_OUTPUT_DIR} ]]; then
  DOC_OUTPUT_DIR=$DEFAULT_DOC_OUTPUT_DIR
fi

# Ensure doc output dir  exists
if [[ -d $DOC_OUTPUT_DIR ]]; then
  echo "Output directory [$DOC_OUTPUT_DIR] already exists; clearing it out and continuing..."
  rm -rf $DOC_OUTPUT_DIR
fi
mkdir -p $DOC_OUTPUT_DIR

# This repo list should always remain in sync with the value in 'download-git-repositories.sh'
GIT_REPOS=("solr" "solr-site" "solr-sandbox" "solr-operator")

for repo in ${GIT_REPOS[@]}; do
  ./export-git-data.sh ${GIT_DIRECTORY}/$repo $DOC_OUTPUT_DIR
done
