#!/bin/bash

set -eu

# Usage: ./download-git-repositories.sh [<git-output-dir>]

# Determine output dir
DEFAULT_GIT_LOCATION="output/git-data"
GIT_OUTPUT_DIR="${1:-}"
if [[ -z "$GIT_OUTPUT_DIR" ]]; then
  GIT_OUTPUT_DIR="$DEFAULT_GIT_LOCATION"
fi

if [[ -d $GIT_OUTPUT_DIR ]]; then
  echo "Output directory [$GIT_OUTPUT_DIR] already exists; clearing it out and continuing..."
  rm -rf $GIT_OUTPUT_DIR
fi
mkdir -p $GIT_OUTPUT_DIR


REPO_URL="git@github.com:apache/charts.git"
# This repo list should always remain in sync with the value in 'convert-git-repositories-to-solr-docs.sh'
GIT_REPOS=("solr" "solr-site" "solr-sandbox" "solr-operator")

pushd $GIT_OUTPUT_DIR
  for repo in ${GIT_REPOS[@]}; do
    REPO_URL="git@github.com:apache/${repo}.git"
    git clone $REPO_URL
  done
popd
