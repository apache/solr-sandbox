#!/bin/bash -x

set -eu

# Usage: ./download-git-repositories.sh /some/output/dir
#
if [[ -z ${1:-} ]]; then
  echo "Ouput directory argument is required but was not provided; exiting"
  exit 1
fi

OUTPUT_DIRECTORY="$1"
if [[ -d $OUTPUT_DIRECTORY ]]; then
  echo "Output directory [$OUTPUT_DIRECTORY] already exists; clearing it out and continuing..."
  rm -rf $OUTPUT_DIRECTORY
fi
mkdir -p $OUTPUT_DIRECTORY


REPO_URL="git@github.com:apache/charts.git"
# This repo list should always remain in sync with the value in 'convert-git-repositories-to-solr-docs.sh'
GIT_REPOS=("solr" "solr-site" "solr-sandbox" "solr-operator")

pushd $OUTPUT_DIRECTORY
  for repo in ${GIT_REPOS[@]}; do
    REPO_URL="git@github.com:apache/${repo}.git"
    git clone $REPO_URL
  done
popd
