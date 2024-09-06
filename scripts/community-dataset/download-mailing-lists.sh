#!/bin/bash

set -eu

# Usage: ./download-mailing-lists.sh /some/output/dir
# TODO Potential Improvements
#   - Arg to pull data for non-solr projects
#   - Arg to only pull data for date-range
#   - Arg to only pull some lists
#   - Arg to customize or omit the 'sleep'

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

CURRENT_YEAR="$(date +%Y)"
CURRENT_MONTH="$(date +%m)"

# Solr's been around forever, but the mailing lists are only around post- project-split
STARTING_YEAR="2021"
STARTING_MONTH="1"

MAILING_LISTS=("dev" "issues" "builds" "commits" "users")

pushd $OUTPUT_DIRECTORY
  for list in ${MAILING_LISTS[@]}; do
    mkdir -p $list

    # Download all data for the mailing list
    pushd $list
      for year in $(seq $STARTING_YEAR $CURRENT_YEAR)
      do
        for month in $(seq 1 12)
        do
          # Iterate through all months, even those that haven't happened yet.  This is technically wrong, but ASF's mbox.lua tool handles it gracefully without a 404, etc.
          # In the case of a month/year with no data, the curl command gets a 200 status and an empty response body
          curl -sk "https://lists.apache.org/api/mbox.lua?list=dev&domain=solr.apache.org&d=${year}-${month}&q=" > ${list}-${year}-${month}.mbox

          # Some small sleep to avoid hitting any rate-limiting or causing any problems for the ASF servers
          sleep 2
        done
      done
    popd
  done
popd
