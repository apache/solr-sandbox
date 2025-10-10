#!/bin/bash

# This library contains utilities for setting up and reading various
# state bits needed to persist from one run of the bench scripts to the
# next


BENCH_LIB_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

BENCH_STATE_ROOT="${BENCH_STATE_ROOT:-$HOME/.solr-benchmarks}"
BENCH_RESULT_DIR="$BENCH_STATE_ROOT/results"
BENCH_LAST_RUN_STATE="$BENCH_STATE_ROOT/last-run.txt"
export BENCH_CHECKOUT_DIR="$BENCH_STATE_ROOT/checkouts"
export BENCH_SOLR_CHECKOUT_DIR="$BENCH_CHECKOUT_DIR/solr"

# Creates the directories and data necessary for running solr-test-health
# utilities.  Depending on the host machine, may need to be run with sudo or
# elevated permissions.
# TODO - make this (and leaf functions) more idempotent
function env_state_bootstrap() {
  mkdir -p $BENCH_STATE_ROOT

  mkdir -p $BENCH_CHECKOUT_DIR
  mkdir -p $BENCH_RESULT_DIR
  source $BENCH_LIB_DIR/solr.sh

  solr_checkout_source_in $BENCH_CHECKOUT_DIR
}

# Wipes all env-state from this host
function env_state_wipe() {
  rm -rf $BENCH_STATE_ROOT
}

#########################################################
##################
# "Last Run IDs"
##################
# Currently these are stored in a flat file with each line formatted as:
#   ^<tag> <last-run-id>$
# Neither the tag nor the last-run-id may contain spaces, and tags should
# not be used that could be a potential run-id (typically a date or commit hash)
#
# In the future perhaps these should be stored with more structure, but this
# is sufficient currently
#########################################################

# Reads a state identifier (usually a commit hash) associated with a particular tag
# Typically this is used to identify the last commit covered by the previous run
#
# Tags and last-run-IDs must not contain whitespace.
#
#   Usage: last_hash=$(env_state_read_last_run_identifer bats-tests)
function env_state_read_last_run_id() {
  if [[ -z $1 ]]; then
    >&2 echo "Required argument 'tag' was not provided"
    return 1
  fi

  local tag=$1

  if [[ ! -f $BENCH_LAST_RUN_STATE ]]; then
    touch $BENCH_LAST_RUN_STATE
  fi

  local tag_line="$(cat $BENCH_LAST_RUN_STATE | grep $tag)"
  if [[ -z "$tag_line" ]]; then
    return
  fi
  echo "$tag_line" | cut -d " " -f 2
}

# Writes a state identifier (usually a commit hash) associated with a particular tag.
# Typically this is used to identify the last commit covered by the previous run.
#
# Tags and last-run-IDs must not contain whitespace.
#
#   Usage: env_state_write_last_run_id "bats-tests" deadbeef
function env_state_write_last_run_id() {
  if [[ -z $1 ]]; then
    >&2 echo "Required argument 'tag' was not provided"
    return 1
  fi
  if [[ -z $2 ]]; then
    >&2 echo "Required argument 'last_run_id' was not provided"
    return 2
  fi

  local tag=$1
  local last_run_id=$2
  local tmp_file="${BENCH_LAST_RUN_STATE}.tmp"

  if [[ ! -f $BENCH_LAST_RUN_STATE ]]; then
    touch $BENCH_LAST_RUN_STATE
  fi

  # If the tag is new grep will return non-zero, so the '|| true' keeps 'set -e' happy
  (cat $BENCH_LAST_RUN_STATE | grep -v $tag > $tmp_file) || true
  echo "$tag $last_run_id" >> $tmp_file
  mv $tmp_file $BENCH_LAST_RUN_STATE
}

#########################################################
##################
# "Benchmark Results"
##################
# Currently these are stored within the "state" root, in a subdirectory of
# the form: `$BENCH_STATE_ROOT/results/<tag>/<commit>`
#
# "Tag" values are typically a branch name, but may be any user identifier
# without spaces or special characters. (e.g. mainJava21).  We may need
# something more elaborate to accomodate multiple variables going forward, but
# lets see how this does as a starting point.
#
# Assumes the cwd is positioned at the root of the solr-sandbox checkout.
#
#   Usage: env_state_store_gatling_result <tag> <commit>
#########################################################
function env_state_store_gatling_result() {
  local tag="${1:-}"
  local commit="${2:-}"

  if [[ -z $tag ]]; then
    >&2 echo "Required argument 'tag' was not provided"
    exit 1
  fi
  if [[ -z $commit ]]; then
    >&2 echo "Required argument 'commit' was not provided"
    exit 1
  fi

  local dest_dir="${BENCH_RESULT_DIR}/${tag}/${commit}"
  mkdir -p $dest_dir

  mv gatling-simulations/build/reports/gatling $dest_dir
}
