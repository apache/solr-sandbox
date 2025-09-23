#!/bin/bash -x

set -eu

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
LIB_DIR="$SCRIPT_DIR/lib"
SANDBOX_CHECKOUT_ROOT="$SCRIPT_DIR/../../"
source $LIB_DIR/env-state.sh
source $LIB_DIR/git.sh
source $LIB_DIR/solr.sh


#####################################
# "Main" - arg parsing and validation
TAG=""
BRANCH="main"
PREVIOUS_END_HASH=""

if [ $# -gt 0 ]; then
  while [ $# -gt 0 ]; do
    case "${1:-}" in
        -t|--tag)
            TAG=${2}
            shift 2
        ;;
        -b|--branch)
            BRANCH=${2}
            shift 2
        ;;
        -l|--last-hash)
            PREVIOUS_END_HASH=${2}
            shift 2
        ;;
        *)
            shift
        ;;
    esac
  done
fi

if [[ -z $TAG ]]; then
  >&2 echo "Required argument 'tag' was not provided"
  return 1
fi

# If no user-provided last-hash, attempt to read it from "state file"
if [[ -z "$PREVIOUS_END_HASH" ]]; then
  PREVIOUS_END_HASH=$(env_state_read_last_run_id "$TAG")
fi

# If we still don't have a last-hash, exit
if [[ -z "$PREVIOUS_END_HASH" ]]; then
  >&2 echo "'-l' is required for the initial run of each tag; exiting..."
  exit 1
fi

##########################################################
# Figure out the 'start' and 'end' points for benchmarking
pushd $BENCH_SOLR_CHECKOUT_DIR
  git_update_checkout $BRANCH
  git_clean

  # 'git_list_commits_since' is inclusive (i.e. it returns the arg in output),
  # so that must be excluded to get the first commit *after* that point
  START_COMMIT=$(git_list_commits_since $PREVIOUS_END_HASH | grep -v $PREVIOUS_END_HASH | head -n 1)
  END_COMMIT=$(git_echo_latest_commit_on_branch $BRANCH)
popd

if [[ "$END_COMMIT" == "$PREVIOUS_END_HASH" ]]; then
  >&2 echo "No new commits since the last cronjob run processed $PREVIOUS_END_HASH; exiting..."
  exit 0
fi

if [[ -z "$START_COMMIT" ]]; then
  >&2 echo "No new commits since the last cronjob run processed $PREVIOUS_END_HASH; exiting..."
  exit 0
fi

############################################################################
# Run benchmarking on all commits since the previous cronjob run, and update
# the "last run" state only if successful
$SCRIPTS_DIR/run-benchmark-on-commits.sh -b $BRANCH -s $START_COMMIT -e $END_COMMIT
env_state_write_last_run_id "$TAG" $END_COMMIT
