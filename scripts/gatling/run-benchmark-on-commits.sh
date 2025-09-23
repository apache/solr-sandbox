#!/bin/bash -x

set -eu

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
LIB_DIR="$SCRIPT_DIR/lib"
SANDBOX_CHECKOUT_ROOT="$SCRIPT_DIR/../../"
source $LIB_DIR/env-state.sh
source $LIB_DIR/git.sh
source $LIB_DIR/solr.sh
source $LIB_DIR/gatling.sh

###########################
# "Main" - begin arg parsing
COMMIT_HASHES=""
START_HASH=""
END_HASH=""
BRANCH="main"

if [ $# -gt 0 ]; then
  while [ $# -gt 0 ]; do
    case "${1:-}" in
        -s|--start-hash)
            START_HASH=${2}
            shift 2
        ;;
        -e|--end-hash)
            END_HASH=${2}
            shift 2
        ;;
        -b|--branch)
            BRANCH=${2}
            shift 2
        ;;
        -c|--commit-hashes)
            COMMIT_HASHES=${2}
            shift 2
        ;;
        *)
            shift
        ;;
    esac
  done
fi

###############################################
# Identify the branch and the commits to run on
env_state_bootstrap

pushd $BENCH_SOLR_CHECKOUT_DIR
  if [[ -n "${COMMIT_HASHES}" ]]; then
    COMMIT_HASHES="$(echo "$COMMIT_HASHES" |  sed 's/,/\n/g')"
  else # [[ -z "${COMMIT_HASHES}" ]]; then
    if [[ -z "${START_HASH}" ]]; then
      >&2 echo "Either '-c' or '-s'/'-e' argument must be provided"
      exit 1
    else
      git_update_checkout $BRANCH
      # TODO Implement this function or copy it over from solr-test-health
      COMMIT_HASHES="$(git_list_commits_since $START_HASH $END_HASH)"
    fi
  fi
popd

####################################################
# Download any benchmark-data needed for simulations
pushd $SANDBOX_CHECKOUT_ROOT
  gatling_download_wiki_data
popd

######################################################
# Iterate over commits, building and benchmarking each
pushd $BENCH_SOLR_CHECKOUT_DIR
  for commit in $(echo "$COMMIT_HASHES") ; do
    echo "Processing commit: [$commit]"

    # Build and start Solr
    git_checkout "$commit"
    solr_kill_all
    solr_build_package
    package_dir="$(solr_get_package_directory)"
    pushd $package_dir
      export START_SOLR_OPTS=" -m 4g "
      solr_start
      if ! solr_is_running "8983" ; then
        >&2 echo "Unable to start Solr; please check logs. Exiting..."
        exit 1
      fi
    popd

    # Run the benchmark(s) and store the result
    #   (currently just wiki-indexing)
    pushd $SANDBOX_CHECKOUT_ROOT 
      ./scripts/gatling/setup_wikipedia_tests.sh
      ./gradlew gatlingRun --simulation index.IndexWikipediaBatchesSimulation
      env_state_store_gatling_result $BRANCH $COMMIT
    popd
    solr_kill_all

  done
popd
