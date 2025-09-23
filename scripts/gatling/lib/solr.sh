#!/bin/bash

function solr_checkout_source_in() {
  if [[ -z $1 ]]; then
    >&2 echo "Required argument 'location' was not provided"
    return 1
  fi

  local location=$1
  if [[ -d $location/solr ]]; then
    # The checkout already exists I guess?
    return 0
  fi

  pushd $location
    git clone git@github.com:apache/solr.git
  popd
}

# Returns with an exit status indicating whether Solr is (or isn't)
# running at the specified port.  Callers provide the port as a required
# argument.  Assumes that the wording directory is positioned so that
# 'bin/solr' can be invoked.
#   Usage: solr_is_running 8983
function solr_is_running() {
  if [[ -z ${1:-} ]]; then
    >&2 echo "Required argument 'port' was not provided"
    return 1
  fi

  local port=$1
  bin/solr assert --started "http://localhost:$port/solr"
  return $?
}

function solr_kill_all() {
  ps -ef | grep solr | grep java | awk {'print $2'} | xargs kill -9
}

# Start Solr (with custom ops pulled from START_SOLR_OPTS)
# Assumes cwd of the Solr package root
function solr_start() {
  bin/solr start ${START_SOLR_OPTS:-}
}

##############################
# Solr build utilities (i.e. working with the Gradle build)
##############################

# Assumes caller is in the root of the Solr checkout directory
function solr_build_package() {
  ./gradlew clean assemble -Pproduction=true
}

# Returns the path (relative to the Solr checkout root) of the package created using 'gradle assemble'
# (Assumes running in the Solr project root)
function solr_get_package_directory() {
  local solr_dist_name="$(ls -l solr/packaging/build/ | awk {'print $9'} | grep solr | grep -v slim)"

  if [[ -z "$solr_dist_name" ]]; then
    >&2 echo "No solr package exists; cannot get directory name"
    return 1
  fi

  echo "solr/packaging/build/$solr_dist_name"
}
