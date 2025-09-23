#!/bin/bash

# Prints (to stdout) the hash of the latest commit on the specified
# branch.  (The current branch is assumed if no branch name is
# specified)
#   Usage: commit_hash=$(git_echo_latest_commit_on_branch branch_9x)
#          commit_hash=$(git_echo_latest_commit_on_branch)
function git_echo_latest_commit_on_branch() {
  local branch_id="${1:-}"
  git log $branch_id -n 1 --format=%H
}

# Runs 'git clean -xfd' to ensure no files are left behind from a
# previous branch.  'gradle.properties' is explicitly not removed so 
# that users may specify custom debug or other settings for gradle
# there
#   Usage: git_clean
function git_clean() {
  git clean -xfd -e gradle.properties
}

function git_checkout() {
  local commit_id="${1}"
  git checkout $commit_id
}

# Ensures a source checkout is up to date via 'git fetch', optionally
# doing a hard reset on a specified branch.  Assumes that the cwd is 
# placed somewhere inside the desired git repository
#   Usage: git_update_checkout branch_9x
function git_update_checkout() {
  git fetch
  if [[ -n $1 ]]; then
    local branch_name=$1
    local branch_name_stash="$(git branch --show-current)"

    git checkout $branch_name
    git reset --hard origin/$branch_name
    git checkout $branch_name_stash
  fi
}

# Prints (to stdout), one per line, the commits since a specified
# commit-hash (inclusive).  A commit hash or branch name can also be
# specified for the upper bound; 'HEAD' will be used if not specified.
#
# Commit hashes are output in oldest-to-newest order.
#   Usage: commit_list="$(git_list_commits_since deadbeef)"
#          commit_list="$(git_list_commits_since deadbeef branch_9x)"
function git_list_commits_since() {
  if [[ -z ${1:-} ]]; then
    >&2 echo "Required argument 'commit_hash' was not provided"
    return 1
  fi

  local commit_hash="$1"
  local upper_bound="${2:-HEAD}"

  echo $commit_hash
  git rev-list --reverse ${commit_hash}..${upper_bound}
}

# Prints (to stdout), the hash of the commit 'N' places back on the
# specified branch.  Expects the CWD to already be stationed within the
# git repository, though the repository needn't be on the specified
# branch.  This function will re-position the repository to the original
# branch before returning to the caller.
#   Usage: several_back_hash=$(git_get_previous_commit_hash main 5)
function git_get_previous_commit_hash() {
  if [[ -z ${1:-} ]]; then
    >&2 echo "Required argument 'branch' was not provided"
    return 1
  fi

  if [[ -z ${2:-} ]]; then
    >&2 echo "Required argument 'num_commits' was not provided"
    return 1
  fi

  local branch="$1"
  local num_commits="$2"

  git_update_checkout $branch &> /dev/null
  local branch_name_stash="$(git branch --show-current)"
  git checkout $branch &> /dev/null
    local commit_hash="$(git rev-parse HEAD~${num_commits})"
  git checkout $branch_name_stash &> /dev/null

  echo $commit_hash
}

# Prints (to stdout) the numeric count of a specified commit-hash from
# the root of the branch where it resides.  'HEAD' is used if no commit-
# hash is specified.
#   Usage: commit_num=$(git_echo_commit_count deadbeef)
function git_echo_commit_count() {
  local commit_id="${1:-HEAD}"
  git rev-list --count $commit_id
}
