#! /usr/bin/env bash

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# shlib.bash — A shell utility library.

# die [ARGS...]
#
# Outputs ARGS to stderr, then exits the script with a failing exit status.
die() {
    echo "$@" >&2
    exit 1
}

# run PROGRAM [ARGS...]
#
# Runs PROGRAM, but informs the user first. Specifically, run outputs "PROGRAM
# ARGS..." to stderr, then executes PROGRAM with the specified ARGS.
run() {
   echo "$*" >&2
   "$@"
}

# runv PROGRAM [ARGS...]
#
# Like run, but prints a more verbose informational message to stderr of the
# form "🚀$ PROGRAM ARGS...".
runv() {
    printf "🚀>$ " >&2
    printf "%q " "$@" >&2
    printf "\n" >&2
    "$@"
}

# command_exists PROGRAM
#
# Returns successfully if PROGRAM exists in $PATH, or unsuccessfully otherwise.
# Outputs nothing.
command_exists() {
    hash "$1" 2>/dev/null
}

# version_compat MINIMUM ACTUAL
# version_compat [VERSIONS...]
#
# Checks whether VERSIONS is in sorted order according to version comparison
# rules. Typical usage is to provide exactly two versions, in which case the
# function checks that ACTUAL is greater than or equal to MINIMUM.
version_compat() {
    printf "%s\n" "$@" | sort --check=silent --version-sort
}

# git_empty_tree
#
# Outputs the 40-character SHA-1 hash of Git's empty tree object.
git_empty_tree() {
    git hash-object -t tree /dev/null
}

# git_files [PATTERNS...]
#
# Lists the files known to Git that match the PATTERNS, with the following
# differences from `git ls-files`:
#
#     1. files matched by a gitignore rule are excluded,
#     2. deleted but unstaged files are excluded,
#     3. symlinks are excluded.
#
git_files() {
    git diff --ignore-submodules=all --raw "$(git_empty_tree)" -- "$@" \
        | awk '$2 != 120000 {print $6}'
}

# try COMMAND [ARGS...]
#
# Runs COMMAND with the specified ARGS without aborting the script if the
# command fails. See also try_last_failed and try_finish.
try() {
    try_last_failed=false
    if ! "$@"; then
        try_failed=true
        try_last_failed=true
    fi
}
try_failed=false

# try_last_failed
#
# Reports whether the last command executed with `try` succeeded or failed.
try_last_failed() {
    $try_last_failed
}

# try_finish
#
# Exits the script with a code that reflects whether all commands executed with
# `try` were successful.
try_finish() {
    if $try_failed; then
        exit 1
    fi
    exit 0
}

ci_init() {
    export RUST_BACKTRACE=full
}

ci_collapsed_heading() {
    echo "---" "$@"
}

ci_uncollapsed_heading() {
    echo "+++" "$@"
}

ci_uncollapse_current_section() {
    echo "^^^ +++"
}

ci_try_passed=0
ci_try_total=0

ci_try() {
    ci_collapsed_heading "$@"

    # Try the command.
    if "$@"; then
        ((++ci_try_passed))
    else
        # The command failed. Tell Buildkite to uncollapse this log section, so
        # that the errors are immediately visible.
        [[ "${SHLIB_NOT_IN_CI-}" ]] || ci_uncollapse_current_section
    fi
    ((++ci_try_total))
}

ci_status_report() {
    ci_uncollapsed_heading "Status report"
    echo "$ci_try_passed/$ci_try_total commands passed"
    if ((ci_try_passed != ci_try_total)); then
        exit 1
    fi
}

# await_postgres [connection options]
#
# Waits for PostgreSQL to become ready. Accepts the same options as the
# underlying pg_isready utility. Gives up after 30 tries, waiting 1s between
# each try.
#
# Note that simply checking for the PostgreSQL port to start accepting
# connections is insufficient, as PostgreSQL can start accepting connections
# and immediately rejecting them with a "the database system is starting up"
# fatal error.
await_postgres() {
    local i=0
    until pg_isready "$@" || ((++i > 30)); do
        echo "waiting 1s for postgres to become ready"
        sleep 1
    done
}

# mapfile_shim [array]
#
# A limited backport of the Bash 4.0 `mapfile` built-in. Reads lines from the
# standard input into the indexed array variable ARRAY. If ARRAY is unspecified,
# the variable MAPFILE is used instead. Other options of `mapfile` are not
# supported.
mapfile_shim() {
    local val
    val=()
    while IFS= read -r line; do
        val+=("$line")
    done
    declare -ag "${1:-MAPFILE}=($(printf "%q " "${val[@]}"))"
}

########################################
# Text-Coloring commands

# [m]essage-[s]uccess: message to the user that something went well
ms() {
    green "$@"
}

# [u]sage-[s]ubcommand: Paint the argument as a subcmd
#
# In usage text, write: "usage: $0 `us CMD`"
us() {
    blue "$@"
}

# [u]sage-[f]lag: Paint the argument as a flag
#
# In usage text, write: "usage: $0 `uf --FLAG`"
uf() {
    green "$@"
}

# [u]sage-[o]ption: Paint the argument as an option
#
# In usage text, write: "usage: $0 `uf --FLAG` `uo OPT`"
uo() {
    green "$@"
}

# [u]sage-[w]arn: Paint the argument as a warning
#
# In usage text, write: "usage: $0 `uw WILL DELETE EVERYTHING`"
uw() {
    red "$@"
}

red() {
    echo -ne "\e[31m$*\e[0m"
}

blue() {
    echo -ne "\e[34m$*\e[0m"
}

green() {
    echo -ne "\e[32m$*\e[0m"
}
