#! /usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
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
   echo "\$ $*" >&2
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
# command fails. See also try_last_failed and try_status_report.
try() {
    ci_collapsed_heading "$@"

    # Try the command.
    if "$@"; then
        result=$?
        try_last_failed=false
        ((++ci_try_passed))
    else
        result=$?
        try_last_failed=true
        # The command failed. Tell Buildkite to uncollapse this log section, so
        # that the errors are immediately visible.
        in_ci && ci_uncollapse_current_section
        echo "^^^ 🚨 Failed: $*"
    fi
    ((++ci_try_total))
    return $result
}
try_last_failed=false

# try_last_failed
#
# Reports whether the last command executed with `try` succeeded or failed.
try_last_failed() {
    $try_last_failed
}

# try_status_report
#
# Exits the script with a code that reflects whether all commands executed with
# `try` were successful.
try_status_report() {
    ci_uncollapsed_heading "Status report"
    echo "$ci_try_passed/$ci_try_total commands passed"
    if ((ci_try_passed != ci_try_total)); then
        exit 1
    fi
}

ci_unimportant_heading() {
    echo "~~~" "$@" >&2
}

ci_collapsed_heading() {
    echo "---" "$@" >&2
}

ci_uncollapsed_heading() {
    echo "+++" "$@" >&2
}

ci_uncollapse_current_section() {
    if in_ci; then
        echo "^^^ +++" >&2
    fi
}

ci_try_passed=0
ci_try_total=0

# read_list PREFIX
#
# Appends the environment variables `PREFIX_0`, `PREFIX_1`, ... `PREFIX_N` to
# the `result` global variable, stopping when `PREFIX_N` is an empty string.
read_list() {
    result=()

    local i=0
    local param="${1}_${i}"

    if [[ "${!1:-}" ]]; then
        echo "error: mzcompose command must be an array, not a string" >&2
        exit 1
    fi

    while [[ "${!param:-}" ]]; do
        result+=("${!param}")
        i=$((i+1))
        param="${1}_${i}"
    done

    [[ ${#result[@]} -gt 0 ]] || return 1
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

# arch_gcc
#
# Computes the host architecture using GCC nomenclature: x86_64 or aarch64.
# Dies if the host architecture is unknown.
arch_gcc() {
    local arch
    arch=$(uname -m)
    case "$arch" in
        x86_64|aarch64) echo "$arch" ;;
        arm64) echo aarch64 ;;
        *) die "unknown host architecture \"$arch\"" ;;
    esac
}

# arch_go [ARCH-GCC]
#
# Converts ARCH-GCC to Go nomenclature: amd64 or arm64. If ARCH-GCC is not
# specified, uses the host architecture.
arch_go() {
    local arch=${1:-$(arch_gcc)}
    case "$arch" in
        x86_64) echo amd64 ;;
        aarch64) echo arm64 ;;
        *) die "unknown host architecture \"$arch\"" ;;
    esac
}

# red [ARGS...]
#
# Prints the provided text in red.
red() {
    echo -ne "\e[31m$*\e[0m"
}

# green [ARGS...]
#
# Prints the provided text in green.
green() {
    echo -ne "\e[32m$*\e[0m"
}

# white [ARGS...]
#
# Prints the provided text in white.
white() {
    echo -ne "\e[97m$*\e[0m"
}

# in_ci
#
# Returns 0 if in CI and 1 otherwise
in_ci() {
    [ -z "${BUILDKITE-}" ] && return 1
    return 0
}

# is_truthy VAR
#
# Returns 0 if the parameter is not one of: 0, '', no, false; and 1 otherwise
is_truthy() {
    if [[ "$1" == "0" || "$1" == "" || "$1" == "no" || "$1" == "false" ]]; then
        return 1
    fi
    return 0
}
