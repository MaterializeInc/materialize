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

# trufflehog_jq_filter_common
#
# Filters out secrets we expect in both checked in files and logs
trufflehog_jq_filter_common() {
  jq -c '
    select(
      (.Raw | contains("user1:password") | not) and
      (.Raw | contains("infra+nightly-canary@materialize.com:XXX") | not) and
      .Raw != "postgres://mz_system:materialize@materialized:5432" and
      .Raw != "postgres://materialize:materialize@materialized:6875" and
      .Raw != "postgres://mz_system:materialize@materialized:6877" and
      .Raw != "postgres://superuser_login:some_bogus_password@materialized2:6875" and
      .Raw != "jdbc:postgresql://127.0.0.1:26257/defaultdb?sslmode=disable" and
      .Raw != "postgres://any:user@materialized:6875" and
      .Raw != "https://materialize:sekurity@schema-registry:8081" and
      .Raw != "postgresql://postgres:postgres@postgres:5432" and
      .Raw != "postgres://postgres:postgres@postgres:5432" and
      .Raw != "sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe" and
      .Raw != "jdbc:postgresql://localhost:6875/materialize" and
      .Raw != "postgres://materialize_user:materialize_pass@postgres.materialize.svc.cluster.local:5432" and
      .Raw != "jdbc:postgresql://%s:%s/materialize" and
      .Raw != "postgres://postgres:$MATERIALIZE_PROD_SANDBOX_RDS_PASSWORD@$MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME:5432" and
      .Raw != "http://user:pass@example.com" and
      .Raw != "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDDC5MP3v1BHOgI\n5SsmrW8mjxzQGOz0IlC5jp1muW/kpEoE9TG317TEnO5Uye6zZudkFCP8YGEiN3Mc\nFbTM7eX6PjAPdnGU7khuUt/20ZM+NX5kWZPrmPTh4WQaDCL7ah1LqzBaUAMaSXq8\niuy7LGJNF8wdx8L5BjDiGTTxZXOg0Haxknc7Mbiwc9z8eb7omvzQzsOwyqocrF2u\nz86TzX1jtHP48i5CxoRHKxE94De3tNxjT/Y3OZlS4QS7iekAOQ04DVV3GIHvRUXN\n2H8ayy4+yOdhHn6ER5Jn3lti1Q5XSrxkrYn7L1Vcj6IwZQhhF5vc+ovxOYb+8ert\nEo97tIkLAgMBAAECggEAQteHHRPKz9Mzs8Sxvo4GPv0hnzFDl0DhUE4PJCKdtYoV\n8dADq2DJiu3LAZS4cJPt7Y63bGitMRg2oyPPM8G9pD5Goy3wq9zjRqexKDlXUCTt\n/T7zofRny7c94m1RWb7ablGq/vBXt90BqnajvVtvDsN+iKAqccQM4ZdI3QdrEmt1\ncHex924itzG/mqbFTAfAmVj1ZsRnJp55Txy2gqq7jX00xDM8+H49SRvUu49N64LQ\n6BUWCgWCJePRtgjSHjboAzPqSkMdaTE/WDY2zgGF3Qfq4f6JCHKfm4QylCH4gYUU\n1Kf7ttmhu9NoZO+hczobKkxP9RtXfyTRH2bsJXy2HQKBgQDhHgavxk/ln5mdMGGw\nrQud2vF9n7UwFiysYxocIC5/CWD0GAhnawchjPypbW/7vKM5Z9zhW3eH1U9P13sa\n2xHfrU5BZ16rxoBbKNpcr7VeEbUBAsDoGV24xjoecp7rB2hZ+mGik5/5Ig1Rk1KH\ndcvYy2KSi1h4Sm+mXwimmA4VDQKBgQDdzW+5FPbdM2sUB2gLMQtn3ICjDSu6IQ+k\nd0p3WlTIT51RUsPXXKkk96O5anUbeB3syY8tSKPGggsaXaeL3o09yIamtERgCnn3\nd9IS+4VKPWQlFUICU1KrD+TO7IYIX04iXBuVE5ihv0q3mslhDotmX4kS38NtKEFF\njLjA2RvAdwKBgAFkIxxw+Ett+hALnX7vAtRd5wIku4TpjisejanA1Si50RyRDXQ+\nKBQf/+u4HmoK12Nibe4Cl7GCMvRGW59l3S1pr8MdtWsQVfi6Puc1usQzDdBMyQ5m\nIbsjlnZbtPm02QM9Vd8gVGvAtx5a77aglrrnPtuy+r/7jccUbURCSkv9AoGAH9m3\nWGmVRZBzqO2jWDATxjdY1ZE3nUPQHjrvG5KCKD2ehqYO72cj9uYEwcRyyp4GFhGf\nmM4cjo3wEDowrBoqSBv6kgfC5dO7TfkL1qP9sPp93gFeeD0E2wGuRrSaTqt46eA2\nKcMloNx6W0FD98cB55KCeY5eXtdwAA/EHBVRMeMCgYAd3n6PcL6rVXyE3+wRTKK4\n+zvx5sjTAnljr5ttbEnpZafzrYIfDpB8NNjexy83AeC0O13LvSHIFoTwP8sywJRO\nRxbPMjhEBdVZ5NxlxYer7yKN+h5OBJfrLswPku7y4vdFYK3x/lMuNQO61hb1VFHc\nT2BDTbF0QSlPxFsv18B9zg==\n-----END PRIVATE KEY-----\n" and
      .Raw != "postgres://materialize:materialize@environmentd:6875" and
      .Raw != "postgres://MATERIALIZE_USERNAME:APP_SPECIFIC_PASSWORD@MATERIALIZE_HOST:6875" and
      .Raw != "jdbc:postgresql://MATERIALIZE_HOST:6875/materialize" and
      .Raw != "postgres://user:password@host:6875" and
      .Raw != "postgres" and
      .Raw != "slt" and
      .Raw != "e6d5833015b170e23ae819e8c5d7eaedb472ca98" and
      .Raw != "postgresql://materialize:AbC123dEf@ep-cool-darkness-123456.us-east-2.aws.neon.tech:5432" and
      .Raw != "d3aa325086974cdfb3912f28e5a8c168" and
      .Raw != "jdbc:postgresql://postgres:5432/postgres" and
      .Raw != "RPSsql12345" and
      .Raw != "RPSsql1234" and
      .Raw != "RPSsql123" and
      .Raw != "RPSsql12" and
      .Raw != "RPSsql1" and
      .Raw != "RPSsql" and
      .Raw != "RPSsq" and
      .Raw != "RPSs" and
      .Raw != "RPS" and
      .Raw != "RP" and
      .Raw != "R"
    )'
}

# trufflehog_jq_filter_files
#
# Filters out secrets we expect only in checked in files
trufflehog_jq_filter_files() {
  trufflehog_jq_filter_common | jq -c '
  select(
    .Raw != "ghp_9fK8sL3x7TqR1vEzYm2pDaN4WjXbQzUtV0aN"
  )'
}

# trufflehog_jq_filter_logs
#
# Filters out secrets we expect only in logs during CI runs
trufflehog_jq_filter_logs() {
  trufflehog_jq_filter_common | jq -c '
  select(
    (.Raw | contains("mz_system:materialize") | not) and
    (.Raw | contains("jdbc:postgresql://postgres") | not) and
    (.Raw | contains("mz_analytics:materialize") | not) and
    (.Raw | contains("mz_support:materialize") | not) and
    (.Raw | contains("jdbc:mysql://mysql") | not) and
    (.Raw | contains("superuser_login:some_bogus_password") | not) and
    (.Raw | contains("postgres:postgres") | not) and
    (.Raw | contains("jdbc:postgresql://127.0.0") | not) and
    (.Raw | contains("jdbc:postgresql://cockroach") | not) and
    (.Raw | contains("materialize:materialize") | not) and
    (.Raw | contains("ExpirationReaper") | not) and
    (.Raw | contains("u1@example.com") | not) and
    .Raw != "[REDACTED]"
  )'
}
