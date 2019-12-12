#! /usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
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
    echo "🚀$ $*" >&2
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

# mapfile_shim [array]
#
# A limited backport of the Bash 4.0 `mapfile` built-in. Reads lines from the
# standard input into the indexed array variable ARRAY. If ARRAY is unspecified,
# the variable MAPFILE is used instead. Other options of `mapfile` are not
# supported.
mapfile_shim() {
    local -n var=${1:-MAPFILE}
    var=()
    while IFS= read -r line; do
        var+=("$line")
    done
}

########################################
# Text-Coloring commands

# [u]sage-[s]ubcommand: Paint the argument as a subcmd
#
# In usage text, write: "usage: $0 `us CMD`"
us() {
    echo -ne "\e[34m$*\e[0m"
}

# [u]sage-[f]lag: Paint the argument as a flag
#
# In usage text, write: "usage: $0 `uf --FLAG`"
uf() {
    echo -ne "\e[32m$*\e[0m"
}

# [u]sage-[o]ption: Paint the argument as an option
#
# In usage text, write: "usage: $0 `uf --FLAG` `uo OPT`"
uo() {
    echo -ne "\e[32m$*\e[0m"
}

# [u]sage-[w]arn: Paint the argument as a warning
#
# In usage text, write: "usage: $0 `uw WILL DELETE EVERYTHING`"
uw() {
    echo -ne "\e[31m$*\e[0m"
}
