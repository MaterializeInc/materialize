# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# shlib.bash — A shell utility library.

die() {
    echo "$@" >&2
    exit 1
}

ci_init() {
    export RUST_BACKTRACE=full
}

ci_collapsed_heading() {
    echo "--- $@"
}

ci_uncollapsed_heading() {
    echo "+++ $@"
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
