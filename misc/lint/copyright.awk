# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# copyright.awk — checks file for missing copyright header.

function done()
{
    if (!copyright) {
        print "lint: copyright: " FILENAME " is missing copyright header" > "/dev/stderr"
        exit 1
    } else if (copyright !~ /Copyright Materialize, Inc./) {
        print "lint: copyright: " FILENAME " has malformatted copyright header" > "/dev/stderr"
        exit 1
    }
    exit 0
}

/^#![ \t\n]*\//         { next }
/^(\/\/|#)?.*Copyright/ { copyright=$0 }
/^[ \t\n]*$/            { next }
!/^(<!--|\/\/|#)/       { done() }
