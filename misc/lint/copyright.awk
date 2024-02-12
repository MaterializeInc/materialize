# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# copyright.awk — checks file for missing copyright header.

function err(s)
{
    print "lint: \033[31merror:\033[0m copyright: " s > "/dev/stderr"
}

function done()
{
    if (!copyright) {
        err(FILENAME " is missing copyright header")
        exit 1
    } else if (copyright !~ /Copyright Materialize, Inc\. and contributors\./) {
        err(FILENAME " has malformatted copyright header")
        print "hint: line " copyright_line " does not include the exact text \"Copyright Materialize, Inc. and contributors.\""
        exit 1
    }
    exit 0
}

/^#![ \t\n]*\//             { next }
/^(\/\/|#|--|\s+"#|;)?.*Copyright/  { copyright=$0; copyright_line=NR }
/^[ \t\n]*$/                { next }
# In case of ipynb files, continue into the JSON
/[{}"\[\]]/                { next }
!/^(<!--|<\?xml|\/\/|#|--|;)/ { done() }
