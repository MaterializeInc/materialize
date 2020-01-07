# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# copyright.awk — checks file for missing copyright header.

function done()
{
    if (!copyright) {
        print "lint: copyright: " FILENAME " is missing copyright header" > "/dev/stderr"
        exit 1
    } else if (copyright !~ /Copyright 20(19|2[0-9]) Materialize, Inc./) {
        print "lint: copyright: " FILENAME " has malformatted copyright header" > "/dev/stderr"
        exit 1
    }
    exit 0
}

/^#![ \t\n]*\//         { next }
/^(\/\/|#)?.*Copyright/ { copyright=$0 }
/^[ \t\n]*$/            { next }
!/^(<!--|\/\/|#)/       { done() }
