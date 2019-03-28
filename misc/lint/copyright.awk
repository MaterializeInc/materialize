# Copyright 2019 Timely Data, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Timely Data, Inc.
#
# copyright.awk — checks file for missing copyright header.

function done()
{
    if (!seen_copyright) {
        print "lint: copyright: " FILENAME " is missing copyright header" > "/dev/stderr"
        exit 1
    }
    exit 0
}

/^#![ \t\n]*\//        { next }
/^(\/\/|#).*Copyright/ { seen_copyright=1 }
/^[ \t\n]*$/           { if (seen_nonblank) { done() } else { next } }
!/^(\/\/|#)/           { done() }
