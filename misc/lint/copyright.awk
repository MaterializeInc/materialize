# Copyright 2019 Timely Data, Inc. All rights reserved.
#
# copyright.awk — checks file for missing copyright header.

function done()
{
    if (!seen_copyright) {
        print "lint: copyright: " FILENAME " is missing copyright header"
        exit 1
    }
    exit 0
}

/^#![[:space:]]*\//    { next }
/^(\/\/|#).*Copyright/ { seen_copyright=1 }
/^[[:space:]]*$/       { if (seen_nonblank) { done() } else { next } }
!/^(\/\/|#)/           { done() }