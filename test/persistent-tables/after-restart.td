# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# The table is expected to have its original data after restart
#

> SELECT * FROM t1;
"2011-11-11 11:11:11" "\\x124"
"<null>" "<null>"

#
# A table that has been dropped previously should not have become resurrected
#

! SELECT * FROM to_be_dropped;
unknown catalog item 'to_be_dropped'

#
# Confirm that same-named tables in different schemas still has different contents
#

> SELECT * FROM schema1.t1;
schema1

> SELECT * FROM schema2.t1;
schema2
