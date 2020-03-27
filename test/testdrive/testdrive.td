# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for testdrive itself.

# Uncomment to test that timeouts happen in the time desired.
#
# Note that the duration string format can be anything parsable
# by the parse_duration create
#
# $ set-sql-timeout duration=2minutes
# $ set-sql-timeout duration=default
# > select * from nonexistent

# Test that hashing rows works and is consistent.

> CREATE VIEW v AS VALUES (1, 'foo'), (2, 'bar'), (3, 'foo'), (1, 'bar')

> SELECT * from v
4 values hashing to 7dd470c8470b085df13552e191a244ab
