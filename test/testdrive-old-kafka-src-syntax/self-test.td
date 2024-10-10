# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test tricky behavior of testdrive itself that isn't currently tested by other tests

# The regex 'foo[a]bar' matches the string 'fooabar' but not itself
$ set no-self-match=foo[a]bar

> CREATE TABLE example (a int)

! SELECT "${no-self-match}" FROM example
contains: column "foo[a]bar" does not exist

! SELECT "${no-self-match}" FROM example
contains: column "${no-self-match}" does not exist

! SELECT "${no-self-match}" FROM example
regex: column "${no-self-match}".*not exist
