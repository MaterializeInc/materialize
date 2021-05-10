# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Regression test for https://github.com/MaterializeInc/materialize/issues/6687
#
# Panic if a sink is created quickly after an index. This test (with
# the bug present) is inherently flakey since it's attempting to test
# a race condition.
#

> CREATE TABLE t (i INT)

> CREATE MATERIALIZED VIEW v AS SELECT count(i) FROM t

# If the timestamp chosen for this sink only cares about the upper,
# *SOMETIMES* this test will crash materialize, depending if the upper
# has already been advanced by update_upper or not, which happens quite
# quickly. Before #6687 was fixed, this test failed locally about 1 out
# of 4 runs.
> CREATE SINK s FROM v INTO AVRO OCF '${testdrive.temp-dir}/github-6687.ocf'
