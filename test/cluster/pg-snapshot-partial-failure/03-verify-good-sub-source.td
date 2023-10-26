# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The non-stalled sub-source should work.
> SELECT COUNT(*) FROM one;
10

# Here we note that the top-level source is also queryable,...
> SELECT * FROM mz_source

# We want to verify that the second subsource won't produce a snapshot but we
# can't do anything better other than sleep for while and check that the upper
# remains at 0. Given the tiny amount of data we'll give just a few seconds
# after we have already confirmed that the first subsource has completed its
# snapshot.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

> SELECT write_frontier FROM mz_internal.mz_frontiers f JOIN mz_objects o ON f.object_id = o.id WHERE o.name = 'two';
0
