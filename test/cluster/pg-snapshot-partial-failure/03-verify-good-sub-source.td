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

$ set-regex match=(\d{13}|u\d{1,3}|\(\d+-\d\d-\d\d\s\d\d:\d\d:\d\d.\d\d\d\)) replacement=<>

# Note that the above regex does NOT match timestamps that are `0`, and also that we check `can respond immediately` == `false`.
# We are explicitly checking that the _upper_ of `two` has moved, but we see that we can't actually query any
# data out of it yet.
> EXPLAIN TIMESTAMP FOR SELECT * FROM two
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: false\n                       timeline: Some(EpochMilliseconds)\n\nsource materialize.public.two (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n"

# Here we note that the top-level source is also queryable,...
> EXPLAIN TIMESTAMP FOR SELECT * FROM mz_source
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: true\n                       timeline: Some(EpochMilliseconds)\n\nsource materialize.public.mz_source (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n"
