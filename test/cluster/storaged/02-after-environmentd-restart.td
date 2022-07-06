# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify that the data ingested while `environmentd` was offline is present,
# then try ingesting more data.

> SELECT * from remote1
one  1
> SELECT * from remote2
one  1

$ kafka-ingest format=bytes topic=remote1
two
$ kafka-ingest format=bytes topic=remote2
two

> SELECT * from remote1
one   1
two   2
> SELECT * from remote2
one  1
two  2
