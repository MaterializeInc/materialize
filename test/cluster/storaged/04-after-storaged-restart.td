# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify that the data ingested while `storaged` was down eventually appears,
# then try ingesting new data.

> SELECT * from remote1
one
two
three
> SELECT * from remote2
one
two
three

$ kafka-ingest format=bytes topic=remote1
four
$ kafka-ingest format=bytes topic=remote2
four

> SELECT * from remote1
one
two
three
four
> SELECT * from remote2
one
two
three
four
