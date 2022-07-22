# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify that the data ingested before `storaged` crashed is still present but
# that newly ingested data does not appear.

> SET transaction_isolation = serializable

> SELECT * from remote1
one
two
> SELECT * from remote2
one
two

$ kafka-ingest format=bytes topic=remote1
three
$ kafka-ingest format=bytes topic=remote2
three

> SELECT * from remote1
one
two
> SELECT * from remote2
one
two

> SET transaction_isolation = 'strict serializable'
