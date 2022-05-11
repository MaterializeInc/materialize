# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> select * from csv
al  sia     mz_offset
---------------------
id  value   1
1   person  2

$ kafka-ingest topic=csv-basic format=bytes
2,animal

> select * from csv
al  sia     mz_offset
---------------------
id  value   1
1   person  2
2   animal  3
