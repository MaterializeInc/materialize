# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SET TRANSACTION_ISOLATION = 'STRICT SERIALIZABLE';
> SET REAL_TIME_RECENCY TO TRUE

$ kafka-ingest topic=input_1 format=bytes repeat=500000
A,B,0

$ kafka-ingest topic=input_2 format=bytes repeat=500000
A,B,0

$ kafka-ingest topic=input_1 format=bytes repeat=500001
A,B,0

$ kafka-ingest topic=input_2 format=bytes repeat=500002
A,B,0
