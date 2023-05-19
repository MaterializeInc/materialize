# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


# Validate existing data
> SELECT * from failpoint
key           text
-------------------
fish          fish
bird          goose

# Validate ingestion can continue
$ kafka-ingest format=bytes topic=failpoint key-format=bytes key-terminator=:
mammal:moose


> SELECT * from failpoint
key           text
-------------------
fish          fish
bird          goose
mammal        moose
