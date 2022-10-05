# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-ingest format=bytes topic=topic
four
five
six

> SELECT * from topic
data         offset
-----------------------------------
one          0
two          1
three        2
four         3
five         4
six          5
