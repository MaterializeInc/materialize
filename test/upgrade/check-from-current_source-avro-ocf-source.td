# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ avro-ocf-append path=avro_ocf_source.ocf
{"a": 9, "b": 10}
{"a": 11, "b": 12}

> SELECT * FROM avro_ocf_source;
1 2 1
3 4 2
5 6 3
7 8 4
9 10 5
11 12 6
