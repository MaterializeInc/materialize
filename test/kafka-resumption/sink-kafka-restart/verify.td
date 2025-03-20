# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> INSERT INTO t VALUES ('C', 3);

$ kafka-verify-data format=avro sink=materialize.public.output sort-messages=true
{"before": null, "after": {"row": {"c1": {"string": "C"}, "c2": {"int": 3}}}}
