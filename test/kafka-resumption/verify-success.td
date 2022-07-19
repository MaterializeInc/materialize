# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-ingest topic=input format=bytes
Hanover,PA,17331

$ set-regex match=\d{13} replacement=<TIMESTAMP>

$ kafka-verify format=avro sink=materialize.public.output sort-messages=true
{"before": null, "after": {"row": {"city": "Brooklyn", "state": "NY", "zip": "11217", "offset":4}},"transaction":{"id":"<TIMESTAMP>"}}
{"before": null, "after": {"row": {"city": "Hanover", "state": "PA", "zip": "17331", "offset":5}},"transaction":{"id":"<TIMESTAMP>"}}
{"before": null, "after": {"row": {"city": "New York", "state": "NY", "zip": "10004", "offset":2}},"transaction":{"id":"<TIMESTAMP>"}}
{"before": null, "after": {"row": {"city": "Rochester", "state": "NY", "zip": "14618", "offset":1}},"transaction":{"id":"<TIMESTAMP>"}}
{"before": null, "after": {"row": {"city": "San Francisco", "state": "CA", "zip": "94114", "offset":3}},"transaction":{"id":"<TIMESTAMP>"}}
