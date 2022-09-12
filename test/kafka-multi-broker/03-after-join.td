# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Ingest some more data and read it back
#

$ set-regex match=\d{13} replacement=<TIMESTAMP>

$ set schema={"type": "record", "name": "schema", "fields": [ {"name": "f1", "type": "string" } ] }

$ kafka-ingest format=avro topic=kafka-multi-broker schema=${schema} timestamp=21
{"f1": "04-01" }

$ kafka-ingest format=avro topic=kafka-multi-broker schema=${schema} timestamp=22
{"f1": "04-02" }

$ kafka-ingest format=avro topic=kafka-multi-broker schema=${schema} timestamp=23
{"f1": "04-03" }

> SELECT f1 FROM kafka_multi_broker
01-01
01-02
01-03
02-01
02-02
02-03
03-01
03-02
03-03
04-01
04-02
04-03

$ kafka-verify format=avro sink=materialize.public.multi_broker_sink sort-messages=true
{"before": null, "after": {"row": {"f1": "01-01" }}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"f1": "01-02" }}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"f1": "01-03" }}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"f1": "02-01" }}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"f1": "02-02" }}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"f1": "02-03" }}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"f1": "03-01" }}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"f1": "03-02" }}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"f1": "03-03" }}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"f1": "04-01" }}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"f1": "04-02" }}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row": {"f1": "04-03" }}, "transaction": {"id": "<TIMESTAMP>"}}
