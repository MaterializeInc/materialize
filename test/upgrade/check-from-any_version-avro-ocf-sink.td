# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> INSERT INTO avro_ocf_table VALUES (3);

# The restart has caused a new .ocf file to be created
# Mz and $ avro-ocf-verify have no memory of the previous file
# so we only verify that the record we just inserted in the source
# appears in the sink
$ avro-ocf-verify sink=materialize.public.avro_ocf_sink
{"before": null, "after": {"row": {"f1": 3}}}

> DROP SINK avro_ocf_sink;

> DROP TABLE avro_ocf_table;
