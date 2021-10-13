# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> INSERT INTO avro_ocf_table VALUES (3);

# Make sure the OCF file has been created and populated before attempting to run $ avro-ocf-verify on it
# As there is no way to wait precisely until that occurs, we have no option but to use mz_sleep()
> SELECT mz_internal.mz_sleep(2);
<null>

$ avro-ocf-verify sink=materialize.public.avro_ocf_sink
{"before": null, "after": {"row": {"f1": 3}}}

> DROP SINK avro_ocf_sink;

> DROP TABLE avro_ocf_table;
