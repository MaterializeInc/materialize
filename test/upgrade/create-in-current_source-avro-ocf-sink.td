# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE TABLE avro_ocf_table (f1 INTEGER NOT NULL);

> CREATE SINK avro_ocf_sink FROM avro_ocf_table INTO AVRO OCF '${testdrive.temp-dir}/avro_ocf_sink.ocf'
