# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-create-topic topic=csv
$ kafka-ingest topic=csv format=bytes
id,value
1,person

>[version>=7800] CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (
    BROKER '${testdrive.kafka-addr}',
    SECURITY PROTOCOL PLAINTEXT
  )

>[version<7800] CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (
    BROKER '${testdrive.kafka-addr}'
  )

> CREATE SOURCE csv_upgrade_no_header
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-csv-${testdrive.seed}')
  FORMAT CSV WITH 2 COLUMNS

> CREATE SOURCE csv_upgrade_no_header_alias (al, sia)
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-csv-${testdrive.seed}')
  FORMAT CSV WITH 2 COLUMNS
