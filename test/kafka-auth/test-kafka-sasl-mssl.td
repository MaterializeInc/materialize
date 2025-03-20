# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# ==> Set up. <==

$ set-from-file ca-crt=/share/secrets/ca.crt
$ set-from-file kafka-crt=/share/secrets/materialized-kafka.crt
$ set-from-file kafka-key=/share/secrets/materialized-kafka.key
$ set-from-file kafka1-crt=/share/secrets/materialized-kafka1.crt
$ set-from-file kafka1-key=/share/secrets/materialized-kafka1.key

> CREATE SECRET kafka_key AS '${kafka-key}'
> CREATE SECRET kafka1_key AS '${kafka1-key}'
> CREATE SECRET password AS 'sekurity';
> CREATE SECRET password_wrong AS 'wrong';

$ kafka-create-topic topic=text-data
$ kafka-ingest topic=text-data format=bytes
banana

# ==> Test invalid configurations. <==

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9097',
    SASL MECHANISMS 'PLAIN',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SSL CERTIFICATE AUTHORITY = '${ca-crt}'
  )
contains:ssl/tls alert bad certificate

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9097',
    SASL MECHANISMS 'PLAIN',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SSL CERTIFICATE '${kafka1-crt}',
    SSL KEY SECRET kafka1_key,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )
contains:ssl/tls alert certificate unknown

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9097',
    SASL MECHANISMS 'PLAIN',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password_wrong,
    SSL CERTIFICATE '${kafka-crt}',
    SSL KEY SECRET kafka_key,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )
contains:Invalid username or password

# ==> Test without an SSH tunnel. <==

> CREATE CONNECTION kafka TO KAFKA (
    BROKER 'kafka:9097',
    SASL MECHANISMS 'PLAIN',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SSL CERTIFICATE '${kafka-crt}',
    SSL KEY SECRET kafka_key,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )

> CREATE SOURCE text_data FROM KAFKA CONNECTION kafka (
    TOPIC 'testdrive-text-data-${testdrive.seed}'
  )

> CREATE TABLE text_data_tbl FROM SOURCE text_data (REFERENCE "testdrive-text-data-${testdrive.seed}") FORMAT TEXT

> SELECT * FROM text_data_tbl
banana

# ==> Test with an SSH tunnel. <==

> CREATE CONNECTION kafka_ssh TO KAFKA (
    BROKER 'kafka:9097' USING SSH TUNNEL testdrive_no_reset_connections.public.ssh,
    SASL MECHANISMS 'PLAIN',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SSL CERTIFICATE '${kafka-crt}',
    SSL KEY SECRET kafka_key,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )

> CREATE SOURCE text_data_ssh FROM KAFKA CONNECTION kafka_ssh (
    TOPIC 'testdrive-text-data-${testdrive.seed}'
  )

> CREATE TABLE text_data_ssh_tbl FROM SOURCE text_data_ssh (REFERENCE "testdrive-text-data-${testdrive.seed}") FORMAT TEXT

> SELECT * FROM text_data_ssh_tbl
banana
