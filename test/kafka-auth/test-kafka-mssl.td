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
> CREATE SECRET garbage_key AS 'garbage'

$ kafka-create-topic topic=text-data
$ kafka-ingest topic=text-data format=bytes
banana

# Test invalid configurations.

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9094',
    SSL CERTIFICATE AUTHORITY = '${ca-crt}'
  )
contains:ssl/tls alert bad certificate

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9094',
    SSL CERTIFICATE '${kafka1-crt}',
    SSL KEY SECRET kafka1_key,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )
contains:ssl/tls alert certificate unknown

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9094',
    SSL CERTIFICATE '${kafka-crt}',
    SSL KEY SECRET kafka1_key,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )
contains:x509 certificate routines::key values mismatch

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9094',
    SSL CERTIFICATE '${kafka-crt}',
    SSL KEY SECRET garbage_key,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )
contains:ssl.key.pem failed: not in PEM format?

# We don't test invalid CAs as they are well covered by test-kafka-ssl.td.

# ==> Test without an SSH tunnel. <==

> CREATE CONNECTION kafka TO KAFKA (
    BROKER 'kafka:9094',
    SSL CERTIFICATE '${kafka-crt}',
    SSL KEY SECRET kafka_key,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )

> BEGIN
> CREATE SOURCE text_data FROM KAFKA CONNECTION kafka (
    TOPIC 'testdrive-text-data-${testdrive.seed}'
  )

> CREATE TABLE text_data_tbl FROM SOURCE text_data (REFERENCE "testdrive-text-data-${testdrive.seed}") FORMAT TEXT
> COMMIT

> SELECT * FROM text_data_tbl
banana

# ==> Test with an SSH tunnel. <==

> CREATE CONNECTION kafka_ssh TO KAFKA (
    BROKER 'kafka:9094' USING SSH TUNNEL testdrive_no_reset_connections.public.ssh,
    SSL CERTIFICATE '${kafka-crt}',
    SSL KEY SECRET kafka_key,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )

> BEGIN
> CREATE SOURCE text_data_ssh FROM KAFKA CONNECTION kafka_ssh (
    TOPIC 'testdrive-text-data-${testdrive.seed}'
  )

> CREATE TABLE text_data_ssh_tbl FROM SOURCE text_data_ssh (REFERENCE "testdrive-text-data-${testdrive.seed}") FORMAT TEXT
> COMMIT

> SELECT * FROM text_data_ssh_tbl
banana
