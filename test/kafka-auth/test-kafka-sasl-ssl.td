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
$ set-from-file ca-selective-crt=/share/secrets/ca-selective.crt

> CREATE SECRET password AS 'sekurity';
> CREATE SECRET password_wrong AS 'wrong';

$ kafka-create-topic topic=text-data
$ kafka-ingest topic=text-data format=bytes
banana

# ==> Test invalid configurations. <==

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9096',
    SASL MECHANISMS 'PLAIN',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SECURITY PROTOCOL SASL_PLAINTEXT
  )
contains:Disconnected: connection closed by peer: receive 0 after POLLIN

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9096',
    SECURITY PROTOCOL SASL_SSL
  )
contains:SASL MECHANISMS must be specified

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9096',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )
contains:SASL MECHANISMS must be specified

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9096',
    SASL MECHANISMS 'PLAIN',
    SASL PASSWORD SECRET password
  )
contains:SASL USERNAME must be specified

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9096',
    SASL MECHANISMS 'PLAIN',
    SASL USERNAME 'materialize',
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )
contains:SASL PASSWORD must be specified

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9096',
    SASL MECHANISMS 'GSSAPI',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )
contains:No provider for SASL mechanism GSSAPI

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9096',
    SASL MECHANISMS 'GARBAGE',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )
contains:Unsupported SASL mechanism: GARBAGE

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9096',
    SASL MECHANISMS 'PLAIN',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password_wrong,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )
contains:Invalid username or password

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9096',
    SASL MECHANISMS 'PLAIN',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password
  )
contains:Invalid CA certificate

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9096',
    SASL MECHANISMS 'PLAIN',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SSL CERTIFICATE AUTHORITY = '${ca-selective-crt}'
  )
contains:Invalid CA certificate

# ==> Test without an SSH tunnel. <==

> CREATE CONNECTION kafka_plain TO KAFKA (
    BROKER 'kafka:9096',
    SASL MECHANISMS 'PLAIN',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )

> BEGIN
> CREATE SOURCE text_data_plain FROM KAFKA CONNECTION kafka_plain (
    TOPIC 'testdrive-text-data-${testdrive.seed}'
  )

> CREATE TABLE text_data_plain_tbl FROM SOURCE text_data_plain (REFERENCE "testdrive-text-data-${testdrive.seed}") FORMAT TEXT
> COMMIT

> SELECT * FROM text_data_plain_tbl
banana

> CREATE CONNECTION kafka_scram_sha_256 TO KAFKA (
    BROKER 'kafka:9096',
    SASL MECHANISMS 'SCRAM-SHA-256',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )

> BEGIN
> CREATE SOURCE text_data_scram_sha_256 FROM KAFKA CONNECTION kafka_scram_sha_256 (
    TOPIC 'testdrive-text-data-${testdrive.seed}'
  )

> CREATE TABLE text_data_scram_sha_256_tbl FROM SOURCE text_data_scram_sha_256 (REFERENCE "testdrive-text-data-${testdrive.seed}") FORMAT TEXT
> COMMIT

> SELECT * FROM text_data_scram_sha_256_tbl
banana

> CREATE CONNECTION kafka_scram_sha_512 TO KAFKA (
    BROKER 'kafka:9096',
    SASL MECHANISMS 'SCRAM-SHA-512',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )

> BEGIN
> CREATE SOURCE text_data_scram_sha_512 FROM KAFKA CONNECTION kafka_scram_sha_512 (
    TOPIC 'testdrive-text-data-${testdrive.seed}'
  )

> CREATE TABLE text_data_scram_sha_512_tbl FROM SOURCE text_data_scram_sha_512 (REFERENCE "testdrive-text-data-${testdrive.seed}") FORMAT TEXT
> COMMIT

> SELECT * FROM text_data_scram_sha_256_tbl
banana

# ==> Test with an SSH tunnel. <==

> CREATE CONNECTION kafka_ssh_plain TO KAFKA (
    BROKER 'kafka:9096' USING SSH TUNNEL testdrive_no_reset_connections.public.ssh,
    SASL MECHANISMS 'PLAIN',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )

> BEGIN
> CREATE SOURCE text_data_ssh_plain FROM KAFKA CONNECTION kafka_ssh_plain (
    TOPIC 'testdrive-text-data-${testdrive.seed}'
  )

> CREATE TABLE text_data_ssh_plain_tbl FROM SOURCE text_data_ssh_plain (REFERENCE "testdrive-text-data-${testdrive.seed}") FORMAT TEXT
> COMMIT

> SELECT * FROM text_data_ssh_plain_tbl
banana

> CREATE CONNECTION kafka_ssh_scram_sha_256 TO KAFKA (
    BROKER 'kafka:9096' USING SSH TUNNEL testdrive_no_reset_connections.public.ssh,
    SASL MECHANISMS 'SCRAM-SHA-256',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )

> BEGIN
> CREATE SOURCE text_data_ssh_scram_sha_256 FROM KAFKA CONNECTION kafka_ssh_scram_sha_256 (
    TOPIC 'testdrive-text-data-${testdrive.seed}'
  )

> CREATE TABLE text_data_ssh_scram_sha_256_tbl FROM SOURCE text_data_ssh_scram_sha_256 (REFERENCE "testdrive-text-data-${testdrive.seed}") FORMAT TEXT
> COMMIT

> SELECT * FROM text_data_ssh_scram_sha_256_tbl
banana

> CREATE CONNECTION kafka_ssh_scram_sha_512 TO KAFKA (
    BROKER 'kafka:9096' USING SSH TUNNEL testdrive_no_reset_connections.public.ssh,
    SASL MECHANISMS 'SCRAM-SHA-512',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )

> BEGIN
> CREATE SOURCE text_data_ssh_scram_sha_512 FROM KAFKA CONNECTION kafka_ssh_scram_sha_512 (
    TOPIC 'testdrive-text-data-${testdrive.seed}'
  )

> CREATE TABLE text_data_ssh_scram_sha_512_tbl FROM SOURCE text_data_ssh_scram_sha_512 (REFERENCE "testdrive-text-data-${testdrive.seed}") FORMAT TEXT
> COMMIT

> SELECT * FROM text_data_ssh_scram_sha_512_tbl
banana
