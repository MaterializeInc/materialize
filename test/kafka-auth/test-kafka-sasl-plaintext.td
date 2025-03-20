# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# ==> Set up. <==

> CREATE SECRET password AS 'sekurity';
> CREATE SECRET password_wrong AS 'wrong';

$ kafka-create-topic topic=text-data
$ kafka-ingest topic=text-data format=bytes
banana

# ==> Test invalid configurations. <==

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9095',
    SASL MECHANISMS 'PLAIN',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password
    -- SECURITY PROTOCOL defaults to SASL_SSL when other SASL options are
    -- specified.
  )
contains:Broker does not support SSL connections

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9095',
    SASL MECHANISMS 'PLAIN',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SECURITY PROTOCOL SASL_SSL
  )
contains:Broker does not support SSL connections

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9095',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SECURITY PROTOCOL SASL_PLAINTEXT
  )
contains:SASL MECHANISMS must be specified

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9095',
    SASL MECHANISMS 'PLAIN',
    SASL PASSWORD SECRET password,
    SECURITY PROTOCOL SASL_PLAINTEXT
  )
contains:SASL USERNAME must be specified

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9095',
    SASL MECHANISMS 'PLAIN',
    SASL USERNAME 'materialize',
    SECURITY PROTOCOL SASL_PLAINTEXT
  )
contains:SASL PASSWORD must be specified

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9095',
    SASL MECHANISMS 'GSSAPI',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SECURITY PROTOCOL SASL_PLAINTEXT
  )
contains:No provider for SASL mechanism GSSAPI

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9095',
    SASL MECHANISMS 'GARBAGE',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SECURITY PROTOCOL SASL_PLAINTEXT
  )
contains:Unsupported SASL mechanism: GARBAGE

! CREATE CONNECTION kafka_plain TO KAFKA (
    BROKER 'kafka:9095',
    SASL MECHANISMS 'PLAIN',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password_wrong,
    SECURITY PROTOCOL SASL_PLAINTEXT
  )
contains:Invalid username or password

# ==> Test without an SSH tunnel. <==

> CREATE CONNECTION kafka_plain TO KAFKA (
    BROKER 'kafka:9095',
    SASL MECHANISMS 'PLAIN',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SECURITY PROTOCOL SASL_PLAINTEXT
  )

> CREATE SOURCE text_data_plain FROM KAFKA CONNECTION kafka_plain (
    TOPIC 'testdrive-text-data-${testdrive.seed}'
  )

> CREATE TABLE text_data_plain_tbl FROM SOURCE text_data_plain (REFERENCE "testdrive-text-data-${testdrive.seed}") FORMAT TEXT

> SELECT * FROM text_data_plain_tbl
banana

> CREATE CONNECTION kafka_scram_sha_256 TO KAFKA (
    BROKER 'kafka:9095',
    SASL MECHANISMS 'SCRAM-SHA-256',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SECURITY PROTOCOL SASL_PLAINTEXT
  )

> CREATE SOURCE text_data_scram_sha_256 FROM KAFKA CONNECTION kafka_scram_sha_256 (
    TOPIC 'testdrive-text-data-${testdrive.seed}'
  )

> CREATE TABLE text_data_scram_sha_256_tbl FROM SOURCE text_data_scram_sha_256 (REFERENCE "testdrive-text-data-${testdrive.seed}") FORMAT TEXT

> SELECT * FROM text_data_scram_sha_256_tbl
banana

> CREATE CONNECTION kafka_scram_sha_512 TO KAFKA (
    BROKER 'kafka:9095',
    SASL MECHANISMS 'SCRAM-SHA-512',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SECURITY PROTOCOL SASL_PLAINTEXT
  )

> CREATE SOURCE text_data_scram_sha_512 FROM KAFKA CONNECTION kafka_scram_sha_512 (
    TOPIC 'testdrive-text-data-${testdrive.seed}'
  )

> CREATE TABLE text_data_scram_sha_512_tbl FROM SOURCE text_data_scram_sha_512 (REFERENCE "testdrive-text-data-${testdrive.seed}") FORMAT TEXT

> SELECT * FROM text_data_scram_sha_256_tbl
banana

# ==> Test with an SSH tunnel. <==

> CREATE CONNECTION kafka_ssh_plain TO KAFKA (
    BROKER 'kafka:9095' USING SSH TUNNEL testdrive_no_reset_connections.public.ssh,
    SASL MECHANISMS 'PLAIN',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SECURITY PROTOCOL SASL_PLAINTEXT
  )

> CREATE SOURCE text_data_ssh_plain FROM KAFKA CONNECTION kafka_ssh_plain (
    TOPIC 'testdrive-text-data-${testdrive.seed}'
  )

> CREATE TABLE text_data_ssh_plain_tbl FROM SOURCE text_data_ssh_plain (REFERENCE "testdrive-text-data-${testdrive.seed}") FORMAT TEXT

> SELECT * FROM text_data_ssh_plain_tbl
banana

> CREATE CONNECTION kafka_ssh_scram_sha_256 TO KAFKA (
    BROKER 'kafka:9095' USING SSH TUNNEL testdrive_no_reset_connections.public.ssh,
    SASL MECHANISMS 'SCRAM-SHA-256',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SECURITY PROTOCOL SASL_PLAINTEXT
  )

> CREATE SOURCE text_data_ssh_scram_sha_256 FROM KAFKA CONNECTION kafka_ssh_scram_sha_256 (
    TOPIC 'testdrive-text-data-${testdrive.seed}'
  )

> CREATE TABLE text_data_ssh_scram_sha_256_tbl FROM SOURCE text_data_ssh_scram_sha_256 (REFERENCE "testdrive-text-data-${testdrive.seed}") FORMAT TEXT

> SELECT * FROM text_data_ssh_scram_sha_256_tbl
banana

> CREATE CONNECTION kafka_ssh_scram_sha_512 TO KAFKA (
    BROKER 'kafka:9095' USING SSH TUNNEL testdrive_no_reset_connections.public.ssh,
    SASL MECHANISMS 'SCRAM-SHA-512',
    SASL USERNAME 'materialize',
    SASL PASSWORD SECRET password,
    SECURITY PROTOCOL SASL_PLAINTEXT
  )

> CREATE SOURCE text_data_ssh_scram_sha_512 FROM KAFKA CONNECTION kafka_ssh_scram_sha_512 (
    TOPIC 'testdrive-text-data-${testdrive.seed}'
  )

> CREATE TABLE text_data_ssh_scram_sha_512_tbl FROM SOURCE text_data_ssh_scram_sha_512 (REFERENCE "testdrive-text-data-${testdrive.seed}") FORMAT TEXT

> SELECT * FROM text_data_ssh_scram_sha_512_tbl
banana
