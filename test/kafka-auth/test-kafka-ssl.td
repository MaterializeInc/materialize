# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_default_connection_validation = true
ALTER SYSTEM SET enable_connection_validation_syntax = true

# ==> Set up. <==

$ set-from-file ca-crt=/share/secrets/ca.crt
$ set-from-file ca-selective-crt=/share/secrets/ca-selective.crt

$ kafka-create-topic topic=text-data
$ kafka-ingest topic=text-data format=bytes
banana

# ==> Test invalid configurations. <==

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9093',
    SECURITY PROTOCOL PLAINTEXT
  )
contains:Disconnected during handshake; broker might require SSL encryption

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9093'
    -- SECURITY PROTOCOL defaults to SSL when no SASL options are specified.
  )
contains:Invalid CA certificate

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9093',
    SSL CERTIFICATE AUTHORITY = '${ca-selective-crt}'
  )
contains:Invalid CA certificate

! CREATE CONNECTION kafka_invalid TO KAFKA (
    BROKER 'kafka:9093',
    SSL CERTIFICATE AUTHORITY = 'this is garbage'
  )
contains:ssl.ca.pem failed: not in PEM format?

# ==> Test without an SSH tunnel. <==

> CREATE CONNECTION kafka TO KAFKA (
    BROKER 'kafka:9093',
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )

> CREATE SOURCE text_data FROM KAFKA CONNECTION kafka (
    TOPIC 'testdrive-text-data-${testdrive.seed}'
  )

> CREATE TABLE text_data_tbl (a)
  FROM SOURCE text_data (REFERENCE "testdrive-text-data-${testdrive.seed}")
  FORMAT TEXT

> SELECT * FROM text_data_tbl
banana

# ==> Test with an SSH tunnel. <==

> CREATE CONNECTION kafka_ssh TO KAFKA (
    BROKER 'kafka:9093' USING SSH TUNNEL testdrive_no_reset_connections.public.ssh,
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

# ALTER CONNECTION

# ALTER CONNECTION for Kafka

! ALTER CONNECTION kafka SET (SSL KEY = 'x') WITH (VALIDATE = true);
contains:invalid SSL KEY: must provide a secret value

> CREATE SECRET IF NOT EXISTS invalid_secret AS 'x'

! ALTER CONNECTION kafka SET (SSL KEY = SECRET invalid_secret) WITH (VALIDATE = true);
contains:option SSL KEY not supported with this configuration

! ALTER CONNECTION kafka SET (SSL CERTIFICATE = 'x') WITH (VALIDATE = true);
contains:SSL KEY must be specified with SSL CERTIFICATE

! ALTER CONNECTION kafka SET (SSL KEY = SECRET invalid_secret), SET (SSL CERTIFICATE = 'x') WITH (VALIDATE = true);
contains:Client creation error

! ALTER CONNECTION kafka SET (SSL CERTIFICATE AUTHORITY = 'x') WITH (VALIDATE = true);
contains:Client creation error

> ALTER CONNECTION kafka RESET (SSL KEY) WITH (VALIDATE = true);

> ALTER CONNECTION kafka RESET (SSL CERTIFICATE) WITH (VALIDATE = true);

! ALTER CONNECTION kafka RESET (SSL CERTIFICATE AUTHORITY) WITH (VALIDATE = true);
contains:Invalid CA certificate

! ALTER CONNECTION kafka RESET (SSL KEY), RESET (SSL CERTIFICATE), RESET (SSL CERTIFICATE AUTHORITY) WITH (VALIDATE = true);
contains:Invalid CA certificate

> ALTER CONNECTION kafka RESET (SSL KEY), RESET (SSL CERTIFICATE), RESET (SSL CERTIFICATE AUTHORITY) WITH (VALIDATE = false);

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'text_data';
stalled

> ALTER CONNECTION kafka
    DROP (SSL KEY),
    DROP (SSL CERTIFICATE),
    SET (SSL CERTIFICATE AUTHORITY '${ca-crt}');

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'text_data';
running

# ALTER CONNECTION for Kafka + SSH

! ALTER CONNECTION testdrive_no_reset_connections.public.ssh SET (HOST = 'abcd') WITH (VALIDATE = true);
contains:failed to lookup address information

! ALTER CONNECTION testdrive_no_reset_connections.public.ssh RESET (HOST);
contains:HOST option is required

! ALTER CONNECTION testdrive_no_reset_connections.public.ssh SET (USER = 'abcd') WITH (VALIDATE = true);
contains:Permission denied

! ALTER CONNECTION testdrive_no_reset_connections.public.ssh RESET (USER);
contains:invalid ALTER CONNECTION: USER option is required

! ALTER CONNECTION testdrive_no_reset_connections.public.ssh SET (PORT = 1) WITH (VALIDATE = true);
contains:Connection refused

#Break SSH connection via host

> ALTER CONNECTION testdrive_no_reset_connections.public.ssh SET (HOST = 'abcd') WITH (VALIDATE = false);

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'text_data_ssh';
stalled

> ALTER CONNECTION testdrive_no_reset_connections.public.ssh SET (HOST = 'ssh-bastion-host') WITH (VALIDATE = true);

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'text_data_ssh';
running

# Break SSH connection via port

> ALTER CONNECTION testdrive_no_reset_connections.public.ssh SET (PORT = 1) WITH (VALIDATE = false);

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'text_data_ssh';
stalled

> ALTER CONNECTION testdrive_no_reset_connections.public.ssh RESET (PORT) WITH (VALIDATE = true);

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'text_data_ssh';
running

# Swap out SSH connection

> SELECT COUNT(*) FROM mz_ssh_tunnel_connections
2

> SELECT COUNT(*) FROM mz_internal.mz_object_dependencies WHERE referenced_object_id = 'u1';
1

! DROP CONNECTION testdrive_no_reset_connections.public.ssh;
contains:still depended upon by connection "kafka_ssh"

> SELECT COUNT(*) FROM mz_internal.mz_object_dependencies WHERE referenced_object_id = 'u1';
1

> ALTER CONNECTION kafka_ssh SET (BROKER 'kafka:9093' USING SSH TUNNEL testdrive_no_reset_connections.public.ssh_backup);

# We've removed all dependencies on testdrive_no_reset_connections.public.ssh, so it could be dropped
> SELECT COUNT(*) FROM mz_internal.mz_object_dependencies WHERE referenced_object_id = 'u1';
0

# Break new SSH tunnel to show that we can fix it

> ALTER CONNECTION testdrive_no_reset_connections.public.ssh_backup SET (PORT = 1) WITH (VALIDATE = false);

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'text_data_ssh';
stalled

$ kafka-ingest topic=text-data format=bytes
papaya

> ALTER CONNECTION testdrive_no_reset_connections.public.ssh_backup RESET (PORT) WITH (VALIDATE = true);

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'text_data_ssh';
running

> SELECT * FROM text_data_tbl
banana
papaya
