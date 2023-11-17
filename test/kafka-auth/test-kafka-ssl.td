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
  ) FORMAT TEXT

> SELECT * FROM text_data
banana

# ==> Test with an SSH tunnel. <==

> CREATE CONNECTION kafka_ssh TO KAFKA (
    BROKER 'kafka:9093' USING SSH TUNNEL testdrive_no_reset_connections.public.ssh,
    SSL CERTIFICATE AUTHORITY '${ca-crt}'
  )

> CREATE SOURCE text_data_ssh FROM KAFKA CONNECTION kafka_ssh (
    TOPIC 'testdrive-text-data-${testdrive.seed}'
  ) FORMAT TEXT

> SELECT * FROM text_data_ssh
banana
