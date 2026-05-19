# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-storage-size=scale=1,workers=1
$ set-arg-default single-replica-cluster=quickstart

#
# Test enforcement that connections are made to global non-private IPs

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_connection_validation_syntax = true
ALTER SYSTEM SET storage_enforce_external_addresses = true

# Setup kafka topic with schema
# must be a subset of the keys in the rows
$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "id", "type": "long"}
    ]
  }

$ set schema={
    "type" : "record",
    "name" : "envelope",
    "fields" : [
      {
        "name": "before",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [
              {
                  "name": "id",
                  "type": "long"
              },
              {
                "name": "creature",
                "type": "string"
              }]
           },
           "null"
         ]
      },
      {
        "name": "after",
        "type": ["row", "null"]
      }
    ]
  }

$ kafka-create-topic topic=csr_test partitions=1

$ kafka-ingest format=avro topic=csr_test key-format=avro key-schema=${keyschema} schema=${schema} timestamp=1
{"id": 1} {"before": {"row": {"id": 1, "creature": "fish"}}, "after": {"row": {"id": 1, "creature": "mudskipper"}}}
{"id": 1} {"before": {"row": {"id": 1, "creature": "mudskipper"}}, "after": {"row": {"id": 1, "creature": "salamander"}}}
{"id": 1} {"before": {"row": {"id": 1, "creature": "salamander"}}, "after": {"row": {"id": 1, "creature": "lizard"}}}


! CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  ) WITH (VALIDATE = true);
contains:Address resolved to a private IP

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_copy_from_remote = true

> CREATE TABLE copy_ssrf_target (a text, b text)

# COPY FROM rejects non-http(s) schemes outright.
! COPY INTO copy_ssrf_target FROM 'file:///etc/passwd' (FORMAT CSV);
contains:only 'http://' and 'https://' urls are supported

! COPY INTO copy_ssrf_target FROM 'ftp://example.com/file.csv' (FORMAT CSV);
contains:only 'http://' and 'https://' urls are supported

# IPv4 / IPv6 loopback literals bypass reqwest's DNS resolver and must be
# caught by the upfront IP-literal check.
! COPY INTO copy_ssrf_target FROM 'http://127.0.0.1:1/file.csv' (FORMAT CSV);
contains:Address resolved to a private IP

! COPY INTO copy_ssrf_target FROM 'http://[::1]:1/file.csv' (FORMAT CSV);
contains:Address resolved to a private IP

# IPv4-mapped IPv6 loopback (`::ffff:127.0.0.1`): on dual-stack sockets this
# reaches loopback, so it must be rejected as IPv4 loopback.
! COPY INTO copy_ssrf_target FROM 'http://[::ffff:127.0.0.1]:1/file.csv' (FORMAT CSV);
contains:Address resolved to a private IP

# Cloud instance metadata service is the canonical SSRF target. 169.254/16 is
# link-local and must be rejected.
! COPY INTO copy_ssrf_target FROM 'http://169.254.169.254/latest/meta-data/' (FORMAT CSV);
contains:Address resolved to a private IP

# Alternative encodings of 169.254.169.254. The WHATWG URL Standard normalizes
# decimal-integer, hex, octal, and mixed-notation IPv4 literals, so they all
# resolve to the same link-local address and must be blocked.
! COPY INTO copy_ssrf_target FROM 'http://2852039166/' (FORMAT CSV);
contains:Address resolved to a private IP

! COPY INTO copy_ssrf_target FROM 'http://0xa9fea9fe/' (FORMAT CSV);
contains:Address resolved to a private IP

! COPY INTO copy_ssrf_target FROM 'http://025177524776/' (FORMAT CSV);
contains:Address resolved to a private IP

! COPY INTO copy_ssrf_target FROM 'http://0251.0376.0251.0376/' (FORMAT CSV);
contains:Address resolved to a private IP

! COPY INTO copy_ssrf_target FROM 'http://0xa9.0xfe.0xa9.0xfe/' (FORMAT CSV);
contains:Address resolved to a private IP

! COPY INTO copy_ssrf_target FROM 'http://169.16689662/' (FORMAT CSV);
contains:Address resolved to a private IP

! COPY INTO copy_ssrf_target FROM 'http://169.254.0xa9fe/' (FORMAT CSV);
contains:Address resolved to a private IP

# IPv6-mapped forms of the metadata IP
! COPY INTO copy_ssrf_target FROM 'http://[::ffff:a9fe:a9fe]/' (FORMAT CSV);
contains:Address resolved to a private IP

! COPY INTO copy_ssrf_target FROM 'http://[::ffff:169.254.169.254]/' (FORMAT CSV);
contains:Address resolved to a private IP

# EC2 metadata alternate IPv6 address (falls in fc00::/7 ULA range)
! COPY INTO copy_ssrf_target FROM 'http://[fd00:ec2::254]/' (FORMAT CSV);
contains:Address resolved to a private IP

# Hostname resolving to loopback exercises the reqwest custom DNS resolver
# path (as opposed to the upfront IP-literal check above).
! COPY INTO copy_ssrf_target FROM 'http://localhost:1/file.csv' (FORMAT CSV);
contains:Address resolved to a private IP

> CREATE SECRET aws_ssrf_secret AS 'unused'

! CREATE CONNECTION aws_ssrf_endpoint_v4 TO AWS (
    ACCESS KEY ID = 'unused',
    SECRET ACCESS KEY = SECRET aws_ssrf_secret,
    ENDPOINT = 'http://127.0.0.1:1/',
    REGION = 'us-east-1'
  ) WITH (VALIDATE = true);
contains:Address resolved to a private IP

! CREATE CONNECTION aws_ssrf_endpoint_v6 TO AWS (
    ACCESS KEY ID = 'unused',
    SECRET ACCESS KEY = SECRET aws_ssrf_secret,
    ENDPOINT = 'http://[::1]/',
    REGION = 'us-east-1'
  ) WITH (VALIDATE = true);
contains:Address resolved to a private IP

! CREATE CONNECTION aws_ssrf_endpoint_mapped TO AWS (
    ACCESS KEY ID = 'unused',
    SECRET ACCESS KEY = SECRET aws_ssrf_secret,
    ENDPOINT = 'http://[::ffff:127.0.0.1]/',
    REGION = 'us-east-1'
  ) WITH (VALIDATE = true);
contains:Address resolved to a private IP

! CREATE CONNECTION aws_ssrf_endpoint_metadata TO AWS (
    ACCESS KEY ID = 'unused',
    SECRET ACCESS KEY = SECRET aws_ssrf_secret,
    ENDPOINT = 'http://169.254.169.254/',
    REGION = 'us-east-1'
  ) WITH (VALIDATE = true);
contains:Address resolved to a private IP

# Alternative metadata IP encodings via AWS endpoint
! CREATE CONNECTION aws_ssrf_endpoint_decimal TO AWS (
    ACCESS KEY ID = 'unused',
    SECRET ACCESS KEY = SECRET aws_ssrf_secret,
    ENDPOINT = 'http://2852039166/',
    REGION = 'us-east-1'
  ) WITH (VALIDATE = true);
contains:Address resolved to a private IP

! CREATE CONNECTION aws_ssrf_endpoint_hex TO AWS (
    ACCESS KEY ID = 'unused',
    SECRET ACCESS KEY = SECRET aws_ssrf_secret,
    ENDPOINT = 'http://0xa9fea9fe/',
    REGION = 'us-east-1'
  ) WITH (VALIDATE = true);
contains:Address resolved to a private IP

! CREATE CONNECTION aws_ssrf_endpoint_octal TO AWS (
    ACCESS KEY ID = 'unused',
    SECRET ACCESS KEY = SECRET aws_ssrf_secret,
    ENDPOINT = 'http://025177524776/',
    REGION = 'us-east-1'
  ) WITH (VALIDATE = true);
contains:Address resolved to a private IP

! CREATE CONNECTION aws_ssrf_endpoint_ipv6_mapped TO AWS (
    ACCESS KEY ID = 'unused',
    SECRET ACCESS KEY = SECRET aws_ssrf_secret,
    ENDPOINT = 'http://[::ffff:a9fe:a9fe]/',
    REGION = 'us-east-1'
  ) WITH (VALIDATE = true);
contains:Address resolved to a private IP

! CREATE CONNECTION aws_ssrf_endpoint_ec2_v6 TO AWS (
    ACCESS KEY ID = 'unused',
    SECRET ACCESS KEY = SECRET aws_ssrf_secret,
    ENDPOINT = 'http://[fd00:ec2::254]/',
    REGION = 'us-east-1'
  ) WITH (VALIDATE = true);
contains:Address resolved to a private IP

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_enforce_external_addresses = false

# Sanity check: with enforcement disabled, a previously-rejected loopback
# literal makes it past validation and instead fails at connect time. The
# absence of the "private IP" message proves the dyncfg actually gates the
# check.
! COPY INTO copy_ssrf_target FROM 'http://127.0.0.1:1/file.csv' (FORMAT CSV);
contains:Connection refused
