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

# A GCP connection validates by POSTing to the `token_uri` from the
# service-account-key JSON. gcp_auth issues that request verbatim, bypassing the
# resolve_address / ENFORCE_EXTERNAL_ADDRESSES path the connections above use, so
# validation must reject any token_uri that is not an HTTPS *.googleapis.com host
# -- otherwise it is an SSRF primitive hidden inside an opaque secret.
#
# The private_key is a throwaway RSA key reused from the repo (src/ccsr/tests and
# the trufflehog allowlist) -- never a real credential. It must parse so gcp_auth
# reaches the token-fetch step that consumes token_uri. Backslashes are doubled:
# standard_conforming_strings is on, so the text->bytea cast turns each "\\n"
# into the literal "\n" the JSON needs.
$ set gcp-ssrf-key="-----BEGIN PRIVATE KEY-----\\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDDC5MP3v1BHOgI\\n5SsmrW8mjxzQGOz0IlC5jp1muW/kpEoE9TG317TEnO5Uye6zZudkFCP8YGEiN3Mc\\nFbTM7eX6PjAPdnGU7khuUt/20ZM+NX5kWZPrmPTh4WQaDCL7ah1LqzBaUAMaSXq8\\niuy7LGJNF8wdx8L5BjDiGTTxZXOg0Haxknc7Mbiwc9z8eb7omvzQzsOwyqocrF2u\\nz86TzX1jtHP48i5CxoRHKxE94De3tNxjT/Y3OZlS4QS7iekAOQ04DVV3GIHvRUXN\\n2H8ayy4+yOdhHn6ER5Jn3lti1Q5XSrxkrYn7L1Vcj6IwZQhhF5vc+ovxOYb+8ert\\nEo97tIkLAgMBAAECggEAQteHHRPKz9Mzs8Sxvo4GPv0hnzFDl0DhUE4PJCKdtYoV\\n8dADq2DJiu3LAZS4cJPt7Y63bGitMRg2oyPPM8G9pD5Goy3wq9zjRqexKDlXUCTt\\n/T7zofRny7c94m1RWb7ablGq/vBXt90BqnajvVtvDsN+iKAqccQM4ZdI3QdrEmt1\\ncHex924itzG/mqbFTAfAmVj1ZsRnJp55Txy2gqq7jX00xDM8+H49SRvUu49N64LQ\\n6BUWCgWCJePRtgjSHjboAzPqSkMdaTE/WDY2zgGF3Qfq4f6JCHKfm4QylCH4gYUU\\n1Kf7ttmhu9NoZO+hczobKkxP9RtXfyTRH2bsJXy2HQKBgQDhHgavxk/ln5mdMGGw\\nrQud2vF9n7UwFiysYxocIC5/CWD0GAhnawchjPypbW/7vKM5Z9zhW3eH1U9P13sa\\n2xHfrU5BZ16rxoBbKNpcr7VeEbUBAsDoGV24xjoecp7rB2hZ+mGik5/5Ig1Rk1KH\\ndcvYy2KSi1h4Sm+mXwimmA4VDQKBgQDdzW+5FPbdM2sUB2gLMQtn3ICjDSu6IQ+k\\nd0p3WlTIT51RUsPXXKkk96O5anUbeB3syY8tSKPGggsaXaeL3o09yIamtERgCnn3\\nd9IS+4VKPWQlFUICU1KrD+TO7IYIX04iXBuVE5ihv0q3mslhDotmX4kS38NtKEFF\\njLjA2RvAdwKBgAFkIxxw+Ett+hALnX7vAtRd5wIku4TpjisejanA1Si50RyRDXQ+\\nKBQf/+u4HmoK12Nibe4Cl7GCMvRGW59l3S1pr8MdtWsQVfi6Puc1usQzDdBMyQ5m\\nIbsjlnZbtPm02QM9Vd8gVGvAtx5a77aglrrnPtuy+r/7jccUbURCSkv9AoGAH9m3\\nWGmVRZBzqO2jWDATxjdY1ZE3nUPQHjrvG5KCKD2ehqYO72cj9uYEwcRyyp4GFhGf\\nmM4cjo3wEDowrBoqSBv6kgfC5dO7TfkL1qP9sPp93gFeeD0E2wGuRrSaTqt46eA2\\nKcMloNx6W0FD98cB55KCeY5eXtdwAA/EHBVRMeMCgYAd3n6PcL6rVXyE3+wRTKK4\\n+zvx5sjTAnljr5ttbEnpZafzrYIfDpB8NNjexy83AeC0O13LvSHIFoTwP8sywJRO\\nRxbPMjhEBdVZ5NxlxYer7yKN+h5OBJfrLswPku7y4vdFYK3x/lMuNQO61hb1VFHc\\nT2BDTbF0QSlPxFsv18B9zg==\\n-----END PRIVATE KEY-----\\n"

# Private address (loopback): reaching it at all is the SSRF. Listed first -- it
# fails fast on vulnerable code instead of hanging like a black-holed metadata
# IP, and testdrive stops at the first failure.
> CREATE SECRET gcp_ssrf_private AS '{"type":"service_account","project_id":"x","client_email":"a@b.com","private_key":"${gcp-ssrf-key}","token_uri":"http://127.0.0.1:1/token"}'
! CREATE CONNECTION gcp_ssrf_private_conn TO GCP (SERVICE ACCOUNT KEY = SECRET gcp_ssrf_private) WITH (VALIDATE = true);
contains:token_uri must be https://oauth2.googleapis.com/token

# Correct host, but plaintext HTTP -- the scheme must be HTTPS.
> CREATE SECRET gcp_ssrf_scheme AS '{"type":"service_account","project_id":"x","client_email":"a@b.com","private_key":"${gcp-ssrf-key}","token_uri":"http://oauth2.googleapis.com/token"}'
! CREATE CONNECTION gcp_ssrf_scheme_conn TO GCP (SERVICE ACCOUNT KEY = SECRET gcp_ssrf_scheme) WITH (VALIDATE = true);
contains:token_uri must be https://oauth2.googleapis.com/token

# Public host that merely ends in the allowlisted suffix -- only a host allowlist,
# not a private-IP check, rejects this.
> CREATE SECRET gcp_ssrf_host AS '{"type":"service_account","project_id":"x","client_email":"a@b.com","private_key":"${gcp-ssrf-key}","token_uri":"https://oauth2.googleapis.com.evil.com/token"}'
! CREATE CONNECTION gcp_ssrf_host_conn TO GCP (SERVICE ACCOUNT KEY = SECRET gcp_ssrf_host) WITH (VALIDATE = true);
contains:token_uri must be https://oauth2.googleapis.com/token

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_enforce_external_addresses = false

# Sanity check: with enforcement disabled, a previously-rejected loopback
# literal makes it past validation and instead fails at connect time. The
# absence of the "private IP" message proves the dyncfg actually gates the
# check.
! COPY INTO copy_ssrf_target FROM 'http://127.0.0.1:1/file.csv' (FORMAT CSV);
contains:Connection refused

# Unlike the resolve_address checks above, the GCP allowlist is unconditional --
# it does not consult storage_enforce_external_addresses. With enforcement now
# off, the malicious token_uri is still rejected.
! CREATE CONNECTION gcp_ssrf_private_conn TO GCP (SERVICE ACCOUNT KEY = SECRET gcp_ssrf_private) WITH (VALIDATE = true);
contains:token_uri must be https://oauth2.googleapis.com/token

# In fact, the GCP allowlist should even apply when VALIDATE = false.
! CREATE CONNECTION gcp_ssrf_private_conn TO GCP (SERVICE ACCOUNT KEY = SECRET gcp_ssrf_private) WITH (VALIDATE = false);
contains:token_uri must be https://oauth2.googleapis.com/token

# The CREATE-time check is not enough: ALTER CONNECTION and ALTER SECRET can both
# install a malicious token_uri into an already-created GCP connection, so the
# allowlist must guard those paths too.

# A benign service-account key whose token_uri is the real Google endpoint. It
# passes the allowlist, so the connection and secret below are created cleanly
# and give the ALTER paths a valid starting point to subvert.
> CREATE SECRET gcp_good AS '{"type":"service_account","project_id":"x","client_email":"a@b.com","private_key":"${gcp-ssrf-key}","token_uri":"https://oauth2.googleapis.com/token"}'
> CREATE CONNECTION gcp_alter_conn TO GCP (SERVICE ACCOUNT KEY = SECRET gcp_good) WITH (VALIDATE = false);

# ALTER CONNECTION: swapping in a SERVICE ACCOUNT KEY that points at a private
# token_uri is the same SSRF primitive as CREATE and must be rejected, whether or
# not validation is requested.
! ALTER CONNECTION gcp_alter_conn SET (SERVICE ACCOUNT KEY = SECRET gcp_ssrf_private) WITH (VALIDATE = true);
contains:token_uri must be https://oauth2.googleapis.com/token

! ALTER CONNECTION gcp_alter_conn SET (SERVICE ACCOUNT KEY = SECRET gcp_ssrf_private) WITH (VALIDATE = false);
contains:token_uri must be https://oauth2.googleapis.com/token

# ALTER SECRET: rewriting the secret behind a GCP connection slips a malicious
# token_uri past the CREATE-time check entirely, since the connection was already
# validated against the benign key above. This must be rejected as well.
! ALTER SECRET gcp_good AS '{"type":"service_account","project_id":"x","client_email":"a@b.com","private_key":"${gcp-ssrf-key}","token_uri":"http://127.0.0.1:1/token"}'
contains:token_uri must be https://oauth2.googleapis.com/token
