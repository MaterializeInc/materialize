# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# OAUTH2 SERVER URL overrides where a REST catalog connection exchanges its
# CREDENTIAL for a bearer token. Without it the endpoint is derived from the
# catalog URL as `<url>/v1/oauth/tokens`, which the Iceberg REST specification
# defines but not every catalog serves.

# The accepted case below points at a host that does not exist, so skip the
# connect-on-create validation. Everything asserted here is decided at plan time.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_default_connection_validation = false

> CREATE SECRET oauth_cred AS 'client-id:client-secret'

> CREATE CONNECTION oauth_override TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'https://example.invalid/api/catalog',
    CREDENTIAL = SECRET oauth_cred,
    OAUTH2 SERVER URL = 'https://example.invalid/oidc/v1/token',
    SCOPE = 'all-apis',
    WAREHOUSE = 'wh'
  );

> SELECT name FROM mz_connections WHERE name = 'oauth_override'
oauth_override

# The option is only meaningful for the OAuth exchange, which s3tablesrest does
# not perform: it authenticates with SigV4 through an AWS connection instead.
! CREATE CONNECTION oauth_on_s3tables TO ICEBERG CATALOG (
    CATALOG TYPE = 's3tablesrest',
    URL = 'https://example.invalid/api/catalog',
    OAUTH2 SERVER URL = 'https://example.invalid/oidc/v1/token',
    WAREHOUSE = 'wh'
  );
contains:s3tablesrest connections do not support OAUTH2 SERVER URL

# Omitting it stays valid, leaving the specification-derived endpoint in place.
> CREATE CONNECTION oauth_default TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'https://example.invalid/api/catalog',
    CREDENTIAL = SECRET oauth_cred,
    WAREHOUSE = 'wh'
  );

> DROP CONNECTION oauth_default
> DROP CONNECTION oauth_override
> DROP SECRET oauth_cred
