---
source: src/testdrive/src/util/postgres.rs
revision: fbe9cc4c2d
---

# testdrive::util::postgres

Provides two utilities for working with PostgreSQL connections in testdrive.
`config_url` converts a `tokio_postgres::Config` to a `url::Url`, failing if the config specifies multiple hosts or ports.
`postgres_client` establishes a connection with retry logic bounded by a timeout, redacting the URL in log output when a `mzp_` app password is detected.
