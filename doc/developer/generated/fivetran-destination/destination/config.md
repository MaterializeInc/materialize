---
source: src/fivetran-destination/src/destination/config.rs
revision: 849327076c
---

# mz-fivetran-destination::destination::config

Connection configuration for `MaterializeDestination`.

`handle_configuration_form_request()` returns the `ConfigurationFormResponse` defining the six user-facing fields: `host`, `user`, `password`, `dbname`, `schema`, and `port`.

`connect(configuration)` opens a TLS-enabled `tokio_postgres::Client` to the Materialize region using the provided configuration map, with the application name set to `materialize_fivetran_destination`. Returns an `(dbname, Client)` pair.
