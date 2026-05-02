---
source: src/fivetran-destination/src/destination/config.rs
revision: 82d92a7fad
---

# mz-fivetran-destination::destination::config

Handles connection configuration for the Fivetran destination.
`handle_configuration_form_request` returns the form definition (host, user, app_password, dbname, cluster) shown to Fivetran users.
`connect` establishes a TLS-authenticated `tokio_postgres` connection to Materialize using a compiled-in CA certificate bundle, and `handle_test_request` validates connectivity and CREATE privilege.
