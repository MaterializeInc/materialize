---
source: src/mz-deploy/src/client/connection.rs
revision: 3f3bdb0535
---

# mz-deploy::client::connection

Database client for mz-deploy.

`Client` holds a `tokio_postgres` connection and a `Profile`. Domain sub-clients (`DeploymentsClient`, `DeploymentsClientMut`, `IntrospectionClient`, `ValidationClient`, `TypeInfoClient`, `ProvisioningClient`, `DevOverlaysClient`) borrow from `Client` and provide focused APIs for their respective concerns.

`Client::connect_with_profile` connects and pins every session to the `_mz_deploy_server` cluster via libpq options. `connect_with_profile_no_pin` skips the pin; it is used by unit tests (ephemeral Docker containers) and by `setup::run` (which creates the cluster and therefore cannot pin to it yet).

TLS behavior is driven by `profile.sslmode`; when unset, loopback hosts default to `prefer` and all other hosts default to `require`. `verify-ca` and `verify-full` modes source CA certificates from `profile.sslrootcert`, then the platform CA hunt paths, then OpenSSL's compiled-in defaults.
