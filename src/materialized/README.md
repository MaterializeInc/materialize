## Unified materialized binary

`materialized` selects a service from the name of the symlink invoking it:
`controllerd`, `adapterd`, `clusterd` or `pubsubd`. The `environmentd` entrypoint
reports that monolithic serving is unsupported.

The all-in-one test image starts controllerd, which initializes the deployment
and manages adapter, replica and relay processes. Adapters serve SQL and HTTP
at the configured listener ports. Controller administration stays on loopback
port 6882, with access control outside controllerd. Relay diagnostics use
loopback port 6883 so they do not displace SASL SQL on 6881.

The image derives the written-plan namespace from the selected adapter
executable unless the caller supplies `--adapter-plan-build` or
`MZ_ADAPTER_PLAN_BUILD`. Adapter allocations are configured by `MZ_ADAPTER_SIZES`.
Existing pre-native catalogs cannot be converted by this image.

Combining services in one executable reduces the image size. This image is for
testing, not production use.
