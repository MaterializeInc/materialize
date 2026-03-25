---
source: src/adapter-types/src/dyncfgs.rs
revision: 5b9fb22e87
---

# mz-adapter-types::dyncfgs

Declares all dynamic configuration flags owned by the adapter layer as `mz_dyncfg::Config` constants.
Covers zero-downtime deployment parameters (`WITH_0DT_*`), feature flags for expression caching, multi-replica sources, password authentication, OIDC settings, MCP endpoint toggles, persist fast-path ordering, S3 Tables region checks, and the user ID pool batch size.
`all_dyncfgs` registers every config constant into a `ConfigSet` for use during bootstrap.
