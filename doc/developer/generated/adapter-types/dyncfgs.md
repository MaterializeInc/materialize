---
source: src/adapter-types/src/dyncfgs.rs
revision: b1e123e786
---

# mz-adapter-types::dyncfgs

Declares all dynamic configuration flags owned by the adapter layer as `mz_dyncfg::Config` constants.
Covers session gating (`ALLOW_USER_SESSIONS`), zero-downtime deployment parameters (`WITH_0DT_*`, `ENABLE_0DT_*`), feature flags for expression caching, multi-replica sources, statement lifecycle logging, introspection subscribes, plan insights optimization thresholds, continual task builtins, password authentication, OIDC settings (`OIDC_ISSUER`, `OIDC_AUDIENCE`, `OIDC_AUTHENTICATION_CLAIM`), console OIDC configuration (`CONSOLE_OIDC_CLIENT_ID`, `CONSOLE_OIDC_SCOPES`), MCP endpoint toggles (`ENABLE_MCP_AGENT`, `ENABLE_MCP_AGENT_QUERY_TOOL`, `ENABLE_MCP_DEVELOPER`), persist fast-path ordering, S3 Tables region checks, and the user ID pool batch size.
`all_dyncfgs` registers every config constant into a `ConfigSet` for use during bootstrap.
