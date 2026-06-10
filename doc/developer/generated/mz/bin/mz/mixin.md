---
source: src/mz/src/bin/mz/mixin.rs
revision: e757b4d11b
---

# mz (bin)::mixin

Provides reusable clap argument mixins: `validate_profile_name` validates that a profile name uses only ASCII letters, digits, underscores, and dashes; `EndpointArgs` holds optional `--cloud-endpoint` and `--admin-endpoint` overrides used by commands that need non-default API endpoints.
