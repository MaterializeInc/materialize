---
name: mz-feature-flags
description: >
  Trigger: adding or changing a feature flag, dyncfg, or system parameter
  default, reviewing a PR that adds one, LaunchDarkly flag setup, or
  bin/lint-test-flags failures in Materialize.
---

# Feature flags

Choosing whether a new feature flag defaults on or off in production is a
product decision that needs context an agent rarely has. When adding a flag,
ask a human which default to use. When reviewing a PR, do not question the
default it chose. However, in tests, a new flag should usually be on, so that
sqllogictest and testdrive exercise the new code path. If
its production default is off, turn it on for CI in
`misc/python/materialize/mzcompose/__init__.py`
(`get_minimal_system_parameters` if tests fail with it off, otherwise
`get_variable_system_parameters`), which `bin/sqllogictest` and mzcompose's
`Materialized` service pass to environmentd as system parameter defaults.
Rust integration tests use `TestHarness::with_system_parameter_default`.
A value listed there overrides the compiled-in default in CI, so changing a
default means changing both. `bin/lint-test-flags` fails unless every new
dyncfg is registered for parallel workload and mzcompose (see the mz-debug-ci
skill). A compiled-in default is not evidence of the production value: check
whether `test/launchdarkly-flag-consistency/mzcompose.py` lists the flag in
`INTENTIONAL_LD_OVERRIDES` or `KNOWN_CROSS_ENV_DIVERGENCES`. A session
variable takes a system default only if `SESSION_SYSTEM_VARS` in
`src/sql/src/session/vars.rs` lists it. Otherwise `--system-parameter-default`
for it only logs a warning at startup. Observed: the LaunchDarkly MCP
`create-flag` tool creates boolean flags that serve true in every environment,
including production. A human must set each environment's off variation and
fallthrough to false.
