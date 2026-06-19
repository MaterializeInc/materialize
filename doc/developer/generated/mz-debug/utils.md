---
source: src/mz-debug/src/utils.rs
revision: 19b7f2055a
---

# mz-debug::utils

Provides utility functions shared across all `mz-debug` modules.
`format_base_path` produces the timestamped output directory path; `zip_debug_folder` walks that directory and writes a zip archive; `create_tracing_log_file` creates the log file inside the output directory; `validate_pg_connection_string` validates a Postgres connection URL for CLI argument parsing.
`get_k8s_auth_mode` reads the Materialize custom resource from Kubernetes to determine the auth mode: `None` for `AuthenticatorKind::None`, `Password` (username + password) for `AuthenticatorKind::Password`, `AuthenticatorKind::Sasl`, or `AuthenticatorKind::Oidc`, and an error for `AuthenticatorKind::Frontegg`.
