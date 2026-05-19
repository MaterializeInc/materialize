---
source: src/sql/src/session/hint.rs
revision: 32e3a61596
---

# mz-sql::session::hint

Defines `ApplicationNameHint`, an enum that classifies the `application_name` session variable into known clients (psql, dbt, web console, mz_psql, Fivetran, Grafana, MCP agent, MCP developer, etc.) or unspecified/unrecognized.
Used as a low-cardinality Prometheus label on adapter metrics; construction is restricted to `from_str` to prevent direct instantiation.
