---
source: src/sql/src/session/hint.rs
revision: b224a6432c
---

# mz-sql::session::hint

Defines `ApplicationNameHint`, an enum that classifies the `application_name` session variable into known clients (psql, dbt, web console, mz_psql, Fivetran, Grafana, etc.) or unspecified/unrecognized.
Used as a low-cardinality Prometheus label on adapter metrics; construction is restricted to `from_str` to prevent direct instantiation.
