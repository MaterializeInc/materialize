---
source: src/sql/src/session/vars/constraints.rs
revision: 0ac4a9a3f1
---

# mz-sql::session::vars::constraints

Defines the `ValueConstraint` enum (`ReadOnly`, `Fixed`, `Domain`) and the `DomainConstraint`/`DynDomainConstraint` traits used to validate variable values at assignment time.
Provides concrete constraint instances: `NUMERIC_NON_NEGATIVE`, `NUMERIC_BOUNDED_0_1_INCLUSIVE`, and `BYTESIZE_AT_LEAST_1MB`.
