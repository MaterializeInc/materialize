---
source: src/sql/src/session/vars/constraints.rs
revision: 2b5b078528
---

# mz-sql::session::vars::constraints

Defines the `ValueConstraint` enum (`ReadOnly`, `Fixed`, `Domain`) and the `DomainConstraint`/`DynDomainConstraint` traits used to validate variable values at assignment time.
Provides concrete constraint instances: `NUMERIC_NON_NEGATIVE`, `NUMERIC_BOUNDED_0_1_INCLUSIVE`, `BYTESIZE_AT_LEAST_1MB`, and `NON_ZERO_DURATION`.
`NON_ZERO_DURATION` is a `DomainConstraint<Value = Duration>` that rejects zero-length durations with an `InvalidParameterValue` error; it is used by `DEFAULT_TIMESTAMP_INTERVAL` to prevent setting that interval to zero.
