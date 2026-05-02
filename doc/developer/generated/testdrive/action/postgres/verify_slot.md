---
source: src/testdrive/src/action/postgres/verify_slot.rs
revision: 2a6ac3ab4c
---

# testdrive::action::postgres::verify_slot

Implements the `postgres-verify-slot` builtin command, which polls a PostgreSQL replication slot until its `active` state matches the expected value.
Used in CDC tests to confirm that a slot has been created and activated (or deactivated) by Materialize.
