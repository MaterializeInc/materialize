---
source: src/testdrive/src/action/postgres/connect.rs
revision: 4faab97f93
---

# testdrive::action::postgres::connect

Implements the `postgres-connect` builtin command, which establishes a named PostgreSQL connection and stores it in the testdrive state.
Connection names may not be URL strings (to prevent accidental misuse of a URL as a name); the URL and optional timeout are configurable arguments.
