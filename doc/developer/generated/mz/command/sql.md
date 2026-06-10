---
source: src/mz/src/command/sql.rs
revision: 4b9700a8af
---

# mz::command::sql

Implements `mz sql run`, which opens an interactive SQL shell by `exec`-replacing the current process with `psql` connected to the active region's SQL address with the profile's app password.
