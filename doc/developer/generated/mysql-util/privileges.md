---
source: src/mysql-util/src/privileges.rs
revision: 12fbe31d24
---

# mysql-util::privileges

Implements `validate_source_privileges`, which issues a `SHOW GRANTS` query (including any active roles on MySQL 8.0+), parses the output with a regex, and verifies that the connected user holds `SELECT`, `LOCK TABLES`, and `REPLICATION SLAVE` for the requested tables.
Before splicing active roles into the `SHOW GRANTS … USING <roles>` query, the roles string is validated against `ROLES_PATTERN`, a `LazyLock<Regex>` allowlist of characters permitted in role specs, to prevent SQL injection from a hostile or compromised upstream server.
The internal `get_object_grant` helper handles the varied quoting styles (`\``, `'`, `"`) and wildcard patterns across MySQL versions using the `fancy_regex` crate with backreferences.
