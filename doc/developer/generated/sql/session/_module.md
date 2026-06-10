---
source: src/sql/src/session.rs
revision: a59a71eb19
---

# mz-sql::session

Groups the session-related abstractions needed by the SQL layer: `hint` (application name classification), `metadata` (`SessionMetadata` trait), `user` (built-in users and role metadata), and `vars` (session/system variable infrastructure).
