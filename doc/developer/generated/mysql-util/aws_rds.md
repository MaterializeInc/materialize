---
source: src/mysql-util/src/aws_rds.rs
revision: f464fac6af
---

# mysql-util::aws_rds

Implements `rds_auth_token`, which generates a signed AWS RDS IAM authentication token (SigV4, query-parameter placement, 900-second expiry) for use as a MySQL password.
Defines `RdsTokenError` to cover the failure modes: missing credentials provider, credential resolution failure, signing parameter build failure, signing failure, and invalid endpoint (unparseable host URL).
