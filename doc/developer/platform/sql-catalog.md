# Materialize Platform: SQL Catalog Mapping

The following is a candidate proposal for how to map the SQL catalog to Platform
concepts.

An ACCOUNT contains REGIONs. Each REGION contains an independent SQL catalog.
A SQL catalog contains DATABASEs, which each contain SCHEMAs, which each contain
OBJECTs. An OBJECT is a SOURCE, SINK, VIEW, TABLE, or INDEX.

At the moment, a DATABASE or SCHEMA is a simple namespace and confers no
additional semantics. We plan to soon think about how DATABASEs map to
TIMELINEs.

INDEXs are special. Their existence is tied to the active CLUSTER.
You set the active cluster by typing:

```sql
USE CLUSTER cluster_name;
```

Typing `CREATE INDEX` or `CREATE MATERIALIZED {VIEW|SOURCE}` will create the
index in the currently-active cluster. Similarly, typing `SHOW INDEXES` will
grow an additional column specifying to which cluster the index belongs.

Role-based access control (RBAC) will permit limiting read/write access at the
DATABASE, SCHEMA, or OBJECT level.

The goal here is to avoid being overly prescriptive about how DATABASEs and
SCHEMAs are used to allow for downstream organizations and tools to develop
opinions about how to structure a Materialize Platform project.

For example, an organization might achieve separate production and staging
environments by creating a `production` database and a `staging` database,
then using RBAC to limit access to `production` to a trusted set of engineers
and service accounts.
