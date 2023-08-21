---
title: "mz_aclitem type"
description: "Expresses a granted privilege on some object"
menu:
  main:
    parent: 'sql-types'
---

`mz_aclitem` data expresses a granted privilege on some object.

## `mz_aclitem` info

Detail | Info
-------|------
**Size** | 26 bytes
**Catalog name** | `mz_catalog.mz_aclitem`
**OID** | 16566

## Details

`mz_aclitem` represents a privilege granted to some user on some object. The format of `mz_aclitem`
is `<grantee>=<privileges>/<grantor>`.
- `<grantee>` is the role ID of the role that has some privilege.
- `<privileges>` is the abbreviation of the privileges that `grantee` has concatenated together.
- `<grantor>` is the role ID of the role that granted the privileges.

A list of all privileges and their abbreviations are below:

| Privilege       | Description                                                                                    | Abbreviation      | Applicable Object Types                       |
|-----------------|------------------------------------------------------------------------------------------------|-------------------|-----------------------------------------------|
| `SELECT`        | Allows reading rows from an object.                                                            | r(”read”)         | Table, View, Materialized View, Source        |
| `INSERT`        | Allows inserting into an object.                                                               | a(”append”)       | Table                                         |
| `UPDATE`        | Allows updating an object (requires SELECT if a read is necessary).                            | w(”write”)        | Table                                         |
| `DELETE`        | Allows deleting from an object (requires SELECT if a read is necessary).                       | d                 | Table                                         |
| `CREATE`        | Allows creating a new object within another object.                                            | C                 | Database, Schema, Cluster                     |
| `USAGE`         | Allows using an object or looking up members of an object.                                     | U                 | Database, Schema, Connection, Secret, Cluster |
| `CREATEROLE`    | Allows creating, altering, deleting roles and the ability to grant and revoke role membership. | R("Role")         | System                                        |
| `CREATEDB`      | Allows creating databases.                                                                     | B("dataBase")     | System                                        |
| `CREATECLUSTER` | Allows creating clusters.                                                                      | N("compute Node") | System                                        |

The `CREATEROLE` privilege is very powerful. It allows roles to grant and revoke membership in
other roles, even if it doesn't have explicit membership in those roles. As a consequence, any role
with this privilege can obtain the privileges of any other role in the system.

If a `mz_aclitem` is casted to `text`, the role IDs are automatically converted to role names.

### Valid casts

For details about casting, including contexts, see [Functions:
Cast](../../functions/cast).

From | To | Required context
-----|----|--------
`mz_aclitem` | `text` | Explicit

### Valid operations

There are no supported operations or functions on `mz_aclitem` types.
