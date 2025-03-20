---
title: "Role-based access control (RBAC)"
description: "Manage roles, privileges, and other access control options in Materialize"
menu:
  main:
    parent: access-control
    weight: 10
---

[//]: # "NOTE(morsapaes) This page will be rewritten as a follow-up."

This page introduces role-based access management (RBAC) in Materialize. RBAC
allows you to apply granular privileges to your Materialize objects and clusters. Organizations
using RBAC can manage user roles and privileges to ensure there is not
unauthorized or improper access to sensitive objects.

In Materialize, RBAC allows organization administrators to:

* Determine which users have read or write privileges for specific objects

* Control how users interact with clusters by giving them different levels of access to
resources

* Prevent accidental operations from unauthorized users

* Isolate access to user-facing data from internal organization data

Materialize object access is also dependent on cluster privileges.
Roles that need access to an object that use compute resources must also have
the same level of access to the cluster. Materialize objects that use compute
resources are:

* Replicas
* Sources
* Sinks
* Indexes
* Materialized views

The next sections go over the concepts of authorization and authentication and
the objects within Materialize.

## RBAC structure

RBAC in practice is a group of roles with assigned privileges.
You can assign specific users to roles or assign privileges to users to inherit
from other roles.

### Roles

A role is a collection of privileges you can apply to users. Roles make it
easier to assign or revoke privileges on Materialize objects. You can group
users into specified roles with different levels of privileges and adjust those
privileges to ensure they have the correct level of access to objects.

### Role attributes

Role attributes are actions available to any role you create. Attributes are
independent of any other object in Materialize and apply to the entire
organization. You can edit these actions when you create the role:

| Name              | Description                                                                 |
|-------------------|-----------------------------------------------------------------------------|
| `INHERIT`         | **Read-only.** Can inherit privileges of other roles.                       |

PostgreSQL uses role attributes to determine if a role is allowed to execute certain statements. In
Materialize these have all been replaced by system privileges.

### Privileges

Privileges are the actions or operations a role is allowed to perform on a
specific object. After you create a role, you can grant it the following
object-specific privileges in Materialize:

| Privilege             | Description                                                                                    | `psql` |
|-----------------------|------------------------------------------------------------------------------------------------|--------|
| `SELECT`              | Allows selecting rows from an object.                                                          | `r`    |
| `INSERT`              | Allows inserting into an object.                                                               | `a`    |
| `UPDATE`              | Allows updating an object (requires `SELECT`).                                                 | `w`    |
| `DELETE`              | Allows deleting from an object (requires `SELECT`).                                            | `d`    |
| `CREATE`              | Allows creating a new object within another object.                                            | `C`    |
| `USAGE`               | Allows using an object or looking up members of an object.                                     | `U`    |
| `CREATEROLE`          | Allows creating, altering, deleting roles and the ability to grant and revoke role membership. | `R`    |
| `CREATEDB`            | Allows creating databases.                                                                     | `B`    |
| `CREATECLUSTER`       | Allows creating clusters.                                                                      | `N`    |
| `CREATENETWORKPOLICY` | Allows creating network policies.                                                              | `P`    |


Note that the system catalog uses the abbreviation of the privilege name.

Objects in Materialize have different levels of privileges available to them.
Materialize supports the following object type privileges:

| Object Type          | Privileges                                                       |
|----------------------|------------------------------------------------------------------|
| `SYSTEM`             | `CREATEROLE`, `CREATEDB`, `CREATECLUSTER`, `CREATENETWORKPOLICY` |
| `DATABASE`           | `USAGE`, `CREATE`                                                |
| `SCHEMA`             | `USAGE`, `CREATE`                                                |
| `TABLE`              | `INSERT`, `SELECT`, `UPDATE`, `DELETE`                           |
| `VIEW`               | `SELECT`                                                         |
| `MATERIALIZED  VIEW` | `SELECT`                                                         |
| `TYPE`               | `USAGE`                                                          |
| `SOURCE`             | `SELECT`                                                         |
| `CONNECTION`         | `USAGE`                                                          |
| `SECRET`             | `USAGE`                                                          |
| `CLUSTER`            | `USAGE`, `CREATE`                                                |

### Inheritance

Inheritance in RBAC allows you to create roles that inherit privileges from
other roles. Inheritance only applies to role privileges; role attributes and
parameters are **not inherited**. Inheriting privileges allows you to minimize
the number of roles you have to manage.
