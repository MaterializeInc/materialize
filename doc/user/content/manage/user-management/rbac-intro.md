---
title: "Role Based Access Management"
description: "How Materialize implements RBAC"
menu:
  main:
    parent: user-management
    weight: 10
---

This document introduces role-based access management (RBAC) in Materialize. RBAC allows you to apply more
granular privileges to your Materialize objects and clusters. Organizations
using RBAC can manage user roles and privileges to ensure there is not
unauthorized or improper access to sensitive objects.

In Materialize, Role-Based Access Control allows organization administrators to: 

* determine which users have read or write permissions for specific objects

* control how users interact with clusters by giving them different levels of access to
resources

* prevent accident operations from unauthorized users

* isolate access to user-facing data from internal organization data

Materialize object access is also dependant on cluster privileges.
Users who need access to an object like a schema or a database must also have
the same level of access to the cluster.

!(#TODO_Diagram)

The next sections go over the concepts of authorization and authentication and
the objects within Materialize.

## Authentication vs. Authorization

First, we need to make a distinction between authentication and authorization.
Authentication dictates who can log in to a system while authorization
determines what a user can access within a system. 

Authentication in Materialize is handled when you sign up or are invited to a
Materialize organization. Your immediate privileges are determined by your
invitation from an administrator or are assigned automatically by Materialize
when you sign up.

Authorization is determined in RBAC by your organization administrator. When you
invite users to your Materialize organization, you have the option to give the
user more elevated privileges in the Materialize administrator console . 

## RBAC Structure

RBAC in practice is a group of roles with assigned privileges and attributes.
You can assign specific users to roles or assign privileges to users to inherit
from other roles. 

### Roles

A role is a collection of permissions you can apply to users. Roles make it
easier to assign or revoke privileges on Materialize objects. You can group
users into specified roles with different levels of permissions and adjust those
permissions to ensure they have the correct level of access to objects.

### Role Attributes

Role attributes are actions available to any role you create. Attributes are
independent of any other object in Materialize and apply to the entire
organization. You can edit these actions when you create the role:

| Name            | Description                                                                     |
|-----------------|---------------------------------------------------------------------------------|
| `CREATEDB`      | Can create a database.                                                          |
| `CREATEROLE`    | Can create, alter, drop, grant membership to, and revoke membership from roles. |
| `INHERIT`       | Can inherit the privileges of roles that it is a member of. On by default.      |
| `CREATECLUSTER` | Can create a cluster.                                                           |

### Privileges

Privileges are the actions or operations a role is allowed to perform. When you
create a role, you can select specific privileges for that role based on the
objects that role needs to access.

Materialize supports the following privileges:

| Privilege | Description                                                              | Abbreviation  |
|-----------|--------------------------------------------------------------------------|---------------|
| `SELECT`  | Allows reading rows from an object.                                      | `r`("read")   |
| `INSERT`  | Allows inserting into an object.                                         | `a`("append") |
| `UPDATE`  | Allows updating an object (requires SELECT if a read is necessary).      | `w`("write")  |
| `DELETE`  | Allows deleting from an object (requires SELECT if a read is necessary). | `d`           |
| `CREATE`  | Allows creating a new object within another object.                      | `C`           |
| `USAGE`   | Allows using an object or looking up members of an object.               | `U`           |

Objects in Materialize have different levels of privileges available to them.
Materialize supports the following object type privileges:

| Object Type          | All Privileges |
|----------------------|----------------|
| `DATABASE`           | `U` `C`             |
| `SCHEMA`             | `U` `C`             |
| `TABLE`              | `a` `r` `w` `d`           |
| `VIEW`               | `r`              |
| `MATERIALIZED  VIEW` | `r`              |
| `TYPE`               | `U`              |
| `SOURCE`             | `r`              |
| `CONNECTION`         | `U`              |
| `SECRET`             | `U`              |
| `CLUSTER`            | `U` `C`          |


### Inheritance

Inheritance in RBAC allows you to create roles that inherit permissions
from other roles. Inheriting permissions allows you to minimize the number of
roles you have to manage.

## Next Steps

In the next guide, you will create a new user and a new role. Later in this
tutorial series, you will assign and manage privileges for roles.
