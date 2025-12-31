---
audience: developer
canonical_url: https://materialize.com/docs/security/appendix/
complexity: advanced
description: '---'
doc_type: reference
keywords:
- Appendix
- 'Note:'
product_area: Security
status: stable
title: Appendix
---

# Appendix

## Purpose
---

If you need to understand the syntax and options for this command, you're in the right place.


---

## Appendix: Built-in roles


This section covers appendix: built-in roles.

## `Public` role

<!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-cloud/db-... -->

<!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-cloud/pub... -->

You can modify the privileges of your organization's `PUBLIC` role as well as
the define default privileges for `PUBLIC`.

## System catalog roles

Certain internal objects may only be queried by superusers or by users
belonging to a particular builtin role, which superusers may
[grant](/sql/grant-role). These include the following:

| Name                  | Description                                                                                                                                                                                                                                                                                                                                                                                                   |
|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `mz_monitor`          | Grants access to objects that reveal actions taken by other users, in particular, SQL statements they have issued. Includes [`mz_recent_activity_log`](/sql/system-catalog/mz_internal#mz_recent_activity_log) and [`mz_notices`](/sql/system-catalog/mz_internal#mz_notices).                                                                                                                                    |
| `mz_monitor_redacted` | Grants access to objects that reveal less sensitive information about actions taken by other users, for example, SQL statements they have issued with constant values redacted. Includes `mz_recent_activity_log_redacted`, [`mz_notices_redacted`](/sql/system-catalog/mz_internal#mz_notices_redacted), and [`mz_statement_lifecycle_history`](/sql/system-catalog/mz_internal#mz_statement_lifecycle_history). |
|                       |


---

## Appendix: Privileges


> **Note:** 
Various SQL operations require additional privileges on related objects, such
as:

- For objects that use compute resources (e.g., indexes, materialized views,
  replicas, sources, sinks), access is also required for the associated cluster.

- For objects in a schema, access is also required for the schema.

For details on SQL operations and needed privileges, see [Appendix: Privileges
by command](/security/appendix/appendix-command-privileges/).


The following privileges are available in Materialize:


#### By Privilege

<!-- Dynamic table: rbac/privileges_objects - see original docs -->

#### By Object

<!-- Dynamic table: rbac/object_privileges - see original docs -->


---

## Appendix: Privileges by commands


<!-- Dynamic table: rbac/command_privileges - see original docs -->