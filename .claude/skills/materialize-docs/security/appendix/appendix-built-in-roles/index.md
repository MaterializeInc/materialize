---
audience: developer
canonical_url: https://materialize.com/docs/security/appendix/appendix-built-in-roles/
complexity: advanced
description: List of predefined built-in roles in Materialize.
doc_type: reference
keywords:
- 'Appendix: Built-in roles'
product_area: Security
status: stable
title: 'Appendix: Built-in roles'
---

# Appendix: Built-in roles

## Purpose
List of predefined built-in roles in Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


List of predefined built-in roles in Materialize.


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