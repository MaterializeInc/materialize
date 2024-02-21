---
title: "Builtin roles"
description: "Roles granting access to system objects"
weight: 40
menu:
  main:
    parent: 'reference'
    name: 'Builtin roles'
    weight: 126
---

Certain internal objects may only be queried by superusers or by users
belonging to a particular builtin role, which superusers may
[grant](../grant-role). These include the following:

| Name                  | Description                                                                                                                                                                                                                                                                                                                                                                                                   |
|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `mz_monitor`          | Grants access to objects that reveal actions taken by other users, in particular, SQL statements they have issued. Includes [`mz_recent_activity_log`](../system-catalog/mz_internal#mz_recent_activity_log) and [`mz_notices`](../system-catalog/mz_internal#mz_notices).                                                                                                                                    |
| `mz_monitor_redacted` | Grants access to objects that reveal less sensitive information about actions taken by other users, for example, SQL statements they have issued with constant values redacted. Includes `mz_recent_activity_log_redacted`, [`mz_notices_redacted`](../system-catalog/mz_internal#mz_notices_redacted), and [`mz_statement_lifecycle_history`](../system-catalog/mz_internal#mz_statement_lifecycle_history). |
|                       |                                                                                                                                                                                                                                                                                                                                                                                                               |
