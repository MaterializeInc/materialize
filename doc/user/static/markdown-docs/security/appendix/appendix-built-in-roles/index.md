# Appendix: Built-in roles

List of predefined built-in roles in Materialize.



## `Public` role

All roles in Materialize are automatically members of
<a href="/security/appendix/appendix-built-in-roles/#public-role" ><code>PUBLIC</code></a>. As
such, every role includes inherited privileges from <code>PUBLIC</code>.

<p>By default, the <code>PUBLIC</code> role has the following privileges:</p>

**Baseline privileges via PUBLIC role:**

| Privilege | Description | On database object(s) |
| --- | --- | --- |
| <code>USAGE</code> | Permission to use or reference an object. | <ul> <li>All <code>*.public</code> schemas (e.g., <code>materialize.public</code>);</li> <li><code>materialize</code> database; and</li> <li><code>quickstart</code> cluster.</li> </ul>  |


**Default privileges on future objects set up for PUBLIC:**

| Object(s) | Object owner | Default Privilege | Granted to | Description |
| --- | --- | --- | --- | --- |
| <a href="/sql/types/" ><code>TYPE</code></a> | <code>PUBLIC</code> | <code>USAGE</code> | <code>PUBLIC</code> | When a <a href="/sql/types/" >data type</a> is created (regardless of the owner), all roles are granted the <code>USAGE</code> privilege. However, to use a data type, the role must also have <code>USAGE</code> privilege on the schema containing the type. |

Default privileges apply only to objects created after these privileges are
defined. They do not affect objects that were created before the default
privileges were set.

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
