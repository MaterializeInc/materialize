# ALTER TABLE
`ALTER TABLE` changes properties of a table.
Use `ALTER TABLE` to:

- Rename a table.
- Change owner of a table.
- Change retain history configuration for the table.

## Syntax


**Rename:**

### Rename

To rename a table:



```mzsql
ALTER TABLE <name> RENAME TO <new_name>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The current name of the table you want to alter.  |
| `<new_name>` | The new name of the table.  |
See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).



**Change owner:**

### Change owner

To change the owner of a table:



```mzsql
ALTER TABLE <name> OWNER TO <new_owner_role>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the table you want to change ownership of.  |
| `<new_owner_role>` | The new owner of the table.  |
To change the owner of a table, you must be the owner of the table and have
membership in the `<new_owner_role>`. See also [Privileges](#privileges).



**(Re)Set retain history config:**

### (Re)Set retain history config

To set the retention history for a user-populated table:



```mzsql
ALTER TABLE <name> SET (RETAIN HISTORY [=] FOR <retention_period>);

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the table you want to alter.  |
| `<retention_period>` | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.  |


To reset the retention history to the default for a user-populated table:



```mzsql
ALTER TABLE <name> RESET (RETAIN HISTORY);

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the table you want to alter.  |





## Privileges

The privileges required to execute this statement are:

- Ownership of the table being altered.
- In addition, to change owners:
  - Role membership in `new_owner`.
  - `CREATE` privileges on the containing schema if the table is namespaced by
  a schema.
