Namespaces are a way to organize Materialize objects logically. In organizations
with multiple objects, namespaces help avoid naming conflicts and make it easier
to manage objects.

## Namespace hierarchy

Materialize follows SQL standard's namespace hierarchy for most objects (for the
exceptions, see [Other objects](#other-objects)).

|                           |             |
|---------------------------| ------------|
| 1st/Highest level:        |  **Database** |
| 2nd level:                |  **Schema**   |
| 3rd level:                | <table><tbody><tr><td><ul><li>**Table**</li><li>**View**</li><li>**Materialized view**</li><li>**Connection**</li></ul></td><td><ul><li>**Source**</li><li>**Sink**</li><li>**Index**</li></ul></td><td><ul><li>**Type**</li><li>**Function**</li><li>**Secret**</li></ul></td></tr></tbody></table>|
| 4th/Lowest level:             | **Column**     |

Each layer in the hierarchy can contain elements from the level immediately
beneath it. That is,

- Databases can contain: schemas;
- Schemas can contain: tables, views, materialized views, connections, sources,
sinks, indexes, types, functions, and secrets;
- Tables, views, and materialized views can contain: columns.


### Qualifying names

Namespaces enable disambiguation and access to objects across different
databases and schemas. Namespaces use the dot notation format
(`<database>.<schema>....`) and allow you to refer to objects by:

- **Fully qualified names**

  Used to reference objects in a different database (Materialize allows
  cross-database queries); e.g.,

  ```
  <Database>.<Schema>
  <Database>.<Schema>.<Source>
  <Database>.<Schema>.<View>
  <Database>.<Schema>.<Table>.<Column>
  ```

  {{< tip >}}

  You can use fully qualified names to reference objects within the same
  database (or within the same database and schema). However, for brevity and
  readability, you may prefer to use qualified names instead.

  {{</ tip >}}

- **Qualified names**

  - Used to reference objects within the same database but different schema, use
    the schema and object name; e.g.,

    ```
    <Schema>.<Source>
    <Schema>.<View>
    <Schema>.<Table>.<Column>
    ```

  - Used to reference objects within the same database and schema, use the
    object name; e.g.,

    ```
    <Source>
    <View>
    <Table>.<Column>
    <View>.<Column>
    ```

## Namespace constraints

All namespaces must adhere to [identifier rules](/sql/identifiers).


## Other objects

The following Materialize objects  exist outside the standard SQL namespace
hierarchy:

- **Clusters**: Referenced directly by its name.

  For example, to create a materialized view in the cluster `cluster1`:

  ```mzsql
  CREATE MATERIALIZED VIEW mv IN CLUSTER cluster1 AS ...;
  ```

- **Cluster replicas**: Referenced as `<cluster-name>.<replica-name>`.

  For example, to delete replica `r1` in cluster `cluster1`:

  ```mzsql
  DROP CLUSTER REPLICA cluster1.r1
  ```

- **Roles**: Referenced by their name. For example, to alter the `manager` role, your SQL statement would be:

  ```mzsql
  ALTER ROLE manager ...
  ```

### Other object namespace constraints

- Two clusters or two roles cannot have the same name. However, a cluster and a
  role can have the same name.

- Replicas can have the same names as long as they belong to different clusters.
  Materialize automatically assigns names to replicas (e.g., `r1`, `r2`).

## Database details

- By default, Materialize regions have a database named `materialize`.
- By default, each database has a schema called `public`.
- You can specify which database you connect to either when you connect (e.g.
  `psql -d my_db ...`) or within SQL using [`SET DATABASE`](/sql/set/) (e.g.
  `SET DATABASE = my_db`).
- Materialize allows cross-database queries.
