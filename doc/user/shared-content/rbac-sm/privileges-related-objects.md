Various SQL operations require additional privileges on related objects, such
as:

- For objects that use compute resources (e.g., indexes, materialized views,
  replicas, sources, sinks), access is also required for the associated cluster.

- For objects in a schema, access is also required for the schema.

For details on SQL operations and needed privileges, see [Appendix: Privileges
by command](/security/appendix/appendix-command-privileges/).
