{{< tip >}}
The `IF NOT EXISTS` option can be useful for idempotent table creation scripts.
However, it only checks whether a table with the same name exists, not whether
the existing table matches the specified table definition. Use with validation
logic to ensure the existing table is the one you intended to create.
{{< /tip >}}
