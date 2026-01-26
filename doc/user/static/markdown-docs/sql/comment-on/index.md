# COMMENT ON

`COMMENT ON` adds or updates the comment of an object.



Use `COMMENT ON` to:

- Add a comment to an object.
- Update the comment to an object.
- Remove the comment from an object.

## Syntax



```mzsql
COMMENT ON <object_type> <name> IS <comment | NULL>;

```

| Syntax element | Description |
| --- | --- |
| `<object_type>` | The type of the object. Supported object types:  - `CLUSTER` - `CLUSTER REPLICA` - `COLUMN` - `CONNECTION` - `DATABASE` - `FUNCTION` - `INDEX` - `MATERIALIZED VIEW` - `NETWORK POLICY` - `ROLE` - `SCHEMA` - `SECRET` - `SINK` - `SOURCE` - `TABLE` - `TYPE` - `VIEW`  |
| `<name>` | The fully qualified name of the object.  |
| `<comment \| NULL>` | - The comment string for the object. - Use `NULL` to remove an existing comment.  |


## Details

`COMMENT ON` stores a comment about an object in the database. Each object can only have one
comment associated with it, so successive calls of `COMMENT ON` to a single object will overwrite
the previous comment.

To read the comment on an object you need to query the [mz_internal.mz_comments](/sql/system-catalog/mz_internal/#mz_comments)
catalog table.

## Privileges

The privileges required to execute this statement are:

<ul>
<li>Ownership of the object being commented on (unless the object is a role).</li>
<li>To comment on a role, you must have the <code>CREATEROLE</code> privilege.</li>
</ul>


For more information on ownership and privileges, see [Role-based access
control](/security/).

## Examples

```mzsql
--- Add comments.
COMMENT ON TABLE foo IS 'this table is important';
COMMENT ON COLUMN foo.x IS 'holds all of the important data';

--- Update a comment.
COMMENT ON TABLE foo IS 'holds non-important data';

--- Remove a comment.
COMMENT ON TABLE foo IS NULL;

--- Read comments.
SELECT * FROM mz_internal.mz_comments;
```
