# SHOW CREATE SOURCE

`SHOW CREATE SOURCE` returns the statement used to create the source.



`SHOW CREATE SOURCE` returns the DDL statement used to create the source.

## Syntax

```sql
SHOW [REDACTED] CREATE SOURCE <source_name>;
```


| Syntax element | Description |
| --- | --- |
| <strong>REDACTED</strong> | If specified, literals will be redacted. |


For available source names, see [`SHOW SOURCES`](/sql/show-sources).

## Examples

```mzsql
SHOW CREATE SOURCE market_orders_raw;
```

```nofmt
                 name                 |                                      create_sql
--------------------------------------+--------------------------------------------------------------------------------------------------------------
 materialize.public.market_orders_raw | CREATE SOURCE "materialize"."public"."market_orders_raw" IN CLUSTER "c" FROM LOAD GENERATOR COUNTER
```

## Privileges

The privileges required to execute this statement are:

<ul>
<li><code>USAGE</code> privileges on the schema containing the source.</li>
</ul>


## Related pages

- [`SHOW SOURCES`](../show-sources)
- [`CREATE SOURCE`](../create-source)
