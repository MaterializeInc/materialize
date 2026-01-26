# SHOW CREATE SINK

`SHOW CREATE SINK` returns the statement used to create the sink.



`SHOW CREATE SINK` returns the DDL statement used to create the sink.

## Syntax

```sql
SHOW [REDACTED] CREATE SINK <sink_name>;
```


| Syntax element | Description |
| --- | --- |
| <strong>REDACTED</strong> | If specified, literals will be redacted. |


For available sink names, see [`SHOW SINKS`](/sql/show-sinks).

## Examples

```mzsql
SHOW SINKS
```

```nofmt
     name
--------------
 my_view_sink
```

```mzsql
SHOW CREATE SINK my_view_sink;
```

```nofmt
               name              |                                                                                                        create_sql
---------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 materialize.public.my_view_sink | CREATE SINK "materialize"."public"."my_view_sink" IN CLUSTER "c" FROM "materialize"."public"."my_view" INTO KAFKA CONNECTION "materialize"."public"."kafka_conn" (TOPIC 'my_view_sink') FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection ENVELOPE DEBEZIUM
```

## Privileges

The privileges required to execute this statement are:

<ul>
<li><code>USAGE</code> privileges on the schema containing the sink.</li>
</ul>


## Related pages

- [`SHOW SINKS`](../show-sinks)
- [`CREATE SINK`](../create-sink)
