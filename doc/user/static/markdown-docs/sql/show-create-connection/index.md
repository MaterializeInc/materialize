# SHOW CREATE CONNECTION

`SHOW CREATE CONNECTION` returns the statement used to create the connection.



`SHOW CREATE CONNECTION` returns the DDL statement used to create the connection.

## Syntax

```sql
SHOW [REDACTED] CREATE CONNECTION <connection_name>;
```


| Syntax element | Description |
| --- | --- |
| <strong>REDACTED</strong> | If specified, literals will be redacted. |


For available connection names, see [`SHOW CONNECTIONS`](/sql/show-connections).

## Examples

```mzsql
SHOW CREATE CONNECTION kafka_connection;
```

```nofmt
    name          |    create_sql
------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 kafka_connection | CREATE CONNECTION "materialize"."public"."kafka_connection" TO KAFKA (BROKER 'unique-jellyfish-0000.us-east-1.aws.confluent.cloud:9092', SASL MECHANISMS = 'PLAIN', SASL USERNAME = SECRET sasl_username, SASL PASSWORD = SECRET sasl_password)
```

## Privileges

The privileges required to execute this statement are:

<ul>
<li><code>USAGE</code> privileges on the schema containing the connection.</li>
</ul>


## Related pages

- [`SHOW CONNECTIONS`](../show-sources)
- [`CREATE CONNECTION`](../create-connection)
