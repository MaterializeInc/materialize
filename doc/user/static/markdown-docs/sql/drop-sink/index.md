# DROP SINK

`DROP SINK` removes a sink from Materialize.



`DROP SINK` removes a sink from Materialize.

Dropping a Kafka sink doesn't drop the corresponding topic. For more information, see the [Kafka documentation](https://kafka.apache.org/documentation/).


## Syntax

```mzsql
DROP SINK [IF EXISTS] <sink_name>;
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified sink does not exist.
`<sink_name>` | The sink you want to drop. You can find available sink names through [`SHOW SINKS`](../show-sinks).

## Examples

```mzsql
SHOW SINKS;
```
```nofmt
my_sink
```
```mzsql
DROP SINK my_sink;
```
```nofmt
DROP SINK
```

## Privileges

The privileges required to execute this statement are:

<ul>
<li>Ownership of the dropped sink.</li>
<li><code>USAGE</code> privileges on the containing schema.</li>
</ul>


## Related pages

- [`SHOW SINKS`](../show-sinks)
- [`CREATE SINK`](../create-sink)
- [`DROP OWNED`](../drop-owned)
