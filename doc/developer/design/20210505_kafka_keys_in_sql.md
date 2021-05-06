# Kafka Keys as Values

## Summary

<!--
// Brief, high-level overview. A few sentences long.
// Be sure to capture the customer impact - framing this as a release note may be useful.
-->

Kafka is unique in our supported sources (although not all possible sources,
e.g. [DynamoDB Streams][ddb]) in that it partitions the data portion of messages
into a Key part and a Value part. The Key is intended to be the equivalent of a
primary key in a database, and it is commonly used that way by Kafka users.
Following from that, the Key is semantically meaningful to the Kafka system --
it has effects on which shard the message goes to, and affects old-message
deletion. We rely on the Key primarily for Upsert logic, ensuring deduplication
of messages based on it.

An important aspect of the Key is that it is a separate data section -- it never
shares backing storage with the Value. This means that it is possible to have
data in the Key that appears nowhere in the Value, putting the responsibility on
consumers or [Kafka connectors][connect] to stitch the data back together.

Materialize does not support accessing the Key part of messages from our SQL
layer, which has been mentioned as a pain point on several occasions. This is a
design to resolve that pain point.

[connect]: https://kafka-tutorials.confluent.io/connect-add-key-to-source/kafka.html
[ddb]: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodbstreams.html#DynamoDBStreams.Client.get_records

## Goals

<!--
// Enumerate the concrete goals that are in scope for the project.
-->

Allowing users to specify that a Key should be part of the dataflow, and
accessible to all SQL processing.

## Non-Goals

<!--
// Enumerate potential goals that are explicitly out of scope for the project
// ie. what could we do or what do we want to do in the future - but are not doing now
-->

* Integrating Key descriptions more tightly into planning or index selection.
* Any specific optimizations

### Related *future* work

We have two other projects that we know that we would eventually like to
implement that may have interactions with this design:

* We want to be able to specify a subset of fields from values as the keys for
  upsert -- although current thinking is that this should probably be done as an
  upsert function.
* We would like to expose message metadata as columns or records to the SQL.

This project does not aim to resolve any aspect of those future projects, but
anything that conflicts with those goals in this design should definitely be
called out.

## Description

<!--
// Describe the approach in detail. If there is no clear frontrunner, feel free to list all approaches in alternatives.
// If applicable, be sure to call out any new testing/validation that will be required
-->

### Syntax and user-visible semantics

Enhance the Kafka create source syntax like so:

```sql
CREATE SOURCE <source-name> FROM KAFKA BROKER '<>'
<format>
[INCLUDE KEY [AS RECORD <key-record-name>]]
```

where `<format>` is either:

* An explicit key/value format:

  ```sql
  KEY FORMAT <specifier>
  VALUE FORMAT <specifier>
  ```

* A bare format that specifies a Confluent Schema Registry (CSR) config

  ```sql
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY ...
  ```

If `INCLUDE KEY` is specified, the source schema will be augmented to prepend
all of the values of the key as columns in the dataflow. Format types that do
not specify subfields (e.g. `FORMAT TEXT`) will use their standard column names
(i.e. `text`).

Alternatively, if the `INCLUDE KEY AS RECORD <name>` syntax is used all fields
will be included as fields on a record with the specified name.

If any key column names colide with value columns using the bare `INCLUDE KEY`
syntax, an error will be raised before dataflow construction suggesting the use
of the `AS RECORD` clause.

#### Usage Examples

##### Text/Text

```sql
CREATE SOURCE text_text FROM KAFKA BROKER '...' TOPIC '...'
KEY FORMAT TEXT VALUE FORMAT TEXT
INCLUDE KEY AS RECORD key
ENVELOPE UPSERT;
```

<details>
<summary>👨‍🔬 It is possible to express the above with a bare format. 👨‍🔬</summary>

```sql
CREATE SOURCE text_text FROM KAFKA BROKER '...' TOPIC '...'
FORMAT TEXT
INCLUDE KEY AS RECORD key
ENVELOPE UPSERT;
```

</details>

This will create a new source, it requires specifying the record syntax because
we use the same name by default, which would result in a name conflict.

Usage in a view looks like:

```sql
CREATE MATERIALIZED VIEW text_text_view AS
SELECT (key).text, text FROM text_text
WHERE ...;
```

##### Text/Avro

```sql
CREATE SOURCE text_avro FROM KAFKA BROKER '...' TOPIC '...'
KEY FORMAT TEXT
VALUE FORMAT AVRO USING SCHEMA '{"type": "record", "name": "value", "fields": [ {"name": "field", "type": "int"} ] }';
```

Usage in a view looks like:

```sql
CREATE MATERIALIZED VIEW text_avro_view AS
SELECT text, field as interesting FROM text_avro;
```

##### Avro/Avro

```sql
CREATE SOURCE avro_avro FROM KAFKA BROKER '...' TOPIC '...'
KEY FORMAT AVRO USING SCHEMA '{"type": "record", "name": "boring", "fields": [ {"name": "key_field", "type": "int"} ] }'
VALUE FORMAT AVRO USING SCHEMA '{"type": "record", "name": "value", "fields": [ {"name": "valueish", "type": "int"} ] }';
```

Usage in a view looks like:

```sql
CREATE MATERIALIZED VIEW text_avro_view AS
SELECT key_field, valueish FROM avro_avro; -- equivalent to SELECT *
```

### Implementation

Mostly standard for new syntax additions. We will need to wait until
`purify_format` to provide any of the specific error-detection because of the
fact that Key schemas may come from the Confluent Schema Registry.

Physically unpacking key fields will happen identically independent of if `AS
RECORD` is specified, only the projection mapping and names available to SQL
will differ.

## Alternatives

<!--
// Similar to the Description section. List of alternative approaches considered, pros/cons or why they were not chosen
-->

### Alternative: Just docs

We could just improve documentation around getting Kafka keys into the value
section using Kafka itself.

### Alternative: Record-valued function

It might be possible to provide something like a `kafka_key()` function that
introduces a demand for the key, pulling it into dataflows.

### Alternative syntax: key and value records

Instead of automatically extracting Key columns at all, we could provide a
syntax like:

```sql
CREATE SOURCE <source-name> FROM KAFKA BROKER '<>'
<format>
[AS KEY VALUE]
```

which which would change emit the Kafka message key as a `key` record, but would
*also* change the message value to be available as a `value` record (no longer
automatically expanding the fields into columns).

Downsides to this approach are that a small bit of syntax dramatically changes
usage patterns from the SQL layer. That has effects both on documentation which
would now have to deal with a niche alternative usage. Changing to `AS KEY
VALUE` would force a rewrite or intermediate view if a user realizes that they
need data from the key after already using a topic for awhile.

### Alternative semantics: Always provide key and value fields

Semantically it would be fine to (semantically) always allow access to key
fields and then rely on the use (or not) of that column to drive the
instantiation of the key.

The largest downsides that I see to this philosophy are:

* Backwards-compatibility: `SELECT *`-type queries will change the semantics of
  which columns they present. Especially undesirable for things like debezium,
  where the key is afaik always a duplicated subset of the value fields.
* Ergonomically: I think that for this to be an unambiguous API we would require
  that key and value were always presented as the `key`/`value` record syntax
  from the previous alternative, which would be both more annoying (for users
  and for docs) and an even larger backcompat change.

## Open questions

<!--
// Anything currently unanswered that needs specific focus. This section may be expanded during the doc meeting as
// other unknowns are pointed out.
// These questions may be technical, product, or anything in-between.
-->

We could make the `AS RECORD <name>` syntax required. The proposed design is
based on the assumption that the common case is that key fields do not conflict
with value fields, but a more conservative option is just requiring users to
specify everything and do more work for value access.
