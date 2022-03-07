# Kafka Headers

## Summary

Support the `headers` section of kafka messages in sources. This is a `string` -> `bytes` multimap of arbitrary
data. The purpose of this design document is to describe how we can implement this.

## Goals

Add basic support for unstructured data from headers that can be cast/converted later
in the sql layer

## Non-Goals

It is a non-goal to support
[Kafka Connect `Header`'s](https://kafka.apache.org/20/javadoc/index.html?org/apache/kafka/connect/header/Header.html),
which are, as far as I can tell, _dynamically-schema'd_ headers built on kafka headers. This is similar to the
dynamic schema's supported by Kafka Connect.


## Description

Add an `INCLUDE HEADERS` syntax that allows users to declare that they want a `headers` column for their source,
as provided in kafka sources. This will provide a list of from (text, bytes (`bytea`)) pairs. This is because
Kafka headers can have multiple values per-key, and preserve order.
Users are expected to cast these bytes values to the types they want in a later view.
This option will only be available for `ENVELOPE NONE` and `ENVELOPE UPSERT`.

## Proposed syntax changes

Kafka source creation will be extended so that `INCLUDE` has a new `HEADERS` option, so the full
syntax becomes:

```
INCLUDE (
    KEY (AS <name>)? |
    TIMESTAMP (AS <name>)? |
    OFFSET (AS <name>)? |
    PARTITION (AS <name>)? |
    TOPIC (AS <name>)? |
    HEADERS (AS <name>)?
)+
```
as an example:
```sql
CREATE MATERIALIZED SOURCE avroavro
FROM KAFKA BROKER '...' TOPI '...`
FORMAT ...
INCLUDE HEADERS
ENVELOPE ...
```

This will add a new column to the row from the kafka source that is of type
`list[Record { key: text, values: bytea}]` with the name `headers`, with
the column name being overrideable

## Implementation notes

Once the planner places the headers in the right spot, all we need to do is pass down if we want the headers as a boolean,
into the kafka source creation. The `SourceDesc` will contain the extra column. The source would pack the `SourceMessage`
with the headers (or an empty list) for each record.

## Extensions

A list of key-value records is not very ergonomic. Over time, we can provide helper functions that improve this.
Some examples:

- `headerstomap`: convert the headers list into a map with "smallest value wins" semantics.
- Some kind of `avro_decode` function: This is more complex as it may require altering how we build dataflows

## Alternatives

- Implement typing as part of the sql definition, either sql types, or some way of declaring how to get a schema
and its `FORMAT`. This would significantly complicate the Kafka `CREATE SOURCE` statement, which is already complex.

- Allow users to pre-declare specific header keys, and provide columns for each one.

  - Collate the headers into a record field, instead of separate columns

- Consider an option that enforces utf8 for header values

## Open questions

- Should we support nullable headers? As far as we can tell, headers can be empty, but not null.
- Can `ENVELOPE DEBEZIUM`, etc. be supported somehow?
  - [This](https://debezium.io/documentation/reference/1.0/configuration/event-flattening.html#_adding_metadata_fields_to_the_header)
  suggests that users can't add headers to debezium messages, but its unclear
- Will some users expect a new value without a header to keep the header value from the previous message? The current design does not handle this
