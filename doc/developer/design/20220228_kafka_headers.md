# Kafka Headers

## Summary

Support the `headers` section of kafka messages in sources. This is a `string` -> `bytes` map of arbitrary
data. The purpose of this design document is to describe a multi-phase implementation of this

## Goals

1. Begin support with basic support for unstructured data from headers that can be cast/converted later
in the sql layer
2. Extend support to include typed, named headers

## Non-Goals

It is a non-goal to support
[Kafka Connect `Header`'s](https://kafka.apache.org/20/javadoc/index.html?org/apache/kafka/connect/header/Header.html),
which are, as far as I can tell, _dynamically-schema'd_ headers built on kafka headers. This is similar to the
dynamic schema's supported by Kafka Connect.


## Description


### Phase 1

Add an `INCLUDE HEADERS` syntax that allows users to declare that they want a `headers` column for their source,
as provided in kafka sources. This will provide a map from text to bytes (`bytea`).
Users are expected to cast these bytes values to the types they want in a later view.
This option will only be available for `ENVELOPE NONE` and `ENVELOPE UPSERT`.

### Phase 2
As an extension to phase 1, allows users to pre-declare their expected types in the source definition, simplifying their usage.

## Proposed syntax changes

In phase 1, kafka source creation will be extended so that `INCLUDE` has a new `HEADERS` option, so the full
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
`map[text=>bytea]` with the name `headers`, if it doesn't clash
with an existing column.

In phase 2, the syntax will be
```
INCLUDE (
    KEY (AS <name>)? |
    TIMESTAMP (AS <name>)? |
    OFFSET (AS <name>)? |
    PARTITION (AS <name>)? |
    TOPIC (AS <name>)? |
    HEADERS \(headername( type)...\)?
)+
```
where `type` will override the type from `bytes` to the given one.


## Implementation notes

In phase 1, once the planner places the headers in the right spot, all we need to do is pass down if we want the headers as a boolean,
into the kafka source creation. The `SourceDesc` will contain the extra column. The source would pack the `SourceMessage`
with the headers (or an empty map) for each record.

In phase 2, the source would need also be given any type information, and convert the types. If we support complex schematization,
refactoring sources so that header's can be decoded in the decode stage.


## Alternatives

- Don't implement phase 2, and leave transformations all in later views

- (Original plan): In phase 1, we could still support only `bytea` header values, but pre-declare the specific header keys we want

- In both phases, we could collate the headers into a record field, instead of upgrading them to full columns (this may only be possible in phase 1).

- In phase 2, we could come up with some way of declaring an actual `schema` with a format. This would complicate the syntax and the implemention
by quite a bit.

- Consider an option that enforces utf8

## Open questions

- In phase 1, how do we deal with header names clashing with other columns? Do we add an `as <name>` syntax?
- In phase 1, is the INCLUDE syntax un-ambiguous?
- Should we support non-nullable headers?
- From https://github.com/MaterializeInc/materialize/issues/8446, @quodlibetor mentioned `self-retracting-envelopes` could cause problems, what are those problems?
- Will some users expect a new value without a header to keep the header value from the previous message? The current design does not handle this
