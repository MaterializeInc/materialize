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

Add an `INCLUDE HEADERS` syntax that allows users to declare specific, named headers, as additional columns
provided in kafka sources. This will provide nullable, bytes-typed values equal to the header with the specified name.
Users are expected to cast these bytes values to the types they want in a later view.

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
    HEADERS \(headername...\)?
)+
```
as an example:
```sql
CREATE MATERIALIZED SOURCE avroavro
FROM KAFKA BROKER '...' TOPI '...`
FORMAT ...
INCLUDE HEADERS (my_special_header)
ENVELOPE ...
```

This will add a new column to the row from the kafka source that is of type
`NULLABLE bytes` with the name `my_special_header`, if it doesn't clash
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

In phase 1, once the planner places the headers in the right spot, all we need to do is pass the header names down
into the kafka source creation, but they should be in the `SourceDesc` somewhere. The source would pack the `SourceMessage`
with the headers (or `null`) for each record, and packing them in the row sent to the envelope would happen in the deode stage.

In phase 2, the source would need also read any type information, and convert the types. If we support complex schematization,
refactoring sources so that header's can also be decoded in the decode stage.


## Alternatives

- In phase 1, we could instead NOT pre-declare the headers' names, and instead collate them into a `json` record (or, we could add a new
`string -> bytes` `map` type and use that). This would simplify the implementation, but would complicate the user experience.

- In both phases, we could collate the headers into a record field, instead of upgrading them to full columns.

- In phase 2, we could come up with some way of declaring an actual `schema` with a format. This would complicate the syntax and the implemention
by quite a bit.

- Consider an option that enforces utf8

## Open questions

- In phase 1, how do we deal with header names clashing with other columns? Do we add an `as <name>` syntax?
- In phase 1, is the INCLUDE syntax un-ambiguous?
- Should we support non-nullable headers?
- From https://github.com/MaterializeInc/materialize/issues/8446, @quodlibetor mentioned `self-retracting-envelopes` could cause problems, what are those problems?
- Will some users expect a new value without a header to keep the header value from the previous message? The current design does not handle this
