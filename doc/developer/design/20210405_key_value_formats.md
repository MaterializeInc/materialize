# Handling Key-Value message transports

## Summary

<!--
// Brief, high-level overview. A few sentences long.
// Be sure to capture the customer impact - framing this as a release note may be useful.
-->

For context, Kafka -- unlike most other message transports -- exposes "key" information as part of
every message. Keys can be arbitrary serialized bytes, but for our customers so far, commonly use
the same serialization as the value of each message.

We do not have general support for Kafka keys, only a little bit of (differently) special-cased
support in our two `UPSERT` envelopes and our `DEBEZIUM` envelope.

The specific proposal here is to move the special casing outside of `FORMAT` (and `ENVELOPE`)
syntax into a new `KEY FORMAT`/`VALUE FORMAT` syntax.

## Goals

<!--
// Enumerate the concrete goals that are in scope for the project.
-->

The inciting goal for this is to unify the syntax and implementation for `UPSERT` and `DEBEZIUM`
envelopes, but an equally-important goal is to set us up to be able to better handle key/value
message transports in the future.

This project should:

* End up with unified syntax for all the envelopes that we provide for Kafka-specific data
* Simplify the implementation of current key/value transports, and envelopes that depend on them

## Non-Goals

<!--
// Enumerate potential goals that are explicitly out of scope for the project
// ie. what could we do or what do we want to do in the future - but are not doing now
-->

While we don't want to specifically make much progress on them, it is important that the work in
this project sets us up for, or at least does not preclude:

1. Supporting other key/value transports
2. Adding new formats to the existing transports
3. Using the `key` fields from Kafka in user dataflows

## Description

<!--
// Describe the approach in detail. If there is no clear frontrunner, feel free to list all approaches in alternatives.
// If applicable, be sure to call out any new testing/validation that will be required
-->

### Syntax Changes

The Kafka source type syntax will change to support explicitly defining the Key encoding via a `KEY
FORMAT` specifier, in which case the value format will need to be specified as `VALUE FORMAT`. The
format syntax fragment will become:

```python
'FORMAT' format_specifier | 'KEY FORMAT' format_specifier 'VALUE FORMAT' format_specifier
```

The plain `FORMAT` specifier will still be allowed to specify an confluent schema registry config,
which may define both the key and value formats.

### Implementation Details

The `Encoding` enum in the sql-parser AST will grow new variants:

* `KeyValue { key: Box<Encoding>, value: Box<Encoding> }` which will be used either if the user
  specifies both a key and a value variant.
* (optionally, as a fast-follow) `Undefined`, which will only be used as the key variant if the
  user specifies the confluent schema registry format and we are unable to determine the schema for
  the key. This allows us to error if the envelope (or later SQL) depends on a key schema.

  The default `ENVELOPE NONE` does not care about the key schema, but initially we will just error
  out in all cases instead of supporting this.

The current syntax for UPSERT will continue to work for at least one release cycle, but will be
normalized to use the new implementation.

### Future: Supporting message keys in dataflows

Thinking about point 3 of non-goals, once key encodings are well-integrated it will be possible as
future work to access Kafka keys with a small amount of new syntax e.g. `'KEEPING' (KEY | VALUE |
BOTH)` or a with-option.

## Alternatives

<!--
// Similar to the Description section. List of alternative approaches considered, pros/cons or why they were not chosen
-->

### Continue adding piecemeal support for keys as part of specific encodings

This was tried in [6289](https://github.com/MaterializeInc/materialize/pull/6286), but it seems
like the wrong idea.

## Open questions

<!--
// Anything currently unanswered that needs specific focus. This section may be expanded during the doc meeting as
// other unknowns are pointed out.
// These questions may be technical, product, or anything in-between.
-->

Unknown.
