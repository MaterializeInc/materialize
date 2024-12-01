# Temporal Filter Expressions

- Associated: #26184

## The Problem

`mz_now()` in temporal filters is limited in ways that prevent users from using it as easily as they would prefer.
Often users want to type `WHERE event_ts >= mz_now() - '30s'::interval` but we error because `mz_now()` must stand alone.

## Success Criteria

Users are able to express at least simple operations on `mz_now()` in temporal filters (the above `WHERE` should function).

## Out of Scope

The [braindump](https://www.notion.so/materialize/mz_now-braindump-3acde85e02d84cb09808d7c63dab009a#3a56d5b83d554ccfb630300b02ffe5aa) for this feature suggested

> For the mz_timestamp type, ... allow the same operators as are allowed for the timestamp type.

We can perhaps solve the immediate problem only implementing a small subset of those operators.

## Solution Proposal

Three separate features can be implemented:

1. Allow more types to be implicitly castable to `mz_timestamp`.
Most time-adjacent types already support this.
We should implement the rest (`time`, `date`).

1. Allow `mz_timestamp` types to be added and subtracted to other `mz_timestamp`s (overflows will error).
Many types are [castable to `mz_timestamp`](https://materialize.com/docs/sql/types/mz_timestamp/) implicitly.
If we allow the addition and subtraction of `mz_timestamp` to `mz_timestamp`,
then expressions like `mz_now() - '30s'::interval` will implicitly cast the `'30s'::interval` to an `mz_timestamp`,
and the subtraction from `mz_now()` will work.
This approach has the benefit that we never need to worry about casting `mz_timestamp` to another type (like `timestamp`) that has different precision, range, and semantics.

1. Teach temporal filters in `linear.rs` to recognize expressions like `mz_now() + some_mz_timestamp > x` and convert them to `mz_now() > x - some_mz_timestamp`.
(`-` and the other inequality operators also supported.)
This logic can happen repeatedly, so `mz_now() + x - y <= z` becomes `mz_now() <= z - x + y`.

A concern about this is that the underlying data gain additional restrictions in order to not produce errors.
For example, `mz_now() - '10 years'::interval > x` is converted into `mz_now() > x + '10 years'::interval`,
so if `x` is within 10 years of u64::MAX milliseconds, the entire materialized view will error and be unreadable at any time.
Due to the range of `mz_timestamp` this seems unlikely to ever matter.
We already have a tiny version of this problem too: the `=`, `<=`, and `>` inequalities all add 1 to the non-`mz_now()` side,
so if the underlying data contain `u64::MAX` and one of those inequalities, the materialized view will error.
Because we already have a small version of this problem and the `mz_timestamp` range is so large (~500M years),
allowing the users to reduce that range seems acceptable.

## Minimal Viable Prototype

An MVP [exists](https://github.com/MaterializeInc/materialize/compare/main...maddyblue:materialize:mznow-expr).
It includes some now-merged code (implicit casting of interval to `mz_timestamp`) since that matches many existing casts and could be safely implemented before this design doc was written.

## Alternatives



## Open questions

- Is it always safe to move expressions around?
Are there some non-obvious problems or concerns not raised here?
