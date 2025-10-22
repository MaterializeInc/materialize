---
title: "Pushdown functions"
description: "Functions for use with the filter pushdown feature"
menu:
  main:
    parent: 'sql-functions'
---

`try_parse_monotonic_iso8601_timestamp` parses a subset of [ISO 8601]
timestamps that matches the 24 character length output
of the javascript [Date.toISOString()] function.
Unlike other parsing functions, inputs that fail to parse return `NULL`
instead of error.

This allows `try_parse_monotonic_iso8601_timestamp` to be used with
the [temporal filter pushdown] feature on `text` timestamps.
This is particularly useful when working with
[JSON sources](/sql/create-source-v1/#json),
or other external data sources that store timestamps as strings.

Specifically, the accepted format is `YYYY-MM-DDThh:mm:ss.sssZ`:

- A 4-digit positive year, left-padded with zeros followed by
- A literal `-` followed by
- A 2-digit month, left-padded with zeros followed by
- A literal `-` followed by
- A 2-digit day, left-padded with zeros followed by
- A literal `T` followed by
- A 2-digit hour, left-padded with zeros followed by
- A literal `:` followed by
- A 2-digit minute, left-padded with zeros followed by
- A literal `:` followed by
- A 2-digit second, left-padded with zeros followed by
- A literal `.`
- A 3-digit millisecond, left-padded with zeros followed by
- A literal `Z`, indicating the UTC time zone.

Ordinary `text`-to-`timestamp` casts will prevent a filter from being pushed down.
Replacing those casts with `try_parse_monotonic_iso8601_timestamp` can unblock that
optimization for your query.

## Examples

```mzsql
SELECT try_parse_monotonic_iso8601_timestamp('2015-09-18T23:56:04.123Z') AS ts;
```
```nofmt
 ts
--------
 2015-09-18 23:56:04.123
```

 <hr/>

```mzsql
SELECT try_parse_monotonic_iso8601_timestamp('nope') AS ts;
```
```nofmt
 ts
--------
 NULL
```

[ISO 8601]: https://en.wikipedia.org/wiki/ISO_8601
[Date.toISOString()]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/toISOString
[temporal filter pushdown]: /transform-data/patterns/temporal-filters/#temporal-filter-pushdown
[jsonb]: /sql/types/jsonb/
