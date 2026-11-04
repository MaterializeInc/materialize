---
title: "jq function"
description: "Applies a jq program to a jsonb value."
menu:
  main:
    parent: 'sql-functions'
---

{{< warn-if-unreleased "v26.50" >}}

`jq` applies a [jq program](https://jqlang.org/manual/) to a
[`jsonb`](/sql/types/jsonb/) value and returns the result as `jsonb`. Use it to
write per-row logic, such as sorting, filtering, or deduplicating a list, that
would otherwise require subqueries. For a walkthrough, see [Write functions
with jq](/transform-data/user-defined-functions/jq/).

## Syntax

{{% include-syntax file="examples/sql_functions/jq" example="syntax" %}}

### Return value

`jq` returns [`jsonb`](/sql/types/jsonb/).

- If `<input>` is `NULL`, `jq` returns `NULL`.
- If the program produces no value, `jq` returns `NULL`.
- If the program produces more than one value, `jq` returns an error. To
  return several values, collect them into an array (`[...]`) or an object.

## Details

- **Program literal.** The program must be a string literal. Dollar-quoted
  strings (`$$ ... $$`) are useful for multi-line programs. Concatenating
  literals, for example `'.' || 'a'`, is allowed.
- **One input, one output.** The program sees only `<input>`. To use several
  columns, pack them with [`jsonb_build_object`](/sql/types/jsonb/#jsonb_build_object)
  or [`jsonb_build_array`](/sql/types/jsonb/#jsonb_build_array). To return
  several values, return an object and extract its fields with `->` or `->>`.
- **Evaluation.** `jq` is evaluated one row at a time, inside the dataflow that
  computes the query. It does not maintain state of its own.
- **Implementation.** Programs run on [jaq](https://github.com/01mf02/jaq), a
  Rust implementation of jq. jaq differs from jq in some
  [details](https://github.com/01mf02/jaq#differences-between-jq-and-jaq).
- **Unavailable filters.** Filters whose result depends on the environment or
  on the current time are not available: `env`, `now`, `localtime`,
  `strflocaltime`, `strptime`, `input`, and `inputs`. `debug` and `stderr`
  have no effect. Module imports (`import`, `include`) are not supported.
- **Numbers.** Integers keep full precision. Other numbers keep their
  precision until the program does arithmetic on them, which converts them to
  64-bit floating point.
- **Errors.** Syntax errors and undefined filters are reported when the query
  is planned. Runtime errors, such as `error("...")`, `halt_error`, or output
  that is not valid JSON (for example, `nan`), are reported as query errors.

{{< warning >}}
jq programs have no evaluation limit. A program that does not terminate keeps
the cluster replica's worker busy indefinitely, and a program that recurses
without bound can crash the replica. Test programs with ad hoc `SELECT`
queries before using them in materialized views or indexes.
{{< /warning >}}

## Examples

Sort a list of objects by a field and keep one field:

```mzsql
SELECT jq(
  '{"items": [{"name": "pear", "price": 3}, {"name": "fig", "price": 1}, {"name": "kiwi", "price": 2}]}',
  '.items | sort_by(.price) | map(.name)'
) AS names;
```

```nofmt
         names
------------------------
 ["fig","kiwi","pear"]
```

Filter a list. Wrap the program in `[...]` to collect several outputs into one
array:

```mzsql
SELECT jq('[1, 2, 3]', '[.[] | select(. > 1)]') AS result;
```

```nofmt
 result
--------
 [2,3]
```

Combine several columns into the input, and return several values as an
object:

```mzsql
SELECT jq(
  jsonb_build_object('name', 'ada', 'langs', jsonb_build_array('en', 'fr')),
  '{name: .name | ascii_upcase, langs: (.langs | length)}'
) AS result;
```

```nofmt
          result
---------------------------
 {"langs":2,"name":"ADA"}
```

## Related pages

- [Write functions with jq](/transform-data/user-defined-functions/jq/)
- [`jsonb` type](/sql/types/jsonb/)
