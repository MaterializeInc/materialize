---
title: "EXECUTE UNIT TEST"
description: "`EXECUTE UNIT TEST` defines a unit test for a view, run by mz-deploy against mocked input data."
menu:
  main:
    parent: "commands"
---

{{< public-preview >}}
`EXECUTE UNIT TEST`
{{< /public-preview >}}

`EXECUTE UNIT TEST` defines a unit test for a view or materialized view. A test
substitutes literal rows for the view's dependencies, runs the view against
that fixed input, and compares the result to a set of expected rows. Tests are
written inline in the same `.sql` file as the view they exercise.

{{< warning >}}
`EXECUTE UNIT TEST` is executed only by [`mz-deploy`](/manage/mz-deploy/), not by
Materialize itself. The Materialize SQL layer parses the statement but rejects
it during planning, so running it through a SQL client such as `psql` returns an
`EXECUTE UNIT TEST statement not yet supported` error. Use
[`mz-deploy test`](/manage/mz-deploy/develop/local-development/#write-and-run-unit-tests)
to discover and run these tests.
{{< /warning >}}

## Syntax

{{% include-syntax file="examples/execute_unit_test" example="syntax" %}}

## Details

### Where tests run

`mz-deploy test` discovers every `EXECUTE UNIT TEST` statement in a project,
then runs each one in a local Materialize Docker container — your remote
database is never touched. For each test, `mz-deploy` creates temporary views
for the mocks and the expected rows, rewrites the target view to read from the
mocks instead of its real dependencies, and computes the symmetric difference
between the view's output and the expected rows. The test passes when that
difference is empty.

### Mocking dependencies

Every object the target view depends on must have a `MOCK` clause; an unmocked
dependency is a validation error. A mock's column names and types must match the
real object's schema, and the target view's output columns must match the
`EXPECTED` column list. Run [`mz-deploy lock`](/manage/mz-deploy/develop/local-development/#lock-types)
to refresh the schema information used for this validation when an external
dependency changes.

### Type normalization

Column types in `MOCK` and `EXPECTED` are normalized before comparison, so
common aliases are interchangeable — for example `int`/`int4`/`integer`,
`bigint`/`int8`, `text`/`varchar`/`string`,
`float`/`float8`/`double precision`, `numeric`/`decimal`, and `json`/`jsonb`.

### Testing temporal logic

Views that filter on [`mz_now()`](/sql/functions/now_and_mz_now/) depend on the
current time, which would make their output non-deterministic in a test. Set
`AT TIME` to pin the value `mz_now()` returns so the test result is stable.

## Examples

### Testing a join

```mzsql
CREATE VIEW user_order_summary AS
SELECT u.id AS user_id, u.name, count(*) AS total_orders
FROM users u
JOIN orders o ON o.user_id = u.id
GROUP BY u.id, u.name;

EXECUTE UNIT TEST test_single_user_single_order
FOR materialize.public.user_order_summary
MOCK materialize.public.users(id bigint, name text) AS (
  SELECT * FROM VALUES (1, 'alice')
),
MOCK materialize.public.orders(id bigint, user_id bigint) AS (
  SELECT * FROM VALUES (10, 1)
)
EXPECTED(user_id bigint, name text, total_orders bigint) AS (
  SELECT * FROM VALUES (1, 'alice', 1)
);
```

### Pinning `mz_now()`

```mzsql
EXECUTE UNIT TEST test_recent_events
FOR materialize.public.recent_events
AT TIME '2024-01-15T12:00:00Z'
MOCK materialize.public.events(id bigint, ts timestamptz) AS (
  SELECT * FROM VALUES
    (1, '2024-01-15T11:59:00Z'::timestamptz),
    (2, '2024-01-14T12:00:00Z'::timestamptz)
)
EXPECTED(id bigint, ts timestamptz) AS (
  SELECT * FROM VALUES (1, '2024-01-15T11:59:00Z'::timestamptz)
);
```

## Related pages

- [Local development with mz-deploy](/manage/mz-deploy/develop/local-development/#write-and-run-unit-tests)
- [`CREATE VIEW`](/sql/create-view/)
- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/)
- [`VALUES`](/sql/values/)
