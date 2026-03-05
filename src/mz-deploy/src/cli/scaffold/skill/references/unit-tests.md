# Unit Test Reference

## Syntax

```
EXECUTE UNIT TEST <name>
FOR <database>.<schema>.<view>
[AT TIME '<expr>']
MOCK <name>(<col> <type>, ...) AS (
  SELECT * FROM VALUES (<val>, ...), ...
),
...
EXPECTED(<col> <type>, ...) AS (
  SELECT * FROM VALUES (<val>, ...), ...
);
```

| Clause | Required | Description |
|--------|----------|-------------|
| `EXECUTE UNIT TEST name` | Yes | Name shown in test output |
| `FOR database.schema.view` | Yes | Fully qualified target view |
| `AT TIME 'expr'` | No | Value for `mz_now()` during test |
| `MOCK fqn(cols) AS (query)` | Yes* | One per dependency of the target |
| `EXPECTED(cols) AS (query)` | Yes | Expected output rows and types |

*Every dependency of the target view must have a corresponding `MOCK`.

## Mock name resolution

Mock names can be specified at three levels of qualification:

- **Unqualified** (`users`) — resolved relative to the target view's database and schema
- **Schema-qualified** (`public.users`) — resolved relative to the target view's database
- **Fully qualified** (`materialize.public.users`) — used as-is

## AT TIME

Views that use `mz_now()` for temporal filters can set the timestamp
used during test execution:

```
EXECUTE UNIT TEST test_recent_events
FOR materialize.public.recent_events
AT TIME '2024-01-15T12:00:00Z'
MOCK ...
EXPECTED ...;
```

## Type normalization

Column types in `MOCK` and `EXPECTED` are normalized before comparison,
so common aliases are interchangeable:

- `int` / `int4` / `integer`
- `bigint` / `int8`
- `text` / `varchar` / `string`
- `float` / `float8` / `double precision`
- `numeric` / `decimal`
- `json` / `jsonb`
- `bool` / `boolean`

## Execution model

1. Each `MOCK` clause becomes a **temporary view** (`CREATE TEMPORARY VIEW`)
   containing the mock data.
2. The target view definition is rewritten to reference mock views instead
   of real dependencies, then created as a temporary view.
3. The `EXPECTED` clause becomes another temporary view.
4. A **symmetric difference** test query compares actual vs. expected:
   - Rows in expected but not in actual are labeled `MISSING`.
   - Rows in actual but not in expected are labeled `UNEXPECTED`.
5. The test **passes** if the query returns zero rows.
6. `DISCARD ALL` cleans up before the next test.

## Example

Given `models/materialize/public/user_order_summary.sql`:

```sql
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
),
EXPECTED(user_id bigint, name text, total_orders bigint) AS (
  SELECT * FROM VALUES (1, 'alice', 1)
);
```

## Common errors

- **Unmocked dependency** — Every table or view the target depends on
  must have a `MOCK` clause. Add the missing mock with the correct
  column schema.
- **Mock schema mismatch** — Mock columns must exactly match the real
  object's schema. Check column names, types, and count. Run
  `mz-deploy gen-data-contracts` to refresh `types.lock` if an external
  dependency changed.
- **Expected schema mismatch** — The `EXPECTED` columns must match the
  target view's output columns. Update column names or types to match.
- **MISSING rows** — Expected rows were not produced by the view. Check
  the mock data and view logic.
- **UNEXPECTED rows** — The view produced rows not in `EXPECTED`. Either
  add them to `EXPECTED` or fix the view logic.
