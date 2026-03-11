# test — Run SQL unit tests against a local Materialize container

Discovers unit tests defined inline in your SQL files, validates them
against the project's type information, and executes each test in an
isolated Docker container. Tests verify view logic without affecting
any remote database.

## Usage

    mz-deploy test [FILTER]

## Writing Tests

Tests are written inline in the same `.sql` file as the view they test,
using the `EXECUTE UNIT TEST` syntax:

    EXECUTE UNIT TEST test_name
    FOR database.schema.view_name
    MOCK database.schema.dependency(col1 TYPE, col2 TYPE) AS (
      SELECT * FROM VALUES (val1, val2), (val3, val4)
    ),
    EXPECTED(col1 TYPE, col2 TYPE) AS (
      SELECT * FROM VALUES (expected1, expected2)
    );

A single SQL file can contain the `CREATE VIEW` (or `CREATE MATERIALIZED
VIEW`) followed by one or more `EXECUTE UNIT TEST` statements.

### Syntax Reference

| Clause | Required | Description |
|--------|----------|-------------|
| `EXECUTE UNIT TEST name` | Yes | Name shown in test output |
| `FOR database.schema.view` | Yes | Fully qualified target view |
| `AT TIME 'expr'` | No | Value for `mz_now()` during test |
| `MOCK fqn(cols) AS (query)` | Yes* | One per dependency of the target |
| `EXPECTED(cols) AS (query)` | Yes | Expected output rows and types |

*Every dependency of the target view must have a corresponding `MOCK`.
Mock names can be unqualified (`users`), schema-qualified
(`public.users`), or fully qualified (`materialize.public.users`) —
partial names are resolved relative to the target view.

### Example

Given a view in `models/materialize/public/user_order_summary.sql`:

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

### AT TIME

Views that use `mz_now()` for temporal filters can set the timestamp
used during test execution:

    EXECUTE UNIT TEST test_recent_events
    FOR materialize.public.recent_events
    AT TIME '2024-01-15T12:00:00Z'
    MOCK ...
    EXPECTED ...;

## Behavior

1. Compiles the project and discovers all `EXECUTE UNIT TEST` statements.
2. Starts a Materialize Docker container (or reuses a running one).
3. Loads type information from `types.lock` (external dependencies) and
   `.mz-deploy/types.cache` (internal types, regenerated if stale).
4. For each test:
   - **Validates** mock columns match the actual schema (no missing or
     extra columns, types must be compatible). Validates expected columns
     match the target view's output schema.
   - **Creates temporary views** for each mock, the expected result, and
     the target view (rewritten to reference mocks instead of real tables).
   - **Runs a test query** that computes a symmetric difference:
     rows in expected but not in actual are labeled `MISSING`;
     rows in actual but not in expected are labeled `UNEXPECTED`.
   - **Passes** if the query returns zero rows.
   - **Cleans up** with `DISCARD ALL` before the next test.
5. Reports a summary: passed, failed, and validation errors.

### Type Normalization

Column types in `MOCK` and `EXPECTED` are normalized before comparison,
so common aliases are interchangeable: `int`/`int4`/`integer`,
`bigint`/`int8`, `text`/`varchar`/`string`, `float`/`float8`/`double precision`,
`numeric`/`decimal`, `json`/`jsonb`, etc.

## Filtering Tests

Pass a filter argument to run a subset of tests. The filter matches
against the fully qualified object name (`database.schema.object`) and
optionally a test name:

    mz-deploy test 'materialize.*'                     # All tests in the materialize database
    mz-deploy test 'materialize.public.*'              # All tests in materialize.public
    mz-deploy test 'materialize.public.my_view'        # All tests for a specific view
    mz-deploy test 'materialize.public.my_view#test1'  # A single named test

A trailing `*` segment acts as a wildcard — it matches all values for
that position and any positions after it. Without a filter, all tests
are run.

If the filter matches no tests, a message is printed and the command
exits successfully.

## Flags

- `--junit-xml <FILE>` — Write test results in JUnit XML format.
- `--docker-image <IMAGE>` — Materialize Docker image to use for running
  tests. Defaults to the image configured in `project.toml`.

## Examples

    mz-deploy test                         # Run all tests
    mz-deploy test 'materialize.public.*'  # Run tests in a specific schema
    mz-deploy test --docker-image img:tag  # Use a specific Docker image
    mz-deploy test --junit-xml results.xml # Export results as JUnit XML

## CI/CD Usage

Use `--junit-xml` to export test results for CI systems like GitHub Actions,
Jenkins, and GitLab CI. The JUnit XML format is the standard for test result
annotations and trend tracking.

    mz-deploy test --junit-xml results.xml

## Error Recovery

- **Docker unavailable** — Install Docker and ensure the daemon is
  running. Tests require a local Materialize container.
- **Unmocked dependency** — Every table or view the target depends on
  must have a `MOCK` clause. Add the missing mock with the correct
  column schema.
- **Mock schema mismatch** — Mock columns must exactly match the real
  object's schema. Check column names, types, and count. Run
  `mz-deploy lock` to refresh `types.lock` if an external
  dependency changed.
- **Expected schema mismatch** — The `EXPECTED` columns must match the
  target view's output columns. Update column names or types to match.
- **MISSING rows** — Expected rows were not produced by the view. Check
  the mock data and view logic.
- **UNEXPECTED rows** — The view produced rows not in `EXPECTED`. Either
  add them to `EXPECTED` or fix the view logic.
- **Types cache stale** — Delete `.mz-deploy/types.cache` and re-run, or
  run `mz-deploy lock` to refresh `types.lock`.

## Exit Codes

- **0** — All tests passed, or no test files found (without a filter).
- **1** — One or more tests failed, validation errors found, or a filter
  was provided that matched no tests.

## Related Commands

- `mz-deploy compile` — Validate SQL without running tests.
- `mz-deploy lock` — Refresh `types.lock` for test
  validation.
