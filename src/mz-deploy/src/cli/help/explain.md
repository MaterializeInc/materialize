# explain — Show the EXPLAIN plan for a materialized view or index

Compiles the project, spins up a local Materialize Docker container, stages
the target object's dependencies in a temporary schema, creates the target,
and runs `EXPLAIN`. Useful for inspecting how Materialize will plan a
materialized view or index before deploying it to production.

Because explanations run against a local Docker container, no live
Materialize connection is required. Docker must be installed and running.

## Usage

    mz-deploy explain <database>.<schema>.<object>
    mz-deploy explain <database>.<schema>.<object>#<index_name>

## Arguments

The target must be a fully qualified object name (`database.schema.object`).

Without `#index_name`, the target must be a materialized view. With
`#index_name`, the named index is explained — the index can be on any
object type that supports indexes.

## Behavior

1. Parses the target name; rejects malformed input.
2. Compiles the project and verifies the target exists with the expected
   type (materialized view, or any object with the named index).
3. Starts a Materialize Docker container, or reuses the existing one. The
   container has a fixed name (`mz-deploy-typecheck`) and is shared across
   `test` and `explain` invocations on this host. Reuse is by name, not
   by image — to pick up a different `--docker-image`, remove the
   container first: `docker rm -f mz-deploy-typecheck`.
4. Creates a temporary schema (`_mz_explain_<timestamp>`) and stages the
   target's transitive dependencies into it as stub tables, indexes, or
   views.
5. Creates the target object in the temporary schema and runs `EXPLAIN`.
6. Prints the plan and drops the temporary schema (best effort).

## Flags

- `--docker-image <IMAGE>` — Materialize Docker image to use. Defaults to
  the image configured in `project.toml`. Only takes effect when a new
  container is created (see step 3).

## Examples

Explain a materialized view:

    mz-deploy explain materialize.analytics.daily_revenue

Explain a specific index:

    mz-deploy explain materialize.analytics.daily_revenue#revenue_by_region_idx

## Error Recovery

- **Docker daemon not running** — Start Docker Desktop or the Docker daemon
  (`sudo systemctl start docker` on Linux) and retry.
- **Image pull failed** — Check connectivity to the registry, or pass a
  different `--docker-image`.
- **Object not found in project** — Verify the fully qualified name and
  that the project compiles cleanly with `mz-deploy compile`.
- **Target is not a materialized view** — Without `#index_name`, only
  materialized views can be explained. Add `#index_name` to explain a
  specific index.
- **Index not found** — The error lists the available indexes on the
  target object.
- **Compile error** — Resolve with `mz-deploy compile` first; `explain`
  re-runs the same compilation.

## Exit Codes

- **0** — Plan printed successfully.
- **1** — Parse error, compile error, target not found, type mismatch,
  Docker error, or `EXPLAIN` failure.

## Related Commands

- `mz-deploy compile` — Compile and validate SQL without deploying.
- `mz-deploy test` — Runs against the same shared Docker container.
- `mz-deploy stage` — Create a full staging deployment.
