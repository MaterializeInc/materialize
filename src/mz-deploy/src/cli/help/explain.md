# mz-deploy explain

Show the EXPLAIN plan for a materialized view or index.

## Usage

    mz-deploy explain <database>.<schema>.<object>
    mz-deploy explain <database>.<schema>.<object>#<index_name>

## Description

The explain command compiles the project, stages the target object's
dependencies in a temporary schema on the live Materialize instance, creates
the target object, and runs `EXPLAIN` to show the query plan.

This is useful for inspecting how Materialize will plan a materialized view
or index before deploying it to production.

## Arguments

The target must be a fully qualified object name (`database.schema.object`).

Without `#index_name`, the target must be a materialized view. With
`#index_name`, the named index is explained — the index can be on any object
type that supports indexes.


## Examples

Explain a materialized view:

    mz-deploy explain materialize.analytics.daily_revenue

Explain a specific index:

    mz-deploy explain materialize.analytics.daily_revenue#revenue_by_region_idx

## Related Commands

- `mz-deploy compile` — Compile and validate SQL without deploying
- `mz-deploy stage` — Create a full staging deployment
