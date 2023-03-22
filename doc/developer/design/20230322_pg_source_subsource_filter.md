- Feature name: Filter PG subsources by schemas using `FOR SCHEMAS`
- Associated:
    - [implementation](https://github.com/MaterializeInc/materialize/pull/18220)
    - [usability epic](https://github.com/MaterializeInc/materialize/issues/17598)

# Summary
[summary]: #summary

Right now you can only ingest tables from a PG publication using an explicit
list or simply ingesting all of them.

If a publication has multiple tables of the same name, even if in different
schemas, this makes it impossible to use the option to ingest all of the
tables––we default to creating a subsource with the same name as the object and
this tries to create two objects with the same name in the same transaction.

I propose we add a `FOR SCHEMAS` option in addition to `FOR TABLES(..)` and `FOR
ALL TABLES` that lets user specify which schemas they want to ingest, e.g. `FOR
SCHEMAS (public)`.

Users would access the feature using syntax akin to the following:

```sql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR SCHEMAS (public, project)
  WITH (SIZE = '3xsmall');
```

# Motivation
[motivation]: #motivation

This makes PG sources more ergonomic.

# Explanation
[explanation]: #explanation

We currently offer two methods of filtering the tables we ingest from the
publication; this simply adds a third where we will only ingest tables whose
namespace/schema matches the values provided by the user.

# Reference explanation
[reference-explanation]: #reference-explanation

I have a sketch of this feature implemented in [a draft PR on
GitHub](https://github.com/MaterializeInc/materialize/pull/18220).

In the SQL AST, we'll add a new `ReferencedSubsources::SubsetSchemas` variant
that will express which schemas the user wants to ingest.

In `pure.rs`, we will add filtering that only ingests table definitions for
tables whose namespace matches the values provided by the user. This will be
done in lieu of the current filtering.

# Rollout
[rollout]: #rollout

Rolling out this feature requires nothing special.

We can validate that the feature performs as intended using the existing
`pg-cdc` testing framework.

## Testing and observability
[testing-and-observability]: #testing-and-observability

This feature will be tested using the `pg-cdc` testdrive tests. We can ensure
that only the tables we expect are ingested.

## Lifecycle
[lifecycle]: #lifecycle

This feature will immediately be released to stable.

# Drawbacks
[drawbacks]: #drawbacks

This has the drawback of adding complexity to the user's mental model of the source's syntax.

The annoyance that this feature solves for could also be mitigated with an
error, so is not stricly necessary. (Though the error could provide this feature
as a meaningful alternative, rather than just pointing out what the error is.)

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

This design is the best option to solve the annoyance of not being able to
ingest publications with tables that have the same name because it gives users a
simple alternative when encountering the error, and provides a control that
falls along the same lines as PG's namespaces, meaning the feature itself will
never run into the same error it's solving for.

# Unresolved questions
[unresolved-questions]: #unresolved-questions

None

# Future work
[future-work]: #future-work

This work does not do anything about the awkwardness of how we should propagate
`IF NOT EXISTS`, though that problem is somewhat analogous.
