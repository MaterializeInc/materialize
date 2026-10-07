# Declared schemas for materialized views

Associated:
- [Replacement materialized views](./20251111_replacement_materialized_views.md)

## The Problem

A materialized view's schema is not stored anywhere.
The catalog stores only the `create_sql`, and every time it is planned (creation, every boot, every replacement) we re-derive the `RelationDesc` from the query:

* Column names and scalar types come from the HIR type of the query (`HirRelationExpr::top_level_typ`).
* Nullability and keys are back-ported from the *locally optimized* MIR (`infer_sql_type_for_catalog` in `src/adapter/src/coord.rs`, called from `CatalogState::parse_item` in `src/adapter/src/catalog/state.rs` and from `optimize::materialized_view::Optimizer`).
* `ASSERT NOT NULL` columns are forced to non-nullable on top of that.

The schema is therefore a function of the SQL text, the schemas of the inputs, *and the optimizer version*.
Any change to nullability or key inference in `mz_transform` changes the schema of existing materialized views on the next upgrade.
The code already contains several workarounds for this:

* On boot, the coordinator calls `StorageController::evolve_nullability_for_bootstrap` to re-register each materialized view's shard schema, because "across versions of Materialize the nullability of columns can change based on updates to our optimizer" (the comment at the call site in `src/adapter/src/coord.rs`).
* The persist columnar encoder marks every Arrow field nullable for the same reason (`RowColumnarEncoder::finish` in `src/repr/src/row/encode.rs`, which cites [database-issues#2488](https://github.com/MaterializeInc/database-issues/issues/2488)).

The drift leaks into everything that consumes the schema:

* Dependent views and materialized views are planned against the materialized view's `RelationDesc`, so drift cascades through the dependency graph.
* Sinks derive their output schemas from the same `RelationDesc` on every boot.
  A nullability change alters the schema we publish downstream.
* Upsert sink `KEY (...)` validation in `plan_create_sink` checks the requested key against the *inferred* keys of the materialized view.
  That check runs whenever the sink's `create_sql` is re-planned, so an optimizer that stops inferring a key can make an existing sink fail to plan.
* Replacement materialized views require the replacement's `RelationDesc` to equal the target's (`RelationDesc::diff` in the `CreateMaterializedView` sequencer), including nullability and keys.
  A replacement whose query is semantically compatible but whose inferred nullability or keys differ is rejected, and the user has no way to state which schema they mean.
* Tooling that wants schema contracts (e.g. the dbt adapter's model contracts) has to emulate them with `ASSERT NOT NULL` and a post-hoc column comparison.

## Success Criteria

* A user can state the full schema of a materialized view: column names, types, nullability, and keys.
* Declared names, types and nullability are independent of the optimizer.
  Re-planning the same `create_sql` with a different optimizer yields the same names, types and nullability.
* A declared `NOT NULL` is enforced: a violation surfaces as an error, independent of the optimizer.
* A declared key is trusted only while the running version's analysis confirms it.
  A key that a later version cannot confirm stops being trusted, and Materialize reports that to the user.
* Replacements are validated against the declared schema, and can be written to fit a given schema.
* No change to the durable catalog format, persist, or the compute protocol.

## Out of Scope

* Schema evolution (`ALTER MATERIALIZED VIEW ... ADD COLUMN`, or replacements with a superset schema).
  A declared schema is the prerequisite for it, but it is a separate design.
* Runtime enforcement of uniqueness.
* `DEFAULT`, `CHECK`, `FOREIGN KEY`, and `COLLATE` on materialized view columns.
* Declared schemas for non-materialized views and indexes.

## Solution Proposal

### Syntax

Extend the existing optional column list of `CREATE MATERIALIZED VIEW` to accept column definitions and table constraints, using the grammar `CREATE TABLE` already parses:

```sql
CREATE [OR REPLACE] [REPLACEMENT] MATERIALIZED VIEW [IF NOT EXISTS] name
  [ ( column_name [, ...] )
  | ( { column_name data_type [ NULL | NOT NULL | PRIMARY KEY | UNIQUE ] ...
      | [ CONSTRAINT name ] { PRIMARY KEY | UNIQUE } ( column_name [, ...] )
      } [, ...] )
  ]
  [FOR target]
  [IN CLUSTER cluster]
  [WITH (...)]
  AS query;
```

For example:

```sql
CREATE MATERIALIZED VIEW order_totals (
    customer_id int8 NOT NULL,
    region text,
    total numeric(38, 2) NOT NULL,
    PRIMARY KEY (customer_id)
) IN CLUSTER c AS
    SELECT customer_id, any_value(region), sum(amount)
    FROM orders GROUP BY customer_id;
```

The bare-name form keeps its current meaning (rename only, schema inferred).
The two forms are mutually exclusive: if any column has a type, every column must have one.
The parser distinguishes them with two tokens of lookahead after `(`: an identifier followed by `,` or `)` starts the bare-name form, anything else starts the definition form.

We keep the object type `MATERIALIZED VIEW` and do not introduce `CREATE MATERIALIZED TABLE`, see [Alternatives](#create-materialized-table).

### Semantics

A materialized view with column definitions has a *declared schema*.
Its names, scalar types and nullability are exactly the declared ones, and nothing inferred by the optimizer is merged in.
Its keys are the declared keys that the running version confirms, see [Keys](#keys).
Without column definitions, behavior is unchanged.

The parts of a declared schema differ in what makes them hold.
Names and scalar types are structural: the query is cast to them, so they hold by construction.
Nullability and keys are semantic properties of the query's output, which are undecidable in general (the query language includes negation, aggregation and `WITH MUTUALLY RECURSIVE`), so the optimizer can only approximate them by analysis.
A declared `NOT NULL` does not rely on analysis, because every row is checked at runtime, so it can be frozen like names and types.
A declared key cannot be checked against the data without a uniqueness arrangement, so it relies on analysis, and analysis differs between versions.

Opting in has a cost: a key or non-null fact the optimizer infers today but the user does not declare is gone.
Dependent queries lose the optimizations it enabled (e.g. `Distinct` or `Reduce` elision), and an upsert sink `KEY (...)` on an undeclared key fails to plan.
Users who want to keep them have to declare them.

**Arity and names.**
The number of declared columns must equal the query's arity.
Columns map by position, and declared names replace the query's names, as the bare-name form does today.

**Types.**
The query output is cast to the declared types with assignment casts (`CastContext::Assignment`), the same rule `INSERT INTO t SELECT ...` applies.
The cast is planned into the HIR expression with the existing `query::cast_relation`, so it is part of the dataflow and appears in `EXPLAIN`.
A column that cannot be assignment-cast is a planning error naming the column and both types.
A cast that fails at runtime (e.g. overflow on `int8` to `int2`) produces an error in the materialized view, like any other evaluation error.

**Nullability.**
A column without `NOT NULL` is nullable, as in `CREATE TABLE`.
This is deliberately not "whatever the optimizer infers", since that would reintroduce the dependency we are removing.
Every `NOT NULL` column (including `PRIMARY KEY` columns) is added to the existing `non_null_assertions`, which the compute sink checks per row and turns into a `MustNotBeNull` error (`src/compute/src/render/sinks.rs`).
The assertion is required for correctness, not just for error reporting.
The persist encoder panics on a null in a non-nullable column (`DatumEncoder::push`), so a declared `NOT NULL` must never reach it unchecked.
Declaring `NOT NULL` is therefore the column-definition spelling of `ASSERT NOT NULL`.
Using `WITH (ASSERT NOT NULL ...)` together with a declared schema, written out or inherited by a replacement, is rejected to avoid two ways to say the same thing in one statement.

**Keys.**
Keys in a `RelationDesc` are consumed by the optimizer of every dependent query and by sinks, and an incorrect key produces wrong results:
`ReduceElision` drops a `DISTINCT` or `GROUP BY` on a key, `RedundantJoin` and `SemijoinIdempotence` drop joins, and an upsert sink keyed on a false key silently keeps one row per key.
This is why `CREATE TABLE` keys are gated behind `unsafe_enable_table_keys`.

A key that version i confirms does not stay valid in version i', even if key inference is correct in both versions.
Inference is relative to the query's semantics and the inputs' declared keys in the version that runs it, and both change between versions:

* Keys derived through scalar functions rely on those functions being injective (`preserves_uniqueness` on the function, carried through `Map` in `MirRelationExpr::keys_with_input_keys`).
  The annotation covers many casts to text, and a change to such a function's output format can break injectivity.
  It has also been wrong: `text_to_name` and `varchar(n)` truncation (#36653), `text` to `"char"` and `bytea` (#36663), and array casts ignoring lower bounds (#37393) each had to be corrected.
* Planner fixes change what a query computes, for example the decorrelation fix for correlated CTEs (#37506).
* Input keys change: builtin relation descs are code, and #36135 changed the key of `mz_dataflow_global_ids` from `(id)` to `(id, global_id)` after the wrong key let the optimizer drop a `DISTINCT`.
  Keys of upstream views and materialized views are re-inferred per version and inherit the same issues.

Keys derived structurally, from `Reduce` or `Distinct` grouping, `TopK` with limit 1, joins on keyed equivalences and filters on literals, do not depend on any function's behavior, but they still depend on the query's semantics and the input keys.
Freezing a declared key into the `RelationDesc` would keep a key alive after the semantics or input keys that justified it changed, and after a fix to the analysis that confirmed it, which today's re-inferred keys pick up on the next upgrade.

Declared keys are therefore *expected* keys, which every plan of the materialized view checks against the keys the running version infers for the (cast) query:

* At creation, for `CREATE` and `EXPLAIN CREATE` alike, a declared `PRIMARY KEY` or `UNIQUE` key that does not contain an inferred key is an error that lists the inferred keys.
  This mirrors the existing upsert sink `KEY` validation.
* When `create_sql` is re-planned, on boot or by `EXPLAIN REPLAN`, the `RelationDesc` keeps the declared keys that the running version confirms and drops the rest.
  The materialized view keeps running, and the dropped key is recorded in a new `mz_internal.mz_materialized_view_unconfirmed_keys` relation (materialized view, key columns, and the version that could not confirm it), so the user learns that the system no longer trusts it.

A dropped key does not tell whether the key is false or the analysis got weaker.
The system cannot decide that, and the design does not try to: it only refuses to trust what the running version cannot confirm, and says so.
Dropping a key changes the materialized view's `RelationDesc` on boot, which today's inferred keys already do: `evolve_nullability_for_bootstrap` re-registers the shard schema with the re-planned desc.

A dropped key must not stop dependents from re-planning, because a re-plan error at catalog load panics (`invalid persisted SQL` in `src/adapter/src/catalog/apply.rs`).
An upsert sink checks its `KEY (...)` against the keys of its input on every plan (`plan_sink` in `src/sql/src/plan/statement/ddl.rs`), so a sink keyed on a dropped key would crash-loop the next boot.
When re-planning persisted SQL, which is planned without a `PlanContext`, an upsert key that is no longer a key of the input is therefore treated as `NOT ENFORCED`: the sink keeps running and the planner adds a notice.
New `CREATE SINK` statements keep the error.
Today's inferred keys can disappear on upgrade in the same way, so this also closes an existing crash.

A `UNIQUE` constraint on nullable columns without `NULLS NOT DISTINCT` is an error.
This includes column-level `UNIQUE`, which has no `NULLS NOT DISTINCT` spelling and therefore requires `NOT NULL`.
(`plan_create_table` stops processing constraints at the first such one instead, which drops it and every later key, so a declared key would silently disappear.)
`PRIMARY KEY` implies `NOT NULL` on its columns, and declaring a primary key column `NULL` is an error.
`RelationDesc` stores key columns sorted, so the declared column order of a key is not preserved, as for tables.

Note that assignment casts can drop keys from the inferred set when the cast function does not report `preserves_uniqueness`.
The user then sees the key check error and can adjust the query or the declared type.

**Types that cannot be spelled.**
The type grammar has no anonymous record type (`ResolvedDataType` has no such variant).
A query producing an anonymous record column can still get a declared schema by declaring a named composite type with `CREATE TYPE ... AS (...)`, since record-to-record casts are implicit.
The named type becomes a dependency of the materialized view, as with tables.

### Replacements

With a declared schema, a replacement is validated against the target's declared schema: names, types and nullability with the same `RelationDesc::diff` check as today, and keys by comparing the declared keys rather than the confirmed ones.
The difference is that both sides are now under the user's control, so a mismatch is something the user wrote, not something the optimizer decided.

A replacement without a column list for a target *with* a declared schema inherits the target's schema.
Purification copies the target's column definitions into the replacement's statement, so inheriting is shorthand for writing them out: the query is cast to them with assignment casts, `NOT NULL` becomes an assertion, keys go through the creation-time check, and the feature flag applies.
The replacement's `create_sql` then states its schema.
Re-planning it on boot does not depend on the target, custom types in the definitions are recorded as dependencies, and `MaterializedView::apply_replacement` keeps taking the column list from the replacement's statement.

Requiring the query's types to match the target's exactly, without casts, would keep accidental casts out of a drop-in replacement.
It needs to know at planning time whether the definitions were inherited, which the stored statement cannot say, and checking exact types when re-planning would make an upgrade that changes a query's inferred type fail on boot.

A replacement with a bare-name list for a target with a declared schema is rejected, because renaming conflicts with inheriting the names.

A replacement *with* column definitions for a target *without* one is allowed if the declared desc equals the target's current desc.
This is the migration path from an implicit to a declared schema for an existing materialized view.

### Implementation sketch

1. **AST and parser** (`mz-sql-parser`).
   Replace `CreateMaterializedViewStatement::columns: Vec<Ident>` with an enum, roughly `Names(Vec<Ident>)` and `Definitions { columns: Vec<ColumnDef<T>>, constraints: Vec<TableConstraint<T>> }`.
   Reuse `Parser::parse_columns` for the definition form.
   Update `AstDisplay` and the parser datadriven tests.
2. **Planner** (`src/sql/src/plan/statement/ddl.rs`).
   `plan_create_table` and `plan_source_export_desc` contain near-identical code turning `ColumnDef`s and `TableConstraint`s into a `SqlRelationType`.
   Materialized views need different semantics in that code (nullable `UNIQUE` is an error rather than dropped, keys are not gated behind `unsafe_enable_table_keys`, and only `NULL`, `NOT NULL`, `PRIMARY KEY` and `UNIQUE` are accepted), so `plan_create_materialized_view` gets its own helper.
   Sharing one helper with options is possible, but it would change `CREATE TABLE` behavior (the dropped-keys issue above), which is a separate decision.
   Apply `cast_relation` to the planned query, add `NOT NULL` columns to `non_null_assertions`, and add `declared_desc: Option<RelationDesc>` to `plan::MaterializedView`.
   The type-name references resolve through the normal name resolution, so dependencies on custom types are recorded in `resolved_ids`.
3. **Optimizer** (`src/adapter/src/optimize/materialized_view.rs`).
   Pass the declared desc to `Optimizer::new`.
   In the global stage, use it as the sink's `from_desc` and `value_desc` when present, otherwise derive it as today.
   Soft-assert that arity and scalar types agree with the local plan's type.
4. **Catalog** (`CatalogState::parse_item`).
   Apply the same rule: `desc = declared_desc` if present, otherwise `infer_sql_type_for_catalog` plus assertions.
   For a declared desc, keep only the declared keys that the inferred type confirms, and record the others for `mz_materialized_view_unconfirmed_keys`.
   This is the single point that makes names, types and nullability optimizer-independent on boot.
5. **Sequencer** (`src/adapter/src/coord/sequencer/inner/create_materialized_view.rs`).
   Run the key check against the keys of the local MIR plan's type, in the optimize stage so that `EXPLAIN CREATE` rejects what `CREATE` rejects.
   A re-plan (`EXPLAIN REPLAN`) drops unconfirmed keys instead, as on boot.
   Reject a replacement without a declared schema for a target with one.
   Schema inheritance for replacements lives in purification (`src/sql/src/pure.rs`), which can read the target from the catalog and add to the statement's resolved ids.
6. **Feature flag** `enable_materialized_view_column_definitions`, off in production and on in the test and CI configuration.
7. **mz-deploy.** Its typecheck catalog uses the declared desc when present.
   Unit tests run a materialized view as a temporary view, which cannot declare a schema, so a declared materialized view is lowered to casts to the declared types.
   Declared `NOT NULL` and keys are not checked in unit tests.
8. **Docs** for `CREATE MATERIALIZED VIEW`, and a note in the dbt adapter that contracts can be expressed directly.

Nothing changes in the durable catalog format (the schema lives in `create_sql`), persist, or compute.
`evolve_nullability_for_bootstrap` still runs for materialized views with declared schemas, and only has an effect when a version drops an unconfirmed key.

### `EXPLAIN ... WITH (schema)`

To make declared schemas easy to write, `EXPLAIN` gets a `schema` option that prints the schema of a dataflow's exports as column definitions.
It applies to materialized views and subscribes, in the text format.
Keys over nullable columns print as `UNIQUE NULLS NOT DISTINCT`, and every key prints as `UNIQUE`, so the output can be pasted into a declaration but does not say which key was declared as `PRIMARY KEY`.
The option is not behind the feature flag, because it is useful for inspecting inferred schemas too.

### Feasibility

The change is small to medium and mostly in the SQL layer.
Every runtime mechanism it needs already exists: assignment casts for `INSERT`, column-definition parsing for `CREATE TABLE`, non-null assertions for `ASSERT NOT NULL`, key validation for upsert sinks, and desc comparison for replacements.
The main risks are:

* **Parser ambiguity** between the two column-list forms.
  Two-token lookahead resolves it, and roundtrip tests cover it.
* **Cast semantics surprising users**, e.g. assignment casts to `varchar(n)` or `numeric(p, s)`.
  This is the same behavior as `INSERT`, see open questions.
* **Key checks failing for reasonable queries**, at creation or after an upgrade, because MIR key inference is incomplete (e.g. through casts or `UNION ALL` of disjoint inputs).
  In particular, `Map` carries a key over to a new column only if exactly one of its expressions preserves uniqueness, and `cast_relation` puts all casts into one `Map`, so declaring two columns with widening casts loses every key through them.
  Users can drop the key from the declaration. Runtime enforcement could be added later for those cases.

### Phase 2: freezing inferred schemas

Declared schemas fix the problem only for materialized views that opt in.
Two follow-ups would remove the implicit schema entirely:

* **Freeze at creation.**
  When a materialized view is created without column definitions, write the inferred schema into `create_sql`, the same way the selected `AS OF` is written back today.
  Inferred keys become expected keys and are re-checked on every plan like declared ones.
  Inferred `NOT NULL` becomes a runtime assertion, which also turns a nullability-inference bug from a persist encoder panic into a query error.
* **Migrate existing materialized views.**
  Rewrite their `create_sql` with the schema their shard currently has.
  `evolve_nullability_for_bootstrap` overwrites the shard schema with the newly inferred desc on every boot, so the migration has to read the shard's latest schema before that call, in the same boot.
  Persist stores the complete `RelationDesc`, including nullability and keys, as the shard's schema (`encode_schema` for `SourceData` in `src/storage-types/src/sources.rs`), so the migration can use the last schema the system committed to rather than the new optimizer's inference.
  Materialized views with columns of anonymous record type cannot be migrated and would keep an implicit schema.

Both change what `SHOW CREATE MATERIALIZED VIEW` prints for users who never asked for it, so they deserve their own decision.

## Minimal Viable Prototype

* Parser, planner, optimizer and catalog changes for names, types, `NULL`/`NOT NULL`, behind the feature flag.
* Key declarations with the check on every plan and `mz_materialized_view_unconfirmed_keys`.
* Replacement validation against declared schemas and schema inheritance.
* Tests:
  * sqllogictest for parsing, casts, error messages, `SHOW CREATE`, key check failures, and the `MustNotBeNull` runtime error.
  * testdrive for an upsert sink keyed on a declared primary key.
  * A platform check that a materialized view with a declared schema survives upgrade and restart with unchanged names, types and nullability, and with its keys confirmed.

## Alternatives

### `CREATE MATERIALIZED TABLE`

A new statement would signal "this object has a fixed schema", but:

* It would be a new object type, or an alias for one, with the cost the replacement design already rejected for replacements as first-class items: duplicated planning and sequencing, and monitoring (`mz_materialized_views`, `EXPLAIN`, replacements) that does not apply out of the box.
* "Table" suggests the object accepts writes (`INSERT`, `UPDATE`), which it does not.
* The only difference to a materialized view is whether the schema is spelled out, and an optional column list expresses that without a new noun.

### Persist the inferred desc separately in the durable catalog

Freezing the inferred desc in a new catalog field fixes drift without any syntax.
It does not let users or replacements *state* the schema, and it creates a second source of truth next to `create_sql` that the two would have to agree with.
Phase 2 achieves the same freezing through `create_sql`.

### Frozen keys

Declared keys could be checked only at creation and then trusted forever, which makes the whole `RelationDesc` stable across versions.
As [Keys](#keys) shows, a key confirmed at creation can become false without any optimizer bug, through changed function or query semantics or changed input keys, and a frozen key would also outlive fixes to the analysis that confirmed it.
The system could not tell the user, because it would never look again.

### Keys without any check

Accepting declared keys without any check (like `KEY (...) NOT ENFORCED` on sinks) is simpler.
The optimizer consumes `RelationDesc` keys, so a mistyped declaration silently produces wrong results downstream, and the check catches the common case of a key the query plainly does not have.
Keys that are informational only would need a place in `RelationDesc` that the optimizer ignores, which does not exist today.

### Nullability stays inferred

Only names and scalar types could be declarable, with nullability left to analysis like keys.
Unlike keys, a declared `NOT NULL` is enforced on every row, so freezing it does not rely on analysis, and leaving it inferred would keep the drift this design sets out to remove: nullability is part of the `RelationDesc` that persist stores as the shard's schema, and that sinks, dependent views and replacements consume.

### Runtime-enforced keys

Checking uniqueness in the dataflow means maintaining a count per key, i.e. an arrangement the size of the materialized view.
It turns declared keys into enforced constraints that could be frozen like `NOT NULL`, but adds a large and surprising cost.
It can be added later as an opt-in, also for keys the optimizer cannot infer.

### Sink-style `KEY (...)` instead of `PRIMARY KEY` / `UNIQUE`

Materialize has two precedents for declaring keys.
User-facing, `CREATE SINK ... KEY (...) [NOT ENFORCED]` validates a requested key against the inferred keys, which is the same check this design applies.
In column-definition lists, `PRIMARY KEY` and `UNIQUE` are what `CREATE TABLE` parses and what purification writes into the `create_sql` of tables created from Postgres, MySQL and SQL Server sources.
Since the declared schema is a column-definition list, we follow the second precedent and reuse its grammar and planning code.
A sink-style `KEY (...)` clause would also invite `NOT ENFORCED`, which we reject for materialized views (see [Keys without any check](#keys-without-any-check)).

### Schema as a `WITH` option

`WITH (SCHEMA (...))` or similar would avoid touching the column list, but it splits naming (column list) from typing (option) and departs from the `CREATE TABLE` grammar users and tools already know.

## Open questions

* Assignment casts, or implicit casts only?
  Implicit-only is stricter and avoids silent truncation-style surprises, but forces users to write casts in the query for common cases such as `numeric` scale.
  The prototype uses assignment casts.
* Should replacement schema inheritance reject casts?
  The prototype allows them, see [Replacements](#replacements).
* Should `NOT NULL` skip the runtime assertion when the optimizer infers non-nullability?
  The assertion is a per-row datum scan in the sink, and skipping it would make performance, though not correctness, depend on the optimizer.
  The prototype never skips it.
* Should a `NOT NULL` violation error the materialized view, or route the row to a dead-letter queue?
  An error blocks reads at the affected times. A dead-letter queue keeps the materialized view readable, but drops data silently unless someone watches the queue.
  The prototype errors.
* Should `mz_materialized_views` expose whether the schema is declared?
  The prototype does not.
* Should a dropped key say why it was dropped?
  Recording at creation how a key was derived (only structural steps, through which uniqueness-preserving functions, from which input keys) would let a later version tell an analysis gap apart from a changed function or a missing input key.
* Do we want Phase 2, and if so, should freezing at creation be the default or opt-in?
