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

* A user can state the schema of a materialized view: column names, types, and nullability.
* Declared names, types and nullability are independent of the optimizer.
  Re-planning the same `create_sql` with a different optimizer yields the same names, types and nullability.
* A declared `NOT NULL` is enforced: a violation surfaces as an error, independent of the optimizer.
* Keys stay what the running version infers, as without a declared schema.
* Replacements are validated against the declared schema, and can be written to fit a given schema.
* No change to the durable catalog format, persist, or the compute protocol.

## Out of Scope

* Schema evolution (`ALTER MATERIALIZED VIEW ... ADD COLUMN`, or replacements with a superset schema).
  A declared schema is the prerequisite for it, but it is a separate design.
* Declaring keys, see [Follow-up: declared keys](#follow-up-declared-keys).
* Runtime enforcement of uniqueness.
* `DEFAULT`, `CHECK`, `FOREIGN KEY`, and `COLLATE` on materialized view columns.
* Declared schemas for non-materialized views and indexes.

## Solution Proposal

### Syntax

Extend the existing optional column list of `CREATE MATERIALIZED VIEW` to accept column definitions, using the grammar `CREATE TABLE` already parses:

```sql
CREATE [OR REPLACE] [REPLACEMENT] MATERIALIZED VIEW [IF NOT EXISTS] name
  [ ( column_name [, ...] )
  | ( column_name data_type [ NULL | NOT NULL ] [, ...] )
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
    total numeric(38, 2) NOT NULL
) IN CLUSTER c AS
    SELECT customer_id, any_value(region), sum(amount)::numeric(38, 2)
    FROM orders GROUP BY customer_id;
```

The bare-name form keeps its current meaning (rename only, schema inferred).
The two forms are mutually exclusive: if any column has a type, every column must have one.
The parser distinguishes them with two tokens of lookahead after `(`: an identifier followed by `,` or `)` starts the bare-name form, anything else starts the definition form.
`PRIMARY KEY` and `UNIQUE` parse as in `CREATE TABLE`, but planning rejects them as not yet supported, which keeps the syntax open for [declared keys](#follow-up-declared-keys).

We keep the object type `MATERIALIZED VIEW` and do not introduce `CREATE MATERIALIZED TABLE`, see [Alternatives](#create-materialized-table).

### Semantics

A materialized view with column definitions has a *declared schema*.
Its names, scalar types and nullability are exactly the declared ones, and nothing inferred by the optimizer is merged in.
Its keys are the keys the running version infers, as without a declared schema, see [Keys](#keys).
Without column definitions, behavior is unchanged.

The parts of a declared schema differ in what makes them hold.
Names and scalar types are structural: the query is cast to them, so they hold by construction.
Nullability and keys are semantic properties of the query's output, which are undecidable in general (the query language includes negation, aggregation and `WITH MUTUALLY RECURSIVE`), so the optimizer can only approximate them by analysis.
A declared `NOT NULL` does not rely on analysis, because every row is checked at runtime, so it can be frozen like names and types.
A declared key cannot be checked against the data without a uniqueness arrangement, so it relies on analysis, and analysis differs between versions.
This design therefore declares only names, types and nullability, and leaves keys to analysis.

Opting in has a cost: a non-null fact the optimizer infers today but the user does not declare is gone.
Dependent queries lose the optimizations it enabled, and users who want to keep it declare the column `NOT NULL`.

**Arity and names.**
The number of declared columns must equal the query's arity.
Columns map by position, and declared names replace the query's names, as the bare-name form does today.

**Types.**
The query output is cast to the declared types with implicit casts (`CastContext::Implicit`), the casts Materialize inserts without being asked, for example `int4` to `int8` or `numeric` to `float8`.
The cast is planned into the HIR expression with the existing `query::cast_relation`, so it is part of the dataflow and appears in `EXPLAIN`.
A column that cannot be implicitly cast is a planning error naming the column and both types, and the user writes the cast in the query.
Implicit casts widen, so the narrowing casts that fail at runtime, `int8` to `int2` or `float8` to `numeric`, and the rounding cast from `numeric` to `numeric(p, s)` all have to be written in the query.
Implicit casts between types that differ only in modifiers, such as `varchar(10)` to `varchar(5)`, do not change values, so planning also requires the cast query's types to equal the declared types, and a mismatch in modifiers needs an explicit cast.
Re-planning persisted SQL casts a remaining modifier mismatch with an assignment cast instead, because an upgrade can change a modifier the query infers and a planning error at catalog load panics.
`numeric` values are then rounded to the declared scale, and strings longer than the declared length error at runtime.
Two implicit casts can still fail or lose precision: `text` to `varchar(n)` or `char(n)` errors on values longer than `n`, and `int8` or `numeric` to `float8` rounds.
A cast that fails at runtime produces an error in the materialized view, like any other evaluation error.
Assignment casts, which `INSERT INTO t SELECT ...` applies, would accept the narrowing casts silently.
A type declared once is easy to forget when the query changes, so a narrowing cast should be visible where the query is written, see [Assignment casts](#assignment-casts).

**Nullability.**
A column without `NOT NULL` is nullable, as in `CREATE TABLE`.
This is deliberately not "whatever the optimizer infers", since that would reintroduce the dependency we are removing.
Every `NOT NULL` column is added to the existing `non_null_assertions`, which the compute sink checks per row and turns into a `MustNotBeNull` error (`src/compute/src/render/sinks.rs`).
The assertion is required for correctness, not just for error reporting.
The persist encoder panics on a null in a non-nullable column (`DatumEncoder::push`), so a declared `NOT NULL` must never reach it unchecked.
Declaring `NOT NULL` is therefore the column-definition spelling of `ASSERT NOT NULL`.
Using `WITH (ASSERT NOT NULL ...)` together with a declared schema, written out or inherited by a replacement, is rejected to avoid two ways to say the same thing in one statement.

**Keys.**
Keys are not declared.
The `RelationDesc` carries the keys the running version infers for the cast query, as for materialized views without a declared schema.
Dependent queries keep the optimizations keys enable, upsert sinks keep validating against them, and keys can change between versions as they do today.
Casts to the declared types can drop keys from the inferred set when the cast function does not report `preserves_uniqueness`.

**Types that cannot be spelled.**
The type grammar has no anonymous record type (`ResolvedDataType` has no such variant).
A query producing an anonymous record column can still get a declared schema by declaring a named composite type with `CREATE TYPE ... AS (...)`, since record-to-record casts are implicit.
The named type becomes a dependency of the materialized view, as with tables.

### Replacements

With a declared schema, a replacement is validated against the target's declared schema with the same `RelationDesc::diff` check as today, which also compares the inferred keys.
The difference is that both sides are now under the user's control, so a mismatch is something the user wrote, not something the optimizer decided.

The check compares declared nullability, not what the replacement's query proves.
A declared `NOT NULL` column whose query can produce NULLs passes it and becomes a runtime assertion, so after the replacement is applied a NULL there makes reads of the materialized view fail.
Against an implicit target the same query is rejected as a nullability mismatch, which blue/green deployments rely on to catch problems before applying a replacement.
To keep that signal, the sequencer emits a notice naming each declared `NOT NULL` column that the replacement's locally optimized query does not prove non-nullable.
It is a notice rather than an error because declaring `NOT NULL` over a nullable query is a legitimate assertion.

A replacement without a column list for a target *with* a declared schema inherits the target's schema.
Purification copies the target's column definitions into the replacement's statement, so inheriting is shorthand for writing them out: the query is cast to them with implicit casts, `NOT NULL` becomes an assertion, and the feature flag applies.
Because casts are implicit, a replacement whose query produces `int8` for an inherited `int2` column fails to plan instead of narrowing silently.
Changing a declared type is a schema change, which replacements do not support (see [Out of Scope](#out-of-scope)).
The replacement's `create_sql` then states its schema.
Re-planning it on boot does not depend on the target, custom types in the definitions are recorded as dependencies, and `MaterializedView::apply_replacement` keeps taking the column list from the replacement's statement.

Requiring the query's types to match the target's before casting would also keep widening casts out of a drop-in replacement.
It needs to know at planning time whether the definitions were inherited, which the stored statement cannot say.
The exact-type check in [Types](#types) runs after the implicit cast, so it catches modifier mismatches but lets widening casts through.

A replacement with a bare-name list for a target with a declared schema is rejected, because renaming conflicts with inheriting the names.

A replacement *with* column definitions for a target *without* one is allowed if the declared desc equals the target's current desc.
This is the migration path from an implicit to a declared schema for an existing materialized view.
`EXPLAIN ... WITH (schema)` on the target prints the definitions to paste.
Purification could fill them in instead, but a replacement without a column list for an implicit target then opts into a declared schema without the user asking, which is why [freezing at creation](#freezing-inferred-schemas-at-creation) skips replacements.

### Implementation sketch

1. **AST and parser** (`mz-sql-parser`).
   Replace `CreateMaterializedViewStatement::columns: Vec<Ident>` with an enum, roughly `Names(Vec<Ident>)` and `Definitions { columns: Vec<ColumnDef<T>>, constraints: Vec<TableConstraint<T>> }`.
   Reuse `Parser::parse_columns` for the definition form.
   Update `AstDisplay` and the parser datadriven tests.
2. **Planner** (`src/sql/src/plan/statement/ddl.rs`).
   `plan_create_table` and `plan_source_export_desc` contain near-identical code turning `ColumnDef`s and `TableConstraint`s into a `SqlRelationType`.
   Materialized views accept only `NULL` and `NOT NULL` and reject `PRIMARY KEY` and `UNIQUE`, so `plan_create_materialized_view` gets its own helper.
   Apply `cast_relation` to the planned query, add `NOT NULL` columns to `non_null_assertions`, and add `declared_desc: Option<RelationDesc>` to `plan::MaterializedView`.
   The type-name references resolve through the normal name resolution, so dependencies on custom types are recorded in `resolved_ids`.
3. **Optimizer** (`src/adapter/src/optimize/materialized_view.rs`).
   Pass the declared desc to `Optimizer::new`.
   In the global stage, use it with the inferred keys as the sink's `from_desc` and `value_desc` when present, otherwise derive it as today.
   Soft-assert that arity and scalar types agree with the local plan's type.
4. **Catalog** (`CatalogState::parse_item`).
   Apply the same rule: the declared desc with the keys of `infer_sql_type_for_catalog` if present, otherwise `infer_sql_type_for_catalog` plus assertions.
   This is the single point that makes names, types and nullability optimizer-independent on boot.
5. **Sequencer** (`src/adapter/src/coord/sequencer/inner/create_materialized_view.rs`).
   Validate a replacement's declared schema against its target's.
   Schema inheritance for replacements lives in purification (`src/sql/src/pure.rs`), which can read the target from the catalog and add to the statement's resolved ids.
   The sequencer still rejects a replacement without a declared schema for a target with one, which only a statement that skipped purification can reach.
6. **Feature flag** `enable_materialized_view_column_definitions`, off in production and on in the test and CI configuration.
7. **mz-deploy.** Its typecheck catalog uses the declared desc when present.
   Unit tests run a materialized view as a temporary view, which cannot declare a schema, so a declared materialized view is lowered to casts to the declared types.
   Declared `NOT NULL` is not checked in unit tests.
8. **Docs** for `CREATE MATERIALIZED VIEW`, and a note in the dbt adapter that contracts can be expressed directly.

Nothing changes in the durable catalog format (the schema lives in `create_sql`), persist, or compute.
`evolve_nullability_for_bootstrap` still runs for materialized views with declared schemas, and only has an effect when the inferred keys change.

### `EXPLAIN ... WITH (schema)`

To make declared schemas easy to write, `EXPLAIN` gets a `schema` option that prints the schema of a dataflow's exports as column definitions.
It applies to materialized views and subscribes, in the text format.
Keys are left out, because column definitions cannot declare them, so the output can be pasted into a declaration.
The option is not behind the feature flag, because it is useful for inspecting inferred schemas too.

### Feasibility

The change is small to medium and mostly in the SQL layer.
Every runtime mechanism it needs already exists: relation casts for `INSERT`, column-definition parsing for `CREATE TABLE`, non-null assertions for `ASSERT NOT NULL`, and desc comparison for replacements.
The main risks are:

* **Parser ambiguity** between the two column-list forms.
  Two-token lookahead resolves it, and roundtrip tests cover it.
* **Implicit casts being too strict**, e.g. `sum(amount)` declared as `numeric(38, 2)` needs a cast in the query.
  The planning error names the column and both types.
* **Casts losing inferred keys.**
  `Map` carries a key over to a new column only if exactly one of its expressions preserves uniqueness, and `cast_relation` puts all casts into one `Map`, so declaring two columns with widening casts loses every key through them.
  Dependents then lose the optimizations those keys enabled.

### Freezing inferred schemas at creation

Declared schemas fix the problem only for materialized views that opt in.
Behind `enable_materialized_view_schema_freezing`, a new materialized view without column definitions gets its inferred schema written into `create_sql` as column definitions, the same way the selected `AS OF` is written back.
The sequencer freezes the schema between local and global optimization, from the same inference catalog loading uses, so the dataflow the statement creates already asserts the frozen `NOT NULL` columns.
Inferred `NOT NULL` becomes a runtime assertion, which turns a nullability-inference bug from a persist encoder panic into a `MustNotBeNull` error, and keys stay inferred.
`ASSERT NOT NULL` options fold into the definitions, and a bare-name list is replaced by them.

Each column type is spelled with item ids and kept only if it plans back to the exact type.
A type that cannot be spelled leaves the schema implicit: anonymous records, and arrays and ranges whose element type has modifiers.
Replacements are not frozen, because a declared target passes its definitions on through purification and an implicit target should stay implicit.
A frozen target counts as declared, so its replacements fall under the nullability notice in [Replacements](#replacements) instead of the creation-time mismatch error, and inherited names override the names in the replacement's query.
Freezing changes what `SHOW CREATE MATERIALIZED VIEW` prints for users who never wrote a schema, which is why it starts behind a flag.

### Phase 2: migrating existing materialized views

Freezing at creation leaves materialized views created before it with an implicit schema.
A migration would rewrite their `create_sql` with the schema their shard currently has.
`evolve_nullability_for_bootstrap` overwrites the shard schema with the newly inferred desc on every boot, so the migration has to read the shard's latest schema before that call, in the same boot.
Persist stores the complete `RelationDesc`, including nullability and keys, as the shard's schema (`encode_schema` for `SourceData` in `src/storage-types/src/sources.rs`), so the migration can use the last schema the system committed to rather than the new optimizer's inference.
Materialized views with types that cannot be spelled keep an implicit schema.

## Minimal Viable Prototype

* Parser, planner, optimizer and catalog changes for names, types, `NULL`/`NOT NULL`, behind the feature flag.
* Replacement validation against declared schemas and schema inheritance.
* Tests:
  * sqllogictest for parsing, casts, error messages, `SHOW CREATE`, inferred keys, and the `MustNotBeNull` runtime error.
  * A platform check that a materialized view with a declared schema survives upgrade and restart with unchanged names, types and nullability.

## Alternatives

### `CREATE MATERIALIZED TABLE`

A new statement would signal "this object has a fixed schema", but:

* It would be a new object type, or an alias for one, with the cost the replacement design already rejected for replacements as first-class items: duplicated planning and sequencing, and monitoring (`mz_materialized_views`, `EXPLAIN`, replacements) that does not apply out of the box.
* "Table" suggests the object accepts writes (`INSERT`, `UPDATE`), which it does not.
* The only difference to a materialized view is whether the schema is spelled out, and an optional column list expresses that without a new noun.

### Persist the inferred desc separately in the durable catalog

Freezing the inferred desc in a new catalog field fixes drift without any syntax.
It does not let users or replacements *state* the schema, and it creates a second source of truth next to `create_sql` that the two would have to agree with.
[Freezing at creation](#freezing-inferred-schemas-at-creation) achieves the same through `create_sql`.

### Nullability stays inferred

Only names and scalar types could be declarable, with nullability left to analysis like keys.
Unlike keys, a declared `NOT NULL` is enforced on every row, so freezing it does not rely on analysis, and leaving it inferred would keep the drift this design sets out to remove: nullability is part of the `RelationDesc` that persist stores as the shard's schema, and that sinks, dependent views and replacements consume.

### Assignment casts

Casting with assignment casts, as `INSERT INTO t SELECT ...` does, accepts every cast that implicit casts accept plus the narrowing ones: `int8` to `int2`, `float8` to `numeric`, `numeric` to `numeric(p, s)`.
It saves writing casts in the query, most often for `numeric` scale.
A narrowing cast that fails at runtime errors the materialized view, and with an inherited schema the cast is not written anywhere in the replacement, so the user does not see it unless they read `EXPLAIN`.
Moving from implicit to assignment casts later accepts strictly more statements, so it stays possible without breaking existing ones.

### Schema as a `WITH` option

`WITH (SCHEMA (...))` or similar would avoid touching the column list, but it splits naming (column list) from typing (option) and departs from the `CREATE TABLE` grammar users and tools already know.

### Skipping the `NOT NULL` assertion on inferred columns

The sink could skip the assertion for columns the optimizer infers to be non-nullable, as performance already depends on the optimizer elsewhere.
Correctness would then depend on nullability inference being right: a null that reaches a non-nullable column panics the persist encoder (`DatumEncoder::push`) instead of producing a `MustNotBeNull` error.
Implicit materialized views carry that risk today, and declared schemas are meant to remove it.
The assertion inspects the asserted datums of each row the sink writes, which the sink encodes anyway, and we have not measured its cost.

## Follow-up: declared keys

Keys of a dataflow are semantic properties, and declaring one in a schema makes it look syntactic.
No analysis decides them, so the system can only check a declared key against what the running version infers, and the answer can change between versions.
The first version therefore leaves keys inferred and rejects `PRIMARY KEY` and `UNIQUE`.
This section records the design for declaring keys later, which a prototype explored.

### Why keys cannot be frozen

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

### Expected keys

Declared keys would therefore be *expected* keys, which every plan of the materialized view checks against the keys the running version infers for the (cast) query:

* At creation, for `CREATE` and `EXPLAIN CREATE` alike, a declared `PRIMARY KEY` or `UNIQUE` key that does not contain an inferred key is an error that lists the inferred keys.
  This mirrors the existing upsert sink `KEY` validation.
* When `create_sql` is re-planned, on boot or by `EXPLAIN REPLAN`, the `RelationDesc` keeps the declared keys that the running version confirms and drops the rest.
  The materialized view keeps running, and the dropped key is recorded in a new `mz_internal.mz_materialized_view_unconfirmed_keys` relation (materialized view, key columns, and the version that could not confirm it), so the user learns that the system no longer trusts it.

A dropped key does not tell whether the key is false or the analysis got weaker.
The system cannot decide that, and the design does not try to: it only refuses to trust what the running version cannot confirm, and says so.
Dropping a key changes the materialized view's `RelationDesc` on boot, which today's inferred keys already do: `evolve_nullability_for_bootstrap` re-registers the shard schema with the re-planned desc.

A dropped key must not stop dependents from re-planning, because a re-plan error at catalog load panics (`invalid persisted SQL` in `src/adapter/src/catalog/apply.rs`).
An upsert sink checks its `KEY (...)` against the keys of its input on every plan (`plan_sink` in `src/sql/src/plan/statement/ddl.rs`), so a sink keyed on a dropped key would crash-loop the next boot.
When re-planning persisted SQL, which is planned without a `PlanContext`, `plan_sink` already treats an upsert key that is no longer a key of the input as `NOT ENFORCED`: the sink keeps running and the planner adds a notice.
New `CREATE SINK` statements keep the error.

A `UNIQUE` constraint on nullable columns without `NULLS NOT DISTINCT` is an error.
This includes column-level `UNIQUE`, which has no `NULLS NOT DISTINCT` spelling and therefore requires `NOT NULL`.
(`plan_create_table` stops processing constraints at the first such one instead, which drops it and every later key, so a declared key would silently disappear.)
`PRIMARY KEY` implies `NOT NULL` on its columns, and declaring a primary key column `NULL` is an error.
`RelationDesc` stores key columns sorted, so the declared column order of a key is not preserved, as for tables.

### Syntax

`PRIMARY KEY` and `UNIQUE` reuse the `CREATE TABLE` grammar and planning code, but they carry SQL's expectation that the database enforces them, and `PRIMARY KEY` implies `NOT NULL`.
An expected key is neither enforced nor trusted, so that grammar suggests a contract the system does not keep, next to `NOT NULL`, which is one.
Materialize has two precedents for declaring keys.
User-facing, `CREATE SINK ... KEY (...) [NOT ENFORCED]` validates a requested key against the inferred keys, which is the check expected keys would apply.
In column-definition lists, `PRIMARY KEY` and `UNIQUE` are what `CREATE TABLE` parses and what purification writes into the `create_sql` of tables created from Postgres, MySQL and SQL Server sources.
A sink-style `KEY (...)` clause would also invite `NOT ENFORCED`, which there means trusted without a check, the opposite of an expected key (see [Keys without any check](#keys-without-any-check)).

A separate option, for example `WITH (EXPECTED KEY (a, b))`, keeps the column definitions to what is a contract and states keys as checked expectations.
Which syntax to use is open.

### Alternatives

#### Frozen keys

Declared keys could be checked only at creation and then trusted forever, which makes the whole `RelationDesc` stable across versions.
As [Why keys cannot be frozen](#why-keys-cannot-be-frozen) shows, a key confirmed at creation can become false without any optimizer bug, through changed function or query semantics or changed input keys, and a frozen key would also outlive fixes to the analysis that confirmed it.
The system could not tell the user, because it would never look again.

#### Keys without any check

Accepting declared keys without any check (like `KEY (...) NOT ENFORCED` on sinks) is simpler.
The optimizer consumes `RelationDesc` keys, so a mistyped declaration silently produces wrong results downstream, and the check catches the common case of a key the query plainly does not have.
Keys that are informational only would need a place in `RelationDesc` that the optimizer ignores, which does not exist today.

#### Runtime-enforced keys

Checking uniqueness in the dataflow means maintaining a count per key, i.e. an arrangement the size of the materialized view.
It turns declared keys into enforced constraints that could be frozen like `NOT NULL`, but adds a large and surprising cost.
An index on exactly the key columns already holds that arrangement, so the check could ride on it instead of building a second one.
The index runs in its own dataflow, possibly on another cluster, and dependents that read the materialized view from persist rely on the key whether or not the index exists, so a violation it finds could be reported but not prevented.
It can be added later as an opt-in, also for keys the optimizer cannot infer.

## Interaction with pinned LIR

[Pinned LIR](./20260826_pinned_lir.md) stores a materialized view's plan instead of re-optimizing it on boot, and is planned to pin the plan's keys with it.
The two designs are orthogonal.
A declared schema is a user-facing contract on a materialized view's output, while pinning is internal to how the system runs the view.
A pinned plan was produced against the declared schema, so its output types are expected to match it.

## Open questions

* Should replacement schema inheritance reject widening casts?
  The prototype allows them, see [Replacements](#replacements).
* Should a `NOT NULL` violation error the materialized view, or route the row to a dead-letter queue?
  An error blocks reads at the affected times. A dead-letter queue keeps the materialized view readable, but drops data silently unless someone watches the queue.
  A dead-letter queue would also be a new mechanism with the data and error consistency problems described in [error handling](./20240609_error_handling.md).
  The prototype errors.
* How should the catalog expose whether a schema is declared?
  It is useful to us for adoption and to users for tracking a migration to declared schemas.
  An `mz_internal` relation fits while the feature is behind a flag, and a column of `mz_materialized_views` once it is stable.
  The prototype does not expose it.
* Do we want Phase 2, migrating existing materialized views to frozen schemas?
* Which syntax should declared keys use, column-definition constraints or a separate option?
  See [Follow-up: declared keys](#follow-up-declared-keys).
