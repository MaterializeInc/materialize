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

* `StorageController::evolve_nullability_for_bootstrap` re-registers each materialized view's shard schema on boot because "across versions of Materialize the nullability of columns can change based on updates to our optimizer".
* The persist columnar encoder marks every Arrow field nullable for the same reason (`RowColumnarEncoder::finish` in `src/repr/src/row/encode.rs`).

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
* A declared schema is independent of the optimizer.
  Re-planning the same `create_sql` with a different optimizer yields the same `RelationDesc`.
* Declared constraints are never trusted without justification.
  Results stay correct if a declared constraint does not hold for the data.
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
Its `RelationDesc` is exactly the declared one: names, scalar types, nullability, and keys come from the column definitions, and nothing inferred by the optimizer is merged in.
Without column definitions, behavior is unchanged.

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
Using `WITH (ASSERT NOT NULL ...)` together with column definitions is rejected to avoid two ways to say the same thing in one statement.

**Keys.**
Keys in a `RelationDesc` are consumed by the optimizer of every dependent query (e.g. to elide `Distinct` and `Reduce`).
An incorrect key produces wrong results, which is why `CREATE TABLE` keys are gated behind `unsafe_enable_table_keys`.
We therefore do not trust declared keys blindly.
At creation, each declared `PRIMARY KEY` or `UNIQUE` key must be a superset of some key the optimizer infers for the (cast) query, otherwise creation fails with an error listing the keys we could prove.
This mirrors the existing upsert sink `KEY` validation.

The check runs only when the statement is first executed (in the sequencer, next to the replacement schema check), not when `create_sql` is re-planned on boot.
This is sound: MIR key inference is conservative, so a key it proved once is a property of the query and its inputs' schemas, not of the optimizer version.
A later optimizer that fails to re-derive the key does not make the key false.
The inputs' keys are themselves either declared (and proven), inferred (and sound), or source keys we already trust today.

A `UNIQUE` constraint on nullable columns without `NULLS NOT DISTINCT` is an error.
(`plan_create_table` treats it as "not a key" instead, which would make a declared key silently disappear.)
`PRIMARY KEY` implies `NOT NULL` on its columns.

Note that assignment casts can drop keys from the inferred set when the cast function does not report `preserves_uniqueness`.
The user then sees the key-proof error and can adjust the query or the declared type.

**Types that cannot be spelled.**
The type grammar has no anonymous record type (`ResolvedDataType` has no such variant).
A query producing an anonymous record column can still get a declared schema by declaring a named composite type with `CREATE TYPE ... AS (...)`, since record-to-record casts are implicit.
The named type becomes a dependency of the materialized view, as with tables.

### Replacements

With a declared schema, a replacement is validated against the target's declared `RelationDesc` with the same `RelationDesc::diff` check as today.
The difference is that both sides are now under the user's control, so a mismatch in nullability or keys is something the user wrote, not something the optimizer decided.

A replacement without column definitions for a target *with* a declared schema inherits the target's schema.
To keep accidental casts out of what is meant to be a drop-in replacement, inheritance requires the query's scalar types to match the target's exactly (no casts).
Nullability and keys are handled as if declared: `NOT NULL` becomes an assertion and keys must be provable.
`MaterializedView::apply_replacement` already takes the column list from the replacement's statement, it has to carry the inherited definitions over so the target's `create_sql` remains self-describing.

A replacement *with* column definitions for a target *without* one is allowed if the declared desc equals the target's current desc.
This is the migration path from an implicit to a declared schema for an existing materialized view.

### Implementation sketch

1. **AST and parser** (`mz-sql-parser`).
   Replace `CreateMaterializedViewStatement::columns: Vec<Ident>` with an enum, roughly `Names(Vec<Ident>)` and `Definitions { columns: Vec<ColumnDef<T>>, constraints: Vec<TableConstraint<T>> }`.
   Reuse `Parser::parse_columns` for the definition form.
   Update `AstDisplay` and the parser datadriven tests.
2. **Planner** (`src/sql/src/plan/statement/ddl.rs`).
   `plan_create_table` and `plan_source_export_desc` contain near-identical code turning `ColumnDef`s and `TableConstraint`s into a `SqlRelationType`.
   Factor that into one helper that also reports which options it accepts, and call it from `plan_create_materialized_view`.
   Apply `cast_relation` to the planned query, add `NOT NULL` columns to `non_null_assertions`, and add `declared_desc: Option<RelationDesc>` to `plan::MaterializedView`.
   The type-name references resolve through the normal name resolution, so dependencies on custom types are recorded in `resolved_ids`.
3. **Optimizer** (`src/adapter/src/optimize/materialized_view.rs`).
   Pass the declared desc to `Optimizer::new`.
   In the global stage, use it as the sink's `from_desc` and `value_desc` when present, otherwise derive it as today.
   Soft-assert that arity and scalar types agree with the local plan's type.
4. **Catalog** (`CatalogState::parse_item`).
   Apply the same rule: `desc = declared_desc` if present, otherwise `infer_sql_type_for_catalog` plus assertions.
   This is the single point that makes the schema optimizer-independent on boot.
5. **Sequencer** (`src/adapter/src/coord/sequencer/inner/create_materialized_view.rs`).
   Add the key proof next to the replacement check, using the keys of the local MIR plan's type.
   Implement schema inheritance for replacements there, since it needs the target's desc.
6. **Feature flag** `enable_materialized_view_column_definitions`, off in production and on in the test and CI configuration.
7. **Docs** for `CREATE MATERIALIZED VIEW`, and a note in the dbt adapter that contracts can be expressed directly.

Nothing changes in the durable catalog format (the schema lives in `create_sql`), persist, or compute.
`evolve_nullability_for_bootstrap` becomes a no-op for materialized views with declared schemas, because their desc no longer changes between versions.

### Feasibility

The change is small to medium and mostly in the SQL layer.
Every runtime mechanism it needs already exists: assignment casts for `INSERT`, column-definition parsing for `CREATE TABLE`, non-null assertions for `ASSERT NOT NULL`, key validation for upsert sinks, and desc comparison for replacements.
The main risks are:

* **Parser ambiguity** between the two column-list forms.
  Two-token lookahead resolves it, and roundtrip tests cover it.
* **Cast semantics surprising users**, e.g. assignment casts to `varchar(n)` or `numeric(p, s)`.
  This is the same behavior as `INSERT`, see open questions.
* **Key proofs failing for reasonable queries** because MIR key inference is incomplete (e.g. through casts or `UNION ALL` of disjoint inputs).
  Users can drop the key from the declaration. Runtime enforcement could be added later for those cases.

### Phase 2: freezing inferred schemas

Declared schemas fix the problem only for materialized views that opt in.
Two follow-ups would remove the implicit schema entirely:

* **Freeze at creation.**
  When a materialized view is created without column definitions, write the inferred schema into `create_sql`, the same way the selected `AS OF` is written back today.
  Inferred keys were proven by the optimizer at creation, so they satisfy the key rule.
  Inferred `NOT NULL` becomes a runtime assertion, which also turns a nullability-inference bug from a persist encoder panic into a query error.
* **Migrate existing materialized views.**
  Rewrite their `create_sql` with the schema their shard currently has.
  Persist stores the complete `RelationDesc`, including nullability and keys, as the shard's schema (`encode_schema` for `SourceData` in `src/storage-types/src/sources.rs`), so the migration can use the last schema the system committed to rather than the new optimizer's inference.
  Materialized views with columns of anonymous record type cannot be migrated and would keep an implicit schema.

Both change what `SHOW CREATE MATERIALIZED VIEW` prints for users who never asked for it, so they deserve their own decision.

## Minimal Viable Prototype

* Parser, planner, optimizer and catalog changes for names, types, `NULL`/`NOT NULL`, behind the feature flag.
* Key declarations with the creation-time proof.
* Replacement validation against declared schemas and schema inheritance.
* Tests:
  * sqllogictest for parsing, casts, error messages, `SHOW CREATE`, key proof failures, and the `MustNotBeNull` runtime error.
  * testdrive for an upsert sink keyed on a declared primary key.
  * A platform check that a materialized view with a declared schema survives upgrade and restart with an unchanged `RelationDesc`.

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

### Unenforced keys

Accepting declared keys without proof (like `KEY (...) NOT ENFORCED` on sinks) is simpler, but the optimizer consumes `RelationDesc` keys, so a wrong declaration silently produces wrong results downstream.
Keys that are informational only would need a place in `RelationDesc` that the optimizer ignores, which does not exist today.

### Runtime-enforced keys

Checking uniqueness in the dataflow means maintaining a count per key, i.e. an arrangement the size of the materialized view.
It removes the incompleteness of the key proof but adds a large and surprising cost.
It can be added later as an opt-in for keys the optimizer cannot prove.

### Sink-style `KEY (...)` instead of `PRIMARY KEY` / `UNIQUE`

Materialize has two precedents for declaring keys.
User-facing, `CREATE SINK ... KEY (...) [NOT ENFORCED]` validates a requested key against the inferred keys, which is the same check this design applies.
In column-definition lists, `PRIMARY KEY` and `UNIQUE` are what `CREATE TABLE` parses and what purification writes into the `create_sql` of tables created from Postgres, MySQL and SQL Server sources.
Since the declared schema is a column-definition list, we follow the second precedent and reuse its grammar and planning code.
A sink-style `KEY (...)` clause would also invite `NOT ENFORCED`, which we reject for materialized views (see [Unenforced keys](#unenforced-keys)).

### Schema as a `WITH` option

`WITH (SCHEMA (...))` or similar would avoid touching the column list, but it splits naming (column list) from typing (option) and departs from the `CREATE TABLE` grammar users and tools already know.

## Open questions

* Assignment casts, or implicit casts only?
  Implicit-only is stricter and avoids silent truncation-style surprises, but forces users to write casts in the query for common cases such as `numeric` scale.
* Should replacement schema inheritance allow casts after all?
* Should `NOT NULL` skip the runtime assertion when the optimizer proves non-nullability?
  The assertion is a per-row datum scan in the sink, and skipping it would make performance, though not correctness, depend on the optimizer.
* Should `mz_materialized_views` expose whether the schema is declared?
* Do we want Phase 2, and if so, should freezing at creation be the default or opt-in?
