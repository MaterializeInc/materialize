# Non-enforced foreign keys

- Associated: (TODO: link the context-graph epic before merging)
- Prior art: `doc/developer/design/20260610_mz_deploy.md` (project-level
  modeling), `doc/developer/catalog-ontology.md` (how the system catalog
  describes its own edges)

## The Problem

An agent pointed at a Materialize environment can enumerate what exists and
almost nothing about how it fits together. Through the MCP endpoint it discovers
data products, reads each one's columns and types, and reads whatever comments a
human left behind. Then it has to answer the question every non-trivial request
turns on: which of these relations join to which, on what columns?

Materialize gives it nothing. We record no referential relationships anywhere.
`FOREIGN KEY` appears in the grammar, but only as a table constraint that
`plan_create_table` parses and throws away behind
`unsafe_enable_table_foreign_key`, kept alive so sqllogictest can feed us
PostgreSQL fixtures. `pg_catalog.pg_constraint` is a `WHERE false` stub, so
PostgreSQL-compatible introspection finds nothing either. The knowledge exists,
in dbt YAML, in diagrams, in the heads of the people who built the model, and
none of those are queryable.

So the agent guesses. It matches `customer_id` against `customers.id` because
the names rhyme, and that inference is right often enough to be dangerous. When
it is wrong the failure is not an error, it is a plausible number. A join on the
wrong column, or at the wrong grain, returns rows. Nobody sees a stack trace.

The framing that matters here is that a context graph is nodes plus edges. We
publish the nodes and we publish a good deal of metadata about them. The edges
between user relations are the missing half, and they are the half that cannot
be derived. Column names are a heuristic. Data profiling is a guess with more
steps. Only the person who designed the model knows that these two columns mean
the same thing, and today there is nowhere for them to say so.

Note that this is not the warehouse-modeling problem. We are not trying to
replace dbt's `relationships` tests or become an ERD tool. The need is narrower
and more immediate: an automated consumer, querying at runtime, needs to know
the edges to write a correct join.

## Success Criteria

- A relationship between any two relations can be declared, named, commented,
  read back from the system catalog, and survives restart and upgrade.
- The declaration works over tables, views, materialized views, and sources.
  Restricting it to tables would exclude most of what users actually model in
  Materialize.
- An agent querying data product details receives the edges its role is
  entitled to see, and only those. Every edge it receives is one it can
  actually follow.
- A PostgreSQL client that discovers join paths through `pg_constraint` finds
  them, and can tell from `convalidated` that they were never verified.
- Declaring a foreign key changes no query's plan and no query's result.
- A relationship cannot outlive either relation it describes.
- A relationship can be declared from the tooling that builds the model, dbt
  and mz-deploy, rather than only by DDL typed at a prompt. An edge nobody can
  check into a repository is an edge nobody maintains.

## Out of Scope

**Enforcement.** The feature is named for what it does not do. Enforcing a
referential constraint in a streaming system means maintaining a validating
dataflow per constraint, and then deciding what to do about a violating row
that is only violating because the other side has not arrived yet. Ordering
across independently-advancing sources is not something the user controls, so a
strict enforcement semantics would reject correct data and a lenient one would
guarantee nothing. Both are worse than being honest that we do not check.

**`ON DELETE` and `ON UPDATE` actions.** Referential actions are a consequence
of enforcement. Without it they have nothing to trigger on.

**Requiring the referenced columns to be a key.** PostgreSQL demands a unique
constraint on the referenced side because it needs one to enforce the
constraint. We are not enforcing, so the requirement would buy nothing and
would reject the common case of an edge into a relation whose key we cannot
prove.

**Use by the optimizer.** Covered under Open questions, because the temptation
is real and the reason to resist it is worth writing down.

**Enforced primary keys.** `PRIMARY KEY ... NOT ENFORCED` already exists on
sources with its own flag. Unifying the two is not part of this.

## Solution Proposal

A foreign key becomes an ordinary durable catalog item, created by its own DDL
statement, owned, commented, and dropped like anything else. The closest model
in the tree is `CREATE METRIC SINK`, which added a new `CatalogItemType` whose
sequencing writes a catalog item and does nothing else.

### Syntax

```sql
CREATE FOREIGN KEY [IF NOT EXISTS] [<name>]
    ON <referencing> (<column>[, ...])
    REFERENCES <referenced> (<column>[, ...])
    NOT ENFORCED;

DROP FOREIGN KEY [IF EXISTS] <name> [CASCADE | RESTRICT];
COMMENT ON FOREIGN KEY <name> IS '<text>';
SHOW [REDACTED] CREATE FOREIGN KEY <name>;
SHOW FOREIGN KEYS [FROM <schema>] [ON <relation>] [LIKE ... | WHERE ...];
```

A top-level `CREATE` statement rather than `ALTER TABLE ... ADD CONSTRAINT`,
because the statement has to be able to name a view, a materialized view, or a
source, and `ALTER TABLE` cannot. The shape deliberately echoes `CREATE INDEX`,
which is the existing statement that attaches a named, schema-scoped object to a
relation with an inferred name and a column list.

`NOT ENFORCED` is required. It carries no information today and the AST does not
store it. Requiring it reserves the bare form, so that if we ever do add an
enforced foreign key we can give it the unqualified spelling without changing
what any already-written statement means.

The name is optional. Omitted, it is inferred as
`<referencing>_<column>_..._fkey`, following the PostgreSQL convention, and then
uniquified by appending an integer the same way an inferred index name is. The
foreign key lives in the schema of the relation it is `ON`, again matching
indexes. `IF NOT EXISTS` requires an explicit name, because inferring a name and
then checking whether that inferred name exists is a coin flip rather than a
question the user asked.

### What planning checks

Both relations must be a table, source, view, or materialized view. The two
column lists must be non-empty, the same length, and free of duplicates within
each list. Every column must resolve unambiguously in its relation.

Paired columns must have compatible types, using the same rule SQL already uses
to decide whether two values can meet in an expression: they must share a type
category and have a common type both cast to implicitly. So `int4` pairs with
`int8`, and `numeric(10,2)` with `numeric(38,10)`, while `text` against `int4`
is rejected. Being stricter would reject pairings that join correctly today.
Being looser would accept pairings that cannot be joined at all.

Self-referencing foreign keys are allowed. A hierarchy is a legitimate edge.

### Privileges

Creating one requires `CREATE` on the schema that will hold it and ownership of
**both** relations. This is stronger than `CREATE INDEX`, which requires
ownership only of the relation it indexes, and it is deliberate. A foreign key
is an assertion about what someone else's data means. Publishing it into a
shared catalog, where agents will act on it, should require authority over both
ends rather than over one end and a schema.

The cost is real and worth naming: a role cannot declare an edge into a shared
dimension table it does not own. It has to ask the owner. We think that is the
right default for a metadata surface that drives automated query generation, and
it is the more conservative direction to relax from later. Declaring edges from
a project is where the cost gets paid, so it is picked up again under
[Declaring edges from a project](#declaring-edges-from-a-project).

### Catalog

Two relations in `mz_internal`, shaped after `mz_indexes` and
`mz_index_columns`:

```
mz_internal.mz_foreign_keys
    id, oid, schema_id, name, referencing_id, referenced_id, owner_id

mz_internal.mz_foreign_key_columns
    foreign_key_id, position, referencing_column, referenced_column
```

Both are materialized views over the raw durable catalog, deriving everything
from the persisted `create_sql`, which is the direction the rest of the catalog
has been moving. Column pairs get a row each with an ordinal position, so the
ordering is explicit and the rows join to `mz_columns`. Columns are identified
by name rather than by position: names are what `create_sql` stores, and they
survive `ALTER TABLE ... ADD COLUMN`.

`mz_internal` rather than `mz_catalog` while the feature is flag-gated.
Promoting it later is additive.

### PostgreSQL compatibility

`pg_catalog.pg_constraint` stops being a stub and starts returning a row per
foreign key, with `contype = 'f'`. This is not decoration. BI tools and ORMs
that auto-discover join paths read `pg_constraint`, not `mz_internal`, so for
that entire class of consumer it is the only surface that exists. Shipping the
edges without it would mean the tools most able to use them still cannot see
them.

The mapping is mechanical. `conrelid` and `confrelid` are the two relations'
OIDs, `conkey` and `confkey` the column positions in the order the constraint
declares them rather than the order the relations store them, and `conname`,
`connamespace`, and `oid` come from the catalog item. The referential-action
columns report no action, and the match type is simple, because there are no
actions and no other match type to report.

One column deserves care. `convalidated` is `false`, always. PostgreSQL sets it
false for a constraint added `NOT VALID`, meaning the constraint is declared but
existing rows were never checked, which is exactly our situation and exactly
what a client needs to know to decide how much to trust the edge. A client that
reads `convalidated` gets an honest answer; one that ignores it gets the join
path it came for.

`pg_get_constraintdef` moves with it, since the stub's own note says it must,
and returning a definition for a constraint that `pg_constraint` now reports is
the difference between a client rendering the constraint and a client erroring
on it.

Only `contype = 'f'` rows appear. Primary-key and unique constraints stay
absent, because Materialize has no user-declared equivalent to report.

**Known gap.** `information_schema.table_constraints`,
`referential_constraints`, and `key_column_usage` are also `WHERE false` stubs,
and in PostgreSQL all three are defined over `pg_constraint`. They stay stubs
here, so a client reading `pg_constraint` sees a foreign key that a client
reading `information_schema` does not. This is a deliberate scope line rather
than an oversight. The three views are a mechanical derivation of the same two
catalog relations and can be filled in without redesigning anything, and doing
so is the obvious next step for anyone who finds the inconsistency in the way.

### Durability

No new durable record. A foreign key persists as an ordinary catalog `Item`, and
its type is recovered from the leading tokens of `create_sql` at boot, the same
mechanism that already distinguishes a `METRIC SINK` from an `INDEX`. The
catalog version does have to move, because four persisted enums gain a variant,
but the migration itself is a no-op: nothing already stored can be using a
variant that did not exist.

Because the item is reconstructed by re-planning `create_sql` on every boot, the
feature flag must be one that is forced on during item parsing. A syntax flag
that can be off at boot takes down the whole catalog, not just the feature.

### Lifecycle

The foreign key names both relations as dependencies, so dropping either one
cascades to it through the machinery that already exists. It is treated the way
an index is treated for the purposes of a non-cascading drop: it does not block
`DROP TABLE`, it is quietly dropped along with the table. The alternative, where
declaring an edge makes an unrelated `DROP TABLE` start failing, would make the
feature something users learn to avoid.

### The MCP surface

This is the reason the feature exists, so it is worth being concrete about the
payload.

`get_data_product_details` returns one row per data product. It gains a
`foreign_keys` object holding the edges that touch that data product, split by
which way they point. Querying `orders`:

```json
{ "references": [
    { "relation": "\"materialize\".\"public\".\"customers\"",
      "columns": [{ "local": "customer_id", "remote": "id" }],
      "description": "one order belongs to one customer" } ],
  "referenced_by": [] }
```

Both directions are present, because an agent looking at `customers` needs to
know that `orders` points at it just as much as an agent looking at `orders`
needs the reverse. Which way an edge points is carried by the key rather than by
the value of some field, so there is nothing to misread: `references` holds the
edges this data product points out along, `referenced_by` the edges pointing at
it. A single array tagged with a `direction` field would be flatter and is
worse: nothing on the line says whether `direction` describes this data product
or the one named beside it.

The column pairs are labelled `local` and `remote` rather than referencing and
referenced, so the agent never has to work out which end of the edge it is
standing on. That is a small thing that removes a whole class of mistake.

The foreign key's name is deliberately absent. A name exists so a human can drop
the thing or comment on it, and neither is something an agent does here. What an
agent needs is the far relation and the column mapping, and including the name
would invite it to be treated as a stable identifier the agent could reason
about or repeat back. The comment does come through, as `description`, because
that is where a human explains what the edge means.

An edge is visible only if **both** relations are data products the role can
read. Privilege alone is not the bar. The endpoint serves materialized views and
indexed views, so an edge into anything else names a relation the agent cannot
fetch through any tool it has, and a join path you cannot follow is worse than
no join path: it invites the agent to plan a query it will then fail to run.

The check falls out of the same SQL that decides what counts as a data product
in the first place, rather than living in a second place that can drift from it.
The cost is that declaring a foreign key onto a plain table records the edge in
the catalog and shows nothing over MCP until that table is served. That is the
honest answer rather than a surprising one.

### Declaring edges from a project

The DDL is the mechanism, not the interface most users will meet. Models in
Materialize are built by dbt or by mz-deploy, out of files in a repository, and
an edge that lives only in someone's psql history drifts from the model the
first time a column is renamed. Both tools need a spelling, and the two
spellings should agree about where the edge goes.

The edge is declared on the referencing side, in the file that defines the
relation named in the `ON` clause. A foreign key asserts what a column of
*this* relation means, so keeping it here keeps one relation's semantics in one
file, beside the query that produces the column. Declaring on the referenced
side would fill `customers`'s file with claims about every relation that
happens to point at it, and would put each claim where the person who wrote the
referencing column will not look. PostgreSQL made the same call, for the same
reason: the constraint belongs to the referencing table.

#### dbt

dbt already has the shape. A model can carry a `foreign_key` constraint whose
`to` field holds a `ref()` or `source()` expression, and dbt-core does the work
around it. It parses the expression, appends it to the node's refs or sources
so the referenced model builds first, and rewrites it into a fully qualified
relation before any adapter code runs.

```yaml
models:
  - name: orders
    constraints:
      - type: foreign_key
        name: orders_customer_fkey
        columns: [customer_id]
        to: ref('customers')
        to_columns: [id]
```

The adapter renders that as a `CREATE FOREIGN KEY` once the relation exists, in
the same place `create_indexes` and `persist_docs` already run. A single-column
edge may instead be written as a column-level constraint, which dbt resolves
identically.

Every materialization that produces a relation a user models against carries
this: view, materialized view, table, incremental, and source table. The
`source` materialization does not, because a `CREATE SOURCE` in Materialize
exports its data through tables rather than being joined directly, so the
useful edge lands on the table.

Declaring one does not require `contract: {enforced: true}`. dbt couples
constraints to contracts because a constraint is normally rendered inline in
the `CREATE` statement, which has a column list to hang it on only once the
contract has produced one. Nothing here needs that. The edge is a statement of
its own, it asserts nothing about types, and requiring a user to declare every
column and its data type in order to publish one join path would price the
feature out of the projects that most need it. `CONSTRAINT_SUPPORT` reports
`NOT_ENFORCED`, which is both the honest answer and one dbt already has a word
for.

#### mz-deploy

mz-deploy needs no new spelling at all. A project file is SQL, and an object's
file already collects the statements that attach to it.

```sql
-- orders.sql
CREATE MATERIALIZED VIEW orders AS ...;

CREATE INDEX orders_id_idx ON orders (id);
CREATE FOREIGN KEY ON orders (customer_id)
    REFERENCES customers (id) NOT ENFORCED;
COMMENT ON FOREIGN KEY orders_customer_id_fkey
    IS 'one order belongs to one customer';
```

The `ON` clause must name the file's own object, which is the self-containment
rule indexes already follow. `REFERENCES` may name any object in the project or
any declared external dependency.

The referenced object is checked to exist and then deliberately left out of the
build graph. A foreign key cannot change a plan. Edges are applied in a pass
once every object exists.

For the same reason a foreign key stays out of the content hash that decides
whether an object gets rebuilt. Indexes are in that hash because an index is
part of what the object is. Grants and comments are not, and an edge belongs
with them. Editing one should cost a `CREATE FOREIGN KEY`, not a rehydration of
a materialized view.

#### Reconciliation

Both tools reconcile rather than accumulate. Before creating, each drops the
foreign keys the catalog holds for the relation it is building, then creates
the set the project declares. Without it a removed edge outlives its
declaration and goes on being served to agents, which is the failure this
design exists to prevent, now with the model's own tooling as the source.

The drop covers the edges whose referencing relation is the one being built,
which is exactly the set that relation's file owns. Edges pointing *at* it,
declared in other files, are left alone.

## Minimal Viable Prototype

The syntax is small and the semantics are metadata, so the prototype is the
flag-gated implementation itself rather than a mock. Validation comes in three
steps.

First, the SQL surface with the catalog relations, behind
`enable_foreign_key`, off in production and on in CI. At that point the feature
is fully exercisable: declare edges over a real schema, read them back, restart,
confirm they survive. This is enough to validate the syntax and the lifecycle
without committing the MCP payload shape.

Second, the MCP column, which is where the design should actually be judged.
The test is whether an agent given the edges writes correct joins it would
otherwise have guessed at. That is a question about the payload shape and the
field descriptions, not about the DDL, and it wants a real model and a real
agent rather than a unit test. Worth doing before the flag goes on by default.

Third, the project surfaces. These are what decide whether declared edges
survive contact with a model that changes every week, and they are also what
supplies the second step with a real model to judge against. An edge that has
to be re-typed by hand after every deploy will not be there when the agent
asks.

## Alternatives

**`ALTER TABLE ... ADD CONSTRAINT`, the PostgreSQL spelling.** The familiar
syntax, and the one users will try first. It cannot name a view, a materialized
view, or a source, which is most of what gets modeled in Materialize, so it
solves the problem for the minority of cases. Supporting it as an additional
spelling over tables only is possible later. Leading with it is not.

**Make the existing table constraint real.** `TableConstraint::ForeignKey` is
already parsed. Promoting it looks like the cheapest path and is the wrong
shape: it ties the constraint's lifetime to `CREATE TABLE`, so an edge could
never be added to an existing relation, dropped on its own, or attached to
anything but a table. Every one of those is a requirement here.

**A property on the referencing relation rather than an item of its own.**
Store the edges as a list hanging off the relation. This loses the independent
name, the independent `DROP`, per-edge ownership, and per-edge comments, and it
turns every add or remove into an `ALTER` that rewrites the relation's
`create_sql`. Catalog items already give us all of that for free.

**Encode the relationship in a comment or a naming convention.** Zero catalog
work, which is the appeal. Also zero validation, no name resolution, no cascade,
and no way for a consumer to distinguish a real declaration from prose that
happens to look like one. A convention that tooling must parse out of free text
is not a contract.

**Put it only in the MCP payload, computed at query time.** Skip the DDL and
infer edges from column names and key metadata inside the view. This is the
guessing we are trying to eliminate, relocated from the agent into the server,
where it would be harder to see and easier to trust.

**Enforce it.** Covered under Out of Scope.

## Open questions

**When these graduate to `mz_catalog`.** Shipping in `mz_internal` is the safe
default while the shape is provisional. What evidence would tell us the shape is
settled?

**Should the optimizer ever use them?** It is a tempting shortcut and it is
unsound. A declared constraint that nothing verifies cannot justify a
transformation whose correctness depends on it. Use an unenforced foreign key
for join elimination and a violating row does not produce an error, it produces
a wrong answer, silently, in a query that used to work. Worth noting that the
optimizer already handles foreign-key-shaped joins well in the cases where it
can prove what it needs: `semijoin_idempotence` recognizes the pattern by
deriving keys from the plan rather than by trusting a declaration. That is the
distinction to hold on to. Derived facts can drive transformations; asserted
ones cannot. If we ever want declarations to inform planning, the prerequisite
is a validation story, and that lands us back in the enforcement question this
design set aside.

