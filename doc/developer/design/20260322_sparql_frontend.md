# SPARQL Frontend for Materialize

- Associated: (TBD — GitHub epic/issue to be created)

## The Problem

Materialize is a streaming database built on differential dataflow, offering
incremental view maintenance with SQL as its query language. However, a
significant class of use cases — knowledge graphs, semantic web applications,
linked data platforms, and ontology-driven systems — express their queries in
SPARQL over RDF data models.

Today, users who want to query RDF/graph-structured data in Materialize must:

1. Manually transform their RDF triples into relational tables.
2. Manually rewrite SPARQL queries as SQL (often involving complex self-joins on
   a triple table, recursive CTEs for property paths, and UNION ALL with NULL
   padding for OPTIONAL patterns).
3. Lose the semantic clarity of SPARQL's graph-native syntax.

Materialize is uniquely suited to SPARQL workloads because:

- **CONSTRUCT + SUBSCRIBE**: SPARQL CONSTRUCT defines a derived RDF graph.
  Combined with Materialize's SUBSCRIBE, this produces an incrementally
  maintained knowledge graph — a capability no existing SPARQL system provides
  with production-grade consistency guarantees.
- **WITH MUTUALLY RECURSIVE**: SPARQL property paths (`foaf:knows+`,
  `rdfs:subClassOf*`) require transitive closure. Materialize's recursive query
  support, powered by differential dataflow, handles these efficiently and
  incrementally.
- **Incremental view maintenance**: Every SPARQL query can become a materialized
  view that updates as the underlying triple data changes, enabling real-time
  knowledge graph applications.

## Success Criteria

1. **SPARQL 1.1 Query coverage**: Support the four query forms — SELECT,
   CONSTRUCT, ASK, DESCRIBE — over data stored in Materialize.
2. **Correct SPARQL semantics**: OPTIONAL (left join with filter-in-ON),
   MINUS (variable-compatibility anti-join), UNION (outer union), and
   FILTER (three-valued logic with error propagation) must match the W3C
   SPARQL 1.1 specification.
3. **Property paths**: `path+`, `path*`, `path?`, sequence, alternative,
   inverse, and negated property sets compile to efficient recursive plans
   using `WITH MUTUALLY RECURSIVE`.
4. **SUBSCRIBE integration**: `SUBSCRIBE` works with all SPARQL query forms.
   CONSTRUCT + SUBSCRIBE emits triple diffs (insertions/retractions).
5. **Catalog as RDF**: The Materialize catalog (schemas, tables, columns, views,
   sources, sinks, clusters, etc.) is exposed as a queryable RDF named graph,
   enabling SPARQL queries over metadata.
6. **CREATE MATERIALIZED VIEW from SPARQL**: SPARQL queries can define
   materialized views, just like SQL queries.
7. **Performance**: For common BGP patterns (star, chain, and hybrid), the
   generated plans should be competitive with hand-written SQL equivalents.
8. **Interoperability**: Results from SPARQL SELECT are wire-compatible with
   the SPARQL Query Results JSON/XML formats. CONSTRUCT results are
   serializable as N-Triples, Turtle, or JSON-LD.

## Out of Scope

- **SPARQL Update (INSERT DATA, DELETE DATA, etc.)**: Write operations are out
  of scope for the initial implementation. Data ingestion continues via SQL
  INSERT, Kafka sources, etc. Future work could add SPARQL Update as syntactic
  sugar over SQL DML.
- **SPARQL SERVICE (federation)**: Federated queries to remote SPARQL endpoints
  are out of scope. This would require implementing the SPARQL Protocol as an
  HTTP client, better suited as a future extension.
- **OWL/RDFS entailment regimes**: Automatic inference (e.g., deriving
  `?x rdf:type :Animal` from `?x rdf:type :Dog` and `:Dog rdfs:subClassOf
  :Animal`) is not built-in. Users can express inference rules as recursive
  CONSTRUCT views manually. A future extension could add entailment as a
  declarative layer.
- **Full SPARQL Protocol / HTTP endpoint**: The initial implementation is
  accessible via the pgwire protocol (same as SQL). A SPARQL Protocol HTTP
  endpoint (`/sparql?query=...`) is future work.
- **Named graph access control**: Fine-grained RBAC on individual named graphs
  is deferred.

## Solution Proposal

### Overview

Add SPARQL as a second query language frontend, parallel to SQL. A SPARQL query
string is parsed into a SPARQL-specific AST, then planned into
`HirRelationExpr` (the same intermediate representation used by the SQL
planner). From HIR onward, the existing optimization and execution pipeline is
reused unchanged.

```
┌─────────────┐    ┌──────────────┐    ┌─────────────────┐
│ SPARQL Text  │───▶│ SPARQL Parser │───▶│ SPARQL AST      │
└─────────────┘    └──────────────┘    └────────┬────────┘
                                                │
                                                ▼
┌─────────────┐    ┌──────────────┐    ┌─────────────────┐
│ SQL Text     │───▶│ SQL Parser   │───▶│ SQL AST         │
└─────────────┘    └──────────────┘    └────────┬────────┘
                                                │
                                    ┌───────────┘
                                    ▼
                           ┌─────────────────┐
                           │ HirRelationExpr  │
                           └────────┬────────┘
                                    │ lower()
                                    ▼
                           ┌─────────────────┐
                           │ MirRelationExpr  │
                           └────────┬────────┘
                                    │ optimize + render
                                    ▼
                           ┌─────────────────┐
                           │ Dataflow (DD)    │
                           └─────────────────┘
```

### Component 1: SPARQL Parser (`mz-sparql-parser`)

A new crate `src/sparql-parser/` that parses SPARQL 1.1 query strings into a
SPARQL AST. This is analogous to `src/sql-parser/` for SQL.

**SPARQL AST types** (key nodes):

```rust
/// A parsed SPARQL query.
enum SparqlQuery {
    Select { ... },
    Construct { template: Vec<TriplePattern>, where_clause: GroupGraphPattern },
    Ask { where_clause: GroupGraphPattern },
    Describe { resources: Vec<VarOrIri>, where_clause: GroupGraphPattern },
}

/// Core graph pattern types (SPARQL algebra).
enum GroupGraphPattern {
    Bgp(Vec<TriplePattern>),
    Join(Box<GroupGraphPattern>, Box<GroupGraphPattern>),
    Optional(Box<GroupGraphPattern>, Box<GroupGraphPattern>, Vec<Filter>),
    Union(Box<GroupGraphPattern>, Box<GroupGraphPattern>),
    Minus(Box<GroupGraphPattern>, Box<GroupGraphPattern>),
    Filter(Vec<Expression>, Box<GroupGraphPattern>),
    Graph(VarOrIri, Box<GroupGraphPattern>),
    Bind(Expression, Variable, Box<GroupGraphPattern>),
    Values(Vec<Variable>, Vec<Vec<Option<RdfTerm>>>),
    SubSelect(Box<SparqlQuery>),
}

/// A triple pattern: (subject, predicate, object).
struct TriplePattern {
    subject: VarOrTerm,
    predicate: VarOrPath,
    object: VarOrTerm,
}

/// Property path expressions.
enum PropertyPath {
    Iri(Iri),
    Inverse(Box<PropertyPath>),
    Sequence(Box<PropertyPath>, Box<PropertyPath>),
    Alternative(Box<PropertyPath>, Box<PropertyPath>),
    ZeroOrMore(Box<PropertyPath>),
    OneOrMore(Box<PropertyPath>),
    ZeroOrOne(Box<PropertyPath>),
    NegatedPropertySet(Vec<Iri>),
}
```

**Parser implementation strategy**: Write a hand-rolled recursive-descent parser
(like the SQL parser), not a parser generator. This gives us full control over
error messages and incremental extension.

### Component 2: SPARQL Planner (`mz-sparql`)

A new crate `src/sparql/` that translates the SPARQL AST into `HirRelationExpr`.
This is analogous to `src/sql/src/plan/` for SQL.

**Key translation rules:**

| SPARQL construct | HirRelationExpr equivalent |
|---|---|
| BGP (Basic Graph Pattern) | N-way `Join` on the triple/quad table with equality predicates on shared variables |
| OPTIONAL { P2 FILTER(e) } | `Join { kind: LeftOuter, on: e, ... }` — filter goes in the ON clause |
| UNION | `Union` of two subplans, padding missing columns with NULL |
| FILTER(expr) | `Filter { predicates: [expr], ... }` |
| MINUS | `Filter` with `NOT EXISTS` subquery on shared variables only |
| BIND(expr AS ?var) | `Map { scalars: [expr], ... }` |
| VALUES | `Constant { rows: [...] }` |
| Subquery | Nested plan via `Let` / subquery |
| GROUP BY + aggregates | `Reduce { group_key, aggregates }` |
| ORDER BY | Ordering metadata (passed through to TopK or output) |
| LIMIT/OFFSET | `TopK { limit, offset }` |
| DISTINCT | `Distinct { ... }` |
| CONSTRUCT | Plan the WHERE clause, then `FlatMap` / `Map` to produce (s, p, o) columns |
| ASK | Plan WHERE clause, wrap in EXISTS (Reduce with count > 0) |
| Property path `+`/`*` | `LetRec` (WITH MUTUALLY RECURSIVE) |

**Variable binding model**: SPARQL variables map to column positions in the
relation. The planner maintains a `HashMap<Variable, usize>` mapping variable
names to column indices. Unbound variables are represented as NULL values, with
careful handling in filter expressions to match SPARQL's three-valued logic.

**OPTIONAL semantics**: The critical subtlety is that `OPTIONAL { P FILTER(e) }`
must translate the filter into the join's ON condition, not as a post-join
filter. HirRelationExpr's `Join { on, kind: LeftOuter }` supports this directly.

**MINUS semantics**: SPARQL MINUS keeps all rows from the left side that are
*not compatible* with any row from the right side, where compatibility is
defined only on *shared* variables. If no variables are shared, MINUS is a
no-op. This translates to an anti-join pattern:
```
Filter { NOT EXISTS(Join on shared_vars only) }
```

**Property paths → LetRec**: `path+` (one-or-more) compiles to:
```
LetRec {
    bindings: [("reachable", id,
        Union(
            base_step,           -- direct edges matching the path
            Join(reachable, base_step)  -- extend by one hop
        )
    )],
    body: Get(id)
}
```
`path*` adds the identity (zero-length paths). This leverages Materialize's
`WITH MUTUALLY RECURSIVE` and differential dataflow's incremental fixpoint.

### Component 3: RDF Data Model in Materialize

**Triple/Quad storage**: RDF data is stored in a standard Materialize table:

```sql
CREATE TABLE rdf_quads (
    subject   TEXT NOT NULL,
    predicate TEXT NOT NULL,
    object    TEXT NOT NULL,
    graph     TEXT NOT NULL DEFAULT 'urn:mz:default'
);

-- Key indexes for common access patterns
CREATE INDEX rdf_spo ON rdf_quads (subject, predicate, object);
CREATE INDEX rdf_pos ON rdf_quads (predicate, object, subject);
CREATE INDEX rdf_osp ON rdf_quads (object, subject, predicate);
CREATE INDEX rdf_gsp ON rdf_quads (graph, subject, predicate);
```

Users can create multiple triple tables (just like they can create multiple
SQL tables). The SPARQL planner resolves which table(s) to query based on a
session variable or explicit `FROM <graph>` / `FROM NAMED <graph>` clauses.

**Data type encoding**: RDF terms are encoded in the TEXT columns using a
convention:
- IRIs: `<http://example.org/foo>`
- Blank nodes: `_:b1`
- Plain literals: `"hello"`
- Language-tagged literals: `"hello"@en`
- Typed literals: `"42"^^<http://www.w3.org/2001/XMLSchema#integer>`

A future optimization could introduce a dictionary-encoding scheme (integer IDs
with a lookup table) for better join performance, but the TEXT-based approach is
simpler to start with and integrates with existing Materialize string functions.

### Component 4: Catalog as RDF (Named Graph `mz:catalog`)

The Materialize catalog is exposed as a virtual RDF named graph that maps
system catalog relations to RDF triples. This is implemented as a set of
built-in views that union catalog metadata into triple form.

**Ontology** (prefix `mz: <urn:materialize:catalog:>`):

```turtle
# Object hierarchy
<database/db1>   rdf:type          mz:Database .
<schema/s1>      rdf:type          mz:Schema .
<schema/s1>      mz:inDatabase     <database/db1> .
<table/t1>       rdf:type          mz:Table .
<table/t1>       mz:inSchema       <schema/s1> .
<table/t1>       mz:hasColumn      <column/t1.c1> .
<column/t1.c1>   rdf:type          mz:Column .
<column/t1.c1>   mz:columnName     "id" .
<column/t1.c1>   mz:columnType     "integer" .
<column/t1.c1>   mz:ordinalPosition "1"^^xsd:integer .
<view/v1>        rdf:type          mz:View .
<view/v1>        mz:inSchema       <schema/s1> .
<source/src1>    rdf:type          mz:Source .
<source/src1>    mz:sourceType     "kafka" .
<sink/snk1>      rdf:type          mz:Sink .
<cluster/c1>     rdf:type          mz:Cluster .
<cluster/c1>     mz:hasReplica     <replica/c1.r1> .
<index/idx1>     rdf:type          mz:Index .
<index/idx1>     mz:onRelation     <table/t1> .
<matview/mv1>    rdf:type          mz:MaterializedView .
```

**Implementation**: A built-in view `mz_catalog.rdf_triples` that unions
queries over `mz_tables`, `mz_columns`, `mz_schemas`, `mz_databases`,
`mz_views`, `mz_sources`, `mz_sinks`, `mz_clusters`, `mz_indexes`, etc.,
projecting each into `(subject, predicate, object, graph)` form. The SPARQL
planner automatically includes this view when querying the `mz:catalog` graph.

This enables queries like:
```sparql
# Find all tables with more than 10 columns
SELECT ?table (COUNT(?col) AS ?ncols)
FROM <urn:materialize:catalog>
WHERE {
    ?table rdf:type mz:Table .
    ?table mz:hasColumn ?col .
}
GROUP BY ?table
HAVING (COUNT(?col) > 10)
```

### Component 4b: Unified Quad Surface — Bridging Relational and RDF

A key design goal is avoiding data silos: users should not have to choose
between "relational" and "RDF" for their data. The system should allow any
Materialize relation to participate in SPARQL queries, and the catalog should
be queryable alongside user data in a single SPARQL query.

**The design has three layers:**

**Layer 1: Explicit RDF mappings via SQL views.** Users project their relational
tables into `(subject, predicate, object, graph)` form using standard SQL views.
This is the primary mechanism for exposing relational data as RDF:

```sql
-- Map a users table to RDF triples
CREATE VIEW users_rdf AS
  SELECT
    '<urn:myapp:user/' || id || '>' AS subject,
    '<http://xmlns.com/foaf/0.1/name>' AS predicate,
    '"' || name || '"' AS object,
    '<urn:myapp:users>' AS graph
  FROM users
  UNION ALL
  SELECT
    '<urn:myapp:user/' || id || '>' AS subject,
    '<http://xmlns.com/foaf/0.1/mbox>' AS predicate,
    '"' || email || '"' AS object,
    '<urn:myapp:users>' AS graph
  FROM users;
```

This approach is explicit, composable, and requires no new DDL. Users control
exactly which columns map to which RDF predicates. The mapping views are
ordinary Materialize views — they can be materialized, indexed, and subscribed
to just like any other view.

**Layer 2: Named graph registry.** The `rdf_quads` table that the SPARQL
planner queries is a union of all registered triple sources:

```sql
CREATE VIEW rdf_quads AS
  SELECT * FROM mz_internal.mz_rdf_catalog_triples   -- system catalog
  UNION ALL SELECT * FROM users_rdf                   -- user mapping
  UNION ALL SELECT * FROM orders_rdf                  -- another mapping
  UNION ALL SELECT * FROM raw_triples;                -- native RDF data
```

The SPARQL planner resolves `rdf_quads` from the catalog and plans against it.
Because `rdf_quads` is a view over unions, the optimizer can push filters down
into individual sources — a `FILTER(?graph = <urn:myapp:users>)` prunes away
all branches except `users_rdf`.

A future enhancement could introduce `ALTER GRAPH ADD SOURCE <view>` syntax
to manage the union declaratively, but the view-based approach works today with
no new DDL.

**Layer 3: FROM / FROM NAMED graph selection.** SPARQL's `FROM <graph>` clause
controls which named graphs are active for a query. The planner translates this
to a filter on the `graph` column of `rdf_quads`:

```sparql
-- Query only the users graph
SELECT ?user ?name
FROM <urn:myapp:users>
WHERE { ?user foaf:name ?name }

-- Query users + catalog together
SELECT ?user ?table
FROM <urn:myapp:users>
FROM <urn:materialize:catalog>
WHERE {
    ?user foaf:name ?name .
    ?table rdf:type mz:Table .
    ?table mz:name ?name .
}
```

**Why this avoids siloing:**

- Relational tables stay relational — no ingestion into a triple store required.
- Users choose what to expose as triples via familiar SQL views.
- Native RDF data (loaded via INSERT or future `FORMAT RDF` sources) coexists
  in the same `rdf_quads` union.
- SPARQL queries transparently span system catalog, user mappings, and native
  RDF in a single query.
- `SUBSCRIBE TO SPARQL` works over these views since they are standard
  Materialize expressions — changes to the underlying relational tables
  propagate incrementally through the mapping views into SPARQL results.
- The mapping views can themselves be materialized for performance, giving
  users control over the computation/freshness tradeoff.

### Component 5: SUBSCRIBE Integration

SPARQL queries integrate with SUBSCRIBE at the plan level. Since SPARQL queries
compile to `HirRelationExpr`, and the existing SUBSCRIBE machinery operates on
plans, the integration is straightforward:

```sql
-- Subscribe to a SPARQL CONSTRUCT (emits triple diffs)
SUBSCRIBE TO SPARQL $$
  CONSTRUCT { ?person foaf:name ?name }
  WHERE { ?person foaf:knows <alice> . ?person foaf:name ?name }
$$;

-- Subscribe to a SPARQL SELECT (emits row diffs)
SUBSCRIBE TO SPARQL $$
  SELECT ?s ?p ?o WHERE { ?s ?p ?o . ?s rdf:type foaf:Person }
$$;
```

For CONSTRUCT + SUBSCRIBE, each output row is `(mz_timestamp, mz_diff, subject,
predicate, object)` — exactly the format needed to maintain a downstream triple
store or knowledge graph incrementally.

### Component 6: Integration Points

**pgwire / session**: Extend the SQL parser's top-level dispatch to recognize
SPARQL queries. Options:

1. **Wrapper syntax**: `SPARQL $$ ... $$` — a SQL statement that wraps a SPARQL
   query string. The SQL parser extracts the string and delegates to the SPARQL
   parser. This is the simplest approach and doesn't require protocol changes.
2. **Session variable**: `SET query_language = 'sparql'` switches the session to
   SPARQL mode, where all subsequent queries are parsed as SPARQL.
3. **Protocol extension**: A new pgwire message type. Too invasive for initial
   implementation.

**Recommended**: Option 1 (`SPARQL $$ ... $$`) for initial implementation, with
option 2 as a follow-up. This works with existing pgwire clients and tools
without modification.

**Coordinator**: The coordinator's statement dispatch (`sequence_plan`) receives
a `Plan` enum. Add a `Plan::SparqlSelect`, `Plan::SparqlConstruct`, etc., or
reuse existing `Plan::Select`/`Plan::Subscribe` variants since the underlying
plan is already `HirRelationExpr`.

**CREATE VIEW / MATERIALIZED VIEW**: Support creating views from SPARQL:
```sql
CREATE MATERIALIZED VIEW friends_graph AS SPARQL $$
  CONSTRUCT { ?a foaf:knows ?b . ?b foaf:knows ?a }
  WHERE { ?a foaf:knows ?b }
$$;
```
The view's output schema is `(subject TEXT, predicate TEXT, object TEXT)` for
CONSTRUCT, or named columns matching SPARQL SELECT variables.

### RDF Type System Mapping

SPARQL has a rich type system. For the initial implementation, we map
conservatively:

| RDF Type | Materialize Type | Notes |
|---|---|---|
| IRI | TEXT | Stored with angle brackets: `<http://...>` |
| Blank node | TEXT | Stored as `_:label` |
| String literal | TEXT | Plain `"value"` |
| Language-tagged | TEXT | `"value"@lang` — language tag is part of the value |
| xsd:integer | TEXT (parsed to INT8 in expressions) | Runtime casting in filter/bind |
| xsd:decimal | TEXT (parsed to NUMERIC) | Runtime casting |
| xsd:double | TEXT (parsed to FLOAT8) | Runtime casting |
| xsd:boolean | TEXT (parsed to BOOL) | Runtime casting |
| xsd:dateTime | TEXT (parsed to TIMESTAMPTZ) | Runtime casting |

The SPARQL planner inserts appropriate CAST expressions when SPARQL operators
require typed comparisons (e.g., `FILTER(?age > 30)` casts the object value
from TEXT to INT8).

A future optimization could use a variant/tagged-union column type to avoid
runtime parsing, but TEXT is sufficient and simple for the initial
implementation.

## Minimal Viable Prototype

The MVP focuses on SPARQL SELECT over a single triple table, without property
paths or CONSTRUCT:

1. Parse basic SPARQL SELECT queries (BGP, FILTER, OPTIONAL, UNION, LIMIT).
2. Plan them against a user-created `rdf_quads` table.
3. Return results via pgwire using `SPARQL $$ ... $$` wrapper syntax.

This validates:
- The parser architecture.
- The SPARQL→HIR translation for core operators.
- The OPTIONAL semantics (filter-in-ON).
- End-to-end query execution through existing Materialize infrastructure.

The prototype intentionally defers: CONSTRUCT, ASK, DESCRIBE, property paths,
aggregation, subqueries, catalog-as-RDF, SUBSCRIBE integration, and result
format serialization.

## Alternatives

### Alternative 1: Target MirRelationExpr Instead of HIR

**Considered**: Compile SPARQL directly to `MirRelationExpr`, bypassing HIR.

**Rejected because**: HIR provides important abstractions that simplify the
SPARQL translation:
- `Join { kind: LeftOuter }` — directly expresses OPTIONAL. MIR only has inner
  joins; outer joins must be manually decomposed.
- `LetRec` with named bindings — cleaner for property paths.
- Type inference and name resolution are handled by the HIR→MIR lowering.

Targeting HIR also means we benefit from all existing HIR→MIR optimizations
without reimplementing them.

### Alternative 2: SPARQL-to-SQL Transpiler

**Considered**: Translate SPARQL to SQL text, then feed it through the existing
SQL parser and planner.

**Rejected because**:
- Double-parsing overhead and fragile string construction.
- Difficult to generate correct SQL for OPTIONAL (filter placement), MINUS
  (variable-compatibility), and property paths (recursive CTEs).
- Error messages would reference generated SQL, not the original SPARQL.
- Limits optimizations: the SQL planner doesn't know the query originated from
  SPARQL and can't apply SPARQL-specific optimizations.

### Alternative 3: Mapping-Based (OBDA/R2RML) Approach

**Considered**: Instead of a triple table, use R2RML-style mappings to expose
existing relational tables as virtual RDF, and rewrite SPARQL queries in terms
of those tables (like Ontop).

**Not rejected, but deferred**: This is a valuable extension but adds
significant complexity. The initial implementation assumes a triple/quad table.
R2RML-style mappings can be layered on later, allowing SPARQL queries over
existing relational data without ETL.

### Alternative 4: Embed an Existing SPARQL Engine

**Considered**: Embed a SPARQL engine (e.g., Oxigraph in Rust) and use it for
parsing/planning.

**Rejected because**:
- Existing engines have their own execution engines; we'd only use the parser.
- Tight coupling to an external project's AST and semantics.
- We need deep integration with Materialize's planning and type system.
- However, we could potentially use Oxigraph's parser as a starting point and
  adapt it, since it's Rust and Apache-2.0 licensed.

## Open Questions

1. **Triple table schema**: ~~Should we enforce a single canonical `rdf_quads`
   table, or allow users to designate any 3/4-column table as a triple store
   via a `CREATE SOURCE ... FORMAT RDF` or similar syntax?~~ **Resolved**: The
   planner resolves a single `rdf_quads` view from the catalog. Users compose
   this view as a union of mapping views, native triple tables, and the catalog
   graph (see Component 4b). No special DDL needed — just SQL views.

2. **Dictionary encoding**: When (if ever) should we introduce integer-ID
   dictionary encoding for IRIs? This is a major performance optimization for
   large RDF datasets but adds complexity. Could be a transparent optimization
   the system applies automatically.

3. **SPARQL result formats**: Should the initial implementation support SPARQL
   JSON/XML result formats, or is pgwire tabular output sufficient? Downstream
   tools (e.g., YASGUI, Jupyter SPARQL kernels) expect standard result formats.

4. **Blank node identity**: How should blank nodes in CONSTRUCT output be
   scoped? Per-query? Per-materialized-view? Blank node identity across
   SUBSCRIBE diffs needs careful design.

5. **Multi-table SPARQL**: ~~Should SPARQL queries be able to join across multiple
   triple tables (e.g., FROM <table1> FROM NAMED <table2>)? How does this
   interact with Materialize schemas?~~ **Resolved**: `rdf_quads` is a union
   view over all triple sources. `FROM <graph>` translates to a filter on the
   `graph` column; the optimizer pushes this down to prune irrelevant branches.
   Multiple graphs are queried by listing multiple `FROM` clauses (see
   Component 4b).

6. **Catalog RDF ontology**: What ontology should the catalog graph use? A
   Materialize-specific vocabulary (`mz:*`)? An existing standard like DCAT,
   VoID, or SD (SPARQL Service Description)?

7. **Parser reuse**: Should we fork/adapt Oxigraph's SPARQL parser, or write
   one from scratch in the style of `mz-sql-parser`? Oxigraph's parser is
   well-tested but may not match our error-handling and AST conventions.

8. **SPARQL DESCRIBE**: The semantics of DESCRIBE are implementation-defined.
   What triples should Materialize return? All triples where the resource is
   subject? Subject or object? Concise Bounded Description (CBD)?
