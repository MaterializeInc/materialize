# SPARQL Frontend — Implementation Prompts

Each prompt below is a self-contained unit of work for one session. They are
ordered by dependency — later prompts build on earlier ones. Each prompt
includes the goal, key files to read, and acceptance criteria.

---

## Phase 1: Parser Foundation

### ~~Prompt 1: Bootstrap `mz-sparql-parser` crate~~

> ~~Create a new crate `src/sparql-parser/` modeled after `src/sql-parser/`.~~
> ~~Set up the crate structure: `Cargo.toml`, `src/lib.rs`, `src/lexer.rs`,~~
> ~~`src/ast.rs`, `src/parser.rs`. Define the core AST types: `SparqlQuery`,~~
> ~~`GroupGraphPattern`, `TriplePattern`, `Expression`, `Variable`, `Iri`,~~
> ~~`RdfLiteral`, `PropertyPath`. The lexer should tokenize SPARQL keywords~~
> ~~(SELECT, CONSTRUCT, WHERE, OPTIONAL, FILTER, UNION, MINUS, BIND, VALUES,~~
> ~~GROUP BY, ORDER BY, LIMIT, OFFSET, ASK, DESCRIBE, GRAPH, PREFIX, BASE,~~
> ~~AS, DISTINCT, REDUCED, HAVING, SERVICE, EXISTS, NOT EXISTS, a).~~
> ~~Write unit tests that lex a simple SPARQL query.~~
>
> ~~Read first: `src/sql-parser/src/lib.rs`, `src/sql-parser/src/lexer.rs`,~~
> ~~`src/sql-parser/src/ast/mod.rs`, `src/sql-parser/Cargo.toml`.~~

### ~~Prompt 2: Parse basic SELECT queries (BGP + FILTER)~~

> ~~Implement parsing of `SELECT ?var1 ?var2 WHERE { ?s ?p ?o . ?s ?p2 ?o2 .~~
> ~~FILTER(?o > 42) }`. This covers: PREFIX declarations, SELECT projection,~~
> ~~basic graph patterns (multiple triple patterns with `.` separator), and~~
> ~~FILTER expressions (comparison, logical operators, BOUND, isIRI, isLiteral,~~
> ~~STR, LANG, DATATYPE). Write parser tests for 10+ queries of increasing~~
> ~~complexity. Ensure good error messages on syntax errors.~~
>
> ~~Read first: W3C SPARQL 1.1 grammar (Section 19), prompt 1 output.~~

### ~~Prompt 3: Parse OPTIONAL, UNION, MINUS, BIND, VALUES~~

> ~~Extend the parser to handle `OPTIONAL { ... }`, `{ ... } UNION { ... }`,~~
> ~~`MINUS { ... }`, `BIND(expr AS ?var)`, and `VALUES (?var) { (val1) (val2) }`.~~
> ~~These are the remaining group graph pattern forms. Test each form individually~~
> ~~and in combination (e.g., nested OPTIONAL inside UNION).~~
>
> ~~Read first: W3C SPARQL 1.1 Sections 6-8, 10.~~

### ~~Prompt 4: Parse CONSTRUCT, ASK, DESCRIBE~~

> ~~Add parsing for the three non-SELECT query forms. CONSTRUCT has a template~~
> ~~(list of triple patterns) and a WHERE clause. ASK has only a WHERE clause.~~
> ~~DESCRIBE has a list of resources/variables and an optional WHERE clause.~~
> ~~Also parse `CONSTRUCT WHERE { ... }` (short form where template = WHERE~~
> ~~pattern). Add tests.~~
>
> ~~Read first: W3C SPARQL 1.1 Sections 16.2-16.4.~~

### ~~Prompt 5: Parse property paths~~

> ~~Add parsing of property path expressions in the predicate position:~~
> ~~`iri`, `^path` (inverse), `path1/path2` (sequence), `path1|path2`~~
> ~~(alternative), `path*`, `path+`, `path?`, `!(iri1|iri2)` (negated~~
> ~~property set). Property paths can appear wherever a predicate is expected.~~
> ~~Pay attention to operator precedence: `/` binds tighter than `|`,~~
> ~~unary operators (`^`, `*`, `+`, `?`) bind tightest.~~
>
> ~~Read first: W3C SPARQL 1.1 Section 9.~~

### ~~Prompt 6: Parse aggregates, subqueries, GRAPH, solution modifiers~~

> ~~Complete the parser with: GROUP BY, HAVING, aggregate functions (COUNT,~~
> ~~SUM, AVG, MIN, MAX, GROUP_CONCAT, SAMPLE), subqueries (SELECT nested in~~
> ~~WHERE), GRAPH ?g { ... } / GRAPH <iri> { ... }, ORDER BY, LIMIT, OFFSET,~~
> ~~DISTINCT, REDUCED. This should cover the full SPARQL 1.1 query grammar.~~
> ~~Add a comprehensive test suite.~~

---

## Phase 2: Planner Core

### ~~Prompt 7: Bootstrap `mz-sparql` planner crate and plan BGPs~~

> ~~Create `src/sparql/` with `Cargo.toml`, `src/lib.rs`, `src/plan.rs`.~~
> ~~Implement the core planner function: `plan_sparql(query: SparqlQuery,~~
> ~~catalog: &dyn SessionCatalog) -> Result<HirRelationExpr, PlanError>`.~~
> ~~Start with BGP planning: given a triple table `rdf_quads(subject, predicate,~~
> ~~object, graph)`, compile each triple pattern to a `Get` + `Filter` (for~~
> ~~concrete terms) or `Get` + `Project` (for variables). Compile multi-pattern~~
> ~~BGPs as `Join` with equality on shared variables. Maintain a variable→column~~
> ~~mapping. Write tests that verify the generated HIR against expected plans.~~
>
> ~~Read first: `src/sql/src/plan/statement.rs`, `src/sql/src/plan/query.rs`,~~
> ~~`src/sql/src/plan/hir.rs`, `src/expr/src/relation.rs`.~~

### ~~Prompt 8: Plan FILTER, OPTIONAL, UNION, MINUS~~

> ~~Extend the planner for the remaining graph pattern forms:~~
> ~~- FILTER → `HirRelationExpr::Filter` with translated expressions~~
> ~~- OPTIONAL → `Join { kind: LeftOuter, on: filter_expr }`~~
>   ~~(critical: filter goes in ON, not post-join)~~
> ~~- UNION → outer union with NULL-padding for asymmetric variables~~
> ~~- MINUS → anti-join pattern using NOT EXISTS on shared variables~~
>
> ~~Pay special attention to OPTIONAL semantics with tests that verify~~
> ~~filter placement. Test MINUS with both shared and disjoint variables.~~
>
> ~~Read first: Chebotko et al. (2009) "Semantics Preserving SPARQL-to-SQL~~
> ~~Translation" for reference translations.~~

### ~~Prompt 9: Plan BIND, VALUES, expressions, and type coercions~~

> ~~Implement planning for BIND (→ Map), VALUES (→ Constant), and the full~~
> ~~SPARQL expression language. Map SPARQL functions to Materialize scalar~~
> ~~functions: string ops (STRLEN, SUBSTR, UCASE, LCASE, STRSTARTS, STRENDS,~~
> ~~CONTAINS, CONCAT, REPLACE, REGEX), numeric ops (+, -, *, /), comparison~~
> ~~(=, !=, <, >, <=, >=), logical (&&, ||, !), type tests (isIRI, isBlank,~~
> ~~isLiteral, isNumeric), accessors (STR, LANG, DATATYPE, IRI, BNODE),~~
> ~~COALESCE, IF, BOUND, EXISTS/NOT EXISTS. Handle RDF type coercions (extract~~
> ~~typed values from TEXT encoding, cast to appropriate Materialize types for~~
> ~~arithmetic/comparison).~~

### ~~Prompt 10: Plan SELECT projection, aggregates, solution modifiers~~

> ~~Implement: SELECT variable projection (→ Project), SELECT * (all in-scope~~
> ~~variables), SELECT expressions (→ Map + Project), DISTINCT (→ Distinct),~~
> ~~GROUP BY + aggregates (→ Reduce), HAVING (→ Filter post-Reduce), ORDER BY~~
> ~~(ordering metadata), LIMIT/OFFSET (→ TopK). Test with aggregation queries~~
> ~~like `SELECT ?type (COUNT(?s) AS ?count) WHERE { ?s rdf:type ?type }~~
> ~~GROUP BY ?type ORDER BY DESC(?count) LIMIT 10`.~~

### Prompt 11: Plan CONSTRUCT, ASK, DESCRIBE

> Implement the three non-SELECT query forms:
> - CONSTRUCT: plan WHERE clause, then Map to produce (subject, predicate,
>   object) columns per template triple pattern, then Union all template
>   triples, then Distinct (CONSTRUCT deduplicates).
> - ASK: plan WHERE clause, wrap in Reduce(count) > 0 → single boolean.
> - DESCRIBE: plan as SELECT over triples where resource appears as subject
>   or object. Use CBD (Concise Bounded Description) or a simpler strategy.
>
> Define output `RelationDesc` for each form.

### Prompt 12: Plan property paths with LetRec

> Implement property path compilation:
> - Simple paths (IRI, inverse, sequence, alternative) → joins/unions/reversal.
> - `path+` → `LetRec` with recursive union (base case + extension step).
> - `path*` → `LetRec` for `path+` unioned with identity.
> - `path?` → union with identity (non-recursive).
> - Negated property set → Filter on predicate NOT IN (...).
>
> Test with transitive closure queries (e.g., `?x rdfs:subClassOf+ ?y`).
> Verify that the generated LetRec matches `WITH MUTUALLY RECURSIVE`
> semantics.
>
> Read first: `src/sql/src/plan/hir.rs` (LetRec variant),
> `src/sql/src/plan/query.rs` (how SQL WMR is planned).

---

## Phase 3: Integration

### Prompt 13: Wire up `SPARQL $$ ... $$` syntax in the SQL parser

> Extend the SQL parser to recognize `SPARQL $body$` as a statement. Add
> `Statement::Sparql(String)` to the SQL AST. In the SQL planner's
> `plan()` dispatch, delegate to the SPARQL parser + planner. Return an
> appropriate `Plan` variant. Test end-to-end: `SPARQL $$ SELECT ?s ?p ?o
> WHERE { ?s ?p ?o } LIMIT 10 $$` should parse, plan, and produce a valid
> `HirRelationExpr`.
>
> Read first: `src/sql-parser/src/parser.rs` (statement parsing),
> `src/sql/src/plan/statement.rs` (plan dispatch),
> `src/adapter/src/coord/sequencer/` (execution).

### Prompt 14: SUBSCRIBE integration for SPARQL queries

> Make `SUBSCRIBE TO SPARQL $$ ... $$` work. Extend the SQL parser to
> accept SPARQL in SUBSCRIBE context. The SPARQL plan (HirRelationExpr) is
> wrapped in a `SubscribePlan` and routed through the existing subscribe
> machinery. Test CONSTRUCT + SUBSCRIBE: verify that triple diffs
> (mz_timestamp, mz_diff, subject, predicate, object) are emitted correctly
> when the underlying triple table changes.
>
> Read first: `src/adapter/src/coord/sequencer/inner/subscribe.rs`,
> `src/sql/src/plan/statement/dml.rs` (plan_subscribe).

### Prompt 15: CREATE [MATERIALIZED] VIEW from SPARQL

> Support `CREATE VIEW name AS SPARQL $$ ... $$` and
> `CREATE MATERIALIZED VIEW name AS SPARQL $$ ... $$`. The SPARQL query
> is planned into HirRelationExpr and stored as a view definition.
> For CONSTRUCT views, the output schema is (subject, predicate, object).
> For SELECT views, the output schema uses SPARQL variable names as column
> names.
>
> Read first: `src/sql/src/plan/statement/ddl.rs` (plan_create_view).

### Prompt 16: Implement catalog-as-RDF named graph

> Create built-in views that expose the Materialize catalog as RDF triples.
> Implement `mz_internal.mz_rdf_catalog_triples(subject, predicate, object,
> graph)` as a UNION over catalog system tables (mz_tables, mz_columns,
> mz_schemas, mz_databases, mz_views, mz_sources, mz_sinks, mz_clusters,
> mz_indexes, mz_materialized_views). Map each catalog object to the
> ontology defined in the design doc. Make the SPARQL planner automatically
> query this view for `FROM <urn:materialize:catalog>`.
>
> Read first: `src/catalog/src/builtin.rs` (builtin view definitions),
> the INFORMATION_SCHEMA views for reference patterns.

---

## Phase 4: Polish and Correctness

### Prompt 17: SPARQL expression edge cases and three-valued logic

> Audit and fix edge cases in SPARQL expression evaluation:
> - Three-valued logic: FILTER errors (e.g., comparing incompatible types)
>   should evaluate to false (not propagate errors).
> - Effective Boolean Value (EBV) rules for FILTER.
> - String equality with language tags (same value, different language → not equal).
> - Numeric type promotion (integer + decimal → decimal).
> - xsd:dateTime comparison.
> Write a comprehensive test suite based on the W3C SPARQL test suite
> (dawg-test-suite).

### Prompt 18: SPARQL result serialization formats

> Implement output formatters for standard SPARQL result formats:
> - SPARQL Query Results JSON (application/sparql-results+json)
> - SPARQL Query Results XML (application/sparql-results+xml)
> - For CONSTRUCT: N-Triples, Turtle, JSON-LD
>
> These can be triggered via a session variable or a COPY TO FORMAT option.
> For pgwire output, keep the default tabular format.

### Prompt 19: W3C SPARQL 1.1 compliance test suite

> Port relevant tests from the W3C SPARQL 1.1 test suite
> (https://www.w3.org/2009/sparql/docs/tests/) into testdrive or
> sqllogictest format. Focus on: basic graph patterns, optionals, union,
> filter, negation (MINUS, NOT EXISTS), property paths, aggregates,
> subqueries, CONSTRUCT, ASK. Track which tests pass/fail and create
> issues for failures.

### Prompt 20: RDF data ingestion (sources)

> Design and implement `CREATE SOURCE ... FORMAT RDF` for ingesting RDF
> data from files (N-Triples, Turtle, RDF/XML) or streaming sources
> (Kafka topics with RDF payloads). The source produces rows in
> `(subject, predicate, object, graph)` format. This is a convenience
> feature — users can also load RDF via INSERT or external ETL.
>
> Read first: `src/storage-types/src/sources/` for source format definitions.
