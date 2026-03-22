# SPARQL Frontend — Worklog

## 2026-03-22: Initial Research

### Codebase Architecture Findings

**SQL frontend pipeline** (what the SPARQL frontend must parallel):

1. **Parser** (`src/sql-parser/`): Hand-rolled recursive-descent parser.
   `Statement<T>` is the top-level AST node. Parser is generic over name
   resolution (`<T: AstInfo>`), using `Raw` pre-resolution and `Aug`
   post-resolution. Key file: `src/sql-parser/src/parser.rs`.

2. **Name resolution** (`src/sql/src/names.rs`): Resolves raw names to catalog
   IDs. Produces `Statement<Aug>` with `ResolvedIds`.

3. **Planner** (`src/sql/src/plan/`): `plan()` in `statement.rs` dispatches on
   statement type. Query planning happens in `query.rs`/`expr.rs`/`scope.rs`.
   Output: `Plan` enum (e.g., `Plan::Select`, `Plan::Subscribe`,
   `Plan::CreateView`).

4. **HIR** (`src/sql/src/plan/hir.rs`): `HirRelationExpr` — has named joins
   with `JoinKind` (Inner, LeftOuter, RightOuter, FullOuter), `LetRec` for
   recursion, `CallTable` for table functions. This is the right target for
   SPARQL — it preserves outer join semantics natively.

5. **Lowering** (`src/sql/src/plan/lowering.rs`): `HirRelationExpr::lower()` →
   `MirRelationExpr`. Decomposes outer joins into inner joins + unions,
   handles decorrelation.

6. **MIR** (`src/expr/src/relation.rs`): `MirRelationExpr` — only inner joins
   (multi-way with equivalence classes), no outer join kind. Used by the
   optimizer and dataflow rendering.

7. **Coordinator** (`src/adapter/src/coord/`): `sequence_plan()` dispatches
   `Plan` variants to execution. Subscribe goes through
   `coord/sequencer/inner/subscribe.rs`.

**Key insight**: Targeting `HirRelationExpr` is clearly correct. It has
`Join { kind: LeftOuter, on: expr }` which maps directly to SPARQL OPTIONAL
with filter-in-ON. MIR would require us to manually decompose outer joins.

**SUBSCRIBE integration**: The `SubscribePlan` contains a `SubscribeFrom` which
can be either an `Id` (subscribe to existing view) or `Query { expr, desc }`
(subscribe to inline query). The SPARQL plan can be wrapped in
`SubscribeFrom::Query` with the appropriate `RelationDesc`.

### SPARQL Algebra → Relational Mapping

**Core mappings** (validated against Chebotko et al. 2009, Cyganiak 2005):

| SPARQL | HIR | Notes |
|---|---|---|
| BGP | Join(Get+Filter, Get+Filter, ...) | Self-joins on quad table, filter on bound positions |
| OPTIONAL | Join { kind: LeftOuter, on: filter } | Filter MUST be in ON clause |
| UNION | Union with NULL padding | SPARQL union is outer-union |
| FILTER | Filter | Three-valued: error → false |
| MINUS | Filter + NOT EXISTS on shared vars | No-op if no shared vars |
| BIND | Map | |
| VALUES | Constant | |
| Subquery | Let + nested plan | |
| CONSTRUCT | Plan WHERE, then Map to (s,p,o) columns | Multiple template triples → FlatMap/Union |
| ASK | Reduce(count) > 0 | |
| Property path + | LetRec (WITH MUTUALLY RECURSIVE) | Mz's killer feature for SPARQL |
| Property path * | LetRec + identity union | |

**Tricky semantics to get right**:

1. **OPTIONAL with FILTER**: `{ ?x :p ?y OPTIONAL { ?y :q ?z FILTER(?z > 5) } }`
   must translate as `LeftJoin(bgp1, bgp2, ?z > 5)`, i.e., the filter is part
   of the join condition. If translated as a post-join filter, it incorrectly
   removes rows where `?y :q ?z` had no match.

2. **MINUS**: `{ ?x :p ?y } MINUS { ?x :q ?z }` keeps all (?x, ?y) pairs
   where no (?x, ?z) exists with the same ?x. But `{ ?x :p ?y } MINUS { ?a :q
   ?b }` (no shared variables) keeps everything from the left — MINUS is a
   no-op when the patterns share no variables.

3. **Blank nodes in CONSTRUCT**: Each solution mapping instantiates the template.
   A blank node `_:b` in the template generates a *fresh* blank node per
   solution row. Need skolemization: `_:b` + row_id → unique identifier.

### RDF Storage Decision

Going with **single quad table** (`subject TEXT, predicate TEXT, object TEXT,
graph TEXT`) for simplicity. Rationale:

- No upfront schema knowledge required.
- Works with any RDF dataset without transformation.
- Self-joins on the quad table are the standard approach; Materialize's
  arrangements (indexes) handle the access patterns.
- CONSTRUCT output is naturally (s, p, o) rows — same schema.
- Dictionary encoding can be a transparent optimization later.

### Catalog-as-RDF Design

The catalog named graph should be queryable as `FROM <urn:materialize:catalog>`.
Implementation: a built-in view that unions system catalog tables into
(subject, predicate, object, graph) form. The SPARQL planner intercepts
this graph IRI and resolves to the built-in view.

Ontology prefix: `mz: <urn:materialize:catalog:>`.

Coverage: databases, schemas, tables, columns, views, materialized views,
sources, sinks, clusters, replicas, indexes, types, roles, connections, secrets
(names only, not values).

Each catalog object gets an IRI like `<urn:materialize:catalog:table/{schema}.{name}>`.

TODO: The user flagged this as important. Need to ensure the ontology is
expressive enough for useful queries (e.g., "find all materialized views
that depend on source X" requires dependency edges).

### Integration Strategy

**`SPARQL $$ ... $$` wrapper syntax**: Simplest approach. The SQL parser
recognizes the `SPARQL` keyword followed by a dollar-quoted string, extracts it,
and the planner delegates to the SPARQL parser + planner. No pgwire changes.
Works with all existing clients.

Precedent: PostgreSQL extensions like `EXECUTE` with string arguments,
`DO $$ ... $$` blocks in PL/pgSQL.

### Prior Art Summary

Key references studied:
- **Ontop**: Leading SPARQL-to-SQL via OBDA/R2RML mappings. Good optimization
  techniques but assumes existing relational schema.
- **Virtuoso**: Native hybrid RDF/SQL store. Internal quad table with heavily
  optimized SPARQL-to-SQL compiler.
- **RDF-3X**: Research system with exhaustive 6-permutation indexing and
  dictionary encoding. Gold standard for performance.
- **Chebotko et al. 2009**: "Semantics Preserving SPARQL-to-SQL Translation" —
  formal correctness proofs for the translation. Key reference for our planner.
- **C-SPARQL / CQELS**: Streaming SPARQL extensions — validates the value
  proposition of SPARQL + SUBSCRIBE.

### Materialize's Unique Advantages for SPARQL

1. **CONSTRUCT + SUBSCRIBE** = incrementally maintained derived knowledge graphs.
   No existing system offers this.
2. **WITH MUTUALLY RECURSIVE** = efficient incremental property paths.
   Transitive closure maintenance via differential dataflow.
3. **Materialized views** = persistent SPARQL query results that update
   automatically.
4. **Multi-temporal** = SPARQL queries at different timestamps (AS OF).

### Important Implementation Detail: SubscribeFrom uses MIR

The codebase explorer confirmed that `SubscribeFrom::Query` holds a
`MirRelationExpr`, not `HirRelationExpr`. This means the SPARQL planner
produces HIR, but we must call `HirRelationExpr::lower()` (from
`src/sql/src/plan/lowering.rs`) before wrapping in a `SubscribePlan`.
Same applies to `SelectPlan` — it contains `HirRelationExpr` as `source`,
but the adapter calls `lower()` during sequencing. So the SPARQL planner
should output HIR and let the existing lowering pipeline handle the rest.

The `Plan` enum variants we'd reuse:
- `Plan::Select(SelectPlan)` for SPARQL SELECT/ASK
- `Plan::Subscribe(SubscribePlan)` for SUBSCRIBE TO SPARQL
- `Plan::CreateView` / `Plan::CreateMaterializedView` for CREATE VIEW AS SPARQL

No new `Plan` variants needed — SPARQL queries produce the same plan types as
SQL queries. The difference is only in parsing and planning, not execution.

### Open Questions (from initial research)

- [ ] Triple table schema: canonical vs user-designated
- [ ] Dictionary encoding: when/whether to introduce
- [ ] Result formats: SPARQL JSON/XML or pgwire only initially
- [ ] Blank node scoping across SUBSCRIBE diffs
- [x] Parser: ~~fork Oxigraph or~~ write from scratch (decided: hand-rolled, matches sql-parser style)
- [ ] DESCRIBE semantics: CBD vs simple subject/object
- [ ] Catalog ontology: custom mz: vs standard vocabulary
- [ ] Should GRAPH patterns map to Mz schemas or to a graph column?

## 2026-03-22: Prompt 1 — Bootstrap `mz-sparql-parser` crate

### What was done

Created `src/sparql-parser/` crate with the following structure:

- **`Cargo.toml`**: Minimal dependencies (only `workspace-hack`). No `mz-ore`
  dependency yet — we don't need stack guards or assertions until parsing is
  more complex. Added to workspace `Cargo.toml` members list (both occurrences).

- **`src/lib.rs`**: Module declarations for `ast`, `lexer`, `parser`.

- **`src/ast.rs`**: Core AST types covering the full SPARQL 1.1 grammar:
  - `SparqlQuery` — top-level query with BASE, PREFIX, form, WHERE, modifiers
  - `QueryForm` — SELECT / CONSTRUCT / ASK / DESCRIBE
  - `GroupGraphPattern` — all graph pattern forms (Basic, Group, Optional,
    Union, Minus, Filter, Bind, Values, SubSelect, Graph, Service)
  - `TriplePattern` with `VarOrTerm` subject/object and `VerbPath` predicate
  - `PropertyPath` — full path algebra (Iri, Inverse, Sequence, Alternative,
    ZeroOrMore, OneOrMore, ZeroOrOne, NegatedSet)
  - `Expression` — comprehensive expression types including arithmetic,
    comparison, logical, built-in functions, aggregates, IN/NOT IN
  - Supporting types: `Variable`, `Iri`, `RdfLiteral`, `GraphTerm`,
    `SelectClause`, `OrderCondition`, `PrefixDecl`, etc.

- **`src/lexer.rs`**: Tokenizer with:
  - `Keyword` enum: 90+ SPARQL keywords (case-insensitive matching)
  - `Token` enum: keywords, variables (`?x`/`$x`), IRIs (`<...>`), prefixed
    names (`ex:foo`, `:foo`), blank nodes (`_:b0`), string/long-string literals,
    numeric literals (integer/decimal/double), language tags (`@en`), datatype
    separator (`^^`), and all operators/punctuation
  - `lex()` function returning `Vec<PosToken>` with byte offsets
  - Comment skipping (`#` to EOL)
  - String escape handling (`\n`, `\t`, `\\`, etc.)
  - 14 unit tests covering: simple SELECT, PREFIX+IRI, FILTER with operators,
    typed literals, language-tagged literals, blank nodes, numeric literals,
    property path operators, comments, case-insensitive keywords, full query,
    string escapes, empty prefix, dollar variables, offset tracking

- **`src/parser.rs`**: Parser skeleton with:
  - `Parser` struct holding tokens and position
  - `peek()`, `next_token()`, `current_pos()`, `error()`, `expected()` helpers
  - `parse()` entry point that lexes then delegates to `parse_query()`
  - `parse_query()` stub returning "not yet implemented" (to be built in prompt 2)
  - 3 unit tests: stub error, lexer error propagation, peek/next mechanics

### Key decisions

1. **No `mz-ore` dependency**: The `assert-no-tracing` feature had compilation
   issues with the `ctor` crate. Since we don't need stack guards yet (no
   recursion in the lexer), we deferred this to prompt 2 when the recursive
   parser is implemented.

2. **Hand-rolled lexer**: Matches the sql-parser crate's approach. The SPARQL
   lexer is simpler than SQL (no dollar-quoting, no extended strings) but has
   unique tokens (variables with `?`/`$`, IRIs in angle brackets, prefixed
   names, `^^` for datatypes, `@` for language tags).

3. **`<` disambiguation**: The lexer uses a heuristic — `<` followed by
   whitespace is the less-than operator; otherwise it starts an IRI. This
   matches SPARQL semantics where IRIs always have content.

4. **AST designed for planner consumption**: Types like `GroupGraphPattern`
   directly reflect the SPARQL algebra operations that the planner (prompt 7+)
   will translate to HIR.

### Test results

18 tests pass, 0 failures, 0 warnings.

## 2026-03-22: Prompt 2 — Parse basic SELECT queries (BGP + FILTER)

### What was done

Implemented the full recursive-descent parser for SELECT queries with basic graph
patterns and FILTER expressions. The parser now handles:

- **Prologue**: BASE and PREFIX declarations with prefix resolution (prefixed
  names are expanded to full IRIs during parsing)
- **SELECT clause**: `SELECT *`, `SELECT ?var1 ?var2`, `SELECT DISTINCT`,
  `SELECT REDUCED`, and `SELECT (expr AS ?var)` expressions
- **WHERE clause**: optional WHERE keyword (SPARQL allows omitting it)
- **Basic graph patterns**: triple patterns with `.` separator, `;` shorthand
  (shared subject), `,` shorthand (shared subject + predicate)
- **Triple pattern terms**: variables (`?x`/`$x`), full IRIs (`<...>`), prefixed
  names (`ex:foo`), `a` keyword (→ `rdf:type`), string literals (simple,
  language-tagged, datatype-annotated), numeric literals, boolean literals,
  blank node labels (`_:b0`)
- **FILTER expressions**: full expression language with proper operator precedence:
  - Logical: `||`, `&&`, `!`
  - Comparison: `=`, `!=`, `<`, `>`, `<=`, `>=`, `IN`, `NOT IN`
  - Arithmetic: `+`, `-`, `*`, `/` (binary and unary)
  - Built-in functions: BOUND, isIRI/isURI, isBlank, isLiteral, isNumeric,
    STR, LANG, DATATYPE, STRLEN, UCASE, LCASE, CONTAINS, STRSTARTS, STRENDS,
    SUBSTR, CONCAT, REPLACE, REGEX, IF, COALESCE, LANGMATCHES, and many more
    (ABS, ROUND, CEIL, FLOOR, hash functions, date/time functions, etc.)
  - Function calls: `iri(args...)` for custom functions
  - Parenthesized sub-expressions

- **Error messages**: clear messages for common errors:
  - Missing `{` → "expected '{'"
  - Unknown prefix → "unknown prefix 'ex:'"
  - Missing SELECT → "expected SELECT"
  - Empty SELECT variables → "expected variable or '*' in SELECT clause"
  - Not-yet-implemented features → "OPTIONAL patterns not yet supported (see prompt 3)"

### Key decisions

1. **Prefix resolution during parsing**: Prefixed names are expanded to full IRIs
   at parse time (stored in the AST as full IRIs). This matches the W3C spec
   which says prefix expansion is a syntactic operation, and keeps the AST clean
   for the planner.

2. **`next_token()` returns `Option<Token>` (cloned)**: Changed from returning
   `&Token` to avoid borrow checker conflicts between peek/advance and mutable
   parser methods. Added `bump()` for advance-without-return.

3. **`eat_keyword()`/`eat()`/`expect()` helpers**: Provide clean consume-if-match
   and consume-or-error patterns. These make the parser code concise and readable.

4. **Group simplification**: When a `{ ... }` block contains only triples (no
   FILTERs), it's represented as `Basic(triples)` rather than
   `Group([Basic(triples)])`. When it contains both triples and FILTERs, it's
   `Group([Basic(triples), Filter(expr)])`. This simplifies downstream processing.

5. **Stub errors for unimplemented features**: OPTIONAL, UNION, MINUS, BIND,
   VALUES, GRAPH, SERVICE, nested groups all produce descriptive "not yet
   supported" errors pointing to the relevant prompt number.

6. **Built-in functions**: Implemented comprehensive coverage including functions
   not explicitly listed in the prompt scope (hash functions, date/time functions,
   etc.) since the infrastructure was already in place and they follow the same
   1-arg or 2-arg patterns. Functions without dedicated AST nodes are mapped to
   `Expression::FunctionCall(Iri, Vec<Expression>)`.

### Limitations / deferred to later prompts

- Solution modifiers (GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET) → prompt 6
- OPTIONAL, UNION, MINUS, BIND, VALUES → prompt 3
- CONSTRUCT, ASK, DESCRIBE → prompt 4
- Property paths → prompt 5
- Aggregates, subqueries, GRAPH → prompt 6
- `<` disambiguation: the lexer requires spaces around `<` in expressions
  (e.g., `?x < 5` works, `?x<5` doesn't). Documented in prompt 1.
- Blank node shorthand `[]` and `[?p ?o]` not yet supported
- `EXISTS`/`NOT EXISTS` graph patterns deferred to prompt 3

### Test results

51 tests pass (18 lexer + 33 parser), 0 failures, 0 warnings.

## 2026-03-22: Prompt 3 — Parse OPTIONAL, UNION, MINUS, BIND, VALUES

### What was done

Extended `parse_group_graph_pattern_sub()` in `src/sparql-parser/src/parser.rs` to
handle all remaining group graph pattern forms. Also added EXISTS/NOT EXISTS which
were deferred from prompt 2.

**OPTIONAL**: Parses `OPTIONAL { ... }` as `GroupGraphPattern::Optional(inner)`.
The inner pattern is a full group graph pattern, so FILTER inside OPTIONAL works
correctly (the FILTER stays inside the Optional node, preserving left-join semantics
for the planner).

**UNION**: Parses `{ P1 } UNION { P2 }` chains. Each side is a full group graph
pattern wrapped in braces. Multi-way unions (`{ P1 } UNION { P2 } UNION { P3 }`)
are parsed left-associatively as `Union(Union(P1, P2), P3)`. The key insight is
that UNION always connects two brace-enclosed groups — the parser reads a `{ ... }`
block, then checks for `UNION` and chains additional `{ ... }` blocks.

**MINUS**: Parses `MINUS { ... }` as `GroupGraphPattern::Minus(inner)`. The inner
pattern is a full group graph pattern.

**BIND**: Parses `BIND(expr AS ?var)` as `GroupGraphPattern::Bind(expr, var)`.
The expression uses the full expression parser (arithmetic, function calls, etc.).

**VALUES**: Parses both single-variable (`VALUES ?x { 1 2 3 }`) and multi-variable
(`VALUES (?x ?y) { (1 2) (3 UNDEF) }`) forms. Added `Keyword::Undef` to the lexer.
UNDEF values are represented as `None` in the `ValuesRow` type (already defined in
prompt 1's AST). Implemented `parse_inline_data()` and `parse_data_block_value()`
helper methods.

**EXISTS / NOT EXISTS**: Added parsing for both `EXISTS { ... }` (as a built-in call
keyword) and `NOT EXISTS { ... }` (special-cased in `parse_primary_expression()` and
`parse_constraint()`). Both produce `Expression::Exists` / `Expression::NotExists`
wrapping a `GroupGraphPattern`. This handles:
- `FILTER EXISTS { ... }`
- `FILTER NOT EXISTS { ... }`
- `FILTER(... && NOT EXISTS { ... })` — NOT EXISTS inside compound expressions

**Nested groups**: `{ ... }` blocks inside a group graph pattern are now parsed as
nested groups, enabling patterns like `{ ?s ?p ?o . { ?a ?b ?c } }`.

### Key decisions

1. **UNION left-associativity**: Multi-way UNION is parsed as nested binary `Union`
   nodes, left-to-right. This matches the W3C SPARQL algebra where UNION is binary.
   The planner will recursively process Union nodes.

2. **Flush-before-non-triple pattern**: All non-triple patterns (OPTIONAL, UNION,
   MINUS, BIND, VALUES, FILTER) flush accumulated triple patterns into a `Basic`
   node before adding the non-triple pattern. This maintains correct scoping — triple
   patterns before an OPTIONAL are in the outer join's left side.

3. **VALUES single-var shorthand**: The SPARQL grammar allows `VALUES ?x { 1 2 3 }`
   (no parens around the variable or values) as a shorthand for a single variable.
   Both forms are supported.

4. **Optional dot after pattern**: Each non-triple pattern consumes an optional
   trailing `.` separator, matching the SPARQL grammar where dots are optional
   between graph pattern forms.

### Limitations / deferred to later prompts

- GRAPH patterns → prompt 6
- SERVICE patterns → not planned for initial implementation
- Subqueries (`{ SELECT ... }` inside WHERE) → prompt 6
- Blank node shorthand `[]` and `[?p ?o]` → not yet supported

### Test results

73 tests pass (18 lexer + 55 parser), 0 failures, 0 warnings.

New parser tests (22):
- OPTIONAL: simple, with FILTER, multiple
- UNION: simple, three-way, with preceding triples
- MINUS: simple, disjoint variables
- BIND: simple, arithmetic expression
- VALUES: single variable, multi-variable, with UNDEF, string literals
- EXISTS: FILTER EXISTS, FILTER NOT EXISTS, NOT EXISTS in compound expression
- Nested groups
- Combinations: OPTIONAL inside UNION, OPTIONAL + MINUS, BIND + VALUES,
  all forms combined in one query

## 2026-03-22: Prompt 4 — Parse CONSTRUCT, ASK, DESCRIBE

### What was done

Extended `parse_query()` to dispatch on all four SPARQL query forms (SELECT,
CONSTRUCT, ASK, DESCRIBE) instead of only SELECT. Added three new parser
methods and updated the error message for unknown query forms.

**CONSTRUCT** (two forms):
- **Full form**: `CONSTRUCT { template } WHERE { pattern }` — parses the
  template as a triples block inside braces via `parse_construct_template()`,
  then parses the WHERE clause normally.
- **Short form**: `CONSTRUCT WHERE { pattern }` — detected by checking if the
  next token after CONSTRUCT is WHERE. The template is extracted from the WHERE
  pattern by collecting all `Basic` triple patterns (via
  `extract_triples_from_pattern()`). This matches the W3C spec Section 16.2.2.

**ASK**: Parses `ASK [WHERE] { pattern }`. Produces `QueryForm::Ask` with just
a WHERE clause. The WHERE keyword is optional (handled by `parse_where_clause`).

**DESCRIBE** (three forms):
- `DESCRIBE <iri>` / `DESCRIBE ?var` — one or more resources
- `DESCRIBE ?x ?y <iri>` — mixed variables and IRIs
- `DESCRIBE *` — represented as empty `resources` vec (the planner interprets
  this as "describe all resources from WHERE clause")
- Optional WHERE clause: if `WHERE` or `{` follows, parse it; otherwise use
  empty `Basic(vec![])`.

### Key decisions

1. **CONSTRUCT WHERE short form**: Rather than re-parsing the template, we
   extract triples from the already-parsed WHERE pattern. This is simpler and
   avoids parsing the same braces twice. The extraction is shallow — it only
   collects `Basic` triples from the top-level and `Group` patterns, which
   matches the spec's intent (CONSTRUCT WHERE only makes sense with simple BGPs).

2. **DESCRIBE * representation**: An empty `resources` vec signals "describe
   all". This avoids adding a new enum variant and is unambiguous since
   `DESCRIBE` without resources is a parse error.

3. **Error message improvement**: The error for unknown query forms now says
   "expected SELECT, CONSTRUCT, ASK, or DESCRIBE" instead of just "expected
   SELECT".

### Test results

95 tests pass (18 lexer + 77 parser), 0 failures, 0 warnings.

New parser tests (22):
- CONSTRUCT: basic, multi-template, semicolon shorthand, empty template,
  WHERE short form, with prefix, with blank node, with literal object
- ASK: basic, with prefix, with WHERE keyword, with FILTER, with OPTIONAL
- DESCRIBE: single IRI, variable, multiple resources, star, with WHERE, no WHERE
- Errors: unknown query form, empty DESCRIBE, CONSTRUCT missing brace
