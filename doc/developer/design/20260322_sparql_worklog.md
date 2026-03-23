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

## 2026-03-22: Prompt 5 — Parse property paths

### What was done

Implemented full property path parsing in `src/sparql-parser/src/parser.rs`,
following the W3C SPARQL 1.1 grammar (Section 9). Property paths can now appear
wherever a predicate is expected in triple patterns.

**Replaced `parse_verb()`**: The former stub that only accepted simple IRIs,
variables, and `a` now delegates to the property path parser for non-variable
predicates. Variables are still handled directly (they cannot be part of property
path expressions in SPARQL 1.1).

**Updated `can_start_verb()`**: Now also recognizes `^` (inverse), `!` (negated),
and `(` (grouped) as valid predicate starters.

**Property path parser** — 8 new methods implementing a recursive-descent parser
with correct operator precedence:

1. `parse_path()` → entry point, delegates to `parse_path_alternative()`
2. `parse_path_alternative()` → `PathSequence ( '|' PathSequence )*` (lowest precedence)
3. `parse_path_sequence()` → `PathEltOrInverse ( '/' PathEltOrInverse )*`
4. `parse_path_elt_or_inverse()` → `PathElt | '^' PathElt`
5. `parse_path_elt()` → `PathPrimary PathMod?`
6. `parse_path_mod()` → optional `*`, `+`, `?` postfix modifiers (highest precedence)
7. `parse_path_primary()` → `iri | 'a' | '!' NegatedSet | '(' Path ')'`
8. `parse_path_negated_property_set()` → `element | '(' element ('|' element)* ')'`
9. `parse_negated_path_element()` → `iri | 'a' | '^' iri | '^' 'a'`
10. `parse_path_iri_or_a()` → IRI or `a` keyword in path context

**Operator precedence** (lowest to highest):
- `|` (alternative) — `ex:a | ex:b`
- `/` (sequence) — `ex:a / ex:b`
- `^` (inverse) — `^ex:a`
- `*`, `+`, `?` (modifiers) — `ex:a+`
- Parentheses override precedence — `(ex:a | ex:b) / ex:c`

So `ex:a/ex:b|ex:c` parses as `(ex:a/ex:b) | ex:c`, and `^ex:p+` parses as
`^(ex:p+)` (modifier binds to primary, then inverse wraps).

### Key decisions

1. **`Token::Question` for `?` modifier**: The lexer already distinguishes
   standalone `?` (when not followed by a name character) from variable tokens.
   The property path modifier parser consumes `Token::Question` after a path
   element.

2. **Negated property sets use `NegatedPathElement`**: The AST already had
   `NegatedPathElement::Forward(Iri)` and `NegatedPathElement::Inverse(Iri)`.
   The parser produces these directly, keeping the AST clean for the planner.

3. **Empty negated set `!()` allowed**: The grammar permits an empty parenthesized
   negated set. It's semantically vacuous (matches all predicates) but harmless
   to parse.

4. **No changes to AST**: All property path AST types were already defined in
   prompt 1. The parser now populates them.

### Test results

120 tests pass (18 lexer + 102 parser), 0 failures, 0 warnings.

New parser tests (25):
- Simple: IRI path, `a` keyword, variable predicate still works
- Unary: inverse (`^`), zero-or-more (`*`), one-or-more (`+`), zero-or-one (`?`)
- Binary: sequence (`/`), alternative (`|`), three-way alternative
- Negated: single IRI, multi-element set, `a` keyword, empty parens
- Parenthesized: grouped path, with modifier (`(ex:a/ex:b)+`)
- Precedence: sequence over alternative, parens overriding precedence
- Combined: inverse with modifier, sequence+inverse+modifier, complex transitive closure
- Integration: paths in CONSTRUCT, paths with FILTER, multiple triples with paths,
  semicolon shorthand with paths

## 2026-03-22: Prompt 6 — Parse aggregates, subqueries, GRAPH, solution modifiers

### What was done

Completed the SPARQL 1.1 parser by implementing all remaining features: solution
modifiers, aggregate functions, subqueries, and GRAPH patterns.

**Solution modifiers** — added `parse_solution_modifiers()` method returning
`(group_by, having, order_by, limit, offset)`. Called after every query form's
WHERE clause (SELECT, CONSTRUCT, ASK, DESCRIBE). Parsing details:

- **GROUP BY**: Supports bare variables (`GROUP BY ?x`), built-in function calls
  (`GROUP BY LCASE(?name)`), and parenthesized expressions with optional AS alias
  (`GROUP BY (YEAR(?date) AS ?yr)`).
- **HAVING**: Parses a single constraint expression (parenthesized or built-in call).
  Aggregates inside HAVING are supported (e.g., `HAVING (COUNT(?s) > 5)`).
- **ORDER BY**: Supports `ASC(expr)`, `DESC(expr)`, and bare expressions (default
  ascending). Multiple conditions are comma-free (space-separated), matching the
  SPARQL grammar. Correctly stops at LIMIT/OFFSET/VALUES keywords.
- **LIMIT / OFFSET**: Parse integer values after the keyword.

**Aggregate functions** — added `parse_aggregate()` method and `is_aggregate_keyword()`
helper. Aggregates are recognized in `parse_primary_expression()` before built-in
functions (since aggregate keywords like MIN/MAX must not fall through to other
handling). Supported aggregates:

- `COUNT(*)`, `COUNT(expr)`, `COUNT(DISTINCT expr)`
- `SUM`, `AVG`, `MIN`, `MAX` — all with optional DISTINCT
- `GROUP_CONCAT(expr)` and `GROUP_CONCAT(expr ; SEPARATOR = "str")`
- `SAMPLE(expr)` with optional DISTINCT

**Subqueries** — `{ SELECT ... }` inside a WHERE clause is now recognized by
peeking past the `{` for a `SELECT` keyword. The subquery is parsed via
`parse_subselect()` which handles SELECT clause, WHERE clause, solution modifiers,
and an optional trailing VALUES clause. Produces `GroupGraphPattern::SubSelect`.

**GRAPH patterns** — `GRAPH <iri> { ... }` and `GRAPH ?var { ... }` are parsed
via a new `parse_var_or_iri()` helper. The inner pattern is a full group graph
pattern, supporting nested subqueries and other pattern forms inside GRAPH blocks.

### Key decisions

1. **GROUP BY / ORDER BY two-word keywords**: The lexer maps "GROUP" to
   `Keyword::GroupBy` and "ORDER" to `Keyword::OrderBy`, but "BY" is lexed as a
   separate `PrefixedName { prefix: "", local: "BY" }` token. Added
   `expect_bare_word()` helper to consume the "BY" token after the keyword.

2. **ORDER BY bare expressions**: Used `parse_expression()` (not `parse_constraint()`)
   for bare ORDER BY items since `parse_constraint` only handles parenthesized
   expressions and built-in calls, while ORDER BY commonly uses bare variables.

3. **Subquery detection**: Peek two tokens ahead (`{` then `SELECT`) to distinguish
   subqueries from nested groups. This avoids backtracking and is unambiguous since
   SELECT cannot start a triple pattern.

4. **Solution modifiers on all query forms**: Per the SPARQL grammar, solution
   modifiers are valid after any query form's WHERE clause. Even ASK gets them
   parsed (for spec compliance), though they're semantically questionable.

### Test results

154 tests pass (18 lexer + 136 parser), 0 failures, 0 warnings, 0 clippy warnings.

New parser tests (34):
- Solution modifiers: GROUP BY simple, GROUP BY multiple, HAVING, ORDER BY simple,
  ORDER BY DESC, ORDER BY ASC, ORDER BY multiple, LIMIT, OFFSET, LIMIT+OFFSET,
  all modifiers combined
- Aggregates: COUNT(*), COUNT(expr), COUNT(DISTINCT), SUM, AVG, MIN/MAX,
  GROUP_CONCAT with separator, GROUP_CONCAT without separator, SAMPLE,
  multiple aggregates in one query
- GRAPH: IRI name, variable name, with preceding triples
- Subqueries: simple, with LIMIT, with aggregates
- Combined: full aggregation query, CONSTRUCT with LIMIT, DESCRIBE with LIMIT,
  aggregate in HAVING, GRAPH with subquery, ORDER BY expression, GROUP BY expression

## 2026-03-22: Prompt 7 — Bootstrap `mz-sparql` planner crate and plan BGPs

### What was done

Created `src/sparql/` crate with the SPARQL-to-HIR query planner. The planner
takes a parsed `SparqlQuery` and produces `HirRelationExpr` (the same HIR used
by the SQL planner), enabling reuse of the entire lowering/optimization/execution
pipeline.

**Crate structure**:
- **`Cargo.toml`**: Dependencies on `mz-sparql-parser` (AST), `mz-repr` (Datum,
  Row, ScalarType), `mz-expr` (BinaryFunc, Id), `mz-sql` (HirRelationExpr,
  HirScalarExpr, JoinKind), `mz-ore`.
- **`src/lib.rs`**: Module declaration for `plan`.
- **`src/plan.rs`**: The planner implementation.

**Core types**:
- `SparqlPlanner` — holds the quad table ID and type. Constructed with
  `SparqlPlanner::new(quad_table_id: GlobalId)`.
- `PlannedRelation` — bundles a `HirRelationExpr` with a `var_map: BTreeMap<String, usize>`
  that tracks which SPARQL variable maps to which column index.
- `PlanError` — error type for planning failures.

**Quad table model**: The planner assumes a single table `(subject TEXT, predicate TEXT,
object TEXT, graph TEXT)` identified by `GlobalId`. Constants: `QUAD_SUBJECT=0`,
`QUAD_PREDICATE=1`, `QUAD_OBJECT=2`, `QUAD_GRAPH=3`.

**Single triple pattern planning** (`plan_triple`):
1. `Get(quad_table)` — scan the quad table (4 columns)
2. For each position (subject, predicate, object):
   - Variable → record in `var_to_quad_col` mapping
   - Concrete term (IRI, literal) → add equality `Filter` predicate
   - Repeated variable (same var in multiple positions) → equality filter between columns
3. `Project` to keep only columns with variables, sorted alphabetically by variable name
4. Output: `PlannedRelation` with the projected relation and variable→column mapping

**Multi-pattern BGP planning** (`plan_bgp`):
1. Plan first triple pattern
2. For each subsequent pattern, join with the accumulated result via `join_on_shared_vars`
3. Empty BGP → `Constant` with one empty row (join identity)

**Join on shared variables** (`join_on_shared_vars`):
1. Find variables present in both left and right relations
2. Build equality predicates: `left.col = right.(col + left_arity)`
3. Multiple shared vars → conjunction via `VariadicFunc::And`
4. No shared vars → cross join (ON = `true`)
5. `Project` to eliminate duplicate columns for shared variables
6. Update var_map: left vars keep their indices, new right vars get appended

**RDF term encoding** (`term_to_string`):
- IRIs: bare IRI string (no angle brackets)
- Simple literals: string value
- Language-tagged: `"value"@lang`
- Typed: `"value"^^<datatype>`
- Blank nodes: `_:label`
- Numeric/boolean: string representation

**mz-sql change**: Added `ColumnRef` to the public re-exports in
`src/sql/src/plan.rs` (needed for downstream crates to construct
`HirScalarExpr::Column` values, though not used in this prompt).

### Key decisions

1. **Separate crate**: `mz-sparql` depends on `mz-sql` (for HIR types) but not
   vice versa. The integration (prompt 13) will happen at the adapter level,
   which already depends on both. No circular dependency.

2. **PlannedRelation abstraction**: Rather than returning bare `HirRelationExpr`,
   we bundle it with the variable→column mapping. This is essential because
   SPARQL variables are named (not positional), and downstream planning steps
   (FILTER, OPTIONAL, projection) need to resolve variable references to column
   indices.

3. **Alphabetical variable ordering**: Variables are sorted by name in the
   output projection. This ensures deterministic column ordering regardless of
   the order variables appear in the query. Makes testing reliable and JOIN
   column alignment predictable.

4. **BinaryFunc::Eq(Eq) pattern**: Materialize's `BinaryFunc` enum uses the
   `#[sqlfunc]` proc macro pattern where each variant wraps a unit struct
   (e.g., `BinaryFunc::Eq(Eq)`). Same for `VariadicFunc::And(And)`.

5. **No catalog dependency yet**: The planner takes a `GlobalId` for the quad
   table rather than resolving it from the catalog. This keeps the initial
   implementation simple and testable. Catalog integration comes in prompt 13.

### Test results

12 tests pass, 0 failures, 1 warning (unused QUAD_ARITY constant).

Tests cover:
- Single triple: all variables, concrete predicate, concrete subject+predicate,
  `a` (rdf:type) shorthand, repeated variable, literal object
- Multi-triple BGP: shared variable join, no shared variables (cross join),
  three-way join, multiple shared variables
- Edge cases: empty BGP, deterministic column ordering

## 2026-03-22: Prompt 8 — Plan FILTER, OPTIONAL, UNION, MINUS

### What was done

Extended the SPARQL planner in `src/sparql/src/plan.rs` to handle the four remaining
group graph pattern forms: FILTER, OPTIONAL, UNION, and MINUS. Also added the
beginning of a SPARQL expression translator (`translate_expression`).

**FILTER** — `apply_filter()` translates a SPARQL `Expression` to `HirScalarExpr`
and wraps the input in `HirRelationExpr::Filter`. The expression translator handles:
- Variable references → `HirScalarExpr::column(idx)` via var_map lookup
- Comparison operators: `=`, `!=`, `<`, `>`, `<=`, `>=` → `BinaryFunc::Eq/NotEq/Lt/Gt/Lte/Gte`
- Logical operators: `&&` → `VariadicFunc::And`, `||` → `VariadicFunc::Or`, `!` → `UnaryFunc::Not`
- `BOUND(?var)` → `Not(IsNull(column))` — true when variable is not null
- Literals (string, numeric, boolean, IRI) → `HirScalarExpr::literal`
- Other expression forms return a "not yet supported (prompt 9)" error

Per SPARQL semantics, FILTERs in a group apply to the entire group (not just
preceding patterns). The `plan_group` method collects all FILTERs and applies
them after all other patterns have been joined.

**OPTIONAL** — `plan_optional()` produces `Join { kind: LeftOuter, on: ... }`.
Critical semantic detail: FILTERs inside OPTIONAL go into the join's ON clause,
not as a post-join filter. This is essential for SPARQL OPTIONAL+FILTER semantics
(a post-join filter would eliminate rows where the optional pattern didn't match,
defeating the purpose of the left outer join).

Implementation:
1. `extract_optional_filters()` separates FILTERs from the inner group
2. Non-filter patterns are planned normally as the right side
3. Shared variable equality predicates + translated filter expressions form the ON clause
4. Project eliminates duplicate columns for shared variables
5. New right-only variables get NULL when the optional pattern doesn't match (LeftOuter semantics)

**UNION** — `plan_union()` implements outer union with NULL-padding:
1. Plan both sides independently
2. Compute combined variable set (sorted, deduplicated)
3. `pad_for_union()` adds NULL columns via `Map` for variables not present on a side
4. `Project` reorders columns to match the target variable order
5. `HirRelationExpr::union()` combines the padded sides

**MINUS** — `plan_minus()` implements anti-join using `Negate + Union + Threshold`:
1. Find shared variables between left and right
2. If no shared variables → MINUS is a no-op (return left unchanged, per SPARQL spec)
3. Project right to shared variables only, then `Distinct`
4. Inner-join left with right_projected on shared variables (acts as semi-join since right is distinct)
5. Project back to left columns only
6. Result = `left.union(matched.negate()).threshold()` — removes matched rows

**`plan_group` rewrite** — The group planner now properly handles mixed pattern types:
- Basic/Group/Union patterns are inner-joined with the accumulator
- FILTER expressions are collected and applied at the end (SPARQL scoping rule)
- OPTIONAL patterns produce left outer joins with the accumulator
- MINUS patterns produce anti-joins against the accumulator
- Empty accumulator is optimized: first non-trivial pattern replaces it instead of joining

### Key decisions

1. **FILTER scoping**: Per the SPARQL spec, FILTERs in a group apply to the whole
   group, not just preceding patterns. We collect them and apply after all joins.
   Exception: FILTERs inside OPTIONAL are extracted and placed in the join ON clause.

2. **MINUS via Negate+Threshold**: Rather than implementing a custom anti-join
   operator, we use the existing `Negate` + `Union` + `Threshold` pattern (same
   as SQL EXCEPT). This is semantically correct and reuses the well-tested
   differential dataflow machinery.

3. **MINUS no-shared-vars optimization**: When MINUS has no shared variables with
   the left side, it has no effect per the SPARQL spec. We short-circuit and return
   the left side unchanged.

4. **Expression translator stub**: Only comparison, logical, BOUND, and literal
   expressions are implemented now. Arithmetic, string functions, type tests, etc.
   are deferred to prompt 9 with a clear error message.

5. **UNION NULL-padding via Map**: Variables missing from one side get `NULL` via
   `Map` + `Project`. All quad table columns are TEXT/nullable, so String NULL is
   the correct padding type.

### Test results

32 tests pass (12 old + 20 new), 0 failures, 1 warning (unused QUAD_ARITY constant).

New tests (20):
- FILTER: simple equality, comparison (Gt), logical AND, unary NOT, BOUND,
  multiple filters in group, variable not in scope (error case)
- OPTIONAL: simple, with inner FILTER (verifies ON placement), multiple OPTIONALs,
  no shared variables (cross left join)
- UNION: same variables, different variables (NULL padding), three-way union
- MINUS: shared variables (Threshold+Negate structure), no shared variables (no-op),
  preserves all left-side variables
- Combined: OPTIONAL inside UNION, FILTER after OPTIONAL, OPTIONAL+MINUS together

## 2026-03-22: Prompt 9 — Plan BIND, VALUES, expressions, and type coercions

### What was done

Extended the SPARQL planner in `src/sparql/src/plan.rs` to handle BIND and VALUES
graph patterns, and completed the SPARQL expression translator with all expression
forms (except aggregates, deferred to prompt 10).

**BIND** — `plan_bind()` adds a computed column via `HirRelationExpr::Map`:
1. Translate the BIND expression in the context of the current variable scope
2. Append the result as a new column via `Map`
3. Register the new variable in `var_map`

In `plan_group`, BIND is processed in order (not collected like FILTERs), since
BIND introduces a new variable that subsequent patterns/FILTERs may reference.

**VALUES** — `plan_values()` creates a `HirRelationExpr::Constant`:
1. Build `SqlRelationType` with one nullable String column per variable
2. Convert each row's terms to string datums (UNDEF → Datum::Null)
3. Pack into `Row` values for the Constant relation
4. In `plan_group`, VALUES is treated like any other joinable pattern

**Expression translator** — completed all remaining forms in `translate_expression`:

- **Arithmetic** (`+`, `-`, `*`, `/`): Cast operands from string to float64 via
  `CastStringToFloat64`, apply the binary op (e.g., `AddFloat64`), cast result
  back via `CastFloat64ToString`. Helper methods: `to_float64()`, `from_float64()`,
  `translate_arithmetic()`.
- **Unary arithmetic**: `UnaryPlus` is identity, `UnaryMinus` uses `NegFloat64`.
- **IF(cond, then, else)** → `HirScalarExpr::If`
- **COALESCE(exprs)** → `VariadicFunc::Coalesce`
- **EXISTS/NOT EXISTS** → `HirScalarExpr::Exists` with correlated subquery.
  `correlate_subquery()` adds equality filters for outer-scope variables that
  appear in the inner pattern.
- **Type tests** (best-effort on string encoding):
  - `isBlank(?x)` → `StartsWith(?x, "_:")`
  - `isIRI(?x)` → `NOT IsNull AND NOT StartsWith("_:") AND NOT StartsWith("\"")`
  - `isLiteral(?x)` → `NOT IsNull AND NOT StartsWith("_:")`
  - `isNumeric(?x)` → regex match `^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$`
- **Accessors** (stub implementations, full extraction deferred to prompt 17):
  - `STR(?x)` → identity (correct for IRIs and simple literals)
  - `LANG(?x)` → returns "" (language tag extraction TBD)
  - `DATATYPE(?x)` → returns xsd:string (datatype extraction TBD)
- **String functions**:
  - `STRLEN` → `CharLength` + `CastInt32ToString`
  - `UCASE` → `Upper`
  - `LCASE` → `Lower`
  - `CONTAINS` → `Position > 0`
  - `STRSTARTS` → `StartsWith`
  - `STRENDS` → `Right(str, CharLength(suffix)) = suffix`
  - `SUBSTR` → `VariadicFunc::Substr` (with string→int64 cast for positions)
  - `CONCAT` → `VariadicFunc::Concat`
  - `REPLACE` → `VariadicFunc::Replace`
  - `REGEX` → `IsRegexpMatchCaseSensitive` / `IsRegexpMatchCaseInsensitive`
- **IN/NOT IN** → expanded to OR chain of equality checks
- **Aggregates** → produce clear error (handled in prompt 10)
- **FunctionCall** → produce error with the IRI

### Key decisions

1. **String-based RDF encoding**: All quad table values are TEXT. Arithmetic
   operations cast to float64 and back. This is correct for numeric RDF literals
   stored as bare strings but won't handle typed literals like `"42"^^<xsd:integer>`
   until prompt 17 adds proper lexical form extraction.

2. **BIND is order-dependent in groups**: Unlike FILTER (which applies to the whole
   group regardless of position), BIND must be processed in sequence because it
   introduces a new variable. The `plan_group` method processes BIND inline, not
   collected-and-deferred like FILTER.

3. **Type test approximations**: The string encoding makes it impossible to
   perfectly distinguish IRIs from simple literals (both are bare strings). The
   `isIRI` and `isLiteral` implementations are best-effort. A future encoding
   change (e.g., adding a type discriminator column) would fix this.

4. **EXISTS correlation**: Inner patterns in EXISTS/NOT EXISTS reference the same
   quad table. Shared variables between outer and inner scope produce equality
   filters on the inner relation, effectively correlating the subquery.

5. **LANG/DATATYPE stubs**: These accessors need to parse the string encoding
   (extract `@lang` suffix or `^^<type>` suffix). Deferred to prompt 17 where
   RDF expression edge cases are addressed systematically.

### Test results

60 tests pass (32 old + 28 new), 0 failures, 1 warning (unused QUAD_ARITY constant).

New tests (28):
- BIND: simple, arithmetic expression, with FILTER using bound variable
- VALUES: single variable, multi-variable (verifies Constant with 2 rows),
  with UNDEF (verifies NULL handling), joined with BGP
- Arithmetic: add, subtract, multiply/divide, unary minus
- IF: if-then-else in BIND
- COALESCE: two-argument coalesce in BIND
- String functions: UCASE/LCASE, STRLEN, CONTAINS, STRSTARTS/STRENDS,
  CONCAT, REGEX (with and without flags), REPLACE, SUBSTR
- IN/NOT IN: three-element list, two-element list with NOT
- Type tests: isBlank, isNumeric
- EXISTS: simple EXISTS, NOT EXISTS
- Combined: BIND+VALUES+FILTER together in one query
- Error: aggregate in FILTER (produces expected error message)

## 2026-03-22: Prompt 10 — Plan SELECT projection, aggregates, solution modifiers

### What was done

Extended the SPARQL planner in `src/sparql/src/plan.rs` to implement the full
query planning pipeline: SELECT projection, GROUP BY + aggregates, HAVING,
DISTINCT/REDUCED, ORDER BY, LIMIT/OFFSET.

**`plan()` rewrite** — The top-level `plan()` method now implements the full
SPARQL query evaluation pipeline:
1. Plan WHERE clause → base relation
2. GROUP BY + aggregates → `HirRelationExpr::Reduce`
3. HAVING → `Filter` post-Reduce
4. SELECT projection → `Map` + `Project`
5. DISTINCT/REDUCED → `Distinct`
6. ORDER BY + LIMIT/OFFSET → `TopK`

**SELECT projection** (`plan_select_projection`):
- `SELECT *` → identity (keep all in-scope variables)
- `SELECT ?v1 ?v2` → `Project` to the named columns in the specified order
- `SELECT (expr AS ?v)` → `Map` to add computed column, then `Project`
- Variables in SELECT must be in scope (error if not)

**GROUP BY + aggregates** (`plan_group_by_and_aggregates`):
- GROUP BY variables become the `group_key` in `Reduce`
- Aggregate expressions in SELECT are extracted and become `aggregates` in `Reduce`
- No GROUP BY + aggregates → single-group aggregation (empty group_key)
- Post-Reduce var_map maps group key variables (first) and aggregate aliases (after)

**Aggregate translation** (`translate_aggregate`):
- `COUNT(*)` → `AggregateFunc::Count` with literal true input
- `COUNT(expr)` / `COUNT(DISTINCT expr)` → `AggregateFunc::Count`
- `SUM(expr)` → cast to float64, then `AggregateFunc::SumFloat64`
- `MIN(expr)` → `AggregateFunc::MinString` (string comparison)
- `MAX(expr)` → `AggregateFunc::MaxString`
- `GROUP_CONCAT(expr)` → `AggregateFunc::StringAgg` (separator handling TBD)
- `SAMPLE(expr)` → approximated with `MinString` (arbitrary value)
- `AVG(expr)` → deferred (requires SUM/COUNT decomposition)

**DISTINCT/REDUCED** — Both map to `HirRelationExpr::Distinct`. REDUCED
technically allows but doesn't require dedup; we always dedup for correctness.

**ORDER BY + LIMIT/OFFSET** (`plan_order_limit`):
- ORDER BY variables → `ColumnOrder` with `desc` and `nulls_last` flags
- LIMIT → `TopK.limit` (Some(literal_int64))
- OFFSET → `TopK.offset` (literal_int64, default 0)
- No modifiers → no TopK wrapper (short-circuit)
- ORDER BY expressions (non-variable) deferred with clear error

**mz-sql change**: Added `AggregateFunc` to the public re-exports in
`src/sql/src/plan.rs` (needed by downstream crates to construct `AggregateExpr`
values for `HirRelationExpr::Reduce`).

### Key decisions

1. **Pipeline ordering matches SPARQL spec**: The SPARQL 1.1 spec defines a
   strict evaluation order: pattern → group → aggregation → having → projection
   → distinct → order → limit/offset. Our pipeline follows this exactly.

2. **Aggregates detected in SELECT**: Rather than requiring explicit declaration,
   the planner scans SELECT expressions for aggregate function calls using
   `has_aggregates_in_select()` / `expr_has_aggregate()`. If any aggregate is
   found, the query goes through the GROUP BY + aggregation path.

3. **String-typed aggregates**: Since all quad table columns are TEXT, we use
   `MinString`/`MaxString` for MIN/MAX and `SumFloat64` (with cast) for SUM.
   This is correct for the current encoding but may need refinement when typed
   literal extraction is added (prompt 17).

4. **AVG deferred**: HIR doesn't have a direct AVG aggregate. The correct
   implementation is SUM/COUNT decomposition, but this requires careful handling
   of NULL semantics and type casting. Deferred to avoid complexity.

5. **TopK for all solution modifiers**: Even ORDER BY without LIMIT uses TopK
   (with limit=None). This is how Materialize handles ordering generally — the
   optimizer can later optimize away the TopK if not needed.

6. **REDUCED = DISTINCT**: The SPARQL spec says REDUCED "permits" but doesn't
   require duplicate elimination. We always eliminate for correctness and
   simplicity.

### Test results

84 tests pass (60 old + 24 new), 0 failures, 1 warning (unused QUAD_ARITY constant).

New tests (24):
- SELECT projection: star, specific variables, single variable, expression (UCASE AS),
  variable ordering preserved, variable not in scope (error)
- DISTINCT: simple, REDUCED (treated as DISTINCT)
- LIMIT: simple, with offset, combined limit+offset
- ORDER BY: single (ASC default), DESC, multiple, with LIMIT
- DISTINCT + ORDER BY: verifies TopK(Distinct(...)) nesting
- Aggregates: COUNT(*), COUNT(DISTINCT), GROUP BY + COUNT, GROUP BY multiple keys,
  MIN/MAX, SUM, full aggregation query (GROUP BY + COUNT + ORDER BY DESC + LIMIT)
- Edge cases: no modifiers → no TopK wrapper

## 2026-03-22: Prompt 11 — Plan CONSTRUCT, ASK, DESCRIBE

### What was done

Extended the `plan()` method in `src/sparql/src/plan.rs` to handle the three
non-SELECT query forms, replacing the catch-all `_ =>` branch with specific
implementations.

**CONSTRUCT** (`plan_construct`):
- For each template triple pattern, creates a `Map` node that produces 3 columns
  (subject, predicate, object) by translating variables to column references and
  concrete terms (IRIs, literals, blank nodes) to string literals.
- Projects to just the 3 new columns (dropping the WHERE clause columns).
- Unions all template triple relations together.
- Wraps in `Distinct` (CONSTRUCT deduplicates per the SPARQL spec).
- Output var_map: `{subject: 0, predicate: 1, object: 2}`.
- Empty template → empty constant with 3-column schema.

**ASK** (`plan_ask`):
- Wraps the WHERE clause result in `Reduce` with no group key and a single
  `COUNT(true)` aggregate → produces a single row with the count.
- Maps `count > 0` to a boolean.
- Projects to just the boolean column.
- Output var_map: `{result: 0}`.
- Solution modifiers (ORDER BY, LIMIT, OFFSET) are not applied — ASK always
  returns a single boolean.

**DESCRIBE** (`plan_describe`):
- For each described IRI resource, generates two scans of the quad table:
  one filtered on `subject = iri` and one on `object = iri`, each projecting
  to (subject, predicate, object).
- Unions all parts and wraps in `Distinct`.
- Output var_map: `{subject: 0, predicate: 1, object: 2}`.
- Currently only supports concrete IRI resources. DESCRIBE with variables
  (which would require correlating with the WHERE clause) returns a descriptive
  error.

**Helper methods added**:
- `var_or_term_to_scalar`: translates `VarOrTerm` to `HirScalarExpr` (variable →
  column ref, term → string literal) for CONSTRUCT template mapping.
- `verb_path_to_scalar`: translates `VerbPath` to `HirScalarExpr` for predicate
  position in templates. Only supports simple IRIs and variables (property paths
  in CONSTRUCT templates are not allowed per the SPARQL spec).
- `plan_describe_direction`: generates a scan for triples where a resource appears
  in subject or object position.

### Key decisions

1. **CONSTRUCT output schema**: Fixed 3-column schema `(subject, predicate, object)`
   rather than using variable names from the template. This matches the standard
   SPARQL CONSTRUCT semantics where the output is a set of triples, not a solution
   set.

2. **ASK uses COUNT + GT**: Rather than using EXISTS/limit-1 pattern, we use
   `Reduce(count) > 0` which is straightforward and correct. The optimizer can
   potentially simplify this.

3. **DESCRIBE is simplified CBD**: We use a basic "find all triples where resource
   is subject or object" strategy rather than full Concise Bounded Description
   (which would recursively follow blank nodes). This is sufficient for a first
   implementation and matches what many SPARQL endpoints do.

4. **DESCRIBE with variables deferred**: Supporting `DESCRIBE ?x WHERE { ... }`
   requires correlating the variable bindings from the WHERE clause with the
   describe scan — essentially a dependent join. This is deferred to a later
   enhancement.

5. **No solution modifiers on ASK**: ORDER BY/LIMIT/OFFSET are meaningless for
   ASK (which always returns one row), so they are not applied.

### Test results

93 tests pass (84 old + 9 new), 0 failures, 1 warning (unused QUAD_ARITY constant).

New tests (9):
- CONSTRUCT: basic (single template), multiple templates (union structure),
  concrete terms in template, empty template
- ASK: basic (verify Reduce+Map+Project structure), with FILTER
- DESCRIBE: single IRI (verify Union structure), multiple IRIs, variable (error)

## 2026-03-22: Prompt 12 — Plan property paths with LetRec

### What was done

Extended the SPARQL planner in `src/sparql/src/plan.rs` to compile all property
path forms into HIR. Added a `Cell<u64>` counter to `SparqlPlanner` for
allocating unique `LocalId`s needed by `LetRec` bindings.

**Infrastructure changes**:
- Added `next_local_id: Cell<u64>` to `SparqlPlanner` for `LocalId` allocation.
- Added `alloc_local_id()` helper and `pp_relation_type()` for the 2-column
  `(start, end)` type shared by all property path results.
- Modified `plan_triple()` to delegate non-simple property paths to a new
  `plan_triple_with_property_path()` method instead of returning an error.

**`plan_triple_with_property_path()`** — integrates a property path result with
the triple pattern's subject and object:
1. Plans the property path → 2-column `(start=0, end=1)` relation.
2. Applies filters for concrete subject/object terms.
3. Adds equality filters for repeated variables (e.g., `?x path ?x`).
4. Projects to only the columns that have variable bindings.

**`plan_property_path()`** — dispatches on `PropertyPath` variant:

- **`Iri`** (`plan_pp_iri`): Scan quad table, filter `predicate = IRI`, project
  to `(subject, object)`. Same as the existing simple-IRI case but returns a
  standalone 2-column relation.

- **`Inverse`** (`plan_pp_inverse`): Plan inner path, swap columns via
  `Project [1, 0]`.

- **`Sequence`** (`plan_pp_sequence`): Chain joins through intermediate nodes.
  For `p1/p2`: join `p1(start, mid)` with `p2(mid, end)` on `p1.end = p2.start`,
  project to `(p1.start, p2.end)`. Generalizes to N-step chains.

- **`Alternative`** (`plan_pp_alternative`): Union of all alternatives.

- **`OneOrMore`** (`plan_pp_one_or_more`): Transitive closure via `LetRec`.
  ```
  LetRec {
      bindings: [("path_plus", id,
          Distinct(
              plan_path(inner)                    -- base: one step
              UNION
              Join(Get(id), plan_path(inner),     -- extend: one more step
                   on: Get.end = step.start)
              → Project(Get.start, step.end)
          )
      )],
      body: Get(id)
  }
  ```
  The `Distinct` inside the binding is critical to ensure convergence — without
  it, the recursive union would grow without bound.

- **`ZeroOrMore`** (`plan_pp_zero_or_more`): `path+ UNION identity`.

- **`ZeroOrOne`** (`plan_pp_zero_or_one`): `path UNION identity`.

- **`NegatedSet`** (`plan_pp_negated_set`): Handles both forward and inverse
  exclusions. Forward IRIs: scan quads where predicate NOT IN forward set,
  project `(subject, object)`. Inverse IRIs: scan quads where predicate NOT IN
  inverse set, project `(object, subject)` [swapped]. Union both parts.

**Identity relation** (`plan_pp_identity`): All distinct nodes in the graph
paired with themselves. Computed as `Distinct(subjects UNION objects)` where
each side projects the same quad column to both start and end.

### Key decisions

1. **2-column property path abstraction**: All property path forms produce a
   uniform 2-column `(start, end)` relation. This clean abstraction makes
   composition trivial (sequence = join on end/start, alternative = union,
   inverse = swap) and integration with triple patterns straightforward.

2. **LetRec for path+ (not path*)**: The recursive `LetRec` implements `path+`
   (one-or-more). `path*` is then `path+ UNION identity`. This avoids having
   the identity pairs pollute the recursive computation — they're added once
   at the end.

3. **Distinct inside LetRec binding**: The binding value includes `Distinct` to
   ensure the recursive computation converges. Without deduplication, each
   iteration would re-discover already-known paths, preventing fixpoint.

4. **No iteration limit**: `LetRec { limit: None, ... }` allows the recursion
   to run until fixpoint. This matches `WITH MUTUALLY RECURSIVE` semantics in
   Materialize. Users can add a LIMIT on the outer query to bound results.

5. **Negated set: separate forward/inverse handling**: Forward exclusions filter
   on predicate and project `(subject, object)`. Inverse exclusions filter on
   predicate and project `(object, subject)`. The union of both correctly
   handles mixed negated sets like `!(ex:knows | ^ex:hates)`.

6. **Identity via quad table scan**: The identity relation for `path*`/`path?`
   extracts all distinct nodes from the quad table (subjects UNION objects, each
   duplicated as both start and end). This ensures that `?x path* ?x` includes
   all nodes even if the path never matches.

### Test results

107 tests pass (93 old + 14 new), 0 failures, 0 warnings.

New tests (14):
- Inverse path: `^ex:knows`
- Sequence: two-step (`ex:knows/ex:name`), three-step (`ex:a/ex:b/ex:c`)
- Alternative: `ex:knows | ex:friendOf`
- OneOrMore: `ex:knows+` (verifies LetRec presence in HIR)
- ZeroOrMore: `ex:knows*`
- ZeroOrOne: `ex:knows?`
- Negated set: forward only (`!(ex:knows|ex:hates)`), mixed forward+inverse
  (`!(ex:knows|^ex:hates)`)
- Concrete subject with path+: `<alice> ex:knows+ ?friend`
- Same variable subject/object: `?x ex:knows+ ?x` (reflexive closure)
- Inverse of sequence: `^(ex:a/ex:b)`
- Property path joined with BGP: `?s ex:knows+ ?friend . ?friend ex:name ?name`
- RDFS subClassOf+: `?x rdfs:subClassOf+ ?y`

## 2026-03-23: Prompt 13 — Wire up `SPARQL $$ ... $$` syntax in the SQL parser

### What was done

Implemented end-to-end integration of `SPARQL $$ ... $$` as a SQL statement:

**SQL parser** (`src/sql-parser/`):
- Added `SPARQL` keyword to the SQL lexer (`keywords.txt`).
- Added `Statement::Sparql(SparqlStatement)` to the SQL AST with `SparqlStatement { body: String }`.
- `parse_sparql()` method reads a dollar-quoted `Token::String` after the `SPARQL` keyword.

**SQL planner** (`src/sql/src/plan/`):
- `plan_sparql()` in `dml.rs`: parses SPARQL body, resolves `rdf_quads` quad table from catalog,
  builds output `RelationDesc` based on query form, returns `Plan::Sparql(SparqlPlan)`.
- `describe_sparql()`: parses SPARQL to determine output schema for pgwire description.
- `sparql_desc()` / `sparql_query_desc()`: helper functions to build `RelationDesc` from SPARQL
  query forms (SELECT → variable names, CONSTRUCT/DESCRIBE → subject/predicate/object, ASK → result).
- Added `Plan::Sparql(SparqlPlan)` variant with `SparqlPlan { query, quad_table_id, desc }`.

**Adapter** (`src/adapter/`):
- `sequence_sparql()` in `sequencer.rs`: calls `mz_sparql::plan::SparqlPlanner::new(quad_table_id)`
  to compile SPARQL → HIR, wraps in `SelectPlan`, sequences as a peek.
- Updated `command.rs`, `command_handler.rs`, `appends.rs`, `catalog_serving.rs`, `rbac.rs` for
  the new statement/plan type.

### Key decisions

1. **Two-phase plan**: The SQL planner produces `Plan::Sparql` containing the parsed SPARQL AST
   and quad table ID. The adapter coordinator then calls the SPARQL planner to produce HIR.
   This avoids a cyclic dependency (mz-sparql depends on mz-sql for HIR types, so mz-sql
   cannot depend on mz-sparql).

2. **Quad table resolution**: The planner resolves `rdf_quads` from the catalog by name. Users
   must create this table before running SPARQL queries. Future work (Prompt 16) will add
   built-in catalog-as-RDF views.

## 2026-03-23: Prompt 14 — SUBSCRIBE integration for SPARQL queries

### What was done

Extended `SUBSCRIBE` to accept SPARQL queries: `SUBSCRIBE TO SPARQL $$ ... $$`.

**SQL parser**:
- Added `Sparql(SparqlStatement)` variant to `SubscribeRelation<T>` enum.
- Updated `parse_subscribe()` to detect the `SPARQL` keyword after `SUBSCRIBE [TO]` and
  parse the dollar-quoted body.
- Updated `AstDisplay` for `SubscribeRelation` and `sql-pretty` for the new variant.

**SQL planner** (`plan_subscribe` in `dml.rs`):
- Added `SubscribeRelation::Sparql` arm in both `describe_subscribe` and `plan_subscribe`.
- `plan_sparql_subscribe()`: parses SPARQL, resolves quad table, builds desc and scope,
  returns `SubscribeFrom::Sparql { query, quad_table_id, desc }`.
- Factored out `resolve_quad_table()` helper shared with `plan_sparql()`.

**Plan types** (`plan.rs`):
- Added `SubscribeFrom::Sparql { query, quad_table_id, desc }` variant with appropriate
  `depends_on()` and `contains_temporal()` implementations.

**Adapter** (`subscribe.rs`):
- In `subscribe_optimize_mir()`, SPARQL subscribes are compiled to HIR → MIR before the
  optimizer runs. The conversion happens in the coordinator (which has access to `SystemVars`
  for lowering config), then replaces `SubscribeFrom::Sparql` with `SubscribeFrom::Query`.
  After that, the standard subscribe optimization pipeline handles it.
- Updated `catalog_serving.rs` for the new `SubscribeFrom` variant in both match sites.
- Updated `optimize/subscribe.rs` with an `unreachable!()` arm (SPARQL is always resolved
  before reaching the optimizer).

### Key decisions

1. **Deferred compilation**: SPARQL → HIR → MIR compilation happens in `subscribe_optimize_mir()`
   rather than in the SQL planner, to avoid the cyclic dependency between `mz-sql` and `mz-sparql`.
   The `SubscribeFrom::Sparql` variant carries the parsed query through the plan layer.

2. **Reuse of subscribe machinery**: After SPARQL is compiled to MIR, it becomes a standard
   `SubscribeFrom::Query` and flows through the existing subscribe optimizer and sequencer
   unchanged. No new subscribe-specific code paths needed.

3. **All subscribe options supported**: AS OF, UP TO, WITH (SNAPSHOT, PROGRESS), and envelope
   options all work with SPARQL subscribes since they operate on the `SubscribePlan` wrapper.

## 2026-03-23: Prompt 15 — CREATE [MATERIALIZED] VIEW from SPARQL

### What was done

Implemented `CREATE VIEW name AS SPARQL $$ ... $$` and
`CREATE MATERIALIZED VIEW name AS SPARQL $$ ... $$` end-to-end.

**SQL parser** (`src/sql-parser/`):
- Extended `parse_view_definition()` to detect `AS SPARQL` after the view name
  and column list. When found, parses the dollar-quoted SPARQL body as a
  `SparqlStatement` and sets it in the new `sparql: Option<SparqlStatement>`
  field on `ViewDefinition<T>`. A dummy `Query` placeholder occupies the
  existing `query` field.
- Extended `parse_create_materialized_view()` with the same `AS SPARQL` detection
  after the `AS` keyword, setting `sparql: Option<SparqlStatement>` on
  `CreateMaterializedViewStatement<T>`.
- Added `dummy_query()` helper that creates a minimal `Query<Raw>` with empty
  VALUES — used as the placeholder `query` field for SPARQL views.
- Updated `AstDisplay` for both types to render `SPARQL $$body$$` instead of
  the query when `sparql` is `Some`.

**Normalization and transform** (`src/sql/src/normalize.rs`, `src/sql/src/ast/transform.rs`):
- When `sparql.is_some()`, skip `QueryNormalizer::visit_query_mut()` and
  `rewrite_query()` — the `query` field is a dummy that should not be touched.

**sql-pretty** (`src/sql-pretty/src/doc.rs`):
- Updated `doc_view_definition()` and `doc_create_materialized_view()` to
  render SPARQL syntax when the `sparql` field is present.

**Plan types** (`src/sql/src/plan.rs`):
- Added `SparqlViewInfo { query, quad_table_id }` struct.
- Added `sparql_info: Option<SparqlViewInfo>` to both `CreateViewPlan` and
  `CreateMaterializedViewPlan`.

**DDL planner** (`src/sql/src/plan/statement/ddl.rs`):
- Modified `plan_view()` to return `(name, view, Option<SparqlViewInfo>)`.
  When SPARQL is detected: resolves the `rdf_quads` quad table from the catalog,
  parses the SPARQL body, builds the output `RelationDesc`, and creates a
  placeholder `HirRelationExpr::Get` on the quad table (ensuring `depends_on()`
  returns the correct dependency set for timeline validation).
- Modified `plan_create_materialized_view()` with the same SPARQL detection
  and placeholder HIR construction.
- Made `dml::resolve_quad_table()` and `dml::sparql_query_desc()` `pub(crate)`
  so they can be called from DDL planning.

**Adapter** (`src/adapter/src/coord/sequencer/inner/`):
- In `create_view_validate()`: when `sparql_info` is present, compiles
  SPARQL → HIR using `mz_sparql::plan::SparqlPlanner` and replaces the
  placeholder `view.expr` before timeline validation and optimization.
- In `create_materialized_view_validate()`: same approach — compiles SPARQL
  to HIR and replaces `materialized_view.expr`.

**Other files updated for new struct fields**:
- `src/sql/src/rbac.rs` — added `sparql_info: _` to plan destructuring.
- `src/catalog/src/memory/objects.rs` — added `sparql` field to
  `CreateMaterializedViewStatement` construction.
- `src/adapter/src/coord/command_handler.rs` — same.
- `src/adapter/src/catalog/migrate.rs` — added `sparql: None` to
  `ViewDefinition` construction.

### Key decisions

1. **Deferred SPARQL compilation**: Like the SUBSCRIBE path, SPARQL → HIR
   compilation happens in the adapter's validate step, not the SQL planner.
   This avoids the cyclic dependency between `mz-sql` and `mz-sparql`. The
   `SparqlViewInfo` struct carries the parsed query through the plan layer.

2. **Placeholder HIR via Get**: The placeholder `HirRelationExpr::Get` on the
   quad table ensures that `depends_on()` returns the correct dependency set
   ({quad_table_id}). This means timeline validation in the validate step works
   correctly even before SPARQL compilation replaces the placeholder.

3. **Optional sparql field**: Rather than creating new AST enum variants (which
   would require many changes across the codebase), we added an optional
   `sparql: Option<SparqlStatement>` field to existing types. When `Some`, the
   `query` field is a dummy placeholder. All code paths that touch `query`
   check `sparql` first and skip query processing when SPARQL is active.

4. **Round-trip correctness**: The `AstDisplay` implementations render
   `AS SPARQL $$body$$` when sparql is set. This means `create_sql`
   normalization produces the correct durable SQL string, and re-parsing that
   string recreates the SPARQL view definition (tested by the parser
   round-trip tests).

5. **Output schema**: Uses the same `sparql_query_desc()` helper as SPARQL
   SELECT — variable names become column names for SELECT views,
   (subject, predicate, object) for CONSTRUCT/DESCRIBE, and (result) for ASK.

6. **All view options work**: OR REPLACE, IF NOT EXISTS, column renaming,
   WITH options (for materialized views), IN CLUSTER, etc. all work because
   the SPARQL path feeds into the same plan creation logic — only the HIR
   expression source differs.

### Test results

All existing tests pass. 4 new parser test cases added to
`src/sql-parser/tests/testdata/ddl`:
- CREATE VIEW ... AS SPARQL (SELECT form)
- CREATE MATERIALIZED VIEW ... AS SPARQL (SELECT form)
- CREATE VIEW ... AS SPARQL (CONSTRUCT form)
- CREATE OR REPLACE VIEW ... AS SPARQL

Full test counts:
- mz-sparql-parser: 154 pass, 0 fail
- mz-sparql: 107 pass, 0 fail
- mz-sql-parser: all pass (existing test expectations updated for new `sparql: None` field)

## 2026-03-23: Prompt 16 — Implement catalog-as-RDF named graph

### What was done

Implemented the catalog-as-RDF feature with three components: a built-in view
exposing catalog metadata as RDF triples, FROM clause parsing in the SPARQL
parser, and planner integration that automatically uses the catalog view for
`FROM <urn:materialize:catalog>`.

**Built-in view** (`mz_internal.mz_rdf_catalog_triples`):
- Defined in `src/catalog/src/builtin.rs` as a `BuiltinView` with schema
  `(subject TEXT, predicate TEXT, object TEXT, graph TEXT)`.
- SQL is a large UNION ALL over catalog system tables, mapping each object to
  the ontology defined in the design doc (`urn:materialize:catalog:` prefix):
  - **Databases**: `rdf:type mz:Database`, `mz:name`
  - **Schemas**: `rdf:type mz:Schema`, `mz:name`, `mz:inDatabase`
  - **Tables**: `rdf:type mz:Table`, `mz:name`, `mz:inSchema`
  - **Columns**: `rdf:type mz:Column`, `mz:columnName`, `mz:columnType`,
    `mz:ordinalPosition`, parent `mz:hasColumn`
  - **Views**: `rdf:type mz:View`, `mz:name`, `mz:inSchema`
  - **Materialized views**: `rdf:type mz:MaterializedView`, `mz:name`,
    `mz:inSchema`, `mz:inCluster`
  - **Sources**: `rdf:type mz:Source`, `mz:name`, `mz:inSchema`, `mz:sourceType`
  - **Sinks**: `rdf:type mz:Sink`, `mz:name`, `mz:inSchema`
  - **Clusters**: `rdf:type mz:Cluster`, `mz:name`
  - **Indexes**: `rdf:type mz:Index`, `mz:name`, `mz:onRelation`, `mz:inCluster`
- Registered in `BUILTINS_STATIC` with OID 17069.
- All graph column values are `'urn:materialize:catalog'`.

**SPARQL parser — FROM/FROM NAMED clauses**:
- Added `DatasetClause { iri: Iri, named: bool }` to the AST.
- Added `from: Vec<DatasetClause>` field to `SparqlQuery`.
- `parse_dataset_clauses()` method: consumes `FROM [NAMED] <iri>` clauses
  between the query form clause and the WHERE clause. Called for all query
  forms (SELECT, CONSTRUCT, ASK, DESCRIBE) and subqueries (empty for subqueries).
- Parser correctly handles prefixed names in FROM clauses (e.g., `FROM g:main`).

**Planner integration**:
- `SparqlPlanner` now takes `catalog_triples_id: Option<GlobalId>` in addition
  to `quad_table_id`. The `quad_table_id` is a `Cell<GlobalId>` to allow
  temporary override during planning.
- `effective_quad_table_id()`: when FROM `<urn:materialize:catalog>` is the sole
  default graph and the catalog triples view is available, returns its ID instead
  of the default quad table.
- `from_graph_filter()`: for non-catalog FROM graphs, builds a filter expression
  on the graph column (`graph = 'g1' OR graph = 'g2' OR ...`). No filter is
  generated when the catalog view is used directly (already scoped).
- In `plan()`, the effective quad table is set before planning the WHERE clause,
  then restored afterward. The graph filter is applied after WHERE planning.

**SQL planner / adapter updates**:
- `resolve_catalog_triples()` helper in `dml.rs`: resolves
  `mz_internal.mz_rdf_catalog_triples` from the catalog, returning `None` if
  unavailable (e.g., in tests).
- `SparqlPlan`, `SparqlViewInfo`, and `SubscribeFrom::Sparql` all gained a
  `catalog_triples_id: Option<GlobalId>` field.
- All 5 adapter call sites (`sequence_sparql`, subscribe, create_view,
  create_materialized_view, catalog_serving) updated to pass the catalog
  triples ID to `SparqlPlanner::new()`.
- `depends_on()` for `SubscribeFrom::Sparql` now includes the catalog triples
  ID in its dependency set.

### Key decisions

1. **Single built-in view, not a table**: The catalog triples are a SQL view
   (not a table) — the data is always up-to-date with the live catalog. This
   matches how `mz_relations` and `mz_objects` are implemented.

2. **FROM clause → quad table override (not filter) for catalog**: When
   `FROM <urn:materialize:catalog>` is specified, the planner scans the catalog
   triples view directly rather than filtering `rdf_quads` on the graph column.
   This is more efficient (avoids scanning the entire quad table) and works even
   when `rdf_quads` doesn't include catalog triples.

3. **Cell<GlobalId> for quad_table_id**: Since `plan()` takes `&self`, we use
   `Cell` to temporarily override the quad table ID during planning. The original
   ID is restored after the WHERE clause is planned, ensuring clean state.

4. **FROM NAMED parsed but not yet acted on**: The parser stores `named: bool`
   on each dataset clause, but the planner currently only processes default graph
   clauses (non-named). GRAPH pattern planning (which would use FROM NAMED) is
   deferred to a later prompt.

5. **Multiple FROM = graph column filter**: When multiple default graphs are
   specified (e.g., `FROM <g1> FROM <g2>`), the planner adds an OR filter on
   the graph column. This implements the SPARQL spec's RDF merge semantics.

### Future work identified

- **HIR extraction to separate crate**: SPARQL views need the same HIR/MIR/
  optimization abstractions as SQL views. Extracting HIR to a separate crate
  would enable this without the current cyclic dependency workaround
  (deferred compilation in the adapter).
- **sqllogictests for SPARQL**: Need to add end-to-end tests using the
  sqllogictest framework for the full SPARQL pipeline.

### Test results

- mz-sparql-parser: 163 tests pass (154 old + 9 new FROM clause tests), 0 failures
- mz-sparql: 112 tests pass (107 old + 5 new FROM/catalog tests), 0 failures
- All crates compile cleanly (mz-sparql-parser, mz-sparql, mz-sql, mz-adapter)

New parser tests (9):
- FROM: single default graph, multiple default graphs, FROM NAMED, mixed
  default+named, no clauses, with CONSTRUCT, with ASK, with DESCRIBE, with prefix

New planner tests (5):
- No FROM clauses (default behavior)
- FROM non-catalog graph (verifies Filter in HIR)
- FROM catalog graph with catalog view ID (verifies User(2) in HIR)
- FROM catalog graph without catalog view (fallback to Filter)
- Multiple FROM graphs (verifies Filter in HIR)

## 2026-03-23: Prompt 17 — SPARQL expression edge cases and three-valued logic

### What was done

Implemented three key improvements to SPARQL expression evaluation in
`src/sparql/src/plan.rs`:

**1. Three-valued logic (error suppression in FILTER)**

Modified `apply_filter()` to wrap every FILTER predicate in
`COALESCE(pred, false)`. This implements SPARQL's three-valued logic where
an error in a FILTER expression evaluates to `false` (row is filtered out)
rather than propagating the error. For example, comparing a string to a
number with `<` would raise a type error in SQL, but in SPARQL it should
silently filter the row.

**2. Effective Boolean Value (EBV) conversion**

Added `is_boolean_expression()` and `to_ebv()` methods. When a FILTER
contains a non-boolean expression (e.g., `FILTER(?label)` to filter empty
strings), EBV conversion is applied per SPARQL 1.1 Section 17.2.2:
- Numeric values: true if non-zero (cast to float64, compare != 0)
- String values: true if non-null and non-empty (char_length > 0)
- Boolean expressions (comparisons, logical ops, BOUND, etc.): pass through

The `is_boolean_expression()` helper statically determines which SPARQL
expression types produce boolean results (23 variants), avoiding unnecessary
EBV conversion for the common case.

**3. Numeric-aware comparison operators**

Replaced all 6 comparison operators (=, !=, <, >, <=, >=) with a new
`translate_comparison()` method that produces:
```
IF(is_numeric(left) AND is_numeric(right),
   func(cast_f64(left), cast_f64(right)),  -- numeric comparison
   func(left, right))                       -- string comparison
```

This correctly handles:
- `9 < 10` (numeric: 9 < 10, not string: "9" > "10")
- `42 = 42.0` (numeric equality despite different string forms)
- `"alice" < "bob"` (string comparison for non-numeric values)

The `is_numeric_check()` helper uses the same regex pattern as `isNumeric()`:
`^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$`.

**4. Improved accessor functions (STR, LANG, DATATYPE)**

Replaced stub implementations of LANG() and DATATYPE() with working
position-based string extraction:
- `LANG()`: finds `"@` marker via Position, extracts language tag with Substr
- `DATATYPE()`: finds `^^<` marker via Position, extracts datatype IRI with
  Substr, falls back to xsd:string
- `STR()`: kept as identity (correct for IRIs and simple strings; full
  typed/tagged literal extraction is a future enhancement)

**5. Removed `translate_binary` helper**

The `translate_binary()` helper was replaced by `translate_comparison()` for
all comparison operators. Since no other callers remained, it was removed.

### Key decisions

1. **COALESCE at FILTER level, not expression level**: Error suppression is
   applied once per FILTER, wrapping the entire predicate. This is more
   efficient than wrapping every sub-expression and matches the SPARQL spec
   where errors only propagate to the FILTER boundary.

2. **Static boolean detection**: Rather than always applying EBV conversion,
   we statically check the SPARQL expression type. This avoids unnecessary
   overhead for the 95%+ case where FILTER expressions are already boolean.

3. **Numeric detection at runtime**: The `is_numeric_check` uses regex
   matching at runtime, not static type analysis. This is correct because
   SPARQL variables can hold any RDF value, and the same variable might be
   numeric in some rows and non-numeric in others.

4. **xsd:dateTime via string comparison**: ISO 8601 dates sort correctly
   as strings due to zero-padded formatting, so the string comparison
   fallback in `translate_comparison` handles dateTime comparison for the
   common case. Known limitation: timezone normalization (e.g.,
   "2023-01-15T10:30:00Z" vs "2023-01-15T05:30:00-05:00") is not handled.

5. **OPTIONAL filters not wrapped**: OPTIONAL's inner FILTER expressions go
   into the LEFT OUTER JOIN ON clause, where NULL/error conditions already
   produce the correct SPARQL OPTIONAL semantics (unmatched row with NULLs).
   No additional COALESCE wrapping needed.

6. **Position-based extraction for LANG/DATATYPE**: Used Position + Substr
   instead of regex capture groups since Materialize's Replace function does
   simple substring replacement, not regex replacement.

### Test results

124 tests pass (112 old + 12 new), 0 failures, 1 warning (unused QUAD_ARITY).

12 existing tests updated to account for new COALESCE wrappers and
numeric-aware comparison structure (using new helpers: `unwrap_coalesce`,
`unwrap_numeric_comparison`, `get_filter_predicate`).

New tests (12):
- Three-valued logic: COALESCE wrapper presence, multiple filters both wrapped
- Numeric-aware comparison: structure verification (IF/then/else), equality,
  all 6 operators, is_numeric regex check presence
- EBV: non-boolean filter gets conversion, boolean filter skips conversion
- Accessors: LANG uses Position, DATATYPE uses Position + xsd:string fallback
- Language tag equality: different tags produce different strings
- Arithmetic + comparison: combined expression planning
