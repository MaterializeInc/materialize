# Ontology explorer

- Associated: MaterializeInc/materialize#37439 (the ontology table pattern)

## The Problem

Materialize already describes two ontologies, and neither is visible in the
Console.

The first is the user's. The documented ontology table pattern
(`/docs/transform-data/patterns/ontology/`) has users curate the join
relationships between their relations in a table, so agents connected over
MCP write correct joins. The live context graph pattern then has users
compose data products over those relations. Nothing shows the result, and
nothing checks it: a row that names a relation that does not exist, or a
column that was renamed, misleads every agent that reads it.

Most environments will never adopt the pattern, though. Their views already
encode how their relations join, and their column names often say what they
reference, so a graph that only drew declared joins would be empty exactly
where it could help most.

The second is the system catalog's. `mz_internal.mz_ontology_*` describes
every catalog relation, how relations join, and which id domain each column
holds. The MCP developer server tells agents to read it first. People have
no way to browse it.

## Success Criteria

- A user can see their relations, the joins their ontology tables declare,
  and the lineage between relations, as one graph.
- An environment with no ontology table still gets a useful graph, built
  from the joins its views make, and each join says where it came from.
- Declared joins that the views contradict, and joins that cannot use the
  relation's indexes, are flagged.
- Every ontology table row is checked against the catalog, and broken rows
  are shown, not dropped.
- The graph is live: a row inserted into an ontology table, a new view, or a
  freshness change shows up without a reload.
- A user can see which relations agents discover as data products, and which
  of those lack the comments agents rely on.
- A user can browse the system catalog ontology, including semantic types.
- The page adds bounded, predictable load to `mz_catalog_server`.

## Out of Scope

- Editing ontology tables from the Console. Users own these tables and write
  them with SQL. The explorer reads them, and for an undeclared join it writes
  the INSERT for the user to run in the SQL Shell.
- Informational key constraints (`FOREIGN KEY ... NOT ENFORCED`). They would
  give joins a first-class home in the catalog, but need their own design and
  change the database. This page changes only the Console.
- A business glossary as a catalog concept. The ontology is whatever the
  user's ontology tables and comments say.
- RDF or OWL export. The export is JSON and SVG.

## Solution Proposal

A page at `/regions/:region/ontology`, behind the `ontology-explorer` flag,
with two tabs that share one explorer.

### The model

Both tabs build an `OntologyGraph` (`platform/ontology/model.ts`): entities,
typed relationships and issues. The canvas, filters and side panel only see
this shape.

- **Data products** (`dataProductGraph.ts`). Entities are user tables, views,
  materialized views and sources. Relationships are inferred from the
  environment's definitions (below), and declared joins come from one
  ontology table the user picks. The ontology table is a userspace
  convention, not a catalog concept, so inferred joins are the default
  reading and declared joins an overlay to check against them. A toolbar box
  fuzzy-finds the table, defaulting to `public.ontology`, kept in the URL as
  `?ontology=`. Relations with `table_name text`, `columns text[]`,
  `referenced_table text` and `referenced_columns text[]` rank first, and a
  pick without those columns is explained rather than read. Names in its
  rows resolve relative to the table's schema, then its database, then
  anywhere they are unique. Each row is checked for unknown or
  ambiguous relations, missing columns, mismatched column counts and join
  columns of incompatible types. Lineage comes from `mz_object_graph_edges`.
  Entities listed by `mz_mcp_data_products` are marked, since that view is
  what the agent MCP server lists.
- **Inferred relationships** (`viewJoins.ts`, `inferredJoins.ts`). They come
  only from definitions and index keys, never from declared joins, so they
  stand alone as an independent reading. They follow the rules of
  `mz_internal.mz_mcp_relationships` in the catalog, and are all drawn as
  inferred joins:
  - A join: two relations' columns a view equates, in `ON`, `WHERE`,
    `USING` or `IN (SELECT ...)`. The direction points at a column that
    alone keys an index, then a column named `id`, then away from a column
    named `*_id`. When nothing settles it, the edge says it is arbitrary.
  - A passed key: a view's output column that is an input key, as in
    `"o"."id" AS "org_id"`, references that key.
  - A derived reference: a column holding the same values as a
    referencing column, through views passing other columns unchanged.
    When a view passes `bids.buyer` through as `winning_bids.buyer`, and
    another joins `winning_bids.buyer` to `users.id`, `bids.buyer`
    references `users.id` too, though no view joins the two.

  A passed column that is not a key only claims the same domain, not the
  same data, since a view may filter. On its own it relates nothing, so it
  is never drawn, only followed to derive references. The catalog
  maintains the direct edges and walks the chains on read: maintaining the
  chains cost 3 to 5.5 times as much per replaced view.

  On the auction demo, every declared join that resolves (11 of 11) is
  connected by a path of inferred joins, and the one declared on a missing
  column, `bids.bidder_id`, gets a warning naming `bids.buyer`, the column
  derived references join to `users.id`. A declared join is confirmed
  when such a path exists, and lists the views along it. An inferred join
  with exactly the declared columns names the ontology table. Each other
  inferred join offers the INSERT that declares it, creating the chosen
  table when it does not exist.
  Joins are read from `create_sql`, which the catalog prints from the
  resolved AST: relations appear as `[u4 AS "db"."schema"."name"]` with
  their catalog ids, and every identifier is quoted. That regular form lets
  a token scan stand in for a parser, with no name resolution in the
  browser. The scan errs toward missing a join: an alias bound to two
  relations, or to a subquery, CTE or table function, never resolves.
- **Warnings on joins.** Beyond the row checks above, a declared join gets a
  warning when inferred relationships relate the same two relations only on
  other columns, and
  any join gets one when its relation has an index covering the join's
  columns but keyed on more of them, and none keyed on exactly them
  (`indexFit.ts`). A join reuses an index only when the key is exactly the
  join's columns, in any order: observed on v26.50, an index on
  `(id, seller, item, end_time)` is read as a full scan by a join on `id`,
  and an index on `(id)` is not used by a join on `(id, seller)`. The
  default index of a relation with no known unique key is the usual cause.
- **Index suggestions.** Listed apart from issues, since nothing is wrong.
  Two cases get a suggested `CREATE INDEX`: columns that two or more views
  join a relation on, with no index covering them, and a materialized view
  listed as an MCP data product with no index. In the first case, each
  view's dataflow arranges the relation itself, and an index on exactly
  those columns lets work on its cluster share one arrangement. In the
  second, agents read the data product from storage. A join only one view
  makes gets no suggestion, since its dataflow arranges the relation once
  either way.
- **System catalog** (`catalogGraph.ts`). Entities, links and properties come
  from the `mz_ontology_*` views, with SQL types joined from `mz_columns`.

### Data and cost

Live data uses the sync engine's scoped collections, so rows survive
navigation within the session and each data set is one SUBSCRIBE:

| Data | Source | Cluster |
| --- | --- | --- |
| Relations | the app-wide `allObjects` SUBSCRIBE | `mz_catalog_server` |
| Lineage | `workflowGraphEdgesCollection` | `mz_catalog_server` |
| Ontology table candidates | SUBSCRIBE over `mz_columns`, `mz_objects`, `mz_indexes` | `mz_catalog_server` |
| Ontology rows | SUBSCRIBE over the chosen ontology table | the table's index cluster |
| Columns and comments | SUBSCRIBE scoped to the drawn schemas | `mz_catalog_server` |
| View definitions | SUBSCRIBE over `mz_views` and `mz_materialized_views` | `mz_catalog_server` |
| Index keys | SUBSCRIBE over `mz_indexes`, `mz_index_columns` and `mz_columns` | `mz_catalog_server` |
| Freshness | SUBSCRIBE over `mz_frontiers` and `mz_hydration_statuses`, scoped to the drawn schemas | `mz_catalog_server` |
| Data products | polled every 60s | `mz_catalog_server` |
| Catalog ontology | read once, it changes only on upgrade | `mz_catalog_server` |

Every catalog relation these read is indexed on `mz_catalog_server`.
Ontology tables are user objects, which `mz_catalog_server` cannot read, so
their rows are read on the cluster of an ontology table's index. The pattern
already tells users to index the table. Without an index the page says so
and shows no joins.

Above 150 relations, the page opens on the schemas the ontology table and
the joins touch, so the columns and freshness subscribes stay small.

A side panel tab streams a relation's rows with SUBSCRIBE on request. It is
not automatic, since it maintains a dataflow on the relation's cluster.

### Layout

Dagre ranks entities with referenced relations above the relations that
reference them, and boxes each schema. A hub such as the role every catalog
object references makes one rank too wide to read, so ranks wider than
`maxCardsPerRow` wrap into rows, and those layouts draw their own edges. A
circular layout is the alternative. The canvas fits the graph on load and
on every layout change.

Cards list their key columns by default, ER diagram style, so a join reads
as a line from one column's row to another's, without a text label. The
join kinds share a color, and the line says how sure the join is: solid
when declared, dashed when inferred.

Above 500 entities the canvas is not drawn, and the page asks for fewer
schemas or a focused entity instead: dagre's recursive ranking overflowed
the stack on a chain of about 3,000 views. A large environment opens on the
ontology table's schemas, or else the one schema with the most inferred
joins.

Hiding a relationship kind only stops drawing it. Every kind still shapes
the layout and decides which entities are isolated, so toggling between
declared and inferred joins moves no card.
Materialize declares no primary or foreign keys, so they are read from the
ontology: a column other relations join to is a key, recorded catalog
primary keys included, and a column that joins to another relation is a
foreign key. A column a relationship names but the relation lacks is drawn
struck through in red. Cards show four key rows and fold the rest. The View
menu turns key columns off for compact cards.

### Interactions

Selecting an entity or relationship opens it in the side panel and is kept
in the URL (`?node=`, `?edge=`, `?focus=`), so it can be linked. An entity
shows its description, columns, relationships and live rows. A relationship
shows its issues, its column pairing and the join it describes, which opens
in the SQL Shell. "Neighborhood" narrows the graph to one entity's
neighbors.

## Alternatives

- **A fixed ontology table name.** Recognizing the table by its columns
  instead lets users keep several, in any schema, under any name.
- **Resolving ontology rows in SQL.** A SUBSCRIBE that joined each ontology
  table to the catalog would have to run on a user cluster, because it reads
  user objects, and would grow with every check. Resolving in the browser
  keeps the user cluster's work to reading the rows, and the checks are
  plain functions with unit tests.
- **Suggesting joins from column names**, such as `customer_id` to
  `customers.id`. They are often right, but nothing in the environment
  confirms them, and a wrong edge drawn beside real ones misleads. Checking
  declared joins against the views and indexes is the better use of the
  graph.
- **Inferring joins with a real parser or the optimizer.** The
  `sql-parser-wasm` crate exposes statement kinds, not the AST, so using it
  means extending and republishing it. `EXPLAIN` plans carry resolved join
  keys but their JSON format is internal, and explaining every view costs a
  round trip each. A catalog relation of join keys, filled in at plan time,
  is the right long-term home and would replace the token scan.
- **Subscribing to `mz_mcp_data_products`.** It checks the role's privileges
  on every object and is not indexed, so it is polled.

## Open questions

- Whether the flag defaults on in self-managed deployments.
- Whether the object detail pages should link to the entity here.
