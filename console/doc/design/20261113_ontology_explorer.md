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

The second is the system catalog's. `mz_internal.mz_ontology_*` describes
every catalog relation, how relations join, and which id domain each column
holds. The MCP developer server tells agents to read it first. People have
no way to browse it.

## Success Criteria

- A user can see their relations, the joins their ontology tables declare,
  and the lineage between relations, as one graph.
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
  them with SQL. The explorer reads them.
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
  materialized views and sources. Join relationships come from ontology
  tables, recognized by shape: any user relation with `table_name text`,
  `columns text[]`, `referenced_table text` and `referenced_columns text[]`.
  Names resolve relative to the ontology table's schema, then its database,
  then anywhere they are unique. Each row is checked for unknown or
  ambiguous relations, missing columns, mismatched column counts and join
  columns of incompatible types. Lineage comes from `mz_object_graph_edges`.
  Entities listed by `mz_mcp_data_products` are marked, since that view is
  what the agent MCP server lists.
- **System catalog** (`catalogGraph.ts`). Entities, links and properties come
  from the `mz_ontology_*` views, with SQL types joined from `mz_columns`.

### Data and cost

Live data uses the sync engine's scoped collections, so rows survive
navigation within the session and each data set is one SUBSCRIBE:

| Data | Source | Cluster |
| --- | --- | --- |
| Relations | the app-wide `allObjects` SUBSCRIBE | `mz_catalog_server` |
| Lineage | `workflowGraphEdgesCollection` | `mz_catalog_server` |
| Ontology tables | SUBSCRIBE over `mz_columns`, `mz_objects`, `mz_indexes` | `mz_catalog_server` |
| Ontology rows | SUBSCRIBE over every ontology table, unioned | an ontology table's index cluster |
| Columns and comments | SUBSCRIBE scoped to the drawn schemas | `mz_catalog_server` |
| Freshness | SUBSCRIBE over `mz_frontiers` and `mz_hydration_statuses`, scoped to the drawn schemas | `mz_catalog_server` |
| Data products | polled every 60s | `mz_catalog_server` |
| Catalog ontology | read once, it changes only on upgrade | `mz_catalog_server` |

Every catalog relation these read is indexed on `mz_catalog_server`.
Ontology tables are user objects, which `mz_catalog_server` cannot read, so
their rows are read on the cluster of an ontology table's index. The pattern
already tells users to index the table. Without an index the page says so
and shows no joins.

Above 150 relations, the page opens on the schemas the ontology tables
describe, so the columns and freshness subscribes stay small.

A side panel tab streams a relation's rows with SUBSCRIBE on request. It is
not automatic, since it maintains a dataflow on the relation's cluster.

### Layout

Dagre ranks entities with referenced relations above the relations that
reference them, and boxes each schema. A hub such as the role every catalog
object references makes one rank too wide to read, so ranks wider than
`maxCardsPerRow` wrap into rows, and those layouts draw their own edges. A
circular layout is the alternative. The canvas fits the graph on load and
on every layout change.

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
- **Subscribing to `mz_mcp_data_products`.** It checks the role's privileges
  on every object and is not indexed, so it is polled.

## Open questions

- Whether the flag defaults on in self-managed deployments.
- Whether the object detail pages should link to the entity here.
