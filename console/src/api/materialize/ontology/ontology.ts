// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { RawBuilder, sql } from "kysely";

import { executeSqlV2, queryBuilder } from "~/api/materialize";

/** The first version with the `mz_internal.mz_ontology_*` views. */
export const CATALOG_ONTOLOGY_MIN_VERSION = "26.25.0";

/**
 * The columns that make a relation an ontology table, following the documented
 * pattern (`/docs/transform-data/patterns/ontology/`). Any user relation with
 * all four is read as one, whatever its name.
 */
export const ONTOLOGY_TABLE_COLUMNS = [
  ["table_name", "text"],
  ["columns", "text[]"],
  ["referenced_table", "text"],
  ["referenced_columns", "text[]"],
] as const;

export function buildCatalogEntitiesQuery() {
  return queryBuilder
    .selectFrom("mz_ontology_entity_types")
    .select(["name", "relation", "properties", "description"])
    .orderBy("name");
}

export function buildCatalogLinksQuery() {
  return queryBuilder
    .selectFrom("mz_ontology_link_types")
    .select([
      "name",
      "source_entity as sourceEntity",
      "target_entity as targetEntity",
      "properties",
      "description",
    ]);
}

/**
 * Every described column with its SQL type. The ontology views carry semantic
 * types but not SQL types, so those come from `mz_columns` by relation name.
 */
export function buildCatalogPropertiesQuery() {
  return queryBuilder
    .selectFrom("mz_ontology_properties as p")
    .innerJoin("mz_ontology_entity_types as e", "e.name", "p.entity_type")
    .leftJoin(
      (eb) =>
        eb
          .selectFrom("mz_columns as c")
          .innerJoin("mz_objects as o", "o.id", "c.id")
          .innerJoin("mz_schemas as s", "s.id", "o.schema_id")
          .where("o.id", "like", "s%")
          .select([
            sql<string>`s.name || '.' || o.name`.as("relation"),
            "c.name",
            "c.type",
            "c.position",
          ])
          .as("col"),
      (join) =>
        join
          .onRef("col.relation", "=", "e.relation")
          .onRef("col.name", "=", "p.column_name"),
    )
    .select([
      "p.entity_type as entityType",
      "p.column_name as columnName",
      "p.semantic_type as semanticType",
      "p.description",
      "col.type",
    ])
    .orderBy("p.entity_type")
    .orderBy("col.position");
}

/** The system catalog ontology, which changes only on upgrade. */
export async function fetchCatalogOntology({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const [entities, links, properties] = await executeSqlV2({
    queries: [
      buildCatalogEntitiesQuery().compile(),
      buildCatalogLinksQuery().compile(),
      buildCatalogPropertiesQuery().compile(),
    ] as const,
    queryKey,
    requestOptions,
  });
  return {
    entities: entities.rows,
    links: links.rows,
    properties: properties.rows,
  };
}

/** Relations the agent MCP server lists as data products, for the current role. */
export function buildDataProductNamesQuery() {
  return queryBuilder
    .selectFrom("mz_mcp_data_products")
    .select("object_name as objectName")
    .distinct();
}

export async function fetchDataProductNames({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const [result] = await executeSqlV2({
    queries: [buildDataProductNamesQuery().compile()] as const,
    queryKey,
    requestOptions,
  });
  return result.rows.map((row) => row.objectName);
}

export interface OntologyTableRow {
  id: string;
  name: string;
  schemaName: string;
  databaseName: string | null;
  /** The cluster of one of the table's indexes, null when unindexed. */
  indexClusterName: string | null;
}

/**
 * User relations shaped like an ontology table. The catalog relations it reads
 * are all indexed on `mz_catalog_server`.
 */
export function buildOntologyTablesQuery(): RawBuilder<OntologyTableRow> {
  const shape = sql.join(
    ONTOLOGY_TABLE_COLUMNS.map(
      ([name, type]) => sql`(${sql.lit(name)}, ${sql.lit(type)})`,
    ),
  );
  return sql<OntologyTableRow>`
SELECT
  o.id,
  o.name,
  s.name AS "schemaName",
  d.name AS "databaseName",
  ic.cluster_name AS "indexClusterName"
FROM mz_catalog.mz_objects o
JOIN mz_catalog.mz_schemas s ON s.id = o.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
LEFT JOIN (
  SELECT i.on_id, min(c.name) AS cluster_name
  FROM mz_catalog.mz_indexes i
  JOIN mz_catalog.mz_clusters c ON c.id = i.cluster_id
  GROUP BY i.on_id
) ic ON ic.on_id = o.id
WHERE o.id LIKE 'u%'
  AND o.type IN ('table', 'view', 'materialized-view', 'source')
  AND o.id IN (
    SELECT id FROM mz_catalog.mz_columns
    WHERE (name, type) IN (${shape})
    GROUP BY id
    HAVING count(*) = ${sql.lit(ONTOLOGY_TABLE_COLUMNS.length)}
  )`;
}

export interface OntologyRowsRow {
  ontologyId: string;
  tableName: string;
  columns: string[];
  referencedTable: string;
  referencedColumns: string[];
}

/**
 * Every row of the given ontology tables. The arrays are read as jsonb so they
 * arrive as JSON arrays, and DISTINCT makes the rows a valid upsert key, since
 * nothing stops an ontology table from holding a row twice.
 */
export function buildOntologyRowsQuery(
  tables: Pick<
    OntologyTableRow,
    "id" | "name" | "schemaName" | "databaseName"
  >[],
): RawBuilder<OntologyRowsRow> | undefined {
  if (tables.length === 0) return undefined;
  const selects = tables.map((table) => {
    const relation = table.databaseName
      ? sql`${sql.id(table.databaseName)}.${sql.id(table.schemaName)}.${sql.id(table.name)}`
      : sql`${sql.id(table.schemaName)}.${sql.id(table.name)}`;
    return sql`SELECT ${sql.lit(table.id)} AS "ontologyId", table_name, columns, referenced_table, referenced_columns FROM ${relation}`;
  });
  return sql<OntologyRowsRow>`
SELECT DISTINCT
  "ontologyId",
  table_name AS "tableName",
  to_jsonb(columns) AS columns,
  referenced_table AS "referencedTable",
  to_jsonb(referenced_columns) AS "referencedColumns"
FROM (${sql.join(selects, sql` UNION ALL `)}) AS rows
WHERE table_name IS NOT NULL AND referenced_table IS NOT NULL`;
}

export const ONTOLOGY_ROWS_UPSERT_KEY = [
  "ontologyId",
  "tableName",
  "columns",
  "referencedTable",
  "referencedColumns",
];

export interface RelationDetailRow {
  id: string;
  /** Zero for the object itself, otherwise the column's position. */
  position: number;
  name: string | null;
  type: string | null;
  comment: string | null;
}

/** Columns and comments of the given relations. */
export function buildRelationDetailsQuery(
  ids: string[],
): RawBuilder<RelationDetailRow> | undefined {
  if (ids.length === 0) return undefined;
  const idList = sql.join(ids.map((id) => sql.lit(id)));
  return sql<RelationDetailRow>`
SELECT c.id, c.position::int4 AS position, c.name, c.type, cm.comment
FROM mz_catalog.mz_columns c
LEFT JOIN mz_internal.mz_comments cm
  ON cm.id = c.id AND cm.object_sub_id = c.position
WHERE c.id IN (${idList})
UNION ALL
SELECT cm.id, 0 AS position, NULL AS name, NULL AS type, cm.comment
FROM mz_internal.mz_comments cm
WHERE cm.id IN (${idList}) AND cm.object_sub_id IS NULL`;
}

export interface RelationVitalsRow {
  id: string;
  /** Milliseconds since the epoch, null for a relation nothing maintains. */
  writeFrontier: string | null;
  /** Null for relations without a compute dataflow, such as tables. */
  hydrated: boolean | null;
}

/**
 * How current each relation is: its write frontier and whether its dataflows
 * are hydrated. A view is maintained only through its indexes, so the
 * frontiers and hydration of a relation's indexes count as its own.
 */
export function buildRelationVitalsQuery(
  ids: string[],
): RawBuilder<RelationVitalsRow> | undefined {
  if (ids.length === 0) return undefined;
  const idList = sql.join(ids.map((id) => sql.lit(id)));
  return sql<RelationVitalsRow>`
WITH targets AS (
  SELECT id AS relation_id, id AS dataflow_id
  FROM mz_catalog.mz_objects WHERE id IN (${idList})
  UNION ALL
  SELECT on_id, id FROM mz_catalog.mz_indexes WHERE on_id IN (${idList})
),
frontiers AS (
  SELECT t.relation_id, max(f.write_frontier) AS write_frontier
  FROM targets t
  JOIN mz_internal.mz_frontiers f ON f.object_id = t.dataflow_id
  GROUP BY t.relation_id
),
hydration AS (
  SELECT t.relation_id, bool_and(h.hydrated) AS hydrated
  FROM targets t
  JOIN mz_internal.mz_hydration_statuses h ON h.object_id = t.dataflow_id
  GROUP BY t.relation_id
)
SELECT
  t.relation_id AS id,
  f.write_frontier::text AS "writeFrontier",
  h.hydrated
FROM (SELECT DISTINCT relation_id FROM targets) t
LEFT JOIN frontiers f ON f.relation_id = t.relation_id
LEFT JOIN hydration h ON h.relation_id = t.relation_id`;
}

/**
 * Up to `limit` distinct rows of `columns` of a relation, kept current.
 * DISTINCT gives the rows an identity to upsert on, since a relation need not
 * have a key. `relation` must already be quoted, as `OntologyNode.relation` is.
 */
export function buildLivePreviewSubscribe(
  relation: string,
  columns: string[],
  limit: number,
): RawBuilder<Record<string, unknown>> {
  const list = sql.join(columns.map((column) => sql.id(column)));
  return sql<
    Record<string, unknown>
  >`SUBSCRIBE (SELECT DISTINCT ${list} FROM ${sql.raw(relation)} LIMIT ${sql.lit(limit)}) WITH (PROGRESS) ENVELOPE UPSERT (KEY (${list}));`;
}
