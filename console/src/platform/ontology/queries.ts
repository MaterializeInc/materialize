// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useLiveQuery } from "@tanstack/react-db";
import { useQuery } from "@tanstack/react-query";
import React from "react";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import { buildSubscribeQuery } from "~/api/materialize/buildSubscribeQuery";
import { CATALOG_SERVER_CLUSTER } from "~/api/materialize/executeSql";
import {
  buildIndexKeysQuery,
  buildOntologyRowsQuery,
  buildOntologyTablesQuery,
  buildRelationDetailsQuery,
  buildRelationVitalsQuery,
  buildViewDefinitionsQuery,
  CATALOG_ONTOLOGY_MIN_VERSION,
  fetchCatalogOntology,
  fetchDataProductNames,
  IndexKeyRow,
  ONTOLOGY_ROWS_UPSERT_KEY,
  OntologyRowsRow,
  OntologyTableRow,
  RelationDetailRow,
  RelationVitalsRow,
  ViewDefinitionRow,
} from "~/api/materialize/ontology/ontology";
import { createScopedSubscribeCollections } from "~/api/materialize/subscribeCollection";
import { useScopedSubscribeCollection } from "~/api/materialize/useScopedSubscribeCollection";
import { useAllObjectsLive } from "~/store/allObjectsCollection";
import { useEnvironmentGate } from "~/store/environments";
import {
  useWorkflowGraphEdgesAvailable,
  workflowGraphEdgesCollection,
} from "~/store/workflowGraphEdgesCollection";

import { buildCatalogGraph } from "./catalogGraph";
import {
  buildDataProductGraph,
  CatalogRelation,
  ENTITY_OBJECT_TYPES,
  groupName,
  parseQualifiedName,
} from "./dataProductGraph";
import { IndexKey } from "./indexFit";
import { OntologyColumn, OntologyGraph } from "./model";
import { resolveOntologyTable } from "./ontologyTableChoice";
import { extractViewJoins, ViewJoins } from "./viewJoins";

/** The first version with `mz_internal.mz_mcp_data_products`. */
const DATA_PRODUCTS_MIN_VERSION = "26.24.0";
// The view checks the role's privileges on every object, about 200ms on a
// small environment, so it is polled slowly.
const DATA_PRODUCTS_REFRESH_MS = 60_000;

export const ontologyQueryKeys = {
  all: () => buildRegionQueryKey("ontology"),
  catalog: () =>
    [...ontologyQueryKeys.all(), buildQueryKeyPart("catalog")] as const,
  dataProducts: () =>
    [...ontologyQueryKeys.all(), buildQueryKeyPart("dataProducts")] as const,
};

/** The system catalog ontology, or `unsupported` before the views existed. */
export function useCatalogOntologyGraph() {
  const supported = useEnvironmentGate(CATALOG_ONTOLOGY_MIN_VERSION);
  const query = useQuery({
    queryKey: ontologyQueryKeys.catalog(),
    queryFn: ({ queryKey, signal }) =>
      fetchCatalogOntology({ queryKey, requestOptions: { signal } }),
    enabled: supported === true,
    // The views are generated from the builtins, so they only change when the
    // environment is upgraded, which reloads the console.
    staleTime: Infinity,
  });
  const graph = React.useMemo(
    () =>
      query.data
        ? buildCatalogGraph({
            entities: query.data.entities.map((entity) => ({
              ...entity,
              properties: asObject(entity.properties),
            })),
            links: query.data.links.map((link) => ({
              ...link,
              properties: asObject(link.properties),
            })),
            properties: query.data.properties,
          })
        : undefined,
    [query.data],
  );
  return {
    graph,
    unsupported: supported === false,
    isLoading: supported === null || query.isLoading,
    isError: query.isError,
  };
}

function asObject(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

const ontologyTablesCollections =
  createScopedSubscribeCollections<OntologyTableRow>({
    id: "ontology-tables",
    getKey: (row) => row.id,
  });

const ontologyRowKey = (row: OntologyRowsRow) =>
  JSON.stringify([
    row.ontologyId,
    row.tableName,
    row.columns,
    row.referencedTable,
    row.referencedColumns,
  ]);

const ontologyRowsCollections =
  createScopedSubscribeCollections<OntologyRowsRow>({
    id: "ontology-rows",
    getKey: ontologyRowKey,
  });

const relationDetailsCollections =
  createScopedSubscribeCollections<RelationDetailRow>({
    id: "ontology-relation-details",
    getKey: (row) => `${row.id}|${row.position}`,
  });

const relationVitalsCollections =
  createScopedSubscribeCollections<RelationVitalsRow>({
    id: "ontology-relation-vitals",
    getKey: (row) => row.id,
  });

const viewDefinitionsCollections =
  createScopedSubscribeCollections<ViewDefinitionRow>({
    id: "ontology-view-definitions",
    getKey: (row) => row.id,
  });

const indexKeysCollections = createScopedSubscribeCollections<IndexKeyRow>({
  id: "ontology-index-keys",
  getKey: (row) => `${row.indexId}|${row.position}`,
});

const VIEW_DEFINITIONS_SUBSCRIBE = buildSubscribeQuery(
  buildViewDefinitionsQuery(),
  { upsertKey: "id" },
);
const INDEX_KEYS_SUBSCRIBE = buildSubscribeQuery(buildIndexKeysQuery(), {
  upsertKey: ["indexId", "position"],
});

const ONTOLOGY_TABLES_SUBSCRIBE = buildSubscribeQuery(
  buildOntologyTablesQuery(),
  { upsertKey: "id" },
);
const CATALOG_SESSION = { clusterName: CATALOG_SERVER_CLUSTER };
const COLLECTION_KEY = "environment";

/** Memoizes a list of ids on its contents. Ids contain no commas. */
function useStableIds(ids: string[]) {
  const key = [...ids].sort().join(",");
  return React.useMemo(() => (key === "" ? [] : key.split(",")), [key]);
}

const NO_ROWS: OntologyRowsRow[] = [];

/**
 * Relations shaped like an ontology table, and the rows of the one chosen,
 * kept live.
 */
function useOntologyTables(chosenId: string | null) {
  const tables = useScopedSubscribeCollection(
    ontologyTablesCollections,
    COLLECTION_KEY,
    ONTOLOGY_TABLES_SUBSCRIBE,
    CATALOG_SESSION,
  );
  const { data: tableRows } = useLiveQuery(
    (q) =>
      tables.collection
        ? q.from({ table: tables.collection.collection })
        : undefined,
    [tables.collection],
  );
  const sortedTables = React.useMemo(
    () => [...(tableRows ?? [])].sort((a, b) => a.id.localeCompare(b.id)),
    [tableRows],
  );

  const chosen = sortedTables.find((table) => table.id === chosenId) ?? null;
  // Ontology tables are user objects, which `mz_catalog_server` cannot read,
  // so the rows are read where the table's index is.
  const readCluster = chosen?.indexClusterName ?? null;
  const chosenKey = chosen
    ? `${chosen.id}:${chosen.databaseName}.${chosen.schemaName}.${chosen.name}`
    : null;
  const rowsSubscribe = React.useMemo(() => {
    if (!chosen || !readCluster) return undefined;
    const query = buildOntologyRowsQuery([chosen]);
    return query
      ? buildSubscribeQuery(query, { upsertKey: ONTOLOGY_ROWS_UPSERT_KEY })
      : undefined;
    // `chosen` is keyed by `chosenKey`, so a new row object for the same
    // table does not restart the subscribe.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [chosenKey, readCluster]);
  const rowsSession = React.useMemo(
    () => (readCluster ? { clusterName: readCluster } : undefined),
    [readCluster],
  );
  const rows = useScopedSubscribeCollection(
    ontologyRowsCollections,
    COLLECTION_KEY,
    rowsSubscribe,
    rowsSession,
  );
  const { data: rowData } = useLiveQuery(
    (q) =>
      rows.collection && rowsSubscribe
        ? q.from({ row: rows.collection.collection })
        : undefined,
    [rows.collection, rowsSubscribe],
  );

  return {
    tables: sortedTables,
    chosen,
    rows: (rowsSubscribe && rowData) || NO_ROWS,
    readCluster,
    tablesReady: tables.status.snapshotComplete,
    rowsReady: !rowsSubscribe || rows.status.snapshotComplete,
    tablesError:
      tables.status.error ?? (tables.isScopeError ? true : undefined),
    rowsError: rowsSubscribe ? rows.status.error : undefined,
  };
}

/** Columns and comments of the given relations, kept live. */
function useRelationDetails(relationIds: string[]) {
  const ids = useStableIds(relationIds);
  const subscribe = React.useMemo(() => {
    const query = buildRelationDetailsQuery(ids);
    return query
      ? buildSubscribeQuery(query, { upsertKey: ["id", "position"] })
      : undefined;
  }, [ids]);
  const { collection, status } = useScopedSubscribeCollection(
    relationDetailsCollections,
    COLLECTION_KEY,
    subscribe,
    CATALOG_SESSION,
  );
  const { data } = useLiveQuery(
    (q) =>
      collection && subscribe
        ? q.from({ row: collection.collection })
        : undefined,
    [collection, subscribe],
  );
  return React.useMemo(() => {
    const columns = new Map<string, OntologyColumn[]>();
    const comments = new Map<string, string>();
    const sorted = [...(data ?? [])].sort((a, b) => a.position - b.position);
    for (const row of sorted) {
      if (row.position === 0) {
        if (row.comment) comments.set(row.id, row.comment);
        continue;
      }
      const list = columns.get(row.id) ?? [];
      list.push({
        name: row.name ?? "",
        type: row.type ?? "",
        description: row.comment,
        semanticType: null,
      });
      columns.set(row.id, list);
    }
    return { columns, comments, ready: !subscribe || status.snapshotComplete };
  }, [data, status.snapshotComplete, subscribe]);
}

// Extraction is pure in the definition, and definitions rarely change, so a
// view is scanned again only when its definition does.
const viewJoinsCache = new Map<string, ViewJoins>();

function viewJoinsOf(createSql: string) {
  let joins = viewJoinsCache.get(createSql);
  if (!joins) {
    joins = extractViewJoins(createSql);
    viewJoinsCache.set(createSql, joins);
  }
  return joins;
}

/**
 * What the environment's own definitions say about joins: the joins in each
 * view, and the keys of each relation's indexes. Both are kept live.
 */
function useJoinEvidence() {
  const views = useScopedSubscribeCollection(
    viewDefinitionsCollections,
    COLLECTION_KEY,
    VIEW_DEFINITIONS_SUBSCRIBE,
    CATALOG_SESSION,
  );
  const { data: viewRows } = useLiveQuery(
    (q) =>
      views.collection
        ? q.from({ view: views.collection.collection })
        : undefined,
    [views.collection],
  );
  const keys = useScopedSubscribeCollection(
    indexKeysCollections,
    COLLECTION_KEY,
    INDEX_KEYS_SUBSCRIBE,
    CATALOG_SESSION,
  );
  const { data: keyRows } = useLiveQuery(
    (q) =>
      keys.collection ? q.from({ key: keys.collection.collection }) : undefined,
    [keys.collection],
  );

  const viewJoins = React.useMemo(() => {
    const map = new Map<string, ViewJoins>();
    for (const row of viewRows ?? []) {
      map.set(row.id, viewJoinsOf(row.createSql));
    }
    return map;
  }, [viewRows]);
  const indexKeys = React.useMemo(() => {
    const byIndex = new Map<string, IndexKeyRow[]>();
    for (const row of keyRows ?? []) {
      byIndex.set(row.indexId, [...(byIndex.get(row.indexId) ?? []), row]);
    }
    const map = new Map<string, IndexKey[]>();
    for (const parts of byIndex.values()) {
      const key = parts
        .sort((a, b) => a.position - b.position)
        .map((part) => part.columnName);
      map.set(parts[0].id, [...(map.get(parts[0].id) ?? []), key]);
    }
    return map;
  }, [keyRows]);
  return {
    viewJoins,
    indexKeys,
    ready: views.status.snapshotComplete && keys.status.snapshotComplete,
  };
}

export interface RelationVitals {
  /** Milliseconds since the epoch, null when nothing maintains the relation. */
  writeFrontier: number | null;
  hydrated: boolean | null;
}

/**
 * Write frontiers and hydration of the given relations, kept live. Frontiers
 * advance about once a second, so read this only where it is drawn, apart
 * from the graph, or every tick re-lays out the canvas.
 */
export function useRelationVitals(relationIds: string[]) {
  // A new id array per render would restart the SUBSCRIBE.
  const ids = useStableIds(relationIds);
  const subscribe = React.useMemo(() => {
    const query = buildRelationVitalsQuery(ids);
    return query ? buildSubscribeQuery(query, { upsertKey: "id" }) : undefined;
  }, [ids]);
  const { collection, status } = useScopedSubscribeCollection(
    relationVitalsCollections,
    COLLECTION_KEY,
    subscribe,
    CATALOG_SESSION,
  );
  const { data } = useLiveQuery(
    (q) =>
      collection && subscribe
        ? q.from({ row: collection.collection })
        : undefined,
    [collection, subscribe],
  );
  const vitals = React.useMemo(() => {
    const map = new Map<string, RelationVitals>();
    for (const row of data ?? []) {
      map.set(row.id, {
        writeFrontier:
          row.writeFrontier !== null ? parseInt(row.writeFrontier, 10) : null,
        hydrated: row.hydrated,
      });
    }
    return map;
  }, [data]);
  return { vitals, isLive: status.snapshotComplete && !status.error };
}

function useDataProductNames() {
  const supported = useEnvironmentGate(DATA_PRODUCTS_MIN_VERSION) === true;
  const { data } = useQuery({
    queryKey: ontologyQueryKeys.dataProducts(),
    queryFn: ({ queryKey, signal }) =>
      fetchDataProductNames({ queryKey, requestOptions: { signal } }),
    enabled: supported,
    // Grants and comments change what agents see, and nothing announces it.
    refetchInterval: DATA_PRODUCTS_REFRESH_MS,
  });
  return React.useMemo(() => new Set(data ?? []), [data]);
}

const NO_LINEAGE: { childId: string; parentId: string }[] = [];

/**
 * The data products ontology: user relations, the joins inferred from the
 * environment's definitions or declared in the ontology table named by
 * `ontologyTableName`, and lineage. Columns and comments are loaded for the
 * relations in `groups` (all groups when null) and for every relation the
 * ontology table names, which validating the joins needs.
 */
export function useDataProductOntologyGraph(
  groups: ReadonlySet<string> | null,
  ontologyTableName: string,
) {
  const objects = useAllObjectsLive();
  const lineageAvailable = useWorkflowGraphEdgesAvailable();
  const { data: lineageRows } = useLiveQuery((q) =>
    q.from({ edge: workflowGraphEdgesCollection.collection }),
  );
  const dataProducts = useDataProductNames();
  const evidence = useJoinEvidence();

  const relations: CatalogRelation[] = React.useMemo(
    () =>
      (objects.data ?? []).filter(
        (object) =>
          object.id.startsWith("u") &&
          ENTITY_OBJECT_TYPES.has(object.objectType),
      ),
    [objects.data],
  );
  const chosenRelation = React.useMemo(
    () => resolveOntologyTable(ontologyTableName, relations),
    [ontologyTableName, relations],
  );
  const ontology = useOntologyTables(chosenRelation?.id ?? null);
  const declaringTables = React.useMemo(
    () => (ontology.chosen ? [ontology.chosen] : []),
    [ontology.chosen],
  );

  const referencedNames = React.useMemo(() => {
    const names = new Set<string>();
    for (const row of ontology.rows) {
      for (const name of [row.tableName, row.referencedTable]) {
        const parts = parseQualifiedName(name);
        if (parts) names.add(parts[parts.length - 1]);
      }
    }
    return names;
  }, [ontology.rows]);

  const details = useRelationDetails(
    relations
      .filter(
        (relation) =>
          !groups ||
          groups.has(groupName(relation)) ||
          referencedNames.has(relation.name),
      )
      .map((relation) => relation.id),
  );

  const graph: OntologyGraph = React.useMemo(
    () =>
      buildDataProductGraph({
        relations,
        ontologyTables: declaringTables,
        ontologyRows: ontology.rows,
        lineage: lineageAvailable ? (lineageRows ?? NO_LINEAGE) : NO_LINEAGE,
        columns: details.columns,
        comments: details.comments,
        dataProducts,
        viewJoins: evidence.viewJoins,
        indexKeys: evidence.indexKeys,
      }),
    [
      relations,
      declaringTables,
      ontology.rows,
      lineageAvailable,
      lineageRows,
      details.columns,
      details.comments,
      dataProducts,
      evidence.viewJoins,
      evidence.indexKeys,
    ],
  );

  const ontologyShapeIds = React.useMemo(
    () => new Set(ontology.tables.map((table) => table.id)),
    [ontology.tables],
  );

  const allGroups = React.useMemo(
    () => [...new Set(relations.map(groupName))].sort(),
    [relations],
  );

  return {
    graph,
    allGroups,
    relations,
    /** Ids of relations shaped like an ontology table. */
    ontologyShapeIds,
    /** The relation the chosen name resolves to, if any. */
    chosenRelation,
    /** The chosen relation, when it is shaped like an ontology table. */
    ontologyTable: ontology.chosen,
    readCluster: ontology.readCluster,
    isLoading:
      !objects.snapshotComplete || !ontology.tablesReady || !ontology.rowsReady,
    detailsReady: details.ready && evidence.ready,
    isError: objects.isError || Boolean(ontology.tablesError),
    rowsError: ontology.rowsError,
  };
}
