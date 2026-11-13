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
  buildOntologyRowsQuery,
  buildOntologyTablesQuery,
  buildRelationDetailsQuery,
  buildRelationVitalsQuery,
  CATALOG_ONTOLOGY_MIN_VERSION,
  fetchCatalogOntology,
  fetchDataProductNames,
  ONTOLOGY_ROWS_UPSERT_KEY,
  OntologyRowsRow,
  OntologyTableRow,
  RelationDetailRow,
  RelationVitalsRow,
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
import { OntologyColumn, OntologyGraph } from "./model";

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

/** Ontology tables in the environment, with their rows, kept live. */
function useOntologyTables() {
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

  // Ontology tables are user objects, which `mz_catalog_server` cannot read.
  // An index pins where reading them is cheap, and once one cluster is chosen
  // the unindexed tables are read there too, from storage.
  const readCluster =
    sortedTables.find((table) => table.indexClusterName)?.indexClusterName ??
    null;
  const tablesKey = sortedTables
    .map((t) => `${t.id}:${t.databaseName}.${t.schemaName}.${t.name}`)
    .join(",");
  const rowsSubscribe = React.useMemo(() => {
    if (!readCluster) return undefined;
    const query = buildOntologyRowsQuery(sortedTables);
    return query
      ? buildSubscribeQuery(query, { upsertKey: ONTOLOGY_ROWS_UPSERT_KEY })
      : undefined;
    // `sortedTables` is keyed by `tablesKey`, so a new array with the same
    // tables does not restart the subscribe.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tablesKey, readCluster]);
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
 * The data products ontology: user relations, the joins declared in ontology
 * tables, and lineage. Columns and comments are loaded for the relations in
 * `groups` (all groups when null) and for every relation an ontology table
 * names, which validating the joins needs.
 */
export function useDataProductOntologyGraph(
  groups: ReadonlySet<string> | null,
) {
  const objects = useAllObjectsLive();
  const lineageAvailable = useWorkflowGraphEdgesAvailable();
  const { data: lineageRows } = useLiveQuery((q) =>
    q.from({ edge: workflowGraphEdgesCollection.collection }),
  );
  const ontology = useOntologyTables();
  const dataProducts = useDataProductNames();

  const relations: CatalogRelation[] = React.useMemo(
    () =>
      (objects.data ?? []).filter(
        (object) =>
          object.id.startsWith("u") &&
          ENTITY_OBJECT_TYPES.has(object.objectType),
      ),
    [objects.data],
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
        ontologyTables: ontology.tables,
        ontologyRows: ontology.rows,
        lineage: lineageAvailable ? (lineageRows ?? NO_LINEAGE) : NO_LINEAGE,
        columns: details.columns,
        comments: details.comments,
        dataProducts,
      }),
    [
      relations,
      ontology.tables,
      ontology.rows,
      lineageAvailable,
      lineageRows,
      details.columns,
      details.comments,
      dataProducts,
    ],
  );

  const allGroups = React.useMemo(
    () => [...new Set(relations.map(groupName))].sort(),
    [relations],
  );

  return {
    graph,
    allGroups,
    ontologyTables: ontology.tables,
    readCluster: ontology.readCluster,
    isLoading:
      !objects.snapshotComplete || !ontology.tablesReady || !ontology.rowsReady,
    detailsReady: details.ready,
    isError: objects.isError || Boolean(ontology.tablesError),
    rowsError: ontology.rowsError,
  };
}
