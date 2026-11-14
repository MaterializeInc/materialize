// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * @module
 * The explorer's view of an ontology: entities, the typed relationships between
 * them, and the problems found while assembling them. Both lenses (data
 * products and the system catalog) build this shape, so the canvas, filters
 * and side panel do not know which lens they draw.
 */

/** Join relationships curated in an ontology table. */
export const JOIN_EDGE_KIND = "join";
/** A derived object reading from another, from `mz_object_graph_edges`. */
export const LINEAGE_EDGE_KIND = "lineage";

/**
 * Relationship kinds. `join` and `lineage` come from the data products lens,
 * the rest are the `kind` values of `mz_internal.mz_ontology_link_types`.
 */
export type OntologyEdgeKind =
  | typeof JOIN_EDGE_KIND
  | typeof LINEAGE_EDGE_KIND
  | "foreign_key"
  | "maps_to"
  | "measures"
  | "depends_on"
  | "union"
  | "other";

export interface OntologyColumn {
  name: string;
  type: string;
  description: string | null;
  semanticType: string | null;
}

export interface OntologyNode {
  id: string;
  label: string;
  /** Nodes are grouped and colored by this, a schema in both lenses. */
  group: string;
  /** Object type, such as `table` or `materialized-view`, or `entity`. */
  kind: string;
  /** The relation to read, quoted and fully qualified. */
  relation: string | null;
  description: string | null;
  /** Columns, when known. Undefined while still loading. */
  columns: OntologyColumn[] | undefined;
  primaryKey: string[];
  /** Catalog object id, for nodes that are user objects. */
  objectId: string | null;
  databaseName: string | null;
  schemaName: string | null;
  clusterName: string | null;
  /** Listed by `mz_mcp_data_products`, so agents discover it over MCP. */
  isDataProduct: boolean;
  /** A name an ontology table refers to that resolves to no object. */
  isUnresolved: boolean;
}

export interface OntologyEdge {
  id: string;
  /** The dependent side: the foreign key holder, or the derived object. */
  source: string;
  /** The referenced side: the key owner, or the upstream object. */
  target: string;
  kind: OntologyEdgeKind;
  /** Relationship name, such as `owned_by`. Joins have none. */
  name: string | null;
  sourceColumns: string[];
  targetColumns: string[];
  description: string | null;
  cardinality: string | null;
  /** A mapping relation that bridges the two sides' id domains. */
  via: string | null;
  note: string | null;
  /** The ontology table that declares a join, fully qualified. */
  declaredIn: string | null;
  issues: OntologyIssue[];
}

export type OntologyIssueSeverity = "error" | "warning";

export interface OntologyIssue {
  severity: OntologyIssueSeverity;
  message: string;
  /** The ontology table that holds the offending row, fully qualified. */
  declaredIn: string | null;
  edgeId: string | null;
}

export interface OntologyGraph {
  nodes: OntologyNode[];
  edges: OntologyEdge[];
  issues: OntologyIssue[];
}

export const EMPTY_GRAPH: OntologyGraph = { nodes: [], edges: [], issues: [] };

/**
 * A short label for an edge, such as `org_id → id` or `owned_by`. Lineage
 * edges have none on the canvas, their dashes already say what they are.
 */
export function edgeLabel(edge: OntologyEdge): string {
  if (edge.kind === LINEAGE_EDGE_KIND) return "";
  if (edge.sourceColumns.length > 0 && edge.targetColumns.length > 0) {
    const columns = `${edge.sourceColumns.join(", ")} → ${edge.targetColumns.join(", ")}`;
    return edge.name ? `${edge.name}: ${columns}` : columns;
  }
  return edge.name ?? edge.kind;
}

export function isErrorEdge(edge: OntologyEdge) {
  return edge.issues.some((issue) => issue.severity === "error");
}
