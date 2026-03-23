/**
 * Shared type definitions for the mz-deploy VS Code extension.
 *
 * Covers three boundaries:
 * 1. **LSP response types** — shapes returned by `mz-deploy/catalog`, `mz-deploy/dag`,
 *    and `mz-deploy/keywords` requests.
 * 2. **Extension ↔ webview messages** — discriminated unions for `postMessage` protocols
 *    between the extension host and the DAG/catalog webview scripts.
 * 3. **Layout types** — intermediate values produced by the DAG layout algorithm.
 */

// ---------------------------------------------------------------------------
// LSP Response Types
// ---------------------------------------------------------------------------

export interface DagNode {
  id: string;
  name: string;
  schema: string;
  is_external?: boolean;
}

export interface DagEdge {
  source: string;
  target: string;
}

export interface DagData {
  objects: DagNode[];
  edges: DagEdge[];
}

export interface Column {
  name: string;
  type_name: string;
  nullable?: boolean;
  comment?: string;
}

export interface Constraint {
  kind: string;
  name: string;
  columns: string[];
  references?: string;
}

export interface Index {
  name: string;
  columns: string[];
  cluster?: string;
}

export interface Grant {
  privilege: string;
  role: string;
}

export interface Property {
  key: string;
  value?: string;
  secret_ref?: string;
  object_ref?: string;
}

export interface Infrastructure {
  connector_type?: string;
  connection_ref?: string;
  source_ref?: string;
  properties?: Property[];
}

export interface CatalogObject {
  id: string;
  name: string;
  schema: string;
  database: string;
  object_type: string;
  file_path?: string;
  columns?: Column[];
  constraints?: Constraint[];
  indexes?: Index[];
  grants?: Grant[];
  dependencies: string[];
  dependents: string[];
  infrastructure?: Infrastructure;
  is_external?: boolean;
  description?: string;
  cluster?: string;
}

export interface SchemaEntry {
  name: string;
  object_ids: string[];
}

export interface DatabaseEntry {
  name: string;
  schemas: SchemaEntry[];
}

export interface CatalogData {
  databases: DatabaseEntry[];
  objects: CatalogObject[];
}

// ---------------------------------------------------------------------------
// Extension ↔ DAG Webview Messages
// ---------------------------------------------------------------------------

export type DagInboundMessage =
  | { type: "dag-data"; data: DagData }
  | { type: "focus"; id: string };

export type DagOutboundMessage =
  | { type: "inspect-object"; id: string }
  | { type: "ready" };

// ---------------------------------------------------------------------------
// Extension ↔ Catalog Webview Messages
// ---------------------------------------------------------------------------

export type CatalogInboundMessage =
  | { type: "catalog-data"; data: CatalogData }
  | { type: "inspect-object"; id: string };

export type CatalogOutboundMessage =
  | { type: "open-file"; path: string }
  | { type: "open-dag"; focusTable?: string }
  | { type: "ready" };

// ---------------------------------------------------------------------------
// DAG Layout Types
// ---------------------------------------------------------------------------

export interface LayoutResult {
  x: number[];
  y: number[];
  totalW: number;
  totalH: number;
  idxMap: Record<string, number>;
  adj: number[][];
}

/** Fill/background/border color triple for schema-based node coloring. */
export interface SchemaColorTriple {
  fill: string;
  bg: string;
  border: string;
}
