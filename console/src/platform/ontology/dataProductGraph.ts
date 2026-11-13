// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  JOIN_EDGE_KIND,
  LINEAGE_EDGE_KIND,
  OntologyColumn,
  OntologyEdge,
  OntologyGraph,
  OntologyIssue,
  OntologyNode,
} from "./model";

/** Object types the data products lens draws as entities. */
export const ENTITY_OBJECT_TYPES = new Set([
  "table",
  "view",
  "materialized-view",
  "source",
]);

export interface CatalogRelation {
  id: string;
  name: string;
  objectType: string;
  schemaName: string;
  databaseName: string | null;
  clusterName: string | null;
  sourceType: string | null;
}

export interface OntologyTableRef {
  id: string;
  name: string;
  schemaName: string;
  databaseName: string | null;
}

/** One row of an ontology table, tagged with the table it came from. */
export interface OntologyRow {
  ontologyId: string;
  tableName: string;
  columns: string[];
  referencedTable: string;
  referencedColumns: string[];
}

export interface LineageEdge {
  childId: string;
  parentId: string;
}

export interface DataProductGraphInput {
  relations: CatalogRelation[];
  ontologyTables: OntologyTableRef[];
  ontologyRows: OntologyRow[];
  lineage: LineageEdge[];
  /** Columns by object id. A missing id means its columns are not loaded. */
  columns: Map<string, OntologyColumn[]>;
  /** Object comments by object id. */
  comments: Map<string, string>;
  /** Fully qualified, quoted names listed by `mz_mcp_data_products`. */
  dataProducts: Set<string>;
}

export function quoteIdent(ident: string) {
  return `"${ident.replace(/"/g, '""')}"`;
}

export function qualifiedName(relation: {
  databaseName: string | null;
  schemaName: string;
  name: string;
}) {
  return [relation.databaseName, relation.schemaName, relation.name]
    .filter((part): part is string => part !== null)
    .map(quoteIdent)
    .join(".");
}

const SIMPLE_IDENTIFIER = /^[a-z_][a-z0-9_$]*$/;

/** A qualified name for reading, quoting only the parts that need it. */
export function displayName(relation: {
  databaseName: string | null;
  schemaName: string;
  name: string;
}) {
  return [relation.databaseName, relation.schemaName, relation.name]
    .filter((part): part is string => part !== null)
    .map((part) => (SIMPLE_IDENTIFIER.test(part) ? part : quoteIdent(part)))
    .join(".");
}

export function groupName(relation: {
  databaseName: string | null;
  schemaName: string;
}) {
  return relation.databaseName
    ? `${relation.databaseName}.${relation.schemaName}`
    : relation.schemaName;
}

/**
 * Splits a possibly qualified SQL name into its parts, folding unquoted parts
 * to lower case as the SQL parser does. Returns null for malformed input.
 */
export function parseQualifiedName(name: string): string[] | null {
  const parts: string[] = [];
  let i = 0;
  const input = name.trim();
  while (i < input.length) {
    let part = "";
    if (input[i] === '"') {
      i++;
      for (;;) {
        if (i >= input.length) return null;
        if (input[i] === '"') {
          if (input[i + 1] === '"') {
            part += '"';
            i += 2;
            continue;
          }
          i++;
          break;
        }
        part += input[i++];
      }
    } else {
      while (i < input.length && input[i] !== ".") part += input[i++];
      part = part.trim().toLowerCase();
    }
    if (part === "") return null;
    parts.push(part);
    if (i < input.length) {
      if (input[i] !== ".") return null;
      i++;
      if (i >= input.length) return null;
    }
  }
  return parts.length >= 1 && parts.length <= 3 ? parts : null;
}

export type NameResolution =
  | { status: "resolved"; relation: CatalogRelation }
  | { status: "missing" }
  | { status: "ambiguous"; candidates: CatalogRelation[] }
  | { status: "invalid" };

/**
 * Resolves a name written in an ontology table. Unqualified and
 * schema-qualified names resolve first relative to the ontology table's own
 * schema and database, then anywhere they are unique, since the table usually
 * sits beside the relations it describes but may be shared across schemas.
 */
export function resolveRelationName(
  name: string,
  context: OntologyTableRef,
  relationsByName: Map<string, CatalogRelation[]>,
): NameResolution {
  const parts = parseQualifiedName(name);
  if (!parts) return { status: "invalid" };
  const candidates = relationsByName.get(parts[parts.length - 1]) ?? [];
  const matching = candidates.filter((relation) => {
    if (parts.length === 3) {
      return (
        relation.databaseName === parts[0] && relation.schemaName === parts[1]
      );
    }
    if (parts.length === 2) return relation.schemaName === parts[0];
    return true;
  });
  const narrowed = [
    matching.filter(
      (relation) =>
        relation.databaseName === context.databaseName &&
        relation.schemaName === context.schemaName,
    ),
    matching.filter(
      (relation) => relation.databaseName === context.databaseName,
    ),
    matching,
  ];
  for (const scope of narrowed) {
    if (scope.length === 1) return { status: "resolved", relation: scope[0] };
    if (scope.length > 1) return { status: "ambiguous", candidates: scope };
  }
  return { status: "missing" };
}

const NUMERIC_TYPES = new Set([
  "smallint",
  "integer",
  "bigint",
  "int2",
  "int4",
  "int8",
  "uint2",
  "uint4",
  "uint8",
  "numeric",
  "real",
  "double precision",
  "float4",
  "float8",
]);

/**
 * Coarse type families. Joining an `integer` to a `bigint` is fine, joining a
 * `text` to a `bigint` fails to plan, so only a family mismatch is reported.
 */
export function typeFamily(type: string) {
  const base = type.replace(/\(.*\)$/, "").trim();
  if (NUMERIC_TYPES.has(base)) return "number";
  if (
    base === "text" ||
    base.startsWith("character") ||
    base === "varchar" ||
    base === "char"
  ) {
    return "text";
  }
  if (base.startsWith("timestamp")) return "timestamp";
  return base;
}

function relationNode(
  relation: CatalogRelation,
  input: DataProductGraphInput,
): OntologyNode {
  const relationName = qualifiedName(relation);
  return {
    id: relation.id,
    label: relation.name,
    group: groupName(relation),
    kind: relation.objectType,
    relation: relationName,
    description: input.comments.get(relation.id) ?? null,
    columns: input.columns.get(relation.id),
    primaryKey: [],
    objectId: relation.id,
    databaseName: relation.databaseName,
    schemaName: relation.schemaName,
    clusterName: relation.clusterName,
    isDataProduct: input.dataProducts.has(relationName),
    isUnresolved: false,
  };
}

function unresolvedNode(
  id: string,
  name: string,
  table: OntologyTableRef,
): OntologyNode {
  return {
    id,
    label: name,
    group: groupName(table),
    kind: "unresolved",
    relation: null,
    description: null,
    columns: [],
    primaryKey: [],
    objectId: null,
    databaseName: table.databaseName,
    schemaName: table.schemaName,
    clusterName: null,
    isDataProduct: false,
    isUnresolved: true,
  };
}

/**
 * Assembles the data products ontology: user relations as entities, the joins
 * curated in ontology tables, and lineage between the relations. Every
 * ontology row is checked against the catalog, and the problems are reported
 * on the edge and in `issues` rather than dropped, since an unresolvable row
 * is exactly what an agent reading the table would trip over.
 */
export function buildDataProductGraph(
  input: DataProductGraphInput,
): OntologyGraph {
  const ontologyIds = new Set(input.ontologyTables.map((table) => table.id));
  const relations = input.relations.filter(
    (relation) =>
      ENTITY_OBJECT_TYPES.has(relation.objectType) &&
      relation.sourceType !== "progress" &&
      !ontologyIds.has(relation.id),
  );
  const nodes = new Map<string, OntologyNode>();
  const relationsByName = new Map<string, CatalogRelation[]>();
  for (const relation of relations) {
    nodes.set(relation.id, relationNode(relation, input));
    const byName = relationsByName.get(relation.name) ?? [];
    byName.push(relation);
    relationsByName.set(relation.name, byName);
  }

  const tablesById = new Map(
    input.ontologyTables.map((table) => [table.id, table]),
  );
  const edges: OntologyEdge[] = [];
  const issues: OntologyIssue[] = [];

  for (const row of input.ontologyRows) {
    const table = tablesById.get(row.ontologyId);
    if (!table) continue;
    const declaredIn = displayName(table);
    const edgeId = [
      JOIN_EDGE_KIND,
      row.ontologyId,
      row.tableName,
      row.columns.join(","),
      row.referencedTable,
      row.referencedColumns.join(","),
    ].join("|");
    const edgeIssues: OntologyIssue[] = [];
    const report = (severity: OntologyIssue["severity"], message: string) =>
      edgeIssues.push({ severity, message, declaredIn, edgeId });

    const endpoint = (name: string) => {
      const resolution = resolveRelationName(name, table, relationsByName);
      if (resolution.status === "resolved") return resolution.relation.id;
      if (resolution.status === "ambiguous") {
        report(
          "error",
          `"${name}" is ambiguous, it matches ${resolution.candidates
            .map(displayName)
            .join(", ")}. Qualify it with a schema.`,
        );
      } else if (resolution.status === "invalid") {
        report("error", `"${name}" is not a valid relation name.`);
      } else {
        report("error", `"${name}" does not match any relation.`);
      }
      const id = `unresolved|${row.ontologyId}|${name}`;
      if (!nodes.has(id)) nodes.set(id, unresolvedNode(id, name, table));
      return id;
    };
    const source = endpoint(row.tableName);
    const target = endpoint(row.referencedTable);

    if (row.columns.length === 0 || row.referencedColumns.length === 0) {
      report("error", "The relationship lists no join columns.");
    } else if (row.columns.length !== row.referencedColumns.length) {
      report(
        "error",
        `${row.columns.length} column(s) pair with ${row.referencedColumns.length} referenced column(s). They pair by position, so the counts must match.`,
      );
    }
    const sourceColumns = input.columns.get(source);
    const targetColumns = input.columns.get(target);
    const findColumn = (
      columns: OntologyColumn[] | undefined,
      nodeId: string,
      name: string,
    ) => {
      if (!columns || nodes.get(nodeId)?.isUnresolved) return undefined;
      const column = columns.find((c) => c.name === name);
      if (!column) {
        report("error", `${nodes.get(nodeId)?.label} has no column "${name}".`);
      }
      return column;
    };
    row.columns.forEach((name, i) => {
      const left = findColumn(sourceColumns, source, name);
      const rightName = row.referencedColumns[i];
      if (rightName === undefined) return;
      const right = findColumn(targetColumns, target, rightName);
      if (left && right && typeFamily(left.type) !== typeFamily(right.type)) {
        report(
          "warning",
          `${name} (${left.type}) pairs with ${rightName} (${right.type}). The join needs a cast.`,
        );
      }
    });
    // Columns past the shorter side's length are already covered by the
    // count mismatch above, but still name a missing referenced column.
    row.referencedColumns
      .slice(row.columns.length)
      .forEach((name) => findColumn(targetColumns, target, name));

    edges.push({
      id: edgeId,
      source,
      target,
      kind: JOIN_EDGE_KIND,
      name: null,
      sourceColumns: row.columns,
      targetColumns: row.referencedColumns,
      description: null,
      cardinality: "many_to_one",
      via: null,
      note: null,
      declaredIn,
      issues: edgeIssues,
    });
    issues.push(...edgeIssues);
  }

  for (const { childId, parentId } of input.lineage) {
    if (childId === parentId) continue;
    if (!nodes.has(childId) || !nodes.has(parentId)) continue;
    edges.push({
      id: `${LINEAGE_EDGE_KIND}|${childId}|${parentId}`,
      source: childId,
      target: parentId,
      kind: LINEAGE_EDGE_KIND,
      name: null,
      sourceColumns: [],
      targetColumns: [],
      description: null,
      cardinality: null,
      via: null,
      note: null,
      declaredIn: null,
      issues: [],
    });
  }

  // Key columns are the referenced side of joins: the docs pattern pairs
  // foreign key columns with the referenced table's primary or unique key.
  for (const edge of edges) {
    if (edge.kind !== JOIN_EDGE_KIND) continue;
    const target = nodes.get(edge.target);
    if (!target || target.isUnresolved) continue;
    for (const column of edge.targetColumns) {
      if (!target.primaryKey.includes(column)) target.primaryKey.push(column);
    }
  }

  return {
    nodes: [...nodes.values()].sort((a, b) => a.id.localeCompare(b.id)),
    edges: edges.sort((a, b) => a.id.localeCompare(b.id)),
    issues,
  };
}
