// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { quoteIdent } from "./dataProductGraph";
import {
  INFERRED_JOIN_EDGE_KIND,
  isErrorEdge,
  JOIN_EDGE_KIND,
  OntologyEdge,
  OntologyIssue,
  OntologyNode,
} from "./model";

/** An index's key columns in order, null for a key expression. */
export type IndexKey = (string | null)[];

const formatKey = (key: IndexKey) =>
  `(${key.map((part) => part ?? "<expression>").join(", ")})`;

/**
 * Warns where a join has an index on its relation that covers its columns
 * but is keyed on more, and none keyed on exactly them.
 *
 * A join reuses an index only when the index key is exactly the join's
 * columns, in any order. Observed on v26.50: with an index on
 * `(id, seller, item, end_time)`, a join on `id` reads it as a full scan and
 * arranges the relation by `id` again, and an index on `(id)` is not used by
 * a join on `(id, seller)` either. A default index, keyed on every column of
 * a relation without a known unique key, is the usual way to get here.
 */
export function tooWideIndexIssues(
  edges: OntologyEdge[],
  nodes: Map<string, OntologyNode>,
  indexKeys: Map<string, IndexKey[]>,
): OntologyIssue[] {
  const issues: OntologyIssue[] = [];
  for (const edge of edges) {
    if (edge.kind !== JOIN_EDGE_KIND && edge.kind !== INFERRED_JOIN_EDGE_KIND) {
      continue;
    }
    if (isErrorEdge(edge) || edge.sourceColumns.length === 0) continue;
    // A declared join the inference also found is warned about once, there.
    if (edge.kind === INFERRED_JOIN_EDGE_KIND && edge.declaredIn) continue;
    const sides: [string, string[]][] = [
      [edge.source, edge.sourceColumns],
      [edge.target, edge.targetColumns],
    ];
    for (const [nodeId, columns] of sides) {
      const node = nodes.get(nodeId);
      const keys = indexKeys.get(nodeId);
      if (!node?.relation || !keys) continue;
      const joinColumns = new Set(columns);
      const covers = (key: IndexKey) =>
        columns.every((column) => key.includes(column));
      const isExact = (key: IndexKey) =>
        key.length === joinColumns.size && covers(key);
      if (keys.some(isExact)) continue;
      const wider = keys.filter(covers);
      if (wider.length === 0) continue;
      const indexes =
        wider.length === 1
          ? `The index on ${node.label} ${formatKey(wider[0])} is`
          : `The indexes on ${node.label} ${wider.map(formatKey).join(", ")} are`;
      const createIndex = `CREATE INDEX ON ${node.relation} (${[...joinColumns].map(quoteIdent).join(", ")})`;
      issues.push({
        severity: "warning",
        message: `${indexes} wider than this join's key ${formatKey([...joinColumns])}, so the join cannot use ${wider.length === 1 ? "it" : "them"} and arranges ${node.label} again. ${createIndex} would serve it.`,
        declaredIn: edge.declaredIn,
        edgeId: edge.id,
      });
    }
  }
  return issues;
}

/** Up to three view names, unqualified, then how many more. */
function listViews(views: Set<string>) {
  const names = [...views].map((view) => view.split(".").pop()!).sort();
  return names.length <= 3
    ? names.join(", ")
    : `${names.slice(0, 3).join(", ")} and ${names.length - 3} more`;
}

/**
 * Suggests indexes that would let work share an arrangement or read from
 * memory:
 *
 * - Columns two or more views join a relation on, with no index whose key
 *   includes them all. Each view's dataflow, and each query running one
 *   of those views, otherwise arranges the relation by those columns itself.
 *   An index keyed on exactly them is reused instead, by work on the
 *   index's cluster. A covering but wider index is `tooWideIndexIssues`'s.
 * - A materialized view listed as an MCP data product with no index.
 *   Agents' reads then come from storage, not memory.
 */
export function indexSuggestions(
  edges: OntologyEdge[],
  nodes: Map<string, OntologyNode>,
  indexKeys: Map<string, IndexKey[]>,
): OntologyIssue[] {
  const issues: OntologyIssue[] = [];
  const joined = new Map<
    string,
    {
      node: OntologyNode;
      columns: string[];
      views: Set<string>;
      edgeId: string;
    }
  >();
  for (const edge of edges) {
    if (!edge.joinedIn?.length || isErrorEdge(edge)) continue;
    const sides: [string, string[]][] = [
      [edge.source, edge.sourceColumns],
      [edge.target, edge.targetColumns],
    ];
    for (const [nodeId, columns] of sides) {
      const node = nodes.get(nodeId);
      if (!node?.relation) continue;
      const key = `${nodeId}|${[...columns].sort().join(",")}`;
      const entry = joined.get(key) ?? {
        node,
        columns,
        views: new Set<string>(),
        edgeId: edge.id,
      };
      edge.joinedIn.forEach((view) => entry.views.add(view));
      joined.set(key, entry);
    }
  }
  for (const { node, columns, views, edgeId } of joined.values()) {
    if (views.size < 2) continue;
    const keys = indexKeys.get(node.id) ?? [];
    if (keys.some((key) => columns.every((column) => key.includes(column)))) {
      continue;
    }
    issues.push({
      severity: "suggestion",
      message: `Views ${listViews(views)} join ${node.label} on ${formatKey(columns)}, and each arranges ${node.label} by it. CREATE INDEX ON ${node.relation} (${columns.map(quoteIdent).join(", ")}) would let them share one arrangement on its cluster.`,
      declaredIn: null,
      edgeId,
    });
  }

  for (const node of nodes.values()) {
    if (!node.isDataProduct || node.kind !== "materialized-view") continue;
    if (!node.relation || (indexKeys.get(node.id) ?? []).length > 0) continue;
    // Several key columns are separate keys, so one composite index on them
    // would serve none.
    const createIndex =
      node.primaryKey.length === 1
        ? `CREATE INDEX ON ${node.relation} (${node.primaryKey.map(quoteIdent).join(", ")})`
        : `CREATE DEFAULT INDEX ON ${node.relation}`;
    issues.push({
      severity: "suggestion",
      message: `Agents read the data product ${node.label} from storage, since no index holds it in memory. ${createIndex} would serve their reads from memory.`,
      declaredIn: null,
      edgeId: null,
      nodeId: node.id,
    });
  }
  return issues;
}
