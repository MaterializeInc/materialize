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
