// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { quoteIdent } from "./dataProductGraph";
import { LINEAGE_EDGE_KIND, OntologyEdge, OntologyNode } from "./model";

const SOURCE_ALIAS = "a";
const TARGET_ALIAS = "b";

/**
 * Writes the join a relationship describes, or null when it has no column
 * pairing to join on. Columns pair by position, as in the ontology table.
 */
export function buildJoinSql(
  edge: OntologyEdge,
  source: OntologyNode,
  target: OntologyNode,
): string | null {
  if (
    edge.kind === LINEAGE_EDGE_KIND ||
    !source.relation ||
    !target.relation ||
    edge.sourceColumns.length === 0 ||
    edge.sourceColumns.length !== edge.targetColumns.length
  ) {
    return null;
  }
  const conditions = edge.sourceColumns.map(
    (column, i) =>
      `${SOURCE_ALIAS}.${quoteIdent(column)} = ${TARGET_ALIAS}.${quoteIdent(edge.targetColumns[i])}`,
  );
  const lines = [
    "SELECT *",
    `FROM ${source.relation} AS ${SOURCE_ALIAS}`,
    `JOIN ${target.relation} AS ${TARGET_ALIAS}`,
    `  ON ${conditions.join("\n AND ")}`,
  ];
  if (edge.via) {
    lines.unshift(
      `-- ${source.label} and ${target.label} use different id domains.`,
      `-- Bridge them through ${edge.via} before joining.`,
    );
  }
  return `${lines.join("\n")}\nLIMIT 100;`;
}
