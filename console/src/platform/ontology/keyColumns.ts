// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { OntologyEdge, OntologyNode } from "./model";

/**
 * `pk` marks a column other relations join to, `fk` a column that joins to
 * another relation. Materialize declares neither, so both are read from the
 * ontology: the primary keys it records and the column pairs of its
 * relationships.
 */
export type KeyRole = "pk" | "fk" | "pkfk";

export interface KeyColumn {
  name: string;
  /** Null when the column is not known, see `missing`. */
  type: string | null;
  role: KeyRole;
  /** Named by a relationship, but not a column of the relation. */
  missing: boolean;
}

/** Key rows drawn on a card before the rest fold into "+N more". */
export const MAX_KEY_ROWS = 4;
export const CARD_HEADER_HEIGHT = 54;
export const KEY_ROW_HEIGHT = 18;
const CARD_BOTTOM_PADDING = 6;
/** A card without key rows. */
export const COMPACT_CARD_HEIGHT = 60;

/** Whether a relationship pairs columns, and so can be drawn row to row. */
export function pairsColumns(edge: OntologyEdge) {
  return edge.sourceColumns.length > 0 && edge.targetColumns.length > 0;
}

/**
 * The key columns of an entity, primary keys first, then in column order.
 * Every relationship in `edges` counts, drawn or not, so a card does not
 * change shape as filters change.
 */
export function keyColumns(
  node: OntologyNode,
  edges: OntologyEdge[],
): KeyColumn[] {
  if (node.isUnresolved) return [];
  const pk = new Set(node.primaryKey);
  const fk = new Set<string>();
  for (const edge of edges) {
    if (!pairsColumns(edge)) continue;
    if (edge.source === node.id) {
      for (const column of edge.sourceColumns) fk.add(column);
    }
    if (edge.target === node.id) {
      for (const column of edge.targetColumns) pk.add(column);
    }
  }
  const position = new Map(
    (node.columns ?? []).map((column, i) => [column.name, i]),
  );
  const columnsKnown = node.columns !== undefined && node.columns.length > 0;
  const keys: KeyColumn[] = [...new Set([...pk, ...fk])].map((name) => {
    const column = node.columns?.find((c) => c.name === name);
    return {
      name,
      type: column?.type ?? null,
      role: pk.has(name) ? (fk.has(name) ? "pkfk" : "pk") : "fk",
      missing: columnsKnown && !column,
    };
  });
  const rank = (key: KeyColumn) => (key.role === "fk" ? 1 : 0);
  return keys.sort(
    (a, b) =>
      rank(a) - rank(b) ||
      (position.get(a.name) ?? Infinity) - (position.get(b.name) ?? Infinity) ||
      a.name.localeCompare(b.name),
  );
}

/** The rows a card draws, and how many more it folds away. */
export function visibleKeyRows(keys: KeyColumn[]) {
  // Folding a single row would cost as much room as drawing it.
  if (keys.length <= MAX_KEY_ROWS + 1) return { rows: keys, hidden: 0 };
  return {
    rows: keys.slice(0, MAX_KEY_ROWS),
    hidden: keys.length - MAX_KEY_ROWS,
  };
}

export function cardHeight(keys: KeyColumn[] | undefined) {
  if (!keys || keys.length === 0) return COMPACT_CARD_HEIGHT;
  const { rows, hidden } = visibleKeyRows(keys);
  const lines = rows.length + (hidden > 0 ? 1 : 0);
  return CARD_HEADER_HEIGHT + lines * KEY_ROW_HEIGHT + CARD_BOTTOM_PADDING;
}

/**
 * Where on a card, measured from its top, a relationship on `column` attaches:
 * the column's row, the "+N more" row when the column is folded away, or the
 * card's middle when it draws no rows.
 */
export function keyRowOffset(keys: KeyColumn[] | undefined, column: string) {
  if (!keys || keys.length === 0) return COMPACT_CARD_HEIGHT / 2;
  const { rows } = visibleKeyRows(keys);
  const index = rows.findIndex((key) => key.name === column);
  const line = index === -1 ? rows.length : index;
  return CARD_HEADER_HEIGHT + line * KEY_ROW_HEIGHT + KEY_ROW_HEIGHT / 2;
}
