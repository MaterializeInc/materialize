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
 * Relationships the environment's definitions imply, with no ontology table.
 * They come only from view and materialized view definitions and index keys,
 * never from declared joins, so declared joins can be checked against them
 * (`compareWithDeclared`). These are the rules of
 * `mz_internal.mz_inferred_relationships`, applied in the browser.
 *
 * Every edge comes from a single view's definition:
 *
 * - A join: two relations' columns a view equates.
 * - A passed key: a view's output column that is an input key unchanged, as
 *   in `"o"."id" AS "org_id"`, which references that key.
 * - Same values: a view's output column that is any other input column
 *   unchanged.
 *
 * Relationships across several views are the paths through these edges, so
 * none are derived here. Same-values edges claim a shared domain, not shared
 * data: a view may filter, so its input column can hold values its output
 * never shows.
 */

import { displayName } from "./dataProductGraph";
import { IndexKey } from "./indexFit";
import {
  INFERRED_JOIN_EDGE_KIND,
  OntologyEdge,
  OntologyIssue,
  OntologyNode,
  SAME_VALUES_EDGE_KIND,
} from "./model";
import { ColumnRef, ViewJoins } from "./viewJoins";

export interface InferenceInput {
  nodes: Map<string, OntologyNode>;
  /** Joins read out of each view's definition, by the view's object id. */
  viewJoins: Map<string, ViewJoins>;
  /** The keys of each relation's indexes, by object id. */
  indexKeys: Map<string, IndexKey[]>;
}

const refKey = (ref: ColumnRef) => `${ref.objectId}.${ref.column}`;
const pairKey = (a: ColumnRef, b: ColumnRef) =>
  [refKey(a), refKey(b)].sort().join("=");
const sourceRef = (edge: OntologyEdge) => ({
  objectId: edge.source,
  column: edge.sourceColumns[0],
});
const targetRef = (edge: OntologyEdge) => ({
  objectId: edge.target,
  column: edge.targetColumns[0],
});

function nodeName(node: OntologyNode) {
  return node.schemaName
    ? displayName({
        databaseName: node.databaseName,
        schemaName: node.schemaName,
        name: node.label,
      })
    : node.label;
}

function columnOf(node: OntologyNode | undefined, column: string) {
  return node?.columns?.find((c) => c.name === column);
}

/** A column named by a definition must exist, once the columns load. */
function hasColumn(nodes: Map<string, OntologyNode>, ref: ColumnRef) {
  const node = nodes.get(ref.objectId);
  return (
    Boolean(node) &&
    (!node!.columns?.length || Boolean(columnOf(node, ref.column)))
  );
}

interface Finding {
  kind: typeof INFERRED_JOIN_EDGE_KIND | typeof SAME_VALUES_EDGE_KIND;
  source: ColumnRef;
  target: ColumnRef;
  /** False when neither side is a known key, and the order is arbitrary. */
  directed: boolean;
  views: Set<string>;
  notes: Set<string>;
}

export function inferJoinEdges(input: InferenceInput): OntologyEdge[] {
  const { nodes } = input;
  const isIndexKey = (ref: ColumnRef) =>
    (input.indexKeys.get(ref.objectId) ?? []).some(
      (key) => key.length === 1 && key[0] === ref.column,
    );
  // An index key outranks a column merely named `id`.
  const keyScore = (ref: ColumnRef) =>
    isIndexKey(ref) ? 2 : ref.column === "id" ? 1 : 0;
  // A column named for what it references, `org_id`, is the referencing side.
  const isReference = (ref: ColumnRef) => /.+_id$/.test(ref.column);

  const findings = new Map<string, Finding>();
  const add = (
    kind: Finding["kind"],
    source: ColumnRef,
    target: ColumnRef,
    directed: boolean,
    viewId: string,
    note: string | null,
  ) => {
    if (source.objectId === target.objectId) return;
    if (!hasColumn(nodes, source) || !hasColumn(nodes, target)) return;
    const key = `${kind}|${pairKey(source, target)}`;
    const finding = findings.get(key) ?? {
      kind,
      source,
      target,
      directed,
      views: new Set<string>(),
      notes: new Set<string>(),
    };
    finding.views.add(nodeName(nodes.get(viewId)!));
    if (note) finding.notes.add(note);
    findings.set(key, finding);
  };

  for (const [viewId, joins] of input.viewJoins) {
    const view = nodes.get(viewId);
    if (!view) continue;
    const pairs = [...joins.pairs];
    for (const using of joins.usingJoins) {
      for (const column of using.columns) {
        // The left side is whichever relation joined before has the column.
        const owners = using.candidates.filter((id) =>
          columnOf(nodes.get(id), column),
        );
        const left =
          owners.length === 1
            ? owners[0]
            : using.candidates.length === 1
              ? using.candidates[0]
              : null;
        if (left) {
          pairs.push({
            left: { objectId: left, column },
            right: { objectId: using.right, column },
          });
        }
      }
    }
    // Direction depends only on the two columns, so a pair is directed the
    // same way in every view that joins it.
    for (const { left, right } of pairs) {
      const difference =
        keyScore(right) - keyScore(left) ||
        Number(isReference(left)) - Number(isReference(right));
      const [source, target] = difference >= 0 ? [left, right] : [right, left];
      add(
        INFERRED_JOIN_EDGE_KIND,
        source,
        target,
        difference !== 0,
        viewId,
        null,
      );
    }
    for (const { column, source } of joins.outputs) {
      const from = nodes.get(source.objectId);
      if (!from) continue;
      const passed = `${view.label}.${column} is ${from.label}.${source.column}, passed through.`;
      const isKey = source.column === "id" || isIndexKey(source);
      add(
        isKey ? INFERRED_JOIN_EDGE_KIND : SAME_VALUES_EDGE_KIND,
        { objectId: viewId, column },
        source,
        true,
        viewId,
        passed,
      );
    }
  }

  return [...findings.values()].map(
    ({ kind, source, target, directed, views, notes }) => ({
      id: [
        kind,
        source.objectId,
        source.column,
        target.objectId,
        target.column,
      ].join("|"),
      source: source.objectId,
      target: target.objectId,
      kind,
      name: null,
      sourceColumns: [source.column],
      targetColumns: [target.column],
      description: null,
      cardinality:
        kind === INFERRED_JOIN_EDGE_KIND && directed ? "many_to_one" : null,
      via: null,
      note:
        [
          ...notes,
          ...(directed
            ? []
            : [
                "Neither column is a known key, so the direction is arbitrary.",
              ]),
        ].join(" ") || null,
      declaredIn: null,
      inferredFrom: [...views].sort(),
      issues: [],
    }),
  );
}

/**
 * The inferred edges between columns, both ways, for following paths. Each
 * entry names the edge it came from.
 */
function columnGraph(edges: OntologyEdge[]) {
  const adjacent = new Map<string, { column: string; edge: OntologyEdge }[]>();
  const link = (a: string, b: string, edge: OntologyEdge) => {
    const list = adjacent.get(a) ?? [];
    list.push({ column: b, edge });
    adjacent.set(a, list);
  };
  for (const edge of edges) {
    const a = refKey(sourceRef(edge));
    const b = refKey(targetRef(edge));
    link(a, b, edge);
    link(b, a, edge);
  }
  return adjacent;
}

/** The edges of a shortest path between two columns, or null for none. */
function findPath(
  adjacent: ReturnType<typeof columnGraph>,
  from: string,
  to: string,
): OntologyEdge[] | null {
  const via = new Map<string, { previous: string; edge: OntologyEdge }>();
  const seen = new Set([from]);
  let frontier = [from];
  while (frontier.length > 0 && !seen.has(to)) {
    const next: string[] = [];
    for (const column of frontier) {
      for (const step of adjacent.get(column) ?? []) {
        if (seen.has(step.column)) continue;
        seen.add(step.column);
        via.set(step.column, { previous: column, edge: step.edge });
        next.push(step.column);
      }
    }
    frontier = next;
  }
  if (!seen.has(to)) return null;
  const path: OntologyEdge[] = [];
  for (let at = to; at !== from;) {
    const step = via.get(at)!;
    path.push(step.edge);
    at = step.previous;
  }
  return path;
}

/**
 * Checks declared joins against inferred edges, in place. A declared join
 * is confirmed when a path of inferred edges connects its columns, and
 * records the views along the shortest one. An inferred edge with exactly
 * the declared columns records where it is declared. A declared join with
 * no such path, between relations that inferred joins relate on other
 * columns, gets a warning, since one of the two is likely wrong.
 */
export function compareWithDeclared(
  declared: OntologyEdge[],
  inferred: OntologyEdge[],
  nodes: Map<string, OntologyNode>,
): OntologyIssue[] {
  const issues: OntologyIssue[] = [];
  const adjacent = columnGraph(inferred);
  const byPair = new Map(
    inferred
      .filter((edge) => edge.kind === INFERRED_JOIN_EDGE_KIND)
      .map((edge) => [pairKey(sourceRef(edge), targetRef(edge)), edge]),
  );
  for (const edge of declared) {
    if (edge.sourceColumns.length !== edge.targetColumns.length) continue;
    const views = new Set<string>();
    let confirmed = 0;
    edge.sourceColumns.forEach((column, i) => {
      const from = { objectId: edge.source, column };
      const to = { objectId: edge.target, column: edge.targetColumns[i] };
      const direct = byPair.get(pairKey(from, to));
      if (direct) direct.declaredIn = edge.declaredIn;
      const path = findPath(adjacent, refKey(from), refKey(to));
      if (!path) return;
      confirmed++;
      for (const step of path) step.inferredFrom.forEach((v) => views.add(v));
    });
    edge.inferredFrom = [...views].sort();
    if (confirmed > 0) continue;

    const used = inferred
      .filter(
        (other) =>
          other.kind === INFERRED_JOIN_EDGE_KIND &&
          ((other.source === edge.source && other.target === edge.target) ||
            (other.source === edge.target && other.target === edge.source)),
      )
      .map((other) =>
        other.source === edge.source
          ? `${other.sourceColumns[0]} = ${other.targetColumns[0]}`
          : `${other.targetColumns[0]} = ${other.sourceColumns[0]}`,
      )
      .sort();
    if (used.length === 0) continue;
    const issue: OntologyIssue = {
      severity: "warning",
      message: `Views relate ${nodes.get(edge.source)?.label} to ${nodes.get(edge.target)?.label} on ${used.join(", ")}, never on the declared columns.`,
      declaredIn: edge.declaredIn,
      edgeId: edge.id,
    };
    edge.issues.push(issue);
    issues.push(issue);
  }
  return issues;
}

/**
 * Same-values edges on a path to a join, inferred or declared. A view passes
 * every column it selects through, so most same-values edges link columns no
 * join uses, such as names and amounts, and drawing them would bury the
 * relationships.
 */
export function sameValuesOnJoinPaths(
  sameValues: OntologyEdge[],
  joins: OntologyEdge[],
): OntologyEdge[] {
  const adjacent = columnGraph(sameValues);
  const reached = new Set<string>();
  const frontier: string[] = [];
  for (const edge of joins) {
    edge.sourceColumns.forEach((column) =>
      frontier.push(refKey({ objectId: edge.source, column })),
    );
    edge.targetColumns.forEach((column) =>
      frontier.push(refKey({ objectId: edge.target, column })),
    );
  }
  frontier.forEach((column) => reached.add(column));
  while (frontier.length > 0) {
    const column = frontier.pop()!;
    for (const step of adjacent.get(column) ?? []) {
      if (reached.has(step.column)) continue;
      reached.add(step.column);
      frontier.push(step.column);
    }
  }
  return sameValues.filter((edge) => reached.has(refKey(sourceRef(edge))));
}
