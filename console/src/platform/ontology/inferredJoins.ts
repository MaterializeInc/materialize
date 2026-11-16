/**
 * @module
 * Relationships the environment's definitions imply, with no ontology table.
 * They come only from view and materialized view definitions and index keys,
 * never from declared joins, so they stand as an independent reading that
 * declared joins can be checked against (`compareWithDeclared`).
 *
 * Two kinds of evidence make an inferred relationship:
 *
 * 1. A join: two relations' columns equated in a view.
 * 2. A passed-through column: a view's output column that is an input key,
 *    as in `"o"."id" AS "org_id"`, references that key. An output column and
 *    the input column it passes through hold the same values, so what either
 *    references, the other does too. This repeats through views over views.
 *
 * Reading the second as a reference is a claim about the columns' domain,
 * not their data: a view may filter, so the input column can hold values the
 * output never shows.
 */

import { displayName } from "./dataProductGraph";
import { IndexKey } from "./indexFit";
import {
  INFERRED_JOIN_EDGE_KIND,
  OntologyEdge,
  OntologyIssue,
  OntologyNode,
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
  source: ColumnRef;
  target: ColumnRef;
  /** False when neither side is a known key, and the order is a guess. */
  directed: boolean;
  views: Set<string>;
  notes: Set<string>;
}

export function inferJoinEdges(input: InferenceInput): OntologyEdge[] {
  const { nodes } = input;
  const isKey = (ref: ColumnRef) =>
    ref.column === "id" ||
    (input.indexKeys.get(ref.objectId) ?? []).some(
      (key) => key.length === 1 && key[0] === ref.column,
    );
  // An index key outranks a column merely named `id`.
  const keyScore = (ref: ColumnRef) =>
    (input.indexKeys.get(ref.objectId) ?? []).some(
      (key) => key.length === 1 && key[0] === ref.column,
    )
      ? 2
      : ref.column === "id"
        ? 1
        : 0;
  // A column named for what it references, `org_id`, is the referencing side.
  const isReference = (ref: ColumnRef) => /.+_id$/.test(ref.column);

  const findings = new Map<string, Finding>();
  const add = (
    source: ColumnRef,
    target: ColumnRef,
    directed: boolean,
    viewId: string,
    note: string | null,
  ) => {
    if (source.objectId === target.objectId) return false;
    if (!hasColumn(nodes, source) || !hasColumn(nodes, target)) return false;
    const key = pairKey(source, target);
    let finding = findings.get(key);
    // Whether this adds a reference the pass-through rounds have not seen.
    let changed = false;
    if (!finding) {
      finding = {
        source,
        target,
        directed,
        views: new Set(),
        notes: new Set(),
      };
      findings.set(key, finding);
      changed = directed;
    } else if (directed && !finding.directed) {
      Object.assign(finding, { source, target, directed });
      changed = true;
    }
    finding.views.add(nodeName(nodes.get(viewId)!));
    if (note) finding.notes.add(note);
    return changed;
  };

  // Joins between a view's inputs.
  for (const [viewId, joins] of input.viewJoins) {
    if (!nodes.has(viewId)) continue;
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
    for (const { left, right } of pairs) {
      const difference =
        keyScore(right) - keyScore(left) ||
        Number(isReference(left)) - Number(isReference(right));
      const [source, target] = difference >= 0 ? [left, right] : [right, left];
      add(source, target, difference !== 0, viewId, null);
    }
  }

  // Columns views pass through, to a fixed point so views over views count.
  // A round continues only after adding a directed finding, and there are
  // finitely many column pairs, so this terminates.
  for (let changed = true; changed;) {
    changed = false;
    const references = new Map<string, ColumnRef[]>();
    for (const finding of findings.values()) {
      if (!finding.directed) continue;
      const list = references.get(refKey(finding.source)) ?? [];
      list.push(finding.target);
      references.set(refKey(finding.source), list);
    }
    for (const [viewId, joins] of input.viewJoins) {
      const view = nodes.get(viewId);
      if (!view) continue;
      for (const { column, source } of joins.outputs) {
        const output = { objectId: viewId, column };
        const from = nodes.get(source.objectId);
        if (!from) continue;
        const passed = `${view.label}.${column} is ${from.label}.${source.column}, passed through.`;
        // A passed key references its source directly. Propagating through
        // it too would add every key it reaches transitively, which grows
        // with the square of a view stack's depth.
        if (isKey(source)) {
          changed = add(output, source, true, viewId, passed) || changed;
          continue;
        }
        for (const target of references.get(refKey(source)) ?? []) {
          changed = add(output, target, true, viewId, passed) || changed;
        }
        // The two columns hold the same values, so what the output references
        // its input column does too.
        for (const target of references.get(refKey(output)) ?? []) {
          changed = add(source, target, true, viewId, passed) || changed;
        }
      }
    }
  }

  return [...findings.values()].map(
    ({ source, target, directed, views, notes }) => ({
      id: [
        INFERRED_JOIN_EDGE_KIND,
        source.objectId,
        source.column,
        target.objectId,
        target.column,
      ].join("|"),
      source: source.objectId,
      target: target.objectId,
      kind: INFERRED_JOIN_EDGE_KIND,
      name: null,
      sourceColumns: [source.column],
      targetColumns: [target.column],
      description: null,
      cardinality: directed ? "many_to_one" : null,
      via: null,
      note:
        [
          ...notes,
          ...(directed
            ? []
            : ["Neither column is a known key, so the direction is a guess."]),
        ].join(" ") || null,
      declaredIn: null,
      inferredFrom: [...views].sort(),
      issues: [],
    }),
  );
}

/**
 * Checks declared joins against inferred ones, in place. Each inferred join a
 * declared one also makes records where it is declared, and each declared
 * join records the views confirming it. A declared join between relations
 * that views relate only on other columns gets a warning, since one of the
 * two is likely wrong.
 */
export function compareWithDeclared(
  declared: OntologyEdge[],
  inferred: OntologyEdge[],
  nodes: Map<string, OntologyNode>,
): OntologyIssue[] {
  const issues: OntologyIssue[] = [];
  const inferredByPair = new Map<string, OntologyEdge>();
  for (const edge of inferred) {
    inferredByPair.set(
      pairKey(
        { objectId: edge.source, column: edge.sourceColumns[0] },
        { objectId: edge.target, column: edge.targetColumns[0] },
      ),
      edge,
    );
  }
  for (const edge of declared) {
    if (edge.sourceColumns.length !== edge.targetColumns.length) continue;
    const confirming = edge.sourceColumns.flatMap((column, i) => {
      const match = inferredByPair.get(
        pairKey(
          { objectId: edge.source, column },
          { objectId: edge.target, column: edge.targetColumns[i] },
        ),
      );
      return match ? [match] : [];
    });
    for (const match of confirming) match.declaredIn = edge.declaredIn;
    edge.inferredFrom = [
      ...new Set(confirming.flatMap((match) => match.inferredFrom)),
    ].sort();
    if (confirming.length > 0) continue;

    const used = inferred
      .filter(
        (other) =>
          (other.source === edge.source && other.target === edge.target) ||
          (other.source === edge.target && other.target === edge.source),
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
