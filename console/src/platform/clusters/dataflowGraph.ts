// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Channel,
  LirOperator,
  Operator,
} from "~/api/materialize/useDataflowStructure";
import { assert } from "~/util";
import { formatBytesShort } from "~/utils/format";

export interface EnrichedOperator extends Operator {
  // First-level.
  channelsInScope: Channel[];
  children: EnrichedOperator[];
  transitiveArrangementRecords: bigint | null;
  transitiveArrangementSizes: bigint | null;
}

export type EnrichedLirOperator = {
  lir_id: string;
  operator: string;
  addresses: string[];
};

function groupBy<T, K>(values: T[], group: (item: T) => K): Map<K, T[]> {
  const output = new Map();
  for (const v of values) {
    const k = group(v);
    if (!output.has(k)) {
      output.set(k, []);
    }
    output.get(k)!.push(v);
  }
  return output;
}

// Returns a map of (stringified) operator address to corresponding operators,
// as well as a designated root.
export function collateOperators(
  operators: Operator[],
  channels: Channel[],
  lirOperators: LirOperator[],
): [Map<string, EnrichedOperator>, EnrichedOperator, EnrichedLirOperator[]] {
  const scopes = groupBy(operators, (o) => o.parentId);
  const channelsByParentScope = groupBy(channels, (ch) =>
    stringifyAddress(ch.fromOperatorAddress.slice(0, -1)),
  );

  const roots = scopes.get(null) || [];

  function walk(
    op: Operator,
    m: Map<string, EnrichedOperator>,
  ): EnrichedOperator {
    assert(!m.has(stringifyAddress(op.address)));
    const children = (scopes.get(op.id) || []).map((ch) => walk(ch, m));
    const channelsInScope =
      channelsByParentScope.get(stringifyAddress(op.address)) || [];
    const ret = {
      ...op,
      children,
      channelsInScope,
      transitiveArrangementRecords:
        children
          .map((child) => child.transitiveArrangementRecords || 0n)
          .reduce((a, b) => a + b, 0n) + (op.arrangementRecords || 0n),
      transitiveArrangementSizes:
        children
          .map((child) => child.transitiveArrangementSizes || 0n)
          .reduce((a, b) => a + b, 0n) + (op.arrangementSizes || 0n),
    };
    m.set(stringifyAddress(ret.address), ret);
    return ret;
  }

  const enrichedLirOperators: EnrichedLirOperator[] = lirOperators.map(
    (lirOp) => ({
      lir_id: lirOp.lir_id,
      operator: lirOp.operator,
      addresses: lirOp.addresses.map((addr) => stringifyAddress(addr)),
    }),
  );

  const m = new Map();

  const enrichedRoots = roots.map((r) => walk(r, m));
  assert(enrichedRoots.length == 1);
  return [m, enrichedRoots[0], enrichedLirOperators];
}

const noArrangementRegionColor = "#12b886";
const noArrangementOperatorColor = "#ffffff";
const arrangementRegionColor = "#7950f2";
const arrangementOperatorColor = "#fab005";

/**
 * Renders an operator address as a key usable both as a DOT identifier and as
 * the SVG element id that {@link scopeToGv} round-trips back through
 * `onClickedNode`.
 *
 * Each component goes through `parseInt`, so the result contains only digits,
 * brackets and commas regardless of what the catalog reported.
 */
export function stringifyAddress(address: string[]) {
  return JSON.stringify(address.map((val) => parseInt(val)));
}

/**
 * Renders `value` as a DOT quoted string, surrounding quotes included.
 *
 * NOTE: backslashes must be escaped before quotes. Escaping only quotes lets a
 * backslash in `value` pair with the one we insert to form `\\`, which
 * Graphviz reads as a single literal backslash, leaving the quote after it free
 * to terminate the string. Everything past that point is then parsed as DOT
 * rather than as text, which is enough to inject attributes such as `href` and
 * turn a rendered node into a link.
 */
function dotString(value: string): string {
  return `"${value.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
}

type DotAttributes = Record<string, string | undefined>;

/**
 * Renders a DOT attribute list, `[key="value",...]`, or the empty string when
 * no attribute is set. Attributes whose value is `undefined` are omitted.
 *
 * Every value is escaped by {@link dotString}, so going through this function
 * is the only sanctioned way to place catalog-derived text in an attribute
 * position. Interpolating an attribute by hand reopens the injection this
 * function exists to close.
 */
function dotAttributes(attributes: DotAttributes): string {
  const rendered: string[] = [];
  for (const [key, value] of Object.entries(attributes)) {
    if (value === undefined) continue;
    rendered.push(`${key}=${dotString(value)}`);
  }
  return rendered.length > 0 ? `[${rendered.join(",")}]` : "";
}

export function scopeToGv(
  scope: EnrichedOperator,
  lir_operators: EnrichedLirOperator[],
): string {
  const chunks = ["digraph {", 'node [style="filled",shape=box];'];
  const addresses = new Set<string>();
  for (const op of scope.children) {
    const isRegion = op.children.length !== 0;
    const hasArrangedData = (op.transitiveArrangementRecords || 0n) > 0n;
    let fillColor;
    if (isRegion) {
      if (hasArrangedData) {
        fillColor = arrangementRegionColor;
      } else {
        fillColor = noArrangementRegionColor;
      }
    } else {
      if (hasArrangedData) {
        fillColor = arrangementOperatorColor;
      } else {
        fillColor = noArrangementOperatorColor;
      }
    }
    const nodeLabelFields = [op.name];
    if (hasArrangedData) {
      nodeLabelFields.push(
        `${op.transitiveArrangementRecords} arranged records`,
      );
      nodeLabelFields.push(
        formatBytesShort(BigInt(op.transitiveArrangementSizes || 0)),
      );
    }
    if (op.elapsedNs > 0) {
      nodeLabelFields.push(
        `scheduled ${Math.round(op.elapsedNs / 1_000_000_000)}s`,
      );
    }

    const opAddressString = stringifyAddress(op.address);
    addresses.add(opAddressString);

    const nodeGv = `${dotString(opAddressString)} ${dotAttributes({
      fillcolor: fillColor,
      tooltip: `Lir ID ${op.lirId}: ${op.lirOperator}`,
      id: opAddressString,
      label: nodeLabelFields.join("\n"),
      class: isRegion ? "region" : "",
    })};`;
    chunks.push(nodeGv);
  }
  const pseudoOperators = new Map<string, string>();
  for (const ch of scope.channelsInScope) {
    let fromAddressKey = stringifyAddress(ch.fromOperatorAddress);
    let toAddressKey = stringifyAddress(ch.toOperatorAddress);

    if (ch.fromOperatorAddress[ch.fromOperatorAddress.length - 1] === "0") {
      fromAddressKey = `${fromAddressKey}:${ch.fromPort}:FROM`;
      pseudoOperators.set(fromAddressKey, `input ${ch.fromPort}`);
    }
    if (ch.toOperatorAddress[ch.toOperatorAddress.length - 1] === "0") {
      toAddressKey = `${toAddressKey}:${ch.toPort}:TO`;
      pseudoOperators.set(toAddressKey, `output ${ch.toPort}`);
    }
    const chanLabel = `${ch.messagesSent > 0 ? `${ch.messagesSent} records` : ""}${ch.batchesSent > 0 ? `\n${ch.batchesSent} batches` : ""}`;
    const chanGv = `${dotString(fromAddressKey)} -> ${dotString(
      toAddressKey,
    )} ${dotAttributes({
      label: chanLabel,
      tooltip: ch.channelType || "unknown channel type",
      style: ch.messagesSent === 0 ? "dashed" : undefined,
    })};`;
    chunks.push(chanGv);
  }

  for (const [k, v] of pseudoOperators) {
    chunks.push(
      `${dotString(k)} ${dotAttributes({
        fillcolor: "lightgrey",
        id: k,
        label: v,
      })}`,
    );
  }

  for (const lir_operator of lir_operators) {
    if (!lir_operator.addresses.some((addr) => addresses.has(addr))) continue;
    // Graphviz treats a subgraph as a cluster based on its name, which it
    // reads after stripping the quotes, so quoting here keeps the clustering
    // behaviour while denying `lir_id` a bare interpolation.
    chunks.push(`subgraph ${dotString(`cluster_${lir_operator.lir_id}`)} {`);
    chunks.push(
      `label=${dotString(
        `LIR ID ${lir_operator.lir_id}: ${lir_operator.operator}`,
      )};`,
    );
    for (const addr of lir_operator.addresses) {
      chunks.push(`${dotString(addr)};`);
    }
    chunks.push("}");
  }

  chunks.push("}");
  const ret = chunks.join("\n");
  return ret;
}
