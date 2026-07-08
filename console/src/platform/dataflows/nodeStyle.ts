// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import type { LirGroupNode, VisibleNode } from "./dataflowGraph";

export const COLORS = {
  noArrangementRegion: "#12b886",
  noArrangementOperator: "#ffffff",
  arrangementRegion: "#7950f2",
  arrangementOperator: "#fab005",
};

// Mirrors theme/colors.ts blue.500 and orange.400. This app doesn't emit
// Chakra tokens as CSS custom properties, so `var(--chakra-colors-*)`
// silently resolves to nothing inside a raw boxShadow/stroke value; these
// need the literal hex.
export const HIGHLIGHT_COLORS = {
  selected: "#0093e6",
  activeMatch: "#fe581d",
  // Same hue as `selected`, for an edge that merely touches the selected
  // node rather than being the clicked thing itself; ChannelEdge pairs this
  // with a thinner stroke than `selected` gets, so the two read as a clear
  // emphasis/de-emphasis pair rather than two unrelated colors.
  connected: "#8fd2f5",
};

// A small, visually distinct palette for LIR group borders/headers, cycled
// by a deterministic hash so the same lirId always gets the same color
// within one render (and across re-renders, since lirId is stable).
const GROUP_PALETTE = [
  "#e8590c",
  "#5f3dc4",
  "#0b7285",
  "#2f9e44",
  "#c2255c",
  "#1971c2",
  "#e67700",
  "#7048e8",
];

export function lirGroupColor(lirId: string): string {
  let hash = 0;
  for (let i = 0; i < lirId.length; i++) {
    hash = (hash * 31 + lirId.charCodeAt(i)) | 0;
  }
  return GROUP_PALETTE[Math.abs(hash) % GROUP_PALETTE.length];
}

export function nodeFillColor(node: VisibleNode): string {
  const arranged = (node.transitive?.arrangementRecords ?? 0n) > 0n;
  const region = node.kind !== "operator" && node.kind !== "port";
  if (region)
    return arranged ? COLORS.arrangementRegion : COLORS.noArrangementRegion;
  return arranged ? COLORS.arrangementOperator : COLORS.noArrangementOperator;
}

export function formatElapsed(ns: bigint): string {
  return `scheduled ${Math.round(Number(ns) / 1e9)}s`;
}

// A channel's `type` column is the raw monomorphized Rust container type
// (e.g. "alloc::vec::Vec<(mz_repr::row::Row, mz_repr::timestamp::Timestamp,
// mz_ore::overflowing::Overflowing<i64>)>"). Path-qualified and generics-heavy,
// so this drops module paths down to the final segment, aliases the two
// generics that always mean the same thing in this position (the diff/count
// column, the decoded error variant), and turns `Vec<X>` into `[X]` to match
// how we'd write the type by hand. Anything it doesn't recognize (batch/trace
// internals) still gets the path-stripping and Vec treatment, which is
// already most of the noise, even without a dedicated alias.
export function prettyPrintChannelType(raw: string): string {
  let s = raw.replace(/(?:[A-Za-z_][A-Za-z0-9_]*::)+/g, "");
  s = s.replace(/\bOverflowing<i64>/g, "Diff");
  s = s.replace(/\bDataflowErrorSer\b/g, "Error");
  // `Vec<...>` -> `[...]`, repeatedly: each pass strips one level of nesting,
  // so a Vec inside a Vec's contents surfaces on the next iteration.
  for (;;) {
    const start = s.indexOf("Vec<");
    if (start === -1) break;
    let depth = 0;
    let end = start + 3;
    for (; end < s.length; end++) {
      if (s[end] === "<") depth++;
      else if (s[end] === ">" && --depth === 0) break;
    }
    s =
      s.slice(0, start) +
      "[" +
      s.slice(start + 4, end) +
      "]" +
      s.slice(end + 1);
  }
  return s;
}

export type FlowNodeData = {
  node: VisibleNode;
  dimmed: boolean;
  color: string;
  selected: boolean;
  activeMatch: boolean;
};

export type FlowGroupData = {
  group: LirGroupNode;
  label: string;
  color: string;
};
