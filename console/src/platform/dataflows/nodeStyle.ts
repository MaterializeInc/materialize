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
  arrangementRegion: "#7950f2",
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

// A muted, light subset of the spectrum for operator fills: deliberately
// less saturated than GROUP_PALETTE so an operator's fill never reads as
// "this is a LIR group" at a glance, while still giving operators of the
// same kind (every "Reduce", every "Map") a consistent, recognizable color
// instead of the old fixed white/orange pair, which made an unarranged
// operator (the common case) invisible against a white canvas.
const OPERATOR_PALETTE = [
  "#a5d8ff", // blue
  "#99e9f2", // cyan
  "#96f2d7", // teal
  "#b2f2bb", // green
  "#d8f5a2", // lime
  "#ffec99", // yellow
  "#ffd8a8", // orange
  "#fcc2d7", // pink
  "#eebefa", // grape
  "#d0bfff", // violet
  "#bac8ff", // indigo
];

export function operatorColor(name: string): string {
  let hash = 0;
  for (let i = 0; i < name.length; i++) {
    hash = (hash * 31 + name.charCodeAt(i)) | 0;
  }
  return OPERATOR_PALETTE[Math.abs(hash) % OPERATOR_PALETTE.length];
}

export function nodeFillColor(node: VisibleNode): string {
  const region = node.kind !== "operator" && node.kind !== "port";
  if (!region) return operatorColor(node.label);
  const arranged = (node.transitive?.arrangementRecords ?? 0n) > 0n;
  return arranged ? COLORS.arrangementRegion : COLORS.noArrangementRegion;
}

// skewRatio's 0 is a "no per-worker data at all" sentinel, not a real ratio
// (a real ratio is always >= 1); labeling it distinctly avoids it reading
// as an implausibly perfect score.
export function formatSkew(ratio: number): string {
  return ratio === 0 ? "no data" : ratio.toFixed(2);
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
