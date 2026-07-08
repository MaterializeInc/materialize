// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import colors from "~/theme/colors";

import type { LirGroupNode, VisibleNode } from "./dataflowGraph";

// Selection/match highlights read the same in light and dark mode (a bright
// ring reads fine against either canvas background), so these pin to the
// raw swatch module instead of a light/dark-aware theme token, only to keep
// them from drifting out of sync with the rest of the palette.
export const HIGHLIGHT_COLORS = {
  selected: colors.blue[500],
  activeMatch: colors.orange[400],
  // Same hue as `selected`, for an edge that merely touches the selected
  // node rather than being the clicked thing itself; ChannelEdge pairs this
  // with a thinner stroke than `selected` gets, so the two read as a clear
  // emphasis/de-emphasis pair rather than two unrelated colors. No exact
  // swatch matches this shade; chosen for the pairing, not a token.
  connected: "#8fd2f5",
};

/** Deterministic 32-bit string hash, for cycling a fixed color palette. */
export function hashString(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++)
    h = (Math.imul(h, 31) + s.charCodeAt(i)) | 0;
  return h;
}

// Cycled by a hash of lirId/operator name so the same id or name always
// gets the same color within one render (and across re-renders, since both
// are stable). Groups and operators share one palette: LirGroupNode's own
// border/label already marks a box as a group, so the fill color doesn't
// need to carry that distinction too.
export function lirGroupColor(
  lirId: string,
  palette: readonly string[],
): string {
  return palette[Math.abs(hashString(lirId)) % palette.length];
}

export function operatorColor(
  name: string,
  palette: readonly string[],
): string {
  return palette[Math.abs(hashString(name)) % palette.length];
}

// WCAG relative luminance of a "#RRGGBB" color, for picking readable text.
function relativeLuminance(hex: string): number {
  const channels = [
    parseInt(hex.slice(1, 3), 16),
    parseInt(hex.slice(3, 5), 16),
    parseInt(hex.slice(5, 7), 16),
  ].map((c) => {
    const s = c / 255;
    return s <= 0.03928 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2];
}

// colors.lineGraph mixes dark and light swatches in both themes (it's
// indexed by hash, not by theme), so a node's fill color carries no
// relationship to the current color mode. Text on top of it needs a
// contrast decision from the fill's actual luminance, not a theme token.
// Pins to raw black/white (not theme foreground tokens) since those exist
// specifically for text on the app's own background, not on an arbitrary
// hashed fill.
export function textColorFor(background: string): string {
  const luminance = relativeLuminance(background);
  // WCAG contrast ratio, (lighter + 0.05) / (darker + 0.05), against each
  // candidate; picking the higher one is equivalent to maximizing contrast.
  const contrastWithWhite = 1.05 / (luminance + 0.05);
  const contrastWithBlack = (luminance + 0.05) / 0.05;
  return contrastWithBlack > contrastWithWhite ? colors.black : colors.white;
}

export function nodeFillColor(
  node: VisibleNode,
  palette: readonly string[],
  accent: { arranged: string; notArranged: string },
): string {
  const region = node.kind !== "operator" && node.kind !== "port";
  if (!region) return operatorColor(node.label, palette);
  const arranged = (node.transitive?.arrangementRecords ?? 0n) > 0n;
  return arranged ? accent.arranged : accent.notArranged;
}

// skewRatio's 0 is a "no per-worker data at all" sentinel, not a real ratio
// (a real ratio is always >= 1). Labeling it distinctly avoids it reading
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
