// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import type { VisibleNode } from "./dataflowGraph";

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
};

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

export type FlowNodeData = {
  node: VisibleNode;
  dimmed: boolean;
  color: string;
  selected: boolean;
  activeMatch: boolean;
};
