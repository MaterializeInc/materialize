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
};
