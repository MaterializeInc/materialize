// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { describe, expect, it } from "vitest";

import {
  buildDataflowStructure,
  deriveVisibleGraph,
  nodeIdOf,
} from "./dataflowGraph";
import { CHANNELS, LIR_SPANS, OPS } from "./dataflowGraph.test";
import { extractPositions, NODE_DIMENSIONS, toElkGraph } from "./elkGraph";

describe("toElkGraph", () => {
  const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);

  it("nests elk children by visible parent", () => {
    const elk = toElkGraph(deriveVisibleGraph(s, new Set()));
    const region = elk.children!.find((c) => c.id === nodeIdOf([5, 1]))!;
    expect(region.children!.map((c) => c.id)).toEqual(
      expect.arrayContaining([
        nodeIdOf([5, 1, 1]),
        nodeIdOf([5, 1, 2]),
        `${nodeIdOf([5, 1])}:in:0`,
      ]),
    );
    const leaf = region.children!.find((c) => c.id === nodeIdOf([5, 1, 1]))!;
    expect(leaf.width).toEqual(NODE_DIMENSIONS.operator.width);
  });

  it("round-trips positions relative to parent", () => {
    const elk = toElkGraph(deriveVisibleGraph(s, new Set()));
    // simulate a layout result
    elk.children![0].x = 10;
    elk.children![0].y = 20;
    const positions = extractPositions(elk);
    expect(positions[elk.children![0].id]).toMatchObject({ x: 10, y: 20 });
  });
});
