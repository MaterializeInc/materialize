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

  it("lists every visible node flat, at its kind's dimensions", () => {
    const elk = toElkGraph(deriveVisibleGraph(s, nodeIdOf([5, 1])));
    const ids = elk.children!.map((c) => c.id);
    expect(ids).toEqual(
      expect.arrayContaining([
        nodeIdOf([5, 1, 1]),
        nodeIdOf([5, 1, 2]),
        `${nodeIdOf([5, 1])}:in:0`,
      ]),
    );
    const leaf = elk.children!.find((c) => c.id === nodeIdOf([5, 1, 1]))!;
    expect(leaf.width).toEqual(NODE_DIMENSIONS.operator.width);
    // Flat: no node carries children of its own.
    for (const c of elk.children!) expect(c.children).toBeUndefined();
  });

  it("round-trips positions", () => {
    const elk = toElkGraph(deriveVisibleGraph(s, s.root));
    // simulate a layout result
    elk.children![0].x = 10;
    elk.children![0].y = 20;
    const positions = extractPositions(elk);
    expect(positions[elk.children![0].id]).toMatchObject({ x: 10, y: 20 });
  });
});
