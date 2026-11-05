// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { act, renderHook } from "@testing-library/react";
import type { ElkNode } from "elkjs/lib/elk-api.js";
import { describe, expect, it, vi } from "vitest";

import type { VisibleGraph } from "./dataflowGraph";
import { useElkLayout } from "./useElkLayout";

// Each layout call parks here until the test resolves it, so a test controls
// the order in which responses arrive.
const pendingLayouts = vi.hoisted(
  () => [] as Array<{ resolve: (node: ElkNode) => void }>,
);

vi.mock("elkjs/lib/elk-api.js", () => ({
  default: class {
    layout() {
      return new Promise<ElkNode>((resolve) => {
        pendingLayouts.push({ resolve });
      });
    }
    terminateWorker() {}
  },
}));
vi.mock("elkjs/lib/elk-worker.min.js?worker", () => ({ default: class {} }));

const GRAPH: VisibleGraph = { nodes: [], edges: [] };
const LAYOUTED: ElkNode = { id: "root", children: [] };

describe("useElkLayout", () => {
  it("a cached key stays laid out when an older request resolves late", async () => {
    const { result, rerender } = renderHook(
      ({ cacheKey }) => useElkLayout(GRAPH, cacheKey),
      { initialProps: { cacheKey: "a" } },
    );
    await act(async () => pendingLayouts[0].resolve(LAYOUTED));
    expect(result.current.layouting).toBe(false);

    // Switch to b, whose layout stays in flight, then back to the cached a.
    rerender({ cacheKey: "b" });
    expect(pendingLayouts).toHaveLength(2);
    rerender({ cacheKey: "a" });
    expect(result.current.layouting).toBe(false);

    await act(async () => pendingLayouts[1].resolve(LAYOUTED));
    expect(result.current.layouting).toBe(false);
    expect(result.current.positions).not.toBeNull();
  });
});
