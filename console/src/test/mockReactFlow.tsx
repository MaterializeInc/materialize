// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";
import { vi } from "vitest";

// jsdom lacks ResizeObserver/DOMMatrixReadOnly, so component tests mock
// @xyflow/react to a flat renderer that preserves the decision logic under
// test (which nodes and labels exist) instead of exercising real layout.
export function mockReactFlow() {
  vi.mock("@xyflow/react", () => ({
    ReactFlow: ({
      nodes,
      children,
    }: {
      nodes: { id: string; data: { node: { label: string } } }[];
      children?: React.ReactNode;
    }) => (
      <div data-testid="react-flow">
        {nodes.map((n) => (
          <div key={n.id} data-testid={`node-${n.id}`}>
            {n.data.node.label}
          </div>
        ))}
        {children}
      </div>
    ),
    Background: () => null,
    Controls: () => null,
    MiniMap: () => null,
    Handle: () => null,
    Position: { Left: "left", Right: "right" },
    BaseEdge: () => null,
    EdgeLabelRenderer: ({ children }: { children: React.ReactNode }) => (
      <>{children}</>
    ),
    getBezierPath: () => ["", 0, 0],
  }));
}
