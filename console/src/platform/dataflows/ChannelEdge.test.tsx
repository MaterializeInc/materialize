// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { render } from "@testing-library/react";
import { Position, ReactFlowProvider } from "@xyflow/react";
import React from "react";

import { ChannelEdge, type ChannelEdgeData } from "./ChannelEdge";
import { HIGHLIGHT_COLORS } from "./nodeStyle";

const BASE_POSITION = {
  id: "e1",
  source: "a",
  target: "b",
  sourceX: 0,
  sourceY: 0,
  targetX: 100,
  targetY: 100,
  sourcePosition: Position.Bottom,
  targetPosition: Position.Top,
};

function renderEdge(data: ChannelEdgeData) {
  // The non-idle label renders through EdgeLabelRenderer, which reads React
  // Flow's store context even outside an actual <ReactFlow> canvas.
  const { container } = render(
    <ReactFlowProvider>
      <svg>
        <ChannelEdge {...BASE_POSITION} data={data} />
      </svg>
    </ReactFlowProvider>,
  );
  return container.querySelector("path")!;
}

const DATA: ChannelEdgeData = {
  messagesSent: 1n,
  batchesSent: 1n,
  channelTypes: [],
  dimmed: false,
  selected: false,
  connected: false,
};

describe("ChannelEdge", () => {
  it("plain edge has no highlight stroke", () => {
    const path = renderEdge(DATA);
    expect(path.style.stroke).toBe("");
  });

  // Touching the selected node (but not being the clicked thing itself)
  // gets a lighter highlight than the directly selected edge does.
  it("a connected edge gets the lighter highlight", () => {
    const path = renderEdge({ ...DATA, connected: true });
    expect(path.style.stroke).toBe(HIGHLIGHT_COLORS.connected);
    expect(path.style.strokeWidth).toBe("1.75");
  });

  it("the directly selected edge gets the stronger highlight, not the connected one", () => {
    const path = renderEdge({ ...DATA, selected: true, connected: true });
    expect(path.style.stroke).toBe(HIGHLIGHT_COLORS.selected);
    expect(path.style.strokeWidth).toBe("2.5");
  });
});
