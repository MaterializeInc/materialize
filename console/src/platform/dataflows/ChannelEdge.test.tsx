// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { fireEvent, render, screen } from "@testing-library/react";
import { Position, ReactFlowProvider, useStoreApi } from "@xyflow/react";
import React from "react";

import { ChannelEdge, type ChannelEdgeData } from "./ChannelEdge";
import type { PortPeer } from "./dataflowGraph";
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

// EdgeLabelRenderer portals into `.react-flow__edgelabel-renderer` inside the
// store's `domNode`, which the real <ReactFlow> wrapper sets on mount; a bare
// <svg> under ReactFlowProvider never gets one, so the portal (and anything
// inside it, like the label or jump buttons) silently renders nothing. This
// stands in for that wrapper, wiring domNode to a real container so the
// portal has somewhere to render into.
const DomNodeSetter = ({ children }: { children: React.ReactNode }) => {
  const store = useStoreApi();
  const ref = React.useRef<HTMLDivElement>(null);
  React.useLayoutEffect(() => {
    store.setState({ domNode: ref.current });
  }, [store]);
  return (
    <div ref={ref}>
      <div className="react-flow__edgelabel-renderer" />
      {children}
    </div>
  );
};

function renderEdge(data: ChannelEdgeData) {
  const { container } = render(
    <ReactFlowProvider>
      <DomNodeSetter>
        <svg>
          <ChannelEdge {...BASE_POSITION} data={data} />
        </svg>
      </DomNodeSetter>
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
  sourceLandings: [],
  targetLandings: [],
};

const LANDING: PortPeer = {
  address: [1, 2],
  label: "Inner",
  messagesSent: 1n,
  batchesSent: 1n,
  channelTypes: [],
  peerPortId: null,
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

  it("shows no jump buttons when nothing is selected, even with a landing", () => {
    renderEdge({ ...DATA, sourceLandings: [LANDING] });
    expect(screen.queryByRole("button")).toBeNull();
  });

  it("shows no jump buttons for an unambiguous edge with nothing to drill into", () => {
    renderEdge({ ...DATA, selected: true });
    expect(screen.queryByRole("button")).toBeNull();
  });

  it("selecting an edge with exactly one landing per side shows a jump button per side", () => {
    const onJumpTo = vi.fn();
    const targetLanding: PortPeer = { ...LANDING, label: "Other" };
    renderEdge({
      ...DATA,
      selected: true,
      sourceLandings: [LANDING],
      targetLandings: [targetLanding],
      onJumpTo,
    });
    fireEvent.click(screen.getByRole("button", { name: /Inner/ }));
    expect(onJumpTo).toHaveBeenCalledWith(LANDING);
    fireEvent.click(screen.getByRole("button", { name: /Other/ }));
    expect(onJumpTo).toHaveBeenCalledWith(targetLanding);
  });

  it("skips the jump button on a side with more than one landing (ambiguous)", () => {
    const onJumpTo = vi.fn();
    renderEdge({
      ...DATA,
      selected: true,
      sourceLandings: [LANDING, { ...LANDING, label: "Other" }],
      onJumpTo,
    });
    expect(screen.queryByRole("button")).toBeNull();
  });
});
