// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { describe, expect, it, vi } from "vitest";

import { renderComponent } from "~/test/utils";

import type { PortPeer, VisibleNode } from "./dataflowGraph";
import type { SelectedEdge } from "./DataflowGraphView";
import { NodeDetailPanel, type Selection } from "./NodeDetailPanel";

const noop = () => {};

const baseNode: VisibleNode = {
  id: "n1",
  kind: "operator",
  label: "Reduce",
  stats: null,
  transitive: null,
  own: null,
  ownSkew: null,
  transitiveSkew: null,
  overheadNs: null,
  childCount: 0,
  lir: [],
  address: null,
  operatorId: null,
  peers: [],
  direction: null,
};

// Distinct, unrelated names for the edge's two ends and the peer, so a test
// can tell from the rendered text alone which one landed in which section.
const edge: SelectedEdge = {
  id: "e1",
  source: "op-up",
  target: "port-1",
  messagesSent: 10n,
  batchesSent: 2n,
  channelTypes: ["rows"],
  sourceLandings: [],
  targetLandings: [],
  sourceLabel: "EdgeSourceLabel",
  targetLabel: "EdgeTargetLabel",
};

const peer: PortPeer = {
  address: [1, 2],
  label: "PeerLabel",
  // Deliberately different from edge's totals, so a test can tell whether
  // the aggregate came from the in-view edge or from peers.
  messagesSent: 999n,
  batchesSent: 999n,
  channelTypes: ["rows"],
  peerPortId: null,
};

const portNode = (direction: "input" | "output"): VisibleNode => ({
  ...baseNode,
  id: "port-1",
  kind: "port",
  label: direction === "input" ? "input 0" : "output 0",
  peers: [peer],
  direction,
});

const renderPanel = (
  selection: Selection,
  overrides: {
    onJumpTo?: (p: PortPeer) => void;
    onSelectNode?: (id: string) => void;
    workerCount?: number;
  } = {},
) =>
  renderComponent(
    <NodeDetailPanel
      selection={selection}
      collapsed={false}
      onToggleCollapsed={noop}
      onJumpTo={overrides.onJumpTo ?? noop}
      onSelectLir={noop}
      onSelectNode={overrides.onSelectNode ?? noop}
      workerCount={overrides.workerCount ?? 1}
    />,
  );

describe("NodeDetailPanel port view", () => {
  it("for an output port, shows the in-view edge as Upstream and the peer as a Downstream jump link", async () => {
    const onJumpTo = vi.fn();
    await renderPanel(
      {
        kind: "node",
        node: portNode("output"),
        connectedEdges: [edge],
      },
      { onJumpTo },
    );

    // Aggregate Records/Batches come from the in-view edge, not the peer,
    // and are shown once (grouped, per formatCount).
    expect(screen.getByText("10")).toBeVisible();
    expect(screen.queryByText("999")).toBeNull();

    // The edge's source is upstream and in-view: plain text, not a link.
    expect(screen.getByText("EdgeSourceLabel")).toBeVisible();
    expect(
      screen.queryByRole("button", { name: /EdgeSourceLabel/ }),
    ).toBeNull();

    // The peer is downstream and out-of-view: a jump link.
    const jumpLink = screen.getByRole("button", { name: /PeerLabel/ });
    await userEvent.click(jumpLink);
    expect(onJumpTo).toHaveBeenCalledWith(peer);
  });

  it("for an input port, reverses it: the peer is Upstream, the in-view edge is Downstream", async () => {
    const onJumpTo = vi.fn();
    await renderPanel(
      {
        kind: "node",
        node: portNode("input"),
        connectedEdges: [edge],
      },
      { onJumpTo },
    );

    const jumpLink = screen.getByRole("button", { name: /PeerLabel/ });
    await userEvent.click(jumpLink);
    expect(onJumpTo).toHaveBeenCalledWith(peer);

    expect(screen.getByText("EdgeTargetLabel")).toBeVisible();
    expect(
      screen.queryByRole("button", { name: /EdgeTargetLabel/ }),
    ).toBeNull();
  });

  it("falls back to summing peers when there's no in-view edge", async () => {
    await renderPanel({
      kind: "node",
      node: portNode("output"),
      connectedEdges: [],
    });

    // With no connected edge, the aggregate must come from the peer instead
    // (both Records and Batches are 999 on this fixture).
    expect(screen.getAllByText("999")).toHaveLength(2);
  });
});

describe("NodeDetailPanel edge view", () => {
  it("shows Kind: edge and Source/Target as links that select that node", async () => {
    const onSelectNode = vi.fn();
    await renderPanel({ kind: "edge", edge }, { onSelectNode });

    expect(screen.getByText("edge")).toBeVisible();

    await userEvent.click(
      screen.getByRole("button", { name: "EdgeSourceLabel" }),
    );
    expect(onSelectNode).toHaveBeenCalledWith("op-up");

    await userEvent.click(
      screen.getByRole("button", { name: "EdgeTargetLabel" }),
    );
    expect(onSelectNode).toHaveBeenCalledWith("port-1");
  });
});

describe("NodeDetailPanel skew gating", () => {
  const skewedOperator: VisibleNode = {
    ...baseNode,
    own: {
      arrangementRecords: 221245721n,
      arrangementSize: 0n,
      elapsedNs: 0n,
      scheduleCount: 0n,
    },
    ownSkew: { cpuSkew: 2, memorySkew: 1.5, scheduleSkew: 1 },
  };

  it("hides Skew on a single-worker replica", async () => {
    await renderPanel(
      { kind: "node", node: skewedOperator },
      { workerCount: 1 },
    );
    expect(screen.queryByText("Skew")).toBeNull();
    // A large count still renders grouped: the separator sits in its own
    // span (per GroupedCount), splitting the value across sibling text
    // nodes, so a plain string matcher can't find it (RTL only matches
    // text confined to one element) — check the full textContent instead.
    expect(
      screen.getByText((_, el) => el?.textContent === "221,245,721"),
    ).toBeVisible();
  });

  it("shows Skew once there's more than one worker to compare across", async () => {
    await renderPanel(
      { kind: "node", node: skewedOperator },
      { workerCount: 2 },
    );
    expect(screen.getByText("Skew")).toBeVisible();
    expect(screen.getByText("CPU skew")).toBeVisible();
  });
});
