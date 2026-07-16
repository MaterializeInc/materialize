// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { fireEvent, screen } from "@testing-library/react";
import type { NodeProps } from "@xyflow/react";
import React from "react";
import { describe, expect, it, vi } from "vitest";

// jsdom has no ReactFlow store context to back the real Handle, and these
// components don't touch anything else from the package.
vi.mock("@xyflow/react", () => ({
  Handle: () => null,
  Position: { Top: "top", Bottom: "bottom" },
}));

import { renderComponent } from "~/test/utils";

import type {
  LirGroupNode as LirGroupInfo,
  VisibleNode,
} from "./dataflowGraph";
import { LirGroupNode, OperatorNode, PortNode, RegionNode } from "./nodes";
import type { FlowGroupData, FlowNodeData } from "./nodeStyle";

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

const baseData: FlowNodeData = {
  node: baseNode,
  dimmed: false,
  color: "#391D7E",
  selected: false,
  activeMatch: false,
  expanded: false,
};

// NodeProps carries a lot of @xyflow/react-internal fields (id, dragging,
// zIndex, isConnectable...) that these components never read, only `data`;
// casting keeps the fixture down to what's actually used.
const nodeProps = (data: FlowNodeData) =>
  ({ data }) as unknown as NodeProps & { data: FlowNodeData };

describe("OperatorNode", () => {
  it("renders the node's label", async () => {
    await renderComponent(<OperatorNode {...nodeProps(baseData)} />);
    expect(screen.getByText("Reduce")).toBeVisible();
  });

  it("renders a stat line when the node has stats", async () => {
    const data = {
      ...baseData,
      node: {
        ...baseNode,
        stats: {
          arrangementRecords: 100n,
          arrangementSize: 1024n,
          elapsedNs: 3_000_000_000n,
          scheduleCount: 0n,
        },
      },
    };
    await renderComponent(<OperatorNode {...nodeProps(data)} />);
    expect(screen.getByText("3s · 100 r · 1 KB")).toBeVisible();
  });

  it("renders no stat line when the node has no stats", async () => {
    await renderComponent(<OperatorNode {...nodeProps(baseData)} />);
    expect(screen.queryByText(/·/)).toBeNull();
  });

  it("fades dimmed nodes out", async () => {
    await renderComponent(
      <OperatorNode {...nodeProps({ ...baseData, dimmed: true })} />,
    );
    expect(screen.getByTestId("node-shell")).toHaveStyle({ opacity: "0.25" });
  });
});

describe("RegionNode", () => {
  it("renders the node's label and child count", async () => {
    const data = {
      ...baseData,
      node: { ...baseNode, kind: "region" as const, childCount: 3 },
    };
    await renderComponent(<RegionNode {...nodeProps(data)} />);
    expect(screen.getByText("Reduce")).toBeVisible();
    expect(screen.getByText("3")).toBeVisible();
  });

  it("is outlined rather than filled, unlike an operator", async () => {
    const region = {
      ...baseData,
      node: { ...baseNode, kind: "region" as const },
    };
    await renderComponent(<RegionNode {...nodeProps(region)} />);
    const regionShell = screen.getByTestId("node-shell");
    expect(regionShell).toHaveStyle({ borderColor: baseData.color });
    expect(regionShell).not.toHaveStyle({ background: baseData.color });
  });
});

describe("RegionNode toggle", () => {
  const regionData = (over: Partial<FlowNodeData> = {}): FlowNodeData => ({
    ...baseData,
    node: { ...baseNode, kind: "region" as const, childCount: 2 },
    ...over,
  });

  it("fires onToggleExpand and stops propagation on the toggle", async () => {
    const onToggleExpand = vi.fn();
    const onNodeClick = vi.fn();
    await renderComponent(
      <div onClick={onNodeClick}>
        <RegionNode {...nodeProps(regionData({ onToggleExpand }))} />
      </div>,
    );
    fireEvent.click(screen.getByTestId("region-toggle"));
    expect(onToggleExpand).toHaveBeenCalledWith("n1");
    expect(onNodeClick).not.toHaveBeenCalled();
  });

  it("renders distinctly when expanded", async () => {
    const { unmount } = await renderComponent(
      <RegionNode {...nodeProps(regionData({ expanded: false }))} />,
    );
    const collapsed = screen.getByTestId("region-toggle").textContent;
    unmount();
    await renderComponent(
      <RegionNode {...nodeProps(regionData({ expanded: true }))} />,
    );
    expect(screen.getByTestId("region-toggle").textContent).not.toBe(collapsed);
  });

  it("lets clicks pass through the expanded container's body to inner nodes, but not through its header", async () => {
    await renderComponent(
      <RegionNode {...nodeProps(regionData({ expanded: true }))} />,
    );
    // The header (toggle + label) must stay clickable so the disclosure
    // triangle and double-click-to-navigate keep working; the body must not
    // intercept clicks, or nodes/edges nested inside the expanded container
    // (rendered as later siblings on the canvas, not children of this
    // component) would never receive them.
    const header = screen.getByTestId("region-toggle").parentElement;
    expect(header).not.toBeNull();
    expect(header).toHaveStyle({ pointerEvents: "auto" });
    const body = header!.parentElement;
    expect(body).not.toBeNull();
    expect(body).toHaveStyle({ pointerEvents: "none" });
  });
});

describe("PortNode", () => {
  it("renders the port's label", async () => {
    const data = {
      ...baseData,
      node: { ...baseNode, kind: "port" as const, label: "input 0" },
    };
    await renderComponent(<PortNode {...nodeProps(data)} />);
    expect(screen.getByText("input 0")).toBeVisible();
  });
});

describe("LirGroupNode", () => {
  const groupInfo: LirGroupInfo = {
    id: "u1/1",
    exportId: "u1",
    lirId: "1",
    operator: "Join",
    nesting: 0,
  };

  it("renders the group's label", async () => {
    const data: FlowGroupData = {
      group: groupInfo,
      label: "LIR 1: Join",
      color: "#391D7E",
    };
    await renderComponent(
      <LirGroupNode
        {...({ data } as unknown as NodeProps & { data: FlowGroupData })}
      />,
    );
    expect(screen.getByText("LIR 1: Join")).toBeVisible();
  });
});
