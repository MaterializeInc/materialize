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

import type { DataflowNode, DataflowStructure } from "./dataflowGraph";
import { LirPanel } from "./LirPanel";

const NO_STATS = {
  arrangementRecords: 0n,
  arrangementSize: 0n,
  elapsedNs: 0n,
  scheduleCount: 0n,
};
const NO_SKEW = { cpuSkew: 0, memorySkew: 0, scheduleSkew: 0 };

const node = (
  id: string,
  overrides: Partial<DataflowNode> = {},
): DataflowNode => ({
  id,
  operatorId: 0n,
  address: [],
  name: id,
  parent: null,
  children: [],
  own: NO_STATS,
  transitive: NO_STATS,
  ownSkew: NO_SKEW,
  ownSkewRaw: NO_SKEW,
  transitiveSkew: NO_SKEW,
  overheadNs: 0n,
  lir: [],
  ...overrides,
});

const emptyStructure: DataflowStructure = {
  root: "root",
  channels: [],
  lirSpans: [],
  nodes: new Map([["root", node("root")]]),
};

const joinSpan = {
  exportId: "u1",
  lirId: "1",
  parentLirId: null,
  nesting: 0,
  operator: "Join",
};
const structureWithLir: DataflowStructure = {
  root: "root",
  channels: [],
  lirSpans: [joinSpan],
  nodes: new Map([
    ["root", node("root", { children: ["op1"], lir: [joinSpan] })],
  ]),
};

// A span covering no logged operator, such as a Constant.
const structureWithEmptySpan: DataflowStructure = {
  ...structureWithLir,
  lirSpans: [
    ...structureWithLir.lirSpans,
    {
      exportId: "u1",
      lirId: "2",
      parentLirId: null,
      nesting: 0,
      operator: "Constant",
    },
  ],
};

const noop = () => {};

describe("LirPanel", () => {
  it("renders nothing when the dataflow has no LIR spans", async () => {
    const { container } = await renderComponent(
      <LirPanel
        structure={emptyStructure}
        collapsed={false}
        onToggleCollapsed={noop}
        onHighlight={noop}
        onSelect={noop}
      />,
    );
    // Not toBeEmptyDOMElement(): the provider wrapper leaves its own hidden
    // Chakra env marker behind even when LirPanel itself renders null.
    expect(container.textContent).toBe("");
    expect(screen.queryByRole("button")).toBeNull();
  });

  it("lists each LIR span, one row per export/lirId", async () => {
    await renderComponent(
      <LirPanel
        structure={structureWithLir}
        collapsed={false}
        onToggleCollapsed={noop}
        onHighlight={noop}
        onSelect={noop}
      />,
    );
    expect(screen.getByText("LIR 1: Join")).toBeVisible();
  });

  it("highlights a span's members on hover, and clears on mouse-leave", async () => {
    const onHighlight = vi.fn();
    await renderComponent(
      <LirPanel
        structure={structureWithLir}
        collapsed={false}
        onToggleCollapsed={noop}
        onHighlight={onHighlight}
        onSelect={noop}
      />,
    );
    const row = screen.getByText("LIR 1: Join");
    await userEvent.hover(row);
    expect(onHighlight).toHaveBeenCalledWith(new Set(["root"]));
    await userEvent.unhover(row);
    expect(onHighlight).toHaveBeenCalledWith(null);
  });

  it("selects a span on click", async () => {
    const onSelect = vi.fn();
    await renderComponent(
      <LirPanel
        structure={structureWithLir}
        collapsed={false}
        onToggleCollapsed={noop}
        onHighlight={noop}
        onSelect={onSelect}
      />,
    );
    await userEvent.click(screen.getByText("LIR 1: Join"));
    expect(onSelect).toHaveBeenCalledWith("u1", "1");
  });

  it("marks the selected span", async () => {
    await renderComponent(
      <LirPanel
        structure={structureWithEmptySpan}
        collapsed={false}
        onToggleCollapsed={noop}
        onHighlight={noop}
        onSelect={noop}
        selectedKey="u1/2"
      />,
    );
    expect(
      screen.getByRole("button", { name: "LIR 2: Constant" }),
    ).toHaveAttribute("aria-current", "true");
    expect(
      screen.getByRole("button", { name: "LIR 1: Join" }),
    ).not.toHaveAttribute("aria-current");
  });

  it("shows only the expand toggle when collapsed", async () => {
    await renderComponent(
      <LirPanel
        structure={structureWithLir}
        collapsed={true}
        onToggleCollapsed={noop}
        onHighlight={noop}
        onSelect={noop}
      />,
    );
    expect(screen.queryByText("LIR 1: Join")).toBeNull();
    expect(
      screen.getByRole("button", { name: "Expand LIR panel" }),
    ).toBeVisible();
  });

  it("lists a span with no member operators, without highlighting anything on hover", async () => {
    const onHighlight = vi.fn();
    await renderComponent(
      <LirPanel
        structure={structureWithEmptySpan}
        collapsed={false}
        onToggleCollapsed={noop}
        onHighlight={onHighlight}
        onSelect={noop}
      />,
    );
    const row = screen.getByText("LIR 2: Constant");
    expect(row).toBeVisible();
    // Highlighting an empty member set would dim every node on the canvas.
    await userEvent.hover(row);
    expect(onHighlight).not.toHaveBeenCalledWith(new Set());
  });

  it("rows are buttons reachable by keyboard, and focus highlights like hover", async () => {
    const onHighlight = vi.fn();
    const onSelect = vi.fn();
    await renderComponent(
      <LirPanel
        structure={structureWithLir}
        collapsed={false}
        onToggleCollapsed={noop}
        onHighlight={onHighlight}
        onSelect={onSelect}
      />,
    );
    const row = screen.getByRole("button", { name: "LIR 1: Join" });
    expect(row).toHaveAttribute("type", "button");

    // Tab past the collapse button to the row.
    await userEvent.tab();
    await userEvent.tab();
    expect(row).toHaveFocus();
    expect(onHighlight).toHaveBeenLastCalledWith(new Set(["root"]));

    await userEvent.keyboard("{Enter}");
    expect(onSelect).toHaveBeenCalledWith("u1", "1");

    await userEvent.tab();
    expect(onHighlight).toHaveBeenLastCalledWith(null);
  });
});
