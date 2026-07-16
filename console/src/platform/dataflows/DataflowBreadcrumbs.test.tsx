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

import { DataflowBreadcrumbs } from "./DataflowBreadcrumbs";
import type { DataflowNode, DataflowStructure } from "./dataflowGraph";

const NO_STATS = {
  arrangementRecords: 0n,
  arrangementSize: 0n,
  elapsedNs: 0n,
  scheduleCount: 0n,
};
const NO_SKEW = { cpuSkew: 0, memorySkew: 0, scheduleSkew: 0 };

const node = (
  id: string,
  name: string,
  parent: string | null,
  children: string[] = [],
): DataflowNode => ({
  id,
  operatorId: 0n,
  address: [],
  name,
  parent,
  children,
  own: NO_STATS,
  transitive: NO_STATS,
  ownSkew: NO_SKEW,
  ownSkewRaw: NO_SKEW,
  transitiveSkew: NO_SKEW,
  overheadNs: 0n,
  lir: [],
});

// root -> child -> grandchild, so the breadcrumb path has two navigable
// links plus the final, current segment.
const structure: DataflowStructure = {
  root: "root",
  channels: [],
  nodes: new Map([
    ["root", node("root", "Dataflow", null, ["child"])],
    ["child", node("child", "RegionA", "root", ["grandchild"])],
    ["grandchild", node("grandchild", "LeafA", "child")],
  ]),
};

describe("DataflowBreadcrumbs", () => {
  it("renders the full path from root to the focused scope", async () => {
    await renderComponent(
      <DataflowBreadcrumbs
        structure={structure}
        focusedScope="grandchild"
        onNavigate={vi.fn()}
      />,
    );
    expect(screen.getByText("Dataflow")).toBeVisible();
    expect(screen.getByText("RegionA")).toBeVisible();
    expect(screen.getByText("LeafA")).toBeVisible();
  });

  it("makes every segment but the last a clickable link back to it", async () => {
    const onNavigate = vi.fn();
    await renderComponent(
      <DataflowBreadcrumbs
        structure={structure}
        focusedScope="grandchild"
        onNavigate={onNavigate}
      />,
    );
    await userEvent.click(screen.getByRole("button", { name: "RegionA" }));
    expect(onNavigate).toHaveBeenCalledWith("child");
    // The current (last) segment isn't a link.
    expect(screen.queryByRole("button", { name: "LeafA" })).toBeNull();
  });

  it("renders only the root when focused there, with no links at all", async () => {
    await renderComponent(
      <DataflowBreadcrumbs
        structure={structure}
        focusedScope="root"
        onNavigate={vi.fn()}
      />,
    );
    expect(screen.getByText("Dataflow")).toBeVisible();
    expect(screen.queryByRole("button")).toBeNull();
  });

  it("throws if the focused scope isn't in the structure", async () => {
    await expect(
      renderComponent(
        <DataflowBreadcrumbs
          structure={structure}
          focusedScope="missing"
          onNavigate={vi.fn()}
        />,
      ),
    ).rejects.toThrow(/missing dataflow node/);
  });
});
