// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import React from "react";
import { describe, expect, it } from "vitest";

import { renderComponent } from "~/test/utils";

import { ObjectDetailPanel } from "./ObjectDetailPanel";
import { MaintainedObjectListItem } from "./queries";

// The tabs' contents query the catalog, and only the tab selection is under
// test here.
vi.mock("./ObjectDetailsCard", () => ({ ObjectDetailsCard: () => null }));
vi.mock("./ObjectMetadata", () => ({ ObjectMetadata: () => null }));
vi.mock("./ObjectFreshness", () => ({ ObjectFreshness: () => null }));

const item: MaintainedObjectListItem = {
  id: "u1",
  name: "orders_mv",
  schemaName: "public",
  databaseName: "materialize",
  objectType: "materialized-view",
  sourceType: null,
  parentSource: null,
  cluster: { id: "u1", name: "quickstart" },
  hydratedReplicas: 1,
  totalReplicas: 1,
  lag: null,
  sourceStatus: null,
};

const renderPanel = (path: string) =>
  renderComponent(
    <ObjectDetailPanel
      item={item}
      lookbackMinutes={15}
      setLookbackMinutes={vi.fn()}
    />,
    { initialRouterEntries: [path] },
  );

describe("ObjectDetailPanel", () => {
  it("opens on the Definition tab", async () => {
    await renderPanel("/maintained-objects/u1");

    expect(
      await screen.findByRole("tab", { name: "Definition" }),
    ).toHaveAttribute("aria-selected", "true");
  });

  it("opens on the Freshness tab when the link asks for it", async () => {
    await renderPanel("/maintained-objects/u1?tab=freshness");

    expect(
      await screen.findByRole("tab", { name: "Freshness" }),
    ).toHaveAttribute("aria-selected", "true");
  });
});
