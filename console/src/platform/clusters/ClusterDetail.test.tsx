// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { Route, Routes } from "react-router-dom";

import { UI_PREVIEWS } from "~/config/uiPreviews";
import { uiPreviewOptInStorageKey } from "~/hooks/useUiPreview";
import { getStore } from "~/jotai";
import { allClusters } from "~/store/allClusters";
import { mockSubscribeState } from "~/test/mockSubscribe";
import { renderComponent, RenderWithPathname } from "~/test/utils";

import ClusterDetail from "./ClusterDetail";
import { buildClusterServerResponse } from "./clustersTestUtils";

const flags: Record<string, boolean> = {};

vi.mock("~/hooks/useFlags", () => ({
  useFlags: () => flags,
}));

vi.mock("~/platform/clusters/ClusterOverview", () => ({
  default: function () {
    return <div>ClusterOverview component</div>;
  },
}));
vi.mock("~/platform/clusters/ClusterPage/ClusterMetrics", () => ({
  ClusterMetrics: () => <div>ClusterMetrics component</div>,
}));
vi.mock("~/platform/clusters/ClusterReplicas", () => ({
  default: () => <div>ClusterReplicas component</div>,
}));
vi.mock("~/platform/clusters/MaterializedViewsList", () => ({
  default: () => <div>MaterializedViewsList component</div>,
}));
vi.mock("~/platform/clusters/IndexList", () => ({
  default: () => <div>IndexList component</div>,
}));
vi.mock("~/platform/clusters/Sources", () => ({
  default: () => <div>Sources component</div>,
}));
vi.mock("~/platform/clusters/Sinks", () => ({
  default: () => <div>Sinks component</div>,
}));

const redesignFlag = UI_PREVIEWS.clusterDetailsRedesign.ldFlag;

const seedClusters = () =>
  getStore().set(
    allClusters,
    mockSubscribeState({
      data: [
        buildClusterServerResponse({ id: "u1", name: "default" }),
        buildClusterServerResponse({ id: "u2", name: "quickstart" }),
      ],
    }),
  );

const renderClusterDetail = (initialPath: string) =>
  renderComponent(
    <RenderWithPathname>
      <Routes>
        <Route path=":clusterId/:clusterName">
          <Route index path="*" element={<ClusterDetail />} />
        </Route>
      </Routes>
    </RenderWithPathname>,
    { initialRouterEntries: [initialPath] },
  );

const expectPathname = async (pathname: string) =>
  waitFor(() =>
    expect(screen.getByTestId("pathname").textContent).toBe(pathname),
  );

describe("ClusterRoutes", () => {
  it("breadcrumb context menu allows switching clusters", async () => {
    const store = getStore();
    store.set(
      allClusters,
      mockSubscribeState({
        data: [
          buildClusterServerResponse({ id: "u1", name: "default" }),
          buildClusterServerResponse({ id: "u2", name: "quickstart" }),
          buildClusterServerResponse({ id: "s1", name: "mz_system" }),
          buildClusterServerResponse({ id: "s2", name: "mz_catalog_server" }),
        ],
      }),
    );
    renderComponent(
      <RenderWithPathname>
        <Routes>
          <Route path=":clusterId/:clusterName">
            <Route index path="*" element={<ClusterDetail />} />
          </Route>
        </Routes>
      </RenderWithPathname>,
      {
        initialRouterEntries: ["/u1/default"],
      },
    );

    await waitFor(() => {
      // The context menu Portal / Menu list combination is setting display: none on
      // elemnts outside the menu, which is really confusing.
      expect(screen.getByText("/u1/default")).toBeVisible();
    });
    expect(screen.getByText("ClusterOverview component")).toBeVisible();
    const user = userEvent.setup();
    user.click(screen.getByRole("button", { name: "Navigation actions" }));
    await waitFor(() => {
      expect(screen.getByText("quickstart")).toBeVisible();
    });
    user.click(screen.getByRole("menuitem", { name: "quickstart" }));

    expect(await screen.findByText("/u2/quickstart")).toBeVisible();
    expect(screen.getByText("ClusterOverview component")).toBeVisible();
  });
});

describe("ClusterDetail redesign preview", () => {
  beforeEach(() => {
    localStorage.clear();
    delete flags[redesignFlag];
    seedClusters();
  });

  describe("when opted in", () => {
    beforeEach(() => {
      flags[redesignFlag] = true;
      localStorage.setItem(
        uiPreviewOptInStorageKey("clusterDetailsRedesign"),
        "true",
      );
    });

    it("shows the cluster title and the redesigned tabs", async () => {
      renderClusterDetail("/u1/default");

      expect(
        await screen.findByRole("heading", { level: 1, name: "default" }),
      ).toBeVisible();
      expect(screen.getByText("small")).toBeVisible();
      expect(screen.getByText("1 replica")).toBeVisible();
      expect(screen.getByRole("link", { name: "Metrics" })).toBeVisible();
      expect(screen.getByRole("link", { name: "Objects" })).toBeVisible();
      expect(screen.getByRole("link", { name: "Replicas" })).toBeVisible();
      expect(
        screen.queryByRole("link", { name: "Materialized Views" }),
      ).not.toBeInTheDocument();
      expect(screen.getByText("ClusterMetrics component")).toBeVisible();
    });

    it("opens the Objects tab on its first object type", async () => {
      const user = userEvent.setup();
      renderClusterDetail("/u1/default");

      await user.click(await screen.findByRole("link", { name: "Objects" }));

      await expectPathname("/u1/default/objects/materialized-views");
      expect(
        await screen.findByText("MaterializedViewsList component"),
      ).toBeVisible();
    });

    it("switches object types from the type tabs", async () => {
      const user = userEvent.setup();
      renderClusterDetail("/u1/default/objects/materialized-views");

      await user.click(await screen.findByRole("tab", { name: "Indexes" }));

      await expectPathname("/u1/default/objects/indexes");
      expect(await screen.findByText("IndexList component")).toBeVisible();
      expect(
        screen.queryByText("MaterializedViewsList component"),
      ).not.toBeInTheDocument();
    });

    it("returns to Metrics from a nested Objects URL", async () => {
      const user = userEvent.setup();
      renderClusterDetail("/u1/default/objects/indexes");

      await user.click(await screen.findByRole("link", { name: "Metrics" }));

      await expectPathname("/u1/default");
      expect(await screen.findByText("ClusterMetrics component")).toBeVisible();
    });

    it("redirects classic tab URLs into the Objects tab", async () => {
      renderClusterDetail("/u1/default/sinks");

      await expectPathname("/u1/default/objects/sinks");
      expect(await screen.findByText("Sinks component")).toBeVisible();
    });

    it("redirects an unknown object type to the first type", async () => {
      renderClusterDetail("/u1/default/objects/tables");

      await expectPathname("/u1/default/objects/materialized-views");
    });

    it("keeps the current tab when switching clusters", async () => {
      const user = userEvent.setup();
      renderClusterDetail("/u1/default/objects/indexes");

      await user.click(
        await screen.findByRole("button", { name: "Navigation actions" }),
      );
      await user.click(
        await screen.findByRole("menuitem", { name: "quickstart" }),
      );

      await expectPathname("/u2/quickstart/objects/indexes");
      expect(
        await screen.findByRole("heading", { level: 1, name: "quickstart" }),
      ).toBeVisible();
    });

    it("switches back to the classic page", async () => {
      const user = userEvent.setup();
      renderClusterDetail("/u1/default");

      await user.click(
        await screen.findByRole("button", {
          name: /Show classic cluster details page/,
        }),
      );

      expect(
        await screen.findByRole("link", { name: "Materialized Views" }),
      ).toBeVisible();
      expect(
        screen.queryByRole("heading", { level: 1, name: "default" }),
      ).not.toBeInTheDocument();
    });
  });

  describe("when not opted in", () => {
    it("offers the preview on the classic page when the flag is on", async () => {
      flags[redesignFlag] = true;
      renderClusterDetail("/u1/default");

      expect(
        await screen.findByRole("button", {
          name: /Show the new cluster details page/,
        }),
      ).toBeVisible();
      expect(screen.getByRole("link", { name: "Overview" })).toBeVisible();
    });

    it("sends an unknown object type to the classic overview", async () => {
      renderClusterDetail("/u1/default/objects/tables");

      await expectPathname("/u1/default");
    });

    it("redirects redesigned Objects URLs to the classic tabs", async () => {
      renderClusterDetail("/u1/default/objects/sinks");

      await expectPathname("/u1/default/sinks");
      expect(await screen.findByText("Sinks component")).toBeVisible();
    });
  });
});
