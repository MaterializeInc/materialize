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
import { Route, Routes, useLocation } from "react-router-dom";

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
// The tabs' own links between each other, under the page's real routes.
vi.mock("~/platform/clusters/ClusterPage/ClusterMetrics", async () => {
  const { Link } = await import("react-router-dom");
  const { workersTabLink } =
    await import("~/platform/clusters/ClusterPage/tabLinks");
  return {
    ClusterMetrics: () => (
      <div>
        ClusterMetrics component
        <Link to={workersTabLink("r1")}>r1 workers</Link>
      </div>
    ),
  };
});
vi.mock("~/platform/clusters/ClusterPage/ClusterWorkers", async () => {
  const { Link } = await import("react-router-dom");
  const { processesTabLink } =
    await import("~/platform/clusters/ClusterPage/tabLinks");
  return {
    ClusterWorkers: () => (
      <div>
        ClusterWorkers component
        <Link {...processesTabLink("r1")}>r1 processes</Link>
      </div>
    ),
  };
});
vi.mock("~/platform/clusters/ClusterPage/Hydration/ClusterHydration", () => ({
  ClusterHydration: () => <div>ClusterHydration component</div>,
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
vi.mock("~/platform/clusters/Sources", async () => {
  const { useConnectorListSortOptions } =
    await import("~/platform/connectors/useConnectorListSortOptions");
  // Syncs its sort to the query string, as the real list does.
  const Sources = () => {
    useConnectorListSortOptions();
    return <div>Sources component</div>;
  };
  return { default: Sources };
});
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

const LocationSearch = () => (
  <div data-testid="location-search">{useLocation().search}</div>
);

const renderClusterDetail = (initialPath: string) =>
  renderComponent(
    <RenderWithPathname>
      <Routes>
        <Route path=":clusterId/:clusterName">
          <Route index path="*" element={<ClusterDetail />} />
        </Route>
      </Routes>
      <LocationSearch />
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
      await renderClusterDetail("/u1/default");

      expect(
        await screen.findByRole("heading", { level: 1, name: "default" }),
      ).toBeVisible();
      expect(screen.getByText("small")).toBeVisible();
      expect(screen.getByText("1 replica")).toBeVisible();
      expect(
        screen
          .getAllByRole("link")
          .map((link) => link.textContent)
          .filter((name) =>
            ["Metrics", "Workers", "Objects", "Hydration", "Replicas"].includes(
              name ?? "",
            ),
          ),
      ).toEqual(["Metrics", "Workers", "Objects", "Hydration"]);
      expect(
        screen.getByRole("button", { name: "Configure cluster" }),
      ).toBeVisible();
      expect(
        screen.queryByRole("link", { name: "Materialized Views" }),
      ).not.toBeInTheDocument();
      expect(screen.getByText("ClusterMetrics component")).toBeVisible();
    });

    it("tags an unmanaged cluster without replicas", async () => {
      getStore().set(
        allClusters,
        mockSubscribeState({
          data: [
            buildClusterServerResponse(
              { id: "u3", name: "adhoc", managed: false },
              [],
            ),
          ],
        }),
      );
      await renderClusterDetail("/u3/adhoc");

      expect(
        await screen.findByRole("heading", { level: 1, name: "adhoc" }),
      ).toBeVisible();
      expect(screen.getByText("Unmanaged")).toBeVisible();
      expect(screen.getByText("No replicas")).toBeVisible();
      expect(screen.queryByText("small")).not.toBeInTheDocument();
    });

    it("opens the configuration drawer from the gear next to the title", async () => {
      const user = userEvent.setup();
      await renderClusterDetail("/u1/default");

      await user.click(
        await screen.findByRole("button", { name: "Configure cluster" }),
      );

      expect(
        await screen.findByRole("dialog", { name: "Configure default" }),
      ).toBeVisible();
      expect(screen.getByRole("button", { name: /^Overview/ })).toHaveAttribute(
        "aria-pressed",
        "true",
      );
    });

    it("opens the configuration drawer over a list that syncs its sort to the URL", async () => {
      const user = userEvent.setup();
      await renderClusterDetail("/u1/default/objects/sources");
      await screen.findByText("Sources component");

      await user.click(
        await screen.findByRole("button", { name: "Configure cluster" }),
      );

      expect(
        await screen.findByRole("dialog", { name: "Configure default" }),
      ).toBeVisible();
    });

    it("opens the classic Replicas tab's links on the drawer's replicas", async () => {
      await renderClusterDetail("/u1/default/replicas");

      await expectPathname("/u1/default");
      expect(
        await screen.findByRole("dialog", { name: "Configure default" }),
      ).toBeVisible();
      expect(screen.getByRole("button", { name: /^Replicas/ })).toHaveAttribute(
        "aria-pressed",
        "true",
      );
    });

    it("opens the Hydration tab from its link", async () => {
      const user = userEvent.setup();
      await renderClusterDetail("/u1/default");

      await user.click(await screen.findByRole("link", { name: "Hydration" }));

      await expectPathname("/u1/default/hydration");
      expect(
        await screen.findByText("ClusterHydration component"),
      ).toBeVisible();
    });

    it("opens the Objects tab on its first object type", async () => {
      const user = userEvent.setup();
      await renderClusterDetail("/u1/default");

      await user.click(await screen.findByRole("link", { name: "Objects" }));

      await expectPathname("/u1/default/objects/materialized-views");
      expect(
        await screen.findByText("MaterializedViewsList component"),
      ).toBeVisible();
    });

    it("switches object types from the type tabs", async () => {
      const user = userEvent.setup();
      await renderClusterDetail("/u1/default/objects/materialized-views");

      await user.click(await screen.findByRole("tab", { name: "Indexes" }));

      await expectPathname("/u1/default/objects/indexes");
      expect(await screen.findByText("IndexList component")).toBeVisible();
      expect(
        screen.queryByText("MaterializedViewsList component"),
      ).not.toBeInTheDocument();
    });

    it("returns to Metrics from a nested Objects URL", async () => {
      const user = userEvent.setup();
      await renderClusterDetail("/u1/default/objects/indexes");

      await user.click(await screen.findByRole("link", { name: "Metrics" }));

      await expectPathname("/u1/default");
      expect(await screen.findByText("ClusterMetrics component")).toBeVisible();
    });

    it("opens the Workers tab", async () => {
      const user = userEvent.setup();
      await renderClusterDetail("/u1/default");

      await user.click(await screen.findByRole("link", { name: "Workers" }));

      await expectPathname("/u1/default/workers");
      expect(await screen.findByText("ClusterWorkers component")).toBeVisible();
    });

    it("carries the replica choice between the Metrics and Workers tabs", async () => {
      await renderClusterDetail("/u1/default?replica=r1");

      expect(
        await screen.findByRole("link", { name: "Workers" }),
      ).toHaveAttribute("href", "/u1/default/workers?replica=r1");
      expect(screen.getByRole("link", { name: "Metrics" })).toHaveAttribute(
        "href",
        "/u1/default?replica=r1",
      );
      expect(screen.getByRole("link", { name: "Objects" })).toHaveAttribute(
        "href",
        "/u1/default/objects",
      );
    });

    it("goes from a replica's CPU to its workers and back to its processes", async () => {
      const user = userEvent.setup();
      await renderClusterDetail("/u1/default");

      await user.click(await screen.findByRole("link", { name: "r1 workers" }));
      await expectPathname("/u1/default/workers");
      expect(screen.getByTestId("location-search")).toHaveTextContent(
        "?replica=r1",
      );

      await user.click(
        await screen.findByRole("link", { name: "r1 processes" }),
      );
      await expectPathname("/u1/default");
      expect(screen.getByTestId("location-search")).toHaveTextContent(
        "?replica=r1",
      );
      expect(await screen.findByText("ClusterMetrics component")).toBeVisible();
    });

    it("redirects classic tab URLs into the Objects tab", async () => {
      await renderClusterDetail("/u1/default/sinks");

      await expectPathname("/u1/default/objects/sinks");
      expect(await screen.findByText("Sinks component")).toBeVisible();
    });

    it("redirects an unknown object type to the first type", async () => {
      await renderClusterDetail("/u1/default/objects/tables");

      await expectPathname("/u1/default/objects/materialized-views");
    });

    it("keeps the current tab when switching clusters", async () => {
      const user = userEvent.setup();
      await renderClusterDetail("/u1/default/objects/indexes");

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
      await renderClusterDetail("/u1/default");

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

    it("keeps keyboard focus on the preview pill when it switches pages", async () => {
      const user = userEvent.setup();
      await renderClusterDetail("/u1/default");

      (
        await screen.findByRole("button", {
          name: /Show classic cluster details page/,
        })
      ).focus();
      await user.keyboard("{Enter}");

      expect(
        await screen.findByRole("button", {
          name: /Show the new cluster details page/,
        }),
      ).toHaveFocus();
    });
  });

  describe("when not opted in", () => {
    it("offers the preview on the classic page when the flag is on", async () => {
      flags[redesignFlag] = true;
      await renderClusterDetail("/u1/default");

      expect(
        await screen.findByRole("button", {
          name: /Show the new cluster details page/,
        }),
      ).toBeVisible();
      expect(screen.getByRole("link", { name: "Overview" })).toBeVisible();
    });

    it("opens the matching redesigned tab when opting in from a classic tab URL", async () => {
      flags[redesignFlag] = true;
      const user = userEvent.setup();
      await renderClusterDetail("/u1/default/indexes");

      await user.click(
        await screen.findByRole("button", {
          name: /Show the new cluster details page/,
        }),
      );

      await expectPathname("/u1/default/objects/indexes");
      expect(await screen.findByText("IndexList component")).toBeVisible();
    });

    it("sends an unknown object type to the classic overview", async () => {
      await renderClusterDetail("/u1/default/objects/tables");

      await expectPathname("/u1/default");
    });

    it("opens the first classic object tab from a redesigned Objects tab link", async () => {
      await renderClusterDetail("/u1/default/objects");

      await expectPathname("/u1/default/materialized-views");
    });

    it("sends the redesigned Workers tab's URL to the classic overview", async () => {
      await renderClusterDetail("/u1/default/workers?replica=r1");

      await expectPathname("/u1/default");
      expect(
        await screen.findByText("ClusterOverview component"),
      ).toBeVisible();
    });

    it("redirects redesigned Objects URLs to the classic tabs", async () => {
      await renderClusterDetail("/u1/default/objects/sinks");

      await expectPathname("/u1/default/sinks");
      expect(await screen.findByText("Sinks component")).toBeVisible();
    });

    it("sends a redesigned Hydration URL to the classic overview", async () => {
      await renderClusterDetail("/u1/default/hydration");

      await expectPathname("/u1/default");
      expect(
        await screen.findByText("ClusterOverview component"),
      ).toBeVisible();
    });
  });
});
