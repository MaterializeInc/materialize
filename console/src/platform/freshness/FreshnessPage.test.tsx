// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { delay, http } from "msw";
import React from "react";
import { describe, expect, it } from "vitest";

import { Cluster } from "~/api/materialize/cluster/clusterList";
import { SchemaWithOptionalDatabase } from "~/api/materialize/schemaList";
import { SubscribeError } from "~/api/materialize/SubscribeManager";
import server from "~/api/mocks/server";
import { allClusters } from "~/store/allClusters";
import { allObjects } from "~/store/allObjects";
import { allSchemas } from "~/store/allSchemas";
import { buildDatabaseObject, renderComponent } from "~/test/utils";

import { NO_VALUE } from "./constants";
import FreshnessPage from "./FreshnessPage";

const renderWithClusters = async (state: {
  data: Cluster[];
  error?: SubscribeError;
  snapshotComplete: boolean;
}) =>
  renderComponent(<FreshnessPage />, {
    initializeState: (store) =>
      store.set(allClusters, { error: undefined, ...state }),
  });

describe("FreshnessPage cluster states", () => {
  it("waits rather than claiming there are no clusters", async () => {
    // An empty list before the subscribe lands is not the same as an empty
    // environment.
    await renderWithClusters({ data: [], snapshotComplete: false });

    expect(await screen.findByTestId("loading-spinner")).toBeVisible();
    expect(
      screen.queryByText(/No clusters to show freshness for/),
    ).not.toBeInTheDocument();
  });

  it("says so when the subscribe fails", async () => {
    await renderWithClusters({
      data: [],
      error: { code: "INTERNAL_ERROR", message: "boom" },
      snapshotComplete: true,
    });

    expect(
      await screen.findByText(/An error occurred loading clusters/),
    ).toBeVisible();
    expect(screen.queryByTestId("loading-spinner")).not.toBeInTheDocument();
  });

  it("reports an empty environment only once it knows", async () => {
    await renderWithClusters({ data: [], snapshotComplete: true });

    expect(
      await screen.findByText(/No clusters to show freshness for/),
    ).toBeVisible();
  });
});

const CLUSTER: Cluster = {
  id: "u1",
  name: "quickstart",
  managed: true,
  replicas: [],
  size: "25cc",
  replicationFactor: 1,
} as unknown as Cluster;

const NOTE = /Freshness is wallclock lag/;

const PROMPT =
  "Select a cluster or schema to see the freshness of its objects.";

// One object on CLUSTER, so a selected cluster has something to query.
const OBJECT = buildDatabaseObject({ id: "u10", schemaId: "u3" });

// Records each SQL request and never answers.
const recordRequests = () => {
  const requests: string[] = [];
  server.use(
    http.post("*/api/sql", async ({ request }) => {
      requests.push(new URL(request.url).searchParams.get("query_key") ?? "");
      await delay("infinite");
    }),
  );
  return requests;
};

describe("FreshnessPage cluster selection", () => {
  const renderAt = async (search: string, { objectsLoaded = true } = {}) =>
    renderComponent(<FreshnessPage />, {
      initializeState: (store) => {
        store.set(allClusters, {
          data: [CLUSTER],
          error: undefined,
          snapshotComplete: true,
        });
        store.set(allObjects, {
          data: objectsLoaded ? [OBJECT] : [],
          error: undefined,
          snapshotComplete: objectsLoaded,
        });
      },
      initialRouterEntries: [search],
    });

  it("says so rather than showing a different cluster", async () => {
    // An old link, or one to a dropped or system cluster. Falling back would
    // show one cluster under a URL naming another.
    await renderAt("/?cluster=u999");

    expect(await screen.findByText(/was not found/)).toBeVisible();
    expect(screen.queryByText("quickstart")).not.toBeInTheDocument();
  });

  it("waits for a selection rather than loading the first cluster", async () => {
    // The sidebar's link names no cluster. Loading the first one
    // alphabetically would spend queries on a cluster nobody asked about.
    const requests = recordRequests();
    await renderAt("/");

    expect(await screen.findByText(PROMPT)).toBeVisible();
    expect(screen.getByText("Select a cluster")).toBeVisible();
    expect(screen.queryByText("quickstart")).not.toBeInTheDocument();
    expect(requests).toEqual([]);
    // The fine print explains numbers that are not on screen yet.
    expect(screen.queryByText(NOTE)).not.toBeInTheDocument();
  });

  it("shows the Summary and the controls before the data arrives", async () => {
    // So a range change, which reloads the graph and tables, cannot take the
    // controls the reader just used off the screen.
    recordRequests();
    await renderAt("/?cluster=u1");

    expect(
      await screen.findByRole("combobox", { name: "Freshness statistic" }),
    ).toBeVisible();
    expect(screen.getByText("Summary")).toBeVisible();
    expect(screen.queryByText("Lag Over Time")).not.toBeInTheDocument();
  });

  it("shows no Summary values before the object list has loaded", async () => {
    // A link can open the page before the objects subscribe delivers its
    // snapshot. The empty list then is not an empty scope.
    recordRequests();
    await renderAt("/?cluster=u1", { objectsLoaded: false });

    const summary = await screen.findByRole("region", {
      name: "Summary percentiles",
    });
    expect(within(summary).queryByText(NO_VALUE)).not.toBeInTheDocument();
  });

  it("words the threshold sentence the same for every statistic", async () => {
    recordRequests();
    await renderAt("/?cluster=u1&exceeded=current");

    expect(await screen.findByText("Highlight objects with")).toBeVisible();
    expect(screen.getByText("lag ≥")).toBeVisible();
  });

  it("labels the time range and object type menus", async () => {
    recordRequests();
    await renderAt("/?cluster=u1");

    expect(await screen.findByText("Time range")).toBeVisible();
    expect(screen.getByText("Object types")).toBeVisible();
    expect(screen.getByRole("combobox", { name: "Time range" })).toBeVisible();
  });

  it("loads a cluster a link names", async () => {
    const requests = recordRequests();
    await renderAt("/?cluster=u1");

    expect(await screen.findByText("quickstart")).toBeVisible();
    await waitFor(() => expect(requests.length).toBeGreaterThan(0));
    expect(screen.queryByText(PROMPT)).not.toBeInTheDocument();
    expect(screen.getByText(NOTE)).toBeVisible();
  });
});

const SCHEMAS: SchemaWithOptionalDatabase[] = [
  { id: "u3", name: "public", databaseId: "u1", databaseName: "materialize" },
  { id: "u9", name: "public", databaseId: "u2", databaseName: "analytics" },
];

describe("FreshnessPage schema scope", () => {
  const renderAt = async (search: string) =>
    renderComponent(<FreshnessPage />, {
      initializeState: (store) => {
        store.set(allClusters, {
          data: [CLUSTER],
          error: undefined,
          snapshotComplete: true,
        });
        store.set(allSchemas, {
          data: SCHEMAS,
          error: undefined,
          snapshotComplete: true,
        });
      },
      initialRouterEntries: [search],
    });

  it("names a linked schema with its database", async () => {
    // Both are `public`, so the bare names could not be told apart.
    await renderAt("/?scope=schema&schema=u9");

    expect(await screen.findByText("analytics.public")).toBeVisible();
    expect(screen.queryByText("quickstart")).not.toBeInTheDocument();
  });

  it("groups schemas under their database, as the Shell's picker does", async () => {
    await renderAt("/?scope=schema");

    await userEvent.click(
      await screen.findByRole("combobox", { name: "Schema" }),
    );

    expect(
      within(screen.getByRole("listbox"))
        .getAllByRole("option")
        .map((option) => option.textContent),
    ).toEqual(["public", "public"]);
    expect(screen.getByText("analytics")).toBeVisible();
    expect(screen.getByText("materialize")).toBeVisible();
  });

  it("says so rather than showing a different schema", async () => {
    await renderAt("/?scope=schema&schema=u999");

    expect(await screen.findByText(/That schema was not found/)).toBeVisible();
  });

  it("waits for a selection when the scope changes", async () => {
    await renderAt("/?cluster=u1");
    expect(await screen.findByText("quickstart")).toBeVisible();

    await userEvent.selectOptions(
      screen.getByRole("combobox", { name: "Show objects by" }),
      "schema",
    );

    expect(await screen.findByText(PROMPT)).toBeVisible();
    expect(screen.getByText("Select a schema")).toBeVisible();
    expect(screen.queryByText(NOTE)).not.toBeInTheDocument();
    expect(screen.queryByText("analytics.public")).not.toBeInTheDocument();
    expect(screen.queryByText("quickstart")).not.toBeInTheDocument();
  });
});
