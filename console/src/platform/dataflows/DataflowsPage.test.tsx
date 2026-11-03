// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import React from "react";
import { Route, Routes } from "react-router-dom";

import type { Cluster } from "~/api/materialize/cluster/clusterList";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { allClusters } from "~/store/allClusters";
import { mockSubscribeState } from "~/test/mockSubscribe";
import { renderComponent } from "~/test/utils";

import DataflowsPage from "./DataflowsPage";

const clusterWith = (replicas: { name: string }[]) =>
  ({ id: "u5", name: "test_cluster", replicas }) as unknown as Cluster;

const dataflowListResult = {
  desc: {
    columns: [
      { name: "id" },
      { name: "name" },
      { name: "records" },
      { name: "size" },
      { name: "elapsedNs" },
    ],
  },
  rows: [["7", "Dataflow: mv", "0", "0", "0"]],
};

// The replica each SQL request was addressed to.
const requestedReplicas: (string | undefined)[] = [];

beforeEach(() => {
  requestedReplicas.length = 0;
  server.use(
    http.post("*/api/sql", ({ request }) => {
      const options = JSON.parse(
        new URL(request.url).searchParams.get("options") ?? "{}",
      );
      requestedReplicas.push(options.cluster_replica);
      return HttpResponse.json({ results: [dataflowListResult] });
    }),
  );
});

const renderPage = (replicas: { name: string }[], search = "") => {
  getStore().set(
    allClusters,
    mockSubscribeState({ data: [clusterWith(replicas)] }),
  );
  return renderComponent(
    <Routes>
      <Route
        path="/clusters/:clusterId/:clusterName/dataflows"
        element={<DataflowsPage />}
      />
    </Routes>,
    {
      initialRouterEntries: [`/clusters/u5/test_cluster/dataflows${search}`],
    },
  );
};

describe("DataflowsPage", () => {
  it("shows an empty state and sends no query for a cluster with no replicas", async () => {
    await renderPage([], "?replica=gone");

    expect(
      await screen.findByText("This cluster has no replicas."),
    ).toBeVisible();
    expect(requestedReplicas).toEqual([]);
  });

  it("falls back to the first replica when ?replica= names none", async () => {
    await renderPage([{ name: "r1" }, { name: "r2" }], "?replica=gone");

    expect(await screen.findByText("Dataflow: mv")).toBeVisible();
    expect(requestedReplicas.length).toBeGreaterThan(0);
    expect(new Set(requestedReplicas)).toEqual(new Set(["r1"]));
    expect(screen.getByRole("combobox")).toHaveValue("r1");
  });

  it("queries the replica named in ?replica= when it exists", async () => {
    await renderPage([{ name: "r1" }, { name: "r2" }], "?replica=r2");

    expect(await screen.findByText("Dataflow: mv")).toBeVisible();
    expect(new Set(requestedReplicas)).toEqual(new Set(["r2"]));
  });
});
