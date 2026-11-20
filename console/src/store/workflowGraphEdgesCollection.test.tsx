// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { waitFor } from "@testing-library/react";
import React from "react";

import { buildColumn } from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { mockSubscribeSocket } from "~/test/mockSubscribeSocket";
import {
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";
import { parseDbVersion } from "~/version/api";

import {
  useSubscribeToWorkflowGraphEdgesCollection,
  workflowGraphEdgesCollection,
} from "./workflowGraphEdgesCollection";

const environmentAt = (version: string) => ({
  ...healthyEnvironment,
  status: {
    ...healthyEnvironment.status,
    version: parseDbVersion(`v${version} (0000000000)`),
  },
});

const Subscriber = () => {
  useSubscribeToWorkflowGraphEdgesCollection();
  return null;
};

const renderSubscriberAt = async (version: string) => {
  server.use(
    mockSubscribeSocket((sql) =>
      sql.includes("mz_object_graph_edges")
        ? {
            columns: [
              buildColumn({ name: "childId" }),
              buildColumn({ name: "parentId" }),
            ],
            rows: [["u2", "u1"]],
          }
        : undefined,
    ),
  );
  await renderComponent(<Subscriber />, {
    initializeState: ({ set }) =>
      setFakeEnvironment(set, "aws/us-east-1", environmentAt(version)),
  });
};

describe("useSubscribeToWorkflowGraphEdgesCollection", () => {
  afterEach(() => {
    workflowGraphEdgesCollection.reset();
  });

  it("fills the edges collection on 26.39.0", async () => {
    await renderSubscriberAt("26.39.0");

    await waitFor(() =>
      expect(
        getStore().get(workflowGraphEdgesCollection.statusAtom)
          .snapshotComplete,
      ).toBe(true),
    );
    expect(workflowGraphEdgesCollection.getRows()).toEqual([
      { childId: "u2", parentId: "u1" },
    ]);
  });

  it("leaves the edges collection empty before 26.39.0", async () => {
    await renderSubscriberAt("26.38.0");

    // Long enough for the subscribe on 26.39.0 to have completed.
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(workflowGraphEdgesCollection.getRows()).toEqual([]);
    expect(
      getStore().get(workflowGraphEdgesCollection.statusAtom).snapshotComplete,
    ).toBe(false);
  });
});
