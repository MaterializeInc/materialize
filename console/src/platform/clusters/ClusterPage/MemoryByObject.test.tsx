// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, within } from "@testing-library/react";
import React from "react";

import { ErrorCode, MzDataType } from "~/api/materialize/types";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { clusterQueryKeys } from "~/platform/clusters/queries";
import { renderComponent } from "~/test/utils";

import { MemoryByObject } from "./MemoryByObject";
import { segmentWidths } from "./memoryByObjectModel";

const flags = vi.hoisted(() => ({ values: {} as Record<string, boolean> }));

vi.mock("~/hooks/useFlags", () => ({ useFlags: () => flags.values }));

const HEAP_LIMIT = 4069523456;

const largestReplicaHandler = (rows: Array<Record<string, string>>) =>
  buildSqlQueryHandlerV2({
    queryKey: clusterQueryKeys.largestClusterReplica({ clusterId: "u1" }),
    results: mapKyselyToTabular({
      columns: buildColumns([
        "name",
        "size",
        { type_oid: MzDataType.numeric, name: "heapLimit" },
      ]),
      rows,
    }),
  });

const largestObjectsKey = clusterQueryKeys.largestMaintainedQueries({
  clusterId: "u1",
  clusterName: "quickstart",
  limit: 10,
  replicaName: "r1",
  replicaHeapLimit: HEAP_LIMIT,
  unifiedSizes: false,
});

const largestObjectsHandler = buildSqlQueryHandlerV2({
  queryKey: largestObjectsKey,
  results: mapKyselyToTabular({
    columns: buildColumns([
      "id",
      "name",
      "size",
      { type_oid: MzDataType.numeric, name: "memoryPercentage" },
      "type",
      "schemaName",
      "databaseName",
      "dataflowId",
      "dataflowName",
    ]),
    rows: [
      {
        id: "u188",
        name: "customer_view",
        size: "5469140917",
        memoryPercentage: "31.8345902256",
        type: "materialized-view",
        schemaName: "public",
        databaseName: "materialize",
        dataflowId: "7",
        dataflowName: "Dataflow: materialize.public.customer_view",
      },
      // The dataflow of a dropped object, which mz_objects no longer names.
      {
        id: "u190",
        name: null,
        size: "424919434",
        memoryPercentage: "11.2686157226",
        type: null,
        schemaName: null,
        databaseName: null,
        dataflowId: "124",
        dataflowName: "Dataflow: materialize.deleted_schema.orphaned_view",
      },
    ],
  }),
});

const renderMemoryByObject = () =>
  renderComponent(<MemoryByObject clusterId="u1" clusterName="quickstart" />);

describe("MemoryByObject", () => {
  beforeEach(() => {
    flags.values = {};
  });

  it("lists each object's memory on the largest replica", async () => {
    flags.values = { "maintained-objects-ui-50": true };
    server.use(
      largestReplicaHandler([
        { name: "r1", size: "25cc", heapLimit: String(HEAP_LIMIT) },
      ]),
      largestObjectsHandler,
    );
    await renderMemoryByObject();

    expect(await screen.findByText("Top 10 objects by memory")).toBeVisible();
    expect(screen.getByText("% of heap limit on r1")).toBeVisible();
    const objects = screen.getByRole("list", { name: "Objects" });
    expect(
      within(objects).getByRole("link", { name: "customer_view" }),
    ).toHaveAttribute(
      "href",
      expect.stringMatching(/\/maintained-objects\/u188$/),
    );
    expect(within(objects).getByText("5.09 GB (31.8%)")).toBeVisible();
  });

  it("links to the workflow graph while the maintained objects page is off", async () => {
    server.use(
      largestReplicaHandler([
        { name: "r1", size: "25cc", heapLimit: String(HEAP_LIMIT) },
      ]),
      largestObjectsHandler,
    );
    await renderMemoryByObject();

    const objects = await screen.findByRole("list", { name: "Objects" });
    expect(
      within(objects).getByRole("link", { name: "customer_view" }),
    ).toHaveAttribute("href", expect.stringMatching(/\/workflow$/));
  });

  it("names a dropped object's dataflow without linking it", async () => {
    server.use(
      largestReplicaHandler([
        { name: "r1", size: "25cc", heapLimit: String(HEAP_LIMIT) },
      ]),
      largestObjectsHandler,
    );
    await renderMemoryByObject();

    const objects = await screen.findByRole("list", { name: "Objects" });
    expect(within(objects).getByText("orphaned_view")).toBeVisible();
    expect(
      within(objects).queryByRole("link", { name: "orphaned_view" }),
    ).not.toBeInTheDocument();
  });

  it("shows nothing for a cluster without replicas", async () => {
    server.use(largestReplicaHandler([]));
    await renderMemoryByObject();

    expect(screen.queryByText(/objects by memory/)).not.toBeInTheDocument();
  });

  it("says the replica may be busy when the objects fail to load", async () => {
    server.use(
      largestReplicaHandler([
        { name: "r1", size: "25cc", heapLimit: String(HEAP_LIMIT) },
      ]),
      buildSqlQueryHandlerV2({
        queryKey: largestObjectsKey,
        results: {
          error: {
            code: ErrorCode.INTERNAL_ERROR,
            message: "largestMaintainedQueries failed",
          },
          notices: [],
        },
      }),
    );
    await renderMemoryByObject();

    expect(
      await screen.findByText(/from replica r1, which might mean it's busy/),
    ).toBeVisible();
  });
});

describe("segmentWidths", () => {
  it("stops the bar at the heap limit", () => {
    expect(segmentWidths([60, 30, 25, null])).toEqual([60, 30, 10, 0]);
  });
});
