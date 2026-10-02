// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, waitFor, within } from "@testing-library/react";
import React from "react";

import { ErrorCode, MzDataType } from "~/api/materialize/types";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
  SqlHandlerOptions,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { clusterQueryKeys } from "~/platform/clusters/queries";
import {
  defaultRegionId,
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";
import { parseDbVersion } from "~/version/api";

import { MemoryByObject } from "./MemoryByObject";
import { segmentWidths } from "./memoryByObjectModel";

const flags = vi.hoisted(() => ({ values: {} as Record<string, boolean> }));

vi.mock("~/hooks/useFlags", () => ({ useFlags: () => flags.values }));

const HEAP_LIMIT = 4069523456;

const largestReplicaHandler = (
  rows: Array<Record<string, string | null>>,
  options?: SqlHandlerOptions,
  replicaName?: string,
) =>
  buildSqlQueryHandlerV2(
    {
      queryKey: clusterQueryKeys.largestClusterReplica({
        clusterId: "u1",
        replicaName,
      }),
      results: mapKyselyToTabular({
        columns: buildColumns([
          "name",
          "size",
          { type_oid: MzDataType.numeric, name: "heapLimit" },
        ]),
        rows,
      }),
    },
    options,
  );

const largestObjectsKey = (replicaHeapLimit: number | null = HEAP_LIMIT) =>
  clusterQueryKeys.largestMaintainedQueries({
    clusterId: "u1",
    clusterName: "quickstart",
    limit: 10,
    replicaName: "r1",
    replicaHeapLimit,
    unifiedSizes: false,
  });

const largestObjectsHandler = (
  rows: Array<Record<string, string | null>> = [
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
    // The dataflow of a dropped object: the left join to mz_objects finds no
    // row, so even its id is NULL.
    {
      id: null,
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
  replicaHeapLimit: number | null = HEAP_LIMIT,
) =>
  buildSqlQueryHandlerV2({
    queryKey: largestObjectsKey(replicaHeapLimit),
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
      rows,
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
      largestObjectsHandler(),
    );
    await renderMemoryByObject();

    // The title shows while loading too, so wait on the subtitle.
    expect(await screen.findByText("% of heap limit on r1")).toBeVisible();
    expect(screen.getByText("Objects by Memory")).toBeVisible();
    const objects = screen.getByRole("list", { name: "Objects" });
    expect(
      within(objects).getByRole("link", { name: "customer_view" }),
    ).toHaveAttribute(
      "href",
      expect.stringMatching(/\/maintained-objects\/u188$/),
    );
    expect(within(objects).getByText("5.09 GB (31.8%)")).toBeVisible();
  });

  it("shows the named replica instead of the largest", async () => {
    server.use(
      largestReplicaHandler(
        [{ name: "r1", size: "25cc", heapLimit: String(HEAP_LIMIT) }],
        undefined,
        "r1",
      ),
      largestObjectsHandler(),
    );
    await renderComponent(
      <MemoryByObject
        clusterId="u1"
        clusterName="quickstart"
        replicaName="r1"
      />,
    );

    expect(await screen.findByText("% of heap limit on r1")).toBeVisible();
    expect(screen.getByText("5.09 GB (31.8%)")).toBeVisible();
  });

  it("links to the workflow graph while the maintained objects page is off", async () => {
    server.use(
      largestReplicaHandler([
        { name: "r1", size: "25cc", heapLimit: String(HEAP_LIMIT) },
      ]),
      largestObjectsHandler(),
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
      largestObjectsHandler(),
    );
    await renderMemoryByObject();

    const objects = await screen.findByRole("list", { name: "Objects" });
    expect(within(objects).getByText("orphaned_view")).toBeVisible();
    expect(
      within(objects).queryByRole("link", { name: "orphaned_view" }),
    ).not.toBeInTheDocument();
  });

  it("lists sizes without a bar when the replica reports no heap limit", async () => {
    server.use(
      largestReplicaHandler([{ name: "r1", size: "25cc", heapLimit: null }]),
      largestObjectsHandler(
        [
          {
            id: "u188",
            name: "customer_view",
            size: "5469140917",
            memoryPercentage: null,
            type: "materialized-view",
            schemaName: "public",
            databaseName: "materialize",
            dataflowId: "7",
            dataflowName: "Dataflow: materialize.public.customer_view",
          },
        ],
        null,
      ),
    );
    await renderMemoryByObject();

    const objects = await screen.findByRole("list", { name: "Objects" });
    expect(within(objects).getByText("5.09 GB")).toBeVisible();
    expect(screen.getByText("Size on r1")).toBeVisible();
    expect(
      screen.queryByRole("img", { name: "Memory by object" }),
    ).not.toBeInTheDocument();
  });

  it("shows the card's title while loading", async () => {
    server.use(largestReplicaHandler([], { waitTimeMs: "infinite" }));
    await renderMemoryByObject();

    expect(
      screen.getByRole("heading", { name: "Objects by Memory" }),
    ).toBeVisible();
    expect(screen.getByTestId("loading-spinner")).toBeVisible();
  });

  it("says when the replica has no objects", async () => {
    server.use(
      largestReplicaHandler([
        { name: "r1", size: "25cc", heapLimit: String(HEAP_LIMIT) },
      ]),
      largestObjectsHandler([]),
    );
    await renderMemoryByObject();

    expect(
      await screen.findByText("No objects are using memory on r1."),
    ).toBeVisible();
    expect(
      screen.getByRole("heading", { name: "Objects by Memory" }),
    ).toBeVisible();
  });

  it("shows nothing for a cluster without replicas", async () => {
    server.use(largestReplicaHandler([]));
    await renderMemoryByObject();

    await waitFor(() =>
      expect(screen.queryByTestId("loading-spinner")).not.toBeInTheDocument(),
    );
    expect(document.body.textContent).toEqual("");
  });

  it("says the replica may be busy when the objects fail to load", async () => {
    server.use(
      largestReplicaHandler([
        { name: "r1", size: "25cc", heapLimit: String(HEAP_LIMIT) },
      ]),
      buildSqlQueryHandlerV2({
        queryKey: largestObjectsKey(),
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
    expect(
      screen.getByRole("heading", { name: "Objects by Memory" }),
    ).toBeVisible();
  });

  it("reads sizes from mz_object_arrangement_sizes on Materialize >= v26.35", async () => {
    const environment = {
      ...healthyEnvironment,
      status: {
        ...healthyEnvironment.status,
        version: parseDbVersion("v26.35.0 (ea0d129f)"),
      },
    };
    // Query keys embed the environment version, so set it before building
    // the handlers that match on them.
    await setFakeEnvironment(getStore().set, defaultRegionId, environment);
    server.use(
      largestReplicaHandler([
        { name: "r1", size: "25cc", heapLimit: String(HEAP_LIMIT) },
      ]),
      buildSqlQueryHandlerV2({
        queryKey: clusterQueryKeys.largestMaintainedQueries({
          clusterId: "u1",
          clusterName: "quickstart",
          limit: 10,
          replicaName: "r1",
          replicaHeapLimit: HEAP_LIMIT,
          unifiedSizes: true,
        }),
        results: mapKyselyToTabular({
          columns: buildColumns([
            "object_id",
            "size",
            { type_oid: MzDataType.numeric, name: "memoryPercentage" },
          ]),
          rows: [
            {
              object_id: "u188",
              size: "5469140917",
              memoryPercentage: "31.8345902256",
            },
            // Missing from the names below: a dropped object's dataflow.
            {
              object_id: "u999",
              size: "424919434",
              memoryPercentage: "11.2686157226",
            },
          ],
        }),
      }),
      buildSqlQueryHandlerV2({
        queryKey: clusterQueryKeys.maintainedObjectNames(["u188", "u999"]),
        results: mapKyselyToTabular({
          columns: buildColumns([
            "id",
            "name",
            "type",
            "schemaName",
            "databaseName",
          ]),
          rows: [
            {
              id: "u188",
              name: "customer_view",
              type: "materialized-view",
              schemaName: "public",
              databaseName: "materialize",
            },
          ],
        }),
      }),
    );
    await renderComponent(
      <MemoryByObject clusterId="u1" clusterName="quickstart" />,
      {
        initializeState: ({ set }) =>
          setFakeEnvironment(set, defaultRegionId, environment),
      },
    );

    const objects = await screen.findByRole("list", { name: "Objects" });
    expect(
      within(objects).getByRole("link", { name: "customer_view" }),
    ).toBeVisible();
    expect(within(objects).getByText("5.09 GB (31.8%)")).toBeVisible();
    expect(within(objects).getByText("u999")).toBeVisible();
    expect(
      within(objects).queryByRole("link", { name: "u999" }),
    ).not.toBeInTheDocument();
  });
});

describe("segmentWidths", () => {
  it("stops the bar at the heap limit", () => {
    expect(segmentWidths([60, 30, 25, null])).toEqual([60, 30, 10, 0]);
  });
});
