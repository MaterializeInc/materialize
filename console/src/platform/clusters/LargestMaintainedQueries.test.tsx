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

import { ErrorCode, MzDataType } from "~/api/materialize/types";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { buildValidMaterializationLagHandler } from "~/test/clusterQueryBuilders";
import {
  defaultRegionId,
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";
import { parseDbVersion } from "~/version/api";

import LargestMaintainedQueries from "./LargestMaintainedQueries";
import { clusterQueryKeys } from "./queries";

const largestMaintainedQueriesColumns = buildColumns([
  "id",
  "name",
  "size",
  { type_oid: MzDataType.numeric, name: "memoryPercentage" },
  "type",
  "schemaName",
  "databaseName",
  "dataflowId",
  "dataflowName",
]);

const largestReplicaColumns = buildColumns([
  "name",
  "size",
  { type_oid: MzDataType.numeric, name: "heapLimit" },
  { type_oid: MzDataType.bool, name: "isHydrated" },
]);

const failedLargestReplicaHandler = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.largestClusterReplica({ clusterId: "u1" }),
  results: {
    error: {
      code: ErrorCode.INTERNAL_ERROR,
      message: "largestClusterReplica failed",
    },
    notices: [],
  },
});

const emptyLargestReplicaHandler = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.largestClusterReplica({ clusterId: "u1" }),
  results: mapKyselyToTabular({
    columns: largestReplicaColumns,
    rows: [],
  }),
});
const successfulLargestReplicaHandler = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.largestClusterReplica({ clusterId: "u1" }),
  results: mapKyselyToTabular({
    columns: largestReplicaColumns,
    rows: [
      {
        name: "r1",
        size: "25cc",
        heapLimit: "4069523456",
        isHydrated: "t",
      },
    ],
  }),
});

const failedLargestQueriesHandler = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.largestMaintainedQueries({
    clusterId: "u1",
    clusterName: "quickstart",
    replicaName: "r1",
    replicaHeapLimit: 4069523456,
    unifiedSizes: false,
  }),
  results: {
    error: {
      code: ErrorCode.INTERNAL_ERROR,
      message: "largestMaintainedQueries failed",
    },
    notices: [],
  },
});

const successfulLargestQueriesHandler = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.largestMaintainedQueries({
    clusterId: "u1",
    clusterName: "quickstart",
    replicaName: "r1",
    replicaHeapLimit: 4069523456,
    unifiedSizes: false,
  }),
  results: mapKyselyToTabular({
    columns: largestMaintainedQueriesColumns,
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
        dataflowName: "Dataflow: materialize.public.people_with_company_name",
      },
      {
        id: "u190",
        name: "orphaned_view",
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

const unifiedSizesColumns = buildColumns([
  "object_id",
  "size",
  { type_oid: MzDataType.numeric, name: "memoryPercentage" },
]);

const objectNamesColumns = buildColumns([
  "id",
  "name",
  "type",
  "schemaName",
  "databaseName",
]);

/** An environment new enough to pass the mz_object_arrangement_sizes gate. */
const environmentV2635 = {
  ...healthyEnvironment,
  status: {
    health: "healthy" as const,
    version: parseDbVersion("v26.35.0 (ea0d129f)"),
    errors: [],
  },
};

describe("LargestMaintainedQueries", () => {
  it("shows an error state when the largest replica query fails", async () => {
    server.use(failedLargestReplicaHandler, successfulLargestQueriesHandler);
    renderComponent(
      <LargestMaintainedQueries clusterId="u1" clusterName="quickstart" />,
    );

    expect(
      await screen.findByText(
        `It's taking longer than usual to fetch fine-grained memory usage about your indexes and materialized views from your cluster, which might mean it's busy.`,
      ),
    ).toBeVisible();
  });

  it("shows nothing when there are no replicas", async () => {
    server.use(emptyLargestReplicaHandler);
    renderComponent(
      <LargestMaintainedQueries clusterId="u1" clusterName="quickstart" />,
    );

    expect(document.body.textContent).toEqual("");
  });

  it("shows an error state when the maintained query data fails to load", async () => {
    server.use(successfulLargestReplicaHandler, failedLargestQueriesHandler);
    renderComponent(
      <LargestMaintainedQueries clusterId="u1" clusterName="quickstart" />,
    );

    expect(
      await screen.findByText(
        `It's taking longer than usual to fetch fine-grained memory usage about your indexes and materialized views from replica r1, which might mean it's busy.`,
      ),
    ).toBeVisible();
  });

  it("renders the maintained queries list", async () => {
    server.use(
      successfulLargestReplicaHandler,
      successfulLargestQueriesHandler,
      buildValidMaterializationLagHandler({ objectIds: ["u188", "u190"] }),
    );
    renderComponent(
      <LargestMaintainedQueries clusterId="u1" clusterName="quickstart" />,
    );
    expect(await screen.findByText("materialize.public")).toBeVisible();
    expect(await screen.findByText("customer_view")).toBeVisible();
    expect(await screen.findByText("Materialized View")).toBeVisible();
    expect(await screen.findByText("5.09 GB (31.8%)")).toBeVisible();

    // Also shows orphaned dataflows
    await screen.findByText("materialize.deleted_schema");
    await screen.findByText("orphaned_view");
  });

  it("renders from mz_object_arrangement_sizes on Materialize >= v26.35", async () => {
    // Query keys embed the environment version at build time, so seed the
    // v26.35 environment before building this test's handlers.
    const { set } = getStore();
    await setFakeEnvironment(set, defaultRegionId, environmentV2635);
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: clusterQueryKeys.largestClusterReplica({ clusterId: "u1" }),
        results: mapKyselyToTabular({
          columns: largestReplicaColumns,
          rows: [
            {
              name: "r1",
              size: "25cc",
              heapLimit: "4069523456",
              isHydrated: "t",
            },
          ],
        }),
      }),
      buildSqlQueryHandlerV2({
        queryKey: clusterQueryKeys.largestMaintainedQueries({
          clusterId: "u1",
          clusterName: "quickstart",
          replicaName: "r1",
          replicaHeapLimit: 4069523456,
          unifiedSizes: true,
        }),
        results: mapKyselyToTabular({
          columns: unifiedSizesColumns,
          rows: [
            {
              object_id: "u188",
              size: "5469140917",
              memoryPercentage: "31.8345902256",
            },
            // Not present in the names result below: an orphaned dataflow
            // whose object was dropped from the catalog.
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
          columns: objectNamesColumns,
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
      buildValidMaterializationLagHandler({ objectIds: ["u188", "u999"] }),
    );
    renderComponent(
      <LargestMaintainedQueries clusterId="u1" clusterName="quickstart" />,
      {
        initializeState: ({ set: initializeSet }) =>
          setFakeEnvironment(initializeSet, defaultRegionId, environmentV2635),
      },
    );

    expect(await screen.findByText("materialize.public")).toBeVisible();
    expect(await screen.findByText("customer_view")).toBeVisible();
    expect(await screen.findByText("Materialized View")).toBeVisible();
    expect(await screen.findByText("5.09 GB (31.8%)")).toBeVisible();
    // Objects missing from the catalog fall back to displaying their id.
    expect(await screen.findByText("u999")).toBeVisible();
  });
});
