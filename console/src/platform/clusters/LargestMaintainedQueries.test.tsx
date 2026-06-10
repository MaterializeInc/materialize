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
import { buildValidMaterializationLagHandler } from "~/test/clusterQueryBuilders";
import { renderComponent } from "~/test/utils";

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
    clusterName: "quickstart",
    replicaName: "r1",
    replicaHeapLimit: 4069523456,
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
    clusterName: "quickstart",
    replicaName: "r1",
    replicaHeapLimit: 4069523456,
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
});
