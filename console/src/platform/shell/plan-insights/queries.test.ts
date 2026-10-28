// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { renderHook, waitFor } from "@testing-library/react";
import { createStore } from "jotai";

import { MzDataType } from "~/api/materialize/types";
import {
  buildColumn,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { getQueryClient } from "~/queryClient";
import { allObjects } from "~/store/allObjects";
import { buildDatabaseObject, createProviderWrapper } from "~/test/utils";

import {
  planInsightsQueryKeys,
  toBlockedDependencies,
  useBlockedDependencies,
} from "./queries";

const PARAMS = {
  objectIds: ["u1"],
  historyId: "history-1",
  commandResultIndex: 0,
  hasSuccessfullyFetchedOnce: false,
};

const SOURCE = buildDatabaseObject({
  id: "u1",
  name: "orders",
  objectType: "source",
  sourceType: "postgres",
  clusterId: "u2",
  clusterName: "ingest",
});

describe("toBlockedDependencies", () => {
  it("labels each import from allObjects and leaves out one it doesn't hold", () => {
    expect(
      toBlockedDependencies(
        [
          {
            targetObjectId: "u1",
            hydrated: false,
            lag: null,
            isOutdated: false,
          },
          {
            targetObjectId: "u99",
            hydrated: true,
            lag: null,
            isOutdated: false,
          },
        ],
        new Map([[SOURCE.id, SOURCE]]),
      ),
    ).toEqual({
      u1: {
        hydrated: false,
        isOutdated: false,
        type: "source",
        sourceType: "postgres",
        clusterId: "u2",
        clusterName: "ingest",
      },
    });
  });
});

const lagHandler = () =>
  buildSqlQueryHandlerV2({
    queryKey: planInsightsQueryKeys.blockedDependencies(PARAMS),
    results: mapKyselyToTabular({
      rows: [
        { targetObjectId: "u1", hydrated: false, lag: null, isOutdated: null },
      ],
      columns: [
        buildColumn({ name: "targetObjectId" }),
        buildColumn({ name: "hydrated", type_oid: MzDataType.bool }),
        buildColumn({ name: "lag", type_oid: MzDataType.interval }),
        buildColumn({ name: "isOutdated", type_oid: MzDataType.bool }),
      ],
    }),
  });

/** Renders the hook with `allObjects` still loading and waits for the lag. */
async function renderBeforeSnapshot() {
  server.use(lagHandler());
  const store = createStore();
  store.set(allObjects, {
    data: [],
    error: undefined,
    snapshotComplete: false,
  });
  const ProviderWrapper = await createProviderWrapper({ store });
  const view = renderHook(() => useBlockedDependencies(PARAMS), {
    wrapper: ProviderWrapper,
  });
  await waitFor(() =>
    expect(
      getQueryClient().getQueryState(
        planInsightsQueryKeys.blockedDependencies(PARAMS),
      )?.status,
    ).toBe("success"),
  );
  return { ...view, store };
}

describe("useBlockedDependencies", () => {
  it("keeps loading until allObjects has its snapshot", async () => {
    const { result, store } = await renderBeforeSnapshot();

    // The notice publishes once loading ends, so published now it would list
    // the import without a type.
    expect(result.current).toEqual({ data: undefined, isLoading: true });

    store.set(allObjects, {
      data: [SOURCE],
      error: undefined,
      snapshotComplete: true,
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.data?.blockedDependencies.u1).toMatchObject({
      hydrated: false,
      type: "source",
    });
  });

  it("stops loading without blocked dependencies when allObjects fails", async () => {
    const { result, store } = await renderBeforeSnapshot();

    store.set(allObjects, {
      data: [],
      error: { code: "XX000", message: "subscribe failed" },
      snapshotComplete: false,
    });

    await waitFor(() =>
      expect(result.current).toEqual({ data: undefined, isLoading: false }),
    );
  });
});
