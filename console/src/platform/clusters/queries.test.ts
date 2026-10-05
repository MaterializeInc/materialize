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

import { DatabaseObject } from "~/api/materialize/objects";
import { ErrorCode, MzDataType } from "~/api/materialize/types";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { roleQueryKeys } from "~/platform/roles/queries";
import { getQueryClient } from "~/queryClient";
import { allObjects } from "~/store/allObjects";
import { createProviderWrapper } from "~/test/utils";

import { useFreshnessObjects, useOwners } from "./queries";

const ownersColumns = buildColumns([
  "id",
  "name",
  { name: "isOwner", type_oid: MzDataType.bool },
]);

const OWNED_ROLE_ID = "u1";
const UNOWNED_ROLE_ID = "u2";
const UNKNOWN_ROLE_ID = "u404";

function buildOwnersHandler(waitTimeMs?: number) {
  return buildSqlQueryHandlerV2(
    {
      queryKey: roleQueryKeys.owners(),
      results: mapKyselyToTabular({
        columns: ownersColumns,
        rows: [
          { id: OWNED_ROLE_ID, name: "my_role", isOwner: true },
          { id: UNOWNED_ROLE_ID, name: "someone_elses_role", isOwner: false },
        ],
      }),
    },
    { waitTimeMs },
  );
}

const errorOwnersHandler = buildSqlQueryHandlerV2({
  queryKey: roleQueryKeys.owners(),
  results: {
    error: {
      message: "Something went wrong",
      code: ErrorCode.INTERNAL_ERROR,
    },
    notices: [],
  },
});

async function renderUseOwners() {
  const ProviderWrapper = await createProviderWrapper();
  return renderHook(() => useOwners(), { wrapper: ProviderWrapper });
}

describe("useOwners", () => {
  it("returns true for a role the user can act as", async () => {
    server.use(buildOwnersHandler());
    const { result } = await renderUseOwners();

    await waitFor(() =>
      expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(true),
    );
  });

  it("returns false for a role the user cannot act as", async () => {
    server.use(buildOwnersHandler());
    const { result } = await renderUseOwners();

    // Settle on a known owner first, so this asserts the resolved answer rather
    // than the in-flight default.
    await waitFor(() =>
      expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(true),
    );
    expect(result.current.isOwner(UNOWNED_ROLE_ID)).toBe(false);
  });

  it("returns false for an owner id missing from the result set", async () => {
    server.use(buildOwnersHandler());
    const { result } = await renderUseOwners();

    await waitFor(() =>
      expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(true),
    );
    expect(result.current.isOwner(UNKNOWN_ROLE_ID)).toBe(false);
  });

  it("returns false until the query resolves", async () => {
    server.use(buildOwnersHandler(50));
    const { result } = await renderUseOwners();

    // An owner must not read as an owner before the query settles, otherwise
    // owner-only controls would appear and then disappear.
    expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(false);

    await waitFor(() =>
      expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(true),
    );
  });

  it("returns false when the query fails", async () => {
    server.use(errorOwnersHandler);
    const { result } = await renderUseOwners();

    // A failed query leaves no ownership data, so isOwner has no flip to wait
    // on. Wait on the query reaching its error state instead.
    await waitFor(() =>
      expect(
        getQueryClient().getQueryState(roleQueryKeys.owners())?.status,
      ).toEqual("error"),
    );
    expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(false);
  });

  it("keeps a stable isOwner reference across re-renders", async () => {
    server.use(buildOwnersHandler());
    const { result, rerender } = await renderUseOwners();

    await waitFor(() =>
      expect(result.current.isOwner(OWNED_ROLE_ID)).toBe(true),
    );
    // Consumers pass isOwner to useMemo dependency arrays, so a new reference on
    // every render would defeat their memoization.
    const firstReference = result.current.isOwner;
    rerender();
    expect(result.current.isOwner).toBe(firstReference);
  });
});

const buildObject = (
  overrides: Partial<DatabaseObject> & { id: string },
): DatabaseObject =>
  ({
    name: "orders_mv",
    objectType: "materialized-view",
    schemaId: "u1",
    schemaName: "public",
    databaseId: "u1",
    databaseName: "materialize",
    sourceType: null,
    isWebhookTable: "false",
    clusterId: "u1",
    clusterName: "quickstart",
    ...overrides,
  }) as DatabaseObject;

async function renderFreshnessObjects(initial: DatabaseObject[]) {
  const store = createStore();
  store.set(allObjects, {
    data: initial,
    error: undefined,
    snapshotComplete: true,
  });
  const ProviderWrapper = await createProviderWrapper({ store });
  // Counted so a test can wait for the re-render the subscribe causes. Without
  // that wait an identity assertion passes before anything has happened.
  const renders = { count: 0 };
  const view = renderHook(
    () => {
      renders.count += 1;
      return useFreshnessObjects("u1");
    },
    { wrapper: ProviderWrapper },
  );
  return { ...view, store, renders };
}

describe("useFreshnessObjects", () => {
  it("keeps its identity when an object on another cluster changes", async () => {
    // `useAllObjects` emits a new array for any change in the environment. A
    // new array here rebuilds the stats and rows chain behind it, so a cluster
    // nobody is looking at would re-render the page.
    const mine = buildObject({ id: "u10" });
    const { result, store, renders } = await renderFreshnessObjects([
      mine,
      buildObject({ id: "u20", clusterId: "u2", name: "elsewhere" }),
    ]);

    const first = result.current;
    expect(first).toHaveLength(1);
    const before = renders.count;

    store.set(allObjects, {
      data: [
        mine,
        buildObject({ id: "u20", clusterId: "u2", name: "renamed" }),
      ],
      error: undefined,
      snapshotComplete: true,
    });

    // The subscribe emitted, so the hook ran again. What it returns must be
    // the array it returned last time.
    await waitFor(() => expect(renders.count).toBeGreaterThan(before));
    expect(result.current).toBe(first);
  });

  it("changes identity when one of its own objects is renamed", async () => {
    const { result, store } = await renderFreshnessObjects([
      buildObject({ id: "u10", name: "before" }),
    ]);

    const first = result.current;
    store.set(allObjects, {
      data: [buildObject({ id: "u10", name: "after" })],
      error: undefined,
      snapshotComplete: true,
    });

    await waitFor(() => expect(result.current[0].objectName).toBe("after"));
    expect(result.current).not.toBe(first);
  });
});
