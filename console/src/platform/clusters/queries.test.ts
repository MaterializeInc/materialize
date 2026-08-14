// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { renderHook, waitFor } from "@testing-library/react";

import { ErrorCode, MzDataType } from "~/api/materialize/types";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { roleQueryKeys } from "~/platform/roles/queries";
import { getQueryClient } from "~/queryClient";
import { createProviderWrapper } from "~/test/utils";

import { useOwners } from "./queries";

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
