// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { act, renderHook, waitFor } from "@testing-library/react";

import {
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { allSchemas } from "~/store/allSchemas";
import { mockSubscribeState } from "~/test/mockSubscribe";
import {
  createProviderWrapper,
  healthyEnvironment,
  setFakeEnvironment,
} from "~/test/utils";

import {
  schemaObjectFilterQueryKeys,
  useSchemaObjectFilters,
} from "./useSchemaObjectFilters";

const validDatabaseListResponseHandler = buildSqlQueryHandlerV2({
  queryKey: schemaObjectFilterQueryKeys.databaseList(),
  results: mapKyselyToTabular({
    columns: buildColumns(["id", "name"]),
    rows: [
      { id: "u1", name: "materialize" },
      { id: "u2", name: "other_db" },
    ],
  }),
});

const ALL_OPTION = "0";
const NAME_FILTER_QUERY_STRING_KEY = "name";

describe("useSchemaObjectFilters", () => {
  beforeEach(() => {
    server.use(validDatabaseListResponseHandler);
    history.pushState(undefined, "", "/");
    const store = getStore();
    store.set(
      allSchemas,
      mockSubscribeState({
        data: [
          {
            id: "u1",
            name: "public",
            databaseId: "u1",
            databaseName: "materialize",
          },
          {
            id: "u2",
            name: "public",
            databaseId: "u2",
            databaseName: "other_db",
          },
        ],
      }),
    );
  });

  it("loads databases and schemas", async () => {
    const ProviderWrapper = await createProviderWrapper({
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
    });
    const { result } = renderHook(
      () => useSchemaObjectFilters(NAME_FILTER_QUERY_STRING_KEY),
      {
        wrapper: ProviderWrapper,
      },
    );
    await waitFor(() => {
      expect(result.current.databaseFilter.databaseList).toEqual([
        {
          id: "u1",
          name: "materialize",
        },
        {
          id: "u2",
          name: "other_db",
        },
      ]);
      expect(result.current.schemaFilter.schemaList).toEqual([
        {
          id: "u1",
          name: "public",
          databaseId: "u1",
          databaseName: "materialize",
        },
        {
          id: "u2",
          name: "public",
          databaseId: "u2",
          databaseName: "other_db",
        },
      ]);
    });
  });

  it("namespace includes database and schema when a schema is selected", async () => {
    const ProviderWrapper = await createProviderWrapper({
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
    });
    const { result } = renderHook(
      () => useSchemaObjectFilters(NAME_FILTER_QUERY_STRING_KEY),
      {
        wrapper: ProviderWrapper,
      },
    );
    await waitFor(() => {
      expect(result.current.databaseFilter.databaseList).not.toBeUndefined();
      expect(result.current.schemaFilter.schemaList).not.toBeNull();
    });
    act(() => {
      result.current.schemaFilter.setSelectedSchema("u1");
    });
    await waitFor(() => {
      expect(location.search).toBe("?namespace=materialize.public");
    });
  });

  it("resets the schema filter when setting a different database", async () => {
    history.pushState(undefined, "", "?namespace=materialize.public");
    const ProviderWrapper = await createProviderWrapper({
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
    });
    const { result } = renderHook(
      () => useSchemaObjectFilters(NAME_FILTER_QUERY_STRING_KEY),
      {
        wrapper: ProviderWrapper,
      },
    );
    await waitFor(() => {
      expect(result.current.databaseFilter.databaseList).not.toBeUndefined();
      expect(result.current.schemaFilter.schemaList).not.toBeNull();
    });

    await act(async () => {
      result.current.databaseFilter.setSelectedDatabase("u2");
    });

    await waitFor(() => expect(location.search).toBe("?namespace=other_db"));
  });

  it("removes the query string value when resetting the filter", async () => {
    history.pushState(undefined, "", "?namespace=materialize.public");
    const ProviderWrapper = await createProviderWrapper({
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
    });
    const { result } = renderHook(
      () => useSchemaObjectFilters(NAME_FILTER_QUERY_STRING_KEY),
      {
        wrapper: ProviderWrapper,
      },
    );
    await waitFor(() => {
      expect(result.current.databaseFilter.databaseList).not.toBeUndefined();
      expect(result.current.schemaFilter.schemaList).not.toBeNull();
    });
    await act(async () => {
      result.current.databaseFilter.setSelectedDatabase(ALL_OPTION);
    });
    await waitFor(() => expect(location.search).toBe(""));
  });
});
