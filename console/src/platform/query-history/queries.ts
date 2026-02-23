// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { usePrevious } from "@chakra-ui/react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { min } from "date-fns";
import { useEffect } from "react";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import { fetchQueryHistoryClusters } from "~/api/materialize/query-history/queryHistoryClusters";
import {
  fetchQueryHistoryStatementInfo,
  fetchQueryHistoryStatementLifecycle,
  LIFECYCLE_EVENT_TYPES,
  LifecycleEventType,
  QueryHistoryStatementInfoParameters,
  QueryHistoryStatementLifecycleParameters,
  QueryHistoryStatementLifecycleRow,
} from "~/api/materialize/query-history/queryHistoryDetail";
import {
  fetchQueryHistoryList,
  QueryHistoryListParameters,
  QueryHistoryListRow,
} from "~/api/materialize/query-history/queryHistoryList";
import { fetchQueryHistoryUsers } from "~/api/materialize/query-history/queryHistoryUsers";
import {
  getRecentQueryData,
  initialPlaceholderDataBuilders,
} from "~/utils/initialPlaceholderData";

export const STATEMENT_LIFECYCLE_POLL_RATE_MS = 1000;

export const queryHistoryQueryKeys = {
  all: () => buildRegionQueryKey("query-history"),
  list: (params?: QueryHistoryListParameters) =>
    [
      ...queryHistoryQueryKeys.all(),
      buildQueryKeyPart(
        "list",
        params
          ? {
              filters: params.filters,
              isRedacted: params.isRedacted ?? false,
              isV0_132_0: params.isV0_132_0 ?? false,
            }
          : undefined,
      ),
    ] as const,

  clusters: () =>
    [...queryHistoryQueryKeys.all(), buildQueryKeyPart("clusters")] as const,
  users: () =>
    [...queryHistoryQueryKeys.all(), buildQueryKeyPart("users")] as const,
  detail: ({ executionId }: { executionId: string }) =>
    [
      ...queryHistoryQueryKeys.all(),
      buildQueryKeyPart("detail", { executionId }),
    ] as const,
  statementInfo: ({
    executionId,
    isRedacted,
  }: QueryHistoryStatementInfoParameters) =>
    [
      ...queryHistoryQueryKeys.detail({ executionId }),
      buildQueryKeyPart("statement-info", {
        isRedacted: isRedacted ?? false,
      }),
    ] as const,
  statementLifecycle: ({
    executionId,
  }: QueryHistoryStatementLifecycleParameters) =>
    [
      ...queryHistoryQueryKeys.detail({ executionId }),
      buildQueryKeyPart("statement-lifecycle"),
    ] as const,
};

export function useFetchQueryHistoryList(
  parameters: QueryHistoryListParameters,
  options?: { enabled?: boolean },
) {
  return useQuery({
    queryKey: queryHistoryQueryKeys.list(parameters),
    queryFn: async ({ queryKey, signal }) => {
      const [, queryKeyParameters] = queryKey;

      return fetchQueryHistoryList({
        queryKey,
        parameters: queryKeyParameters,
        requestOptions: { signal },
      });
    },
    enabled: options?.enabled,
  });
}

export function useFetchQueryHistoryClusters() {
  return useQuery({
    queryKey: queryHistoryQueryKeys.clusters(),
    queryFn: ({ queryKey, signal }) => {
      return fetchQueryHistoryClusters({
        queryKey,
        requestOptions: { signal },
      });
    },
  });
}

export function useFetchQueryHistoryUsers() {
  return useQuery({
    queryKey: queryHistoryQueryKeys.users(),
    queryFn: ({ queryKey, signal }) => {
      return fetchQueryHistoryUsers({ queryKey, requestOptions: { signal } });
    },
    select: (data) => {
      return data.rows.map((row) => ({
        id: row.email,
        name: row.email,
      }));
    },
  });
}

export function useFetchQueryHistoryStatementInfo(
  parameters: QueryHistoryStatementInfoParameters,
  options?: { enabled?: boolean },
) {
  const queryClient = useQueryClient();

  const { buildPlaceholderDataReturn, buildQueryFnReturn } =
    initialPlaceholderDataBuilders<QueryHistoryListRow>();

  return useQuery({
    queryKey: queryHistoryQueryKeys.statementInfo(parameters),
    queryFn: async ({ queryKey, signal }) => {
      const [, { executionId }, { isRedacted }] = queryKey;

      const data = await fetchQueryHistoryStatementInfo({
        queryKey,
        parameters: { executionId, isRedacted },
        requestOptions: { signal },
      });

      const [infoRows] = data.rows;

      const res = {
        info: infoRows,
        shouldRedirect: !infoRows,
      };

      return buildQueryFnReturn(res);
    },

    placeholderData: (previousData) => {
      if (previousData) {
        return previousData;
      }
      // If the query is not in the cache, we need to look for it in the list query
      const listQueryResults = getRecentQueryData<
        NonNullable<ReturnType<typeof useFetchQueryHistoryList>["data"]>
      >({
        queryClient,
        queryKey: queryHistoryQueryKeys.list(),
      });

      if (!listQueryResults) {
        return undefined;
      }

      for (const listQueryData of listQueryResults) {
        const [, data] = listQueryData;

        const rows = data.rows;

        if (!rows) {
          return undefined;
        }

        const initialPlaceholderData = rows.find(
          (row) => row.executionId === parameters.executionId,
        );

        if (initialPlaceholderData) {
          return buildPlaceholderDataReturn(initialPlaceholderData);
        }
      }

      return undefined;
    },
    enabled: options?.enabled,
  });
}

export type LifecycleObject = {
  [key in LifecycleEventType]?: QueryHistoryStatementLifecycleRow;
};

export function useFetchQueryHistoryStatementLifecycle(
  parameters: QueryHistoryStatementLifecycleParameters,
) {
  const queryClient = useQueryClient();
  const queryResult = useQuery({
    queryKey: queryHistoryQueryKeys.statementLifecycle(parameters),
    queryFn: async ({ queryKey, signal }) => {
      const [, { executionId }] = queryKey;

      const data = await fetchQueryHistoryStatementLifecycle({
        queryKey,
        parameters: { executionId },
        requestOptions: { signal },
      });

      const lifecycleRows = data.rows;

      const lifecycleObject = lifecycleRows.reduce<LifecycleObject>(
        (accum, lifecycleRow) => {
          const eventType = lifecycleRow.eventType as LifecycleEventType;

          if (LIFECYCLE_EVENT_TYPES.includes(eventType)) {
            accum[eventType] = lifecycleRow;
          }

          return accum;
        },
        {},
      );

      /**
       * There's an unlikely edge case where compute-dependencies and storage-dependencies can have a greater
       * timestamp than execution-finished.
       */
      LIFECYCLE_EVENT_TYPES.forEach((key) => {
        const currentLifecycleObject = lifecycleObject[key];

        if (
          key !== "execution-finished" &&
          currentLifecycleObject &&
          lifecycleObject["execution-finished"]?.occurredAt
        ) {
          currentLifecycleObject.occurredAt = min([
            currentLifecycleObject.occurredAt,
            lifecycleObject["execution-finished"].occurredAt,
          ]);
        }
      });

      return lifecycleObject;
    },
    refetchInterval: (query) => {
      const lifecycle = query.state.data;

      if (lifecycle?.["execution-finished"]) {
        return false;
      }

      return STATEMENT_LIFECYCLE_POLL_RATE_MS;
    },
  });

  const prevIsExecutionFinished = usePrevious(
    !!queryResult.data?.["execution-finished"],
  );
  const isExecutionFinished = !!queryResult.data?.["execution-finished"];

  /**
   * When the execution has finished, invalidate the details page since the statement info data
   * will be inconsistent otherwise
   */
  useEffect(() => {
    if (!prevIsExecutionFinished && isExecutionFinished) {
      queryClient.invalidateQueries({
        queryKey: queryHistoryQueryKeys.detail({
          executionId: parameters.executionId,
        }),
      });
    }
  }, [
    prevIsExecutionFinished,
    isExecutionFinished,
    parameters.executionId,
    queryClient,
  ]);

  return queryResult;
}
