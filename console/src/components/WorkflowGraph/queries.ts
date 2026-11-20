// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  keepPreviousData,
  useQuery,
  useSuspenseQuery,
} from "@tanstack/react-query";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import {
  fetchWorkflowGraph,
  WorkflowGraphParams,
} from "~/api/materialize/workflowGraph";
import {
  fetchWorkflowGraphNodes,
  WorkflowGraphNodesParams,
  WorkflowGraphNodesQueryParams,
} from "~/api/materialize/workflowGraphNodes";

export const workflowGraphQueryKeys = {
  all: () => buildRegionQueryKey("workflow"),
  graph: (params: WorkflowGraphParams) =>
    [
      ...workflowGraphQueryKeys.all(),
      buildQueryKeyPart("graph", params),
    ] as const,
  nodes: (params: WorkflowGraphNodesQueryParams) =>
    [
      ...workflowGraphQueryKeys.all(),
      buildQueryKeyPart("nodes", {
        objectIds: params.objectIds,
      }),
    ] as const,
};

export function useWorkflowGraph(params: WorkflowGraphParams) {
  return useSuspenseQuery({
    queryKey: workflowGraphQueryKeys.graph(params),
    queryFn: ({ queryKey, signal }) => {
      const [, paramsFromKey] = queryKey;
      return fetchWorkflowGraph({
        queryKey,
        params: paramsFromKey,
        requestOptions: { signal },
      });
    },
  });
}

/** Node details (statuses, connection, creation time) without suspending. */
export function useWorkflowGraphNodeDetails(params: WorkflowGraphNodesParams) {
  return useQuery({
    queryKey: workflowGraphQueryKeys.nodes(params),
    queryFn: ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      return fetchWorkflowGraphNodes({
        queryKey,
        params: { objectIds: queryKeyParams.objectIds },
        requestOptions: { signal },
      });
    },
    // A live graph's node set changes without navigating, which changes the
    // key. The nodes still in it keep their details until the new set's load.
    placeholderData: keepPreviousData,
  });
}

export function useWorkflowGraphNodes(params: WorkflowGraphNodesParams) {
  return useSuspenseQuery({
    queryKey: workflowGraphQueryKeys.nodes(params),
    queryFn: ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      return fetchWorkflowGraphNodes({
        queryKey,
        params: {
          objectIds: queryKeyParams.objectIds,
        },
        requestOptions: { signal },
      });
    },
  });
}
