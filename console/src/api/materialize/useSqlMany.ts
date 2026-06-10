// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import {
  NEW_QUERY_ABORT_MESSAGE,
  SqlRequest,
  useSqlApiRequest,
  UseSqlApiRequestOptions,
} from ".";

/**
 * A React hook that runs possibly several SQL queries
 * (in one request) against the current environment.
 * @param request - SQL request to execute in the environment coord or current global coord.
 */
export function useSqlMany(
  request?: SqlRequest,
  options?: UseSqlApiRequestOptions,
) {
  const { abortRequest, runSql, ...inner } = useSqlApiRequest(options);
  // If the sql query changes, execute a new query and abort the previous query.
  React.useEffect(() => {
    runSql(request);

    return () => {
      abortRequest(NEW_QUERY_ABORT_MESSAGE);
    };
  }, [request, runSql, abortRequest]);

  const refetch = React.useCallback(() => runSql(request), [request, runSql]);

  const isError = inner.error !== null;

  return {
    ...inner,
    /** When no data has been loaded and query is currently fetching */
    isInitiallyLoading: request ? inner.isInitiallyLoading : false,
    loading: request ? inner.loading : false,
    refetch,
    isError,
  };
}
