// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey, useQueryClient } from "@tanstack/react-query";
import deepEqual from "fast-deep-equal";
import React from "react";

export function useCachedQueryData<TData = unknown>(queryKey: QueryKey) {
  const queryClient = useQueryClient();
  const [data, setData] = React.useState<TData | undefined>(() =>
    queryClient.getQueryData<TData>(queryKey),
  );

  React.useEffect(() => {
    const unsubscribe = queryClient.getQueryCache().subscribe((event) => {
      if (deepEqual(event.query.queryKey, queryKey)) {
        // setTimeout avoids the "Cannot update a component while rendering a different component" error
        setTimeout(() => {
          setData(event.query.state.data);
        }, 0);
      }
    });

    return () => {
      unsubscribe();
    };
  }, [queryClient, queryKey]);

  return data;
}
