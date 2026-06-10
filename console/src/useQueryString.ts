// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";
import { useLocation, useNavigate } from "react-router-dom";

export const useQueryStringState = (queryStringKey: string) => {
  const navigate = useNavigate();
  const location = useLocation();
  const [value, setValue] = React.useState<string | undefined>();

  const setSelectedValue = React.useCallback(
    (val: string | undefined) => {
      const searchParams = new URLSearchParams(location.search);
      setValue(val);
      if (!val) {
        searchParams.delete(queryStringKey);
      } else {
        searchParams.set(queryStringKey, val);
      }
      navigate(
        `${location.pathname}?${searchParams.toString()}${location.hash}`,
        {
          replace: true,
        },
      );
    },
    [
      location.hash,
      location.pathname,
      location.search,
      navigate,
      queryStringKey,
    ],
  );

  React.useEffect(() => {
    const url = new URL(window.location.toString());
    const val = url.searchParams.get(queryStringKey);
    if (val) {
      setSelectedValue(val);
    }
  }, [queryStringKey, setSelectedValue]);

  return [value, setSelectedValue] as const;
};
