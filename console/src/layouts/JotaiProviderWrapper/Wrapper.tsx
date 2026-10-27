// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Provider as JotaiProvider } from "jotai";
import React from "react";

import { getStore, resetStore } from "~/jotai";

import { ResetStoreContext } from "./Context";

export const JotaiProviderWrapper = (props: React.PropsWithChildren) => {
  const [key, setKey] = React.useState(0);

  const reset = React.useCallback(() => {
    setKey(key + 1);
    resetStore();
  }, [key]);

  return (
    <ResetStoreContext.Provider value={reset}>
      <JotaiProvider key={key} store={getStore()}>
        {props.children}
      </JotaiProvider>
    </ResetStoreContext.Provider>
  );
};
