// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useAtom } from "jotai";
import React from "react";
import { Route } from "react-router-dom";

import { CodeMirrorProvider } from "~/components/CommandBlock";
import { SentryRoutes } from "~/sentry";
import { currentRegionIdAtom } from "~/store/environments";

import Shell from "./Shell";
import { ShellVirtualizedListProvider } from "./ShellVirtualizedList";

const ShellRoutes = () => {
  const [currentRegionId] = useAtom(currentRegionIdAtom);

  return (
    <SentryRoutes>
      <Route
        path="/"
        element={
          // Remount the Shell when the region changes to clear any local state
          <ShellVirtualizedListProvider key={currentRegionId}>
            <CodeMirrorProvider>
              <Shell />
            </CodeMirrorProvider>
          </ShellVirtualizedListProvider>
        }
      />
    </SentryRoutes>
  );
};

export default ShellRoutes;
