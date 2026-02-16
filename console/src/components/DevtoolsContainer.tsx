// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { VStack } from "@chakra-ui/react";
import { ReactQueryDevtools } from "@tanstack/react-query-devtools";
import { DevTools as JotaiDevtools } from "jotai-devtools";
import React, { Suspense } from "react";

import { getStore } from "~/jotai";
import { DEVTOOL_BUTTONS_Z_INDEX } from "~/layouts/zIndex";

const SwitchStackModal = React.lazy(() => import("./SwitchStackModal"));

export const DevtoolsContainer = () => {
  return (
    <VStack
      position="fixed"
      bottom="4"
      right="4"
      zIndex={DEVTOOL_BUTTONS_Z_INDEX}
    >
      <ReactQueryDevtools buttonPosition="relative" />
      <JotaiDevtools position="bottom-right" store={getStore()} />
      <Suspense fallback={null}>
        <SwitchStackModal />
      </Suspense>
    </VStack>
  );
};
