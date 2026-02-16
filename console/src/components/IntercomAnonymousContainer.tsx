// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { VStack } from "@chakra-ui/react";
import React from "react";

import { useIntercomAnonymous } from "~/hooks/useIntercom";
import { DEVTOOL_BUTTONS_Z_INDEX } from "~/layouts/zIndex";

// This is a container for anonymous/ unauthenticated users
export const IntercomAnonymousContainer = () => {
  useIntercomAnonymous();
  return (
    <VStack
      position="fixed"
      bottom="4"
      right="4"
      zIndex={DEVTOOL_BUTTONS_Z_INDEX + 1}
    ></VStack>
  );
};
