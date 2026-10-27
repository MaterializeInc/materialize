// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { BoxProps, Center, Spinner } from "@chakra-ui/react";
import React from "react";

export const LoadingContainer = (props: BoxProps) => {
  return (
    <Center flex={1} css={{ width: "100%", height: "100%" }} {...props}>
      <Spinner data-testid="loading-spinner" />
    </Center>
  );
};
