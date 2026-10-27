// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Flex, useTheme } from "@chakra-ui/react";
import React from "react";

import { LoadingAnimation } from "~/icons";
import { MaterializeTheme } from "~/theme";

const LoadingScreen = () => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Flex
      direction="column"
      height="100vh"
      width="100vw"
      align="center"
      justify="center"
    >
      <LoadingAnimation
        height="auto"
        width="128px"
        fill={colors.accent.purple}
      />
    </Flex>
  );
};

export default LoadingScreen;
