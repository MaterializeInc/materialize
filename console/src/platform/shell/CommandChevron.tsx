// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, BoxProps, useTheme } from "@chakra-ui/react";
import React from "react";

import ChevronRightIcon from "~/svg/ChevronRightIcon";
import { MaterializeTheme } from "~/theme";

type CommandChevronProps = BoxProps;

const CommandChevron = (props: CommandChevronProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Box
      {...props}
      fontSize="lg"
      lineHeight="5"
      height="5"
      color={props.color ?? colors.accent.purple}
      userSelect="none"
    >
      <ChevronRightIcon
        height="5"
        width="5"
        color={colors.accent.brightPurple}
      />
    </Box>
  );
};

export default CommandChevron;
