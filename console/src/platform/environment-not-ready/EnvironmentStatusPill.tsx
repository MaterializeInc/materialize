// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { BoxProps, Flex, Text, useTheme } from "@chakra-ui/react";
import React from "react";

import { Environment } from "~/store/environments";
import { MaterializeTheme } from "~/theme";

import {
  getBackgroundColor,
  getBorderColor,
  getIcon,
  getText,
  getTextColor,
} from "./utils";

export type EnvironmentStatusPillProps = BoxProps & {
  environment: Environment;
  regionId: string;
};

const EnvironmentStatusPill = ({
  environment,
  regionId,
  ...boxProps
}: EnvironmentStatusPillProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Flex
      width="100%"
      alignItems="center"
      gap="6px"
      borderRadius="40px"
      paddingY="4px"
      paddingLeft="8px"
      paddingRight="12px"
      textAlign="left"
      textStyle="text-ui-med"
      border="1px solid"
      borderColor={getBorderColor(colors, environment)}
      backgroundColor={getBackgroundColor(colors, environment)}
      color={getTextColor(colors, environment)}
      {...boxProps}
    >
      {getIcon(environment)}
      <Text noOfLines={1}>{getText(environment, regionId)}</Text>
    </Flex>
  );
};

export default EnvironmentStatusPill;
