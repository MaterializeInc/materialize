// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Flex, FlexProps, Text, useTheme } from "@chakra-ui/react";
import React from "react";

import { RoleItem } from "~/api/materialize/roles/rolesList";
import { WORKFLOW_GRAPH_NODE_Z_INDEX } from "~/layouts/zIndex";
import { MaterializeTheme } from "~/theme";

/** Width of a role node, 280px plus 2px for borders */
export const ROLE_NODE_WIDTH = 200;
/** Height of a role node, 58px plus 2px for borders */
export const ROLE_NODE_HEIGHT = 60;

export const RoleGraphNode = ({
  roleData,
  isSelected,
  isFiltered = true,
  ...flexProps
}: {
  roleData: RoleItem;
  isSelected: boolean;
  isFiltered?: boolean;
} & Omit<FlexProps, "role">) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();

  const memberCount = Number(roleData.memberCount);
  const userLabel = memberCount === 1 ? "user" : "users";

  return (
    <Flex
      direction="column"
      position="absolute"
      zIndex={WORKFLOW_GRAPH_NODE_Z_INDEX}
      opacity={isFiltered ? 1 : 0.3}
      transition="opacity 0.2s"
      {...flexProps}
    >
      <Box
        as="button"
        backgroundColor={colors.background.secondary}
        border="1px solid"
        borderColor={
          isSelected ? colors.accent.brightPurple : colors.border.secondary
        }
        borderRadius="8px"
        boxShadow={
          isSelected
            ? shadows.input.focus
            : "0px 0.5px 2.5px 0 rgba(0, 0, 0, 0.08)"
        }
        px="4"
        py="3"
        width={`${ROLE_NODE_WIDTH}px`}
        textAlign="left"
      >
        <Text
          textStyle="text-ui-med"
          overflow="hidden"
          textOverflow="ellipsis"
          whiteSpace="nowrap"
          mb="1"
        >
          {roleData.roleName}
        </Text>
        <Text
          textStyle="text-small"
          fontWeight="500"
          color={colors.foreground.secondary}
        >
          {memberCount} {userLabel}
        </Text>
      </Box>
    </Flex>
  );
};
