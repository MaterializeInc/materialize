// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Button, HStack, useTheme } from "@chakra-ui/react";
import React from "react";
import { NavLink } from "react-router-dom";

import { PageHeading } from "~/layouts/BaseLayout";
import { MaterializeTheme } from "~/theme";

export interface RolesViewHeaderProps {
  onCreateRole: () => void;
  searchInput?: React.ReactNode;
}

export const RolesViewHeader = ({
  onCreateRole,
  searchInput,
}: RolesViewHeaderProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <HStack width="100%" justifyContent="space-between" mb={4}>
      <PageHeading>Roles</PageHeading>
      <HStack spacing="4">
        {searchInput}
        <HStack
          spacing="0"
          border="1px solid"
          borderColor={colors.border.secondary}
          borderRadius="md"
        >
          <NavLink to="/roles" end>
            {({ isActive }) => (
              <Button
                size="sm"
                variant="ghost"
                borderRadius="md"
                bg={isActive ? colors.background.secondary : "transparent"}
              >
                Table
              </Button>
            )}
          </NavLink>
          <NavLink to="/roles/graph">
            {({ isActive }) => (
              <Button
                size="sm"
                variant="ghost"
                borderRadius="md"
                bg={isActive ? colors.background.secondary : "transparent"}
              >
                Graph
              </Button>
            )}
          </NavLink>
        </HStack>
        <Button variant="primary" size="sm" onClick={onCreateRole}>
          Create new role
        </Button>
      </HStack>
    </HStack>
  );
};

export default RolesViewHeader;
