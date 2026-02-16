// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Box,
  Button,
  MenuItem,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useDisclosure,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { useState } from "react";
import { useNavigate } from "react-router-dom";

import { CodeBlock } from "~/components/copyableComponents";
import OverflowMenu, { OVERFLOW_BUTTON_WIDTH } from "~/components/OverflowMenu";
import { SideDrawer } from "~/components/SideDrawer";
import { AdminShieldIcon } from "~/icons";
import {
  EmptyListHeader,
  EmptyListHeaderContents,
  EmptyListWrapper,
  IconBox,
  SampleCodeBoxWrapper,
} from "~/layouts/listPageComponents";
import docUrls from "~/mz-doc-urls.json";
import { RoleItem } from "~/store/allRoles";
import { MaterializeTheme } from "~/theme";

import DropRoleMenuItem from "./DropRoleMenuItem";
import { RoleDetails } from "./RoleDetails";

const ROLE_CREATE_SQL = `CREATE ROLE <role_name>;

-- Grant a role to another role
GRANT <role_name> TO <other_role>;`;

export const RolesEmptyState = ({
  onCreateRole,
}: {
  onCreateRole?: () => void;
}) => {
  return (
    <EmptyListWrapper>
      <EmptyListHeader>
        <IconBox type="Empty">
          <Box mt="1px">
            <AdminShieldIcon />
          </Box>
        </IconBox>

        <EmptyListHeaderContents
          title="No available roles"
          helpText="Create a role to manage access control in Materialize."
        />
      </EmptyListHeader>
      {onCreateRole && (
        <Button variant="primary" onClick={onCreateRole}>
          Create new role
        </Button>
      )}
      <SampleCodeBoxWrapper docsUrl={docUrls["/docs/sql/create-role/"]}>
        <CodeBlock
          title="Create a role"
          contents={ROLE_CREATE_SQL}
          lineNumbers
        />
      </SampleCodeBoxWrapper>
    </EmptyListWrapper>
  );
};

export const RolesEmptyFilteredState = ({
  clearFilters,
}: {
  clearFilters: () => void;
}) => {
  return (
    <EmptyListWrapper>
      <Text textStyle="heading-sm">No roles match the selected filter</Text>
      <Button variant="primary" onClick={clearFilters}>
        Clear filters
      </Button>
    </EmptyListWrapper>
  );
};

export const RolesList = ({ roles }: { roles: RoleItem[] }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const navigate = useNavigate();
  const {
    isOpen: isDrawerOpen,
    onOpen: onDrawerOpen,
    onClose: onDrawerClose,
  } = useDisclosure();
  const [selectedRoleName, setSelectedRoleName] = useState<string | null>(null);

  const handleRowClick = (roleName: string) => {
    setSelectedRoleName(roleName);
    onDrawerOpen();
  };

  const handleEditClick = (roleName: string, e?: React.MouseEvent) => {
    e?.stopPropagation();
    navigate(`/roles/edit/${encodeURIComponent(roleName)}`);
  };

  return (
    <>
      <VStack alignItems="flex-start" width="100%" spacing={6}>
        <Table variant="linkable" data-testid="roles-table">
          <Thead>
            <Tr>
              <Th>Role name</Th>
              <Th>Number of users</Th>
              <Th width={OVERFLOW_BUTTON_WIDTH}></Th>
            </Tr>
          </Thead>
          <Tbody>
            {roles.map((role) => (
              <Tr
                key={role.roleName}
                onClick={() => handleRowClick(role.roleName)}
                cursor="pointer"
                _hover={{
                  bg: colors.background.secondary,
                }}
              >
                <Td>
                  <Text textStyle="text-ui-med">{role.roleName}</Text>
                </Td>
                <Td>
                  <Text textStyle="text-ui-reg">
                    {role.memberCount.toString()}
                  </Text>
                </Td>
                <Td>
                  <OverflowMenu
                    items={[
                      {
                        visible: true,
                        render: () => (
                          <MenuItem
                            onClick={(e) => handleEditClick(role.roleName, e)}
                          >
                            Edit role
                          </MenuItem>
                        ),
                      },
                      {
                        visible: true,
                        render: () => (
                          <DropRoleMenuItem
                            roleName={role.roleName}
                            ownedObjectsCount={role.ownedObjectsCount}
                          />
                        ),
                      },
                    ]}
                  />
                </Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      </VStack>

      <SideDrawer
        isOpen={isDrawerOpen}
        onClose={onDrawerClose}
        title={selectedRoleName}
        width="66%"
        trapFocus={false}
      >
        {selectedRoleName && <RoleDetails roleName={selectedRoleName} />}
      </SideDrawer>
    </>
  );
};
