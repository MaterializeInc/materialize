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
  HStack,
  IconButton,
  Menu,
  MenuButton,
  MenuItem,
  MenuList,
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
import React, { useMemo, useState } from "react";

import Alert from "~/components/Alert";
import { LoadingContainer } from "~/components/LoadingContainer";
import SearchInput from "~/components/SearchInput";
import { AdminShieldIcon } from "~/icons";
import {
  MainContentContainer,
  PageHeader,
  PageHeading,
} from "~/layouts/BaseLayout";
import {
  EmptyListHeader,
  EmptyListHeaderContents,
  EmptyListWrapper,
  IconBox,
} from "~/layouts/listPageComponents";
import OverflowMenuIcon from "~/svg/OverflowMenuIcon";
import { MaterializeTheme } from "~/theme";

import EditRolesModal from "./EditRolesModal";
import { useUsersList } from "./queries";
import { ALL_ROLES_FILTER_ID, RoleFilter } from "./RoleFilter";
import { useUserRoleFilters } from "./useUserRoleFilters";

const UsersEmptyState = () => {
  return (
    <EmptyListWrapper>
      <EmptyListHeader>
        <IconBox type="Empty">
          <Box mt="1px">
            <AdminShieldIcon />
          </Box>
        </IconBox>
        <EmptyListHeaderContents
          title="No users found"
          helpText="Users will appear here once they have been created."
        />
      </EmptyListHeader>
    </EmptyListWrapper>
  );
};

const UsersEmptyFilteredState = ({
  clearFilters,
}: {
  clearFilters: () => void;
}) => {
  return (
    <EmptyListWrapper>
      <Text textStyle="heading-sm">No users match the selected filters</Text>
      <Button variant="primary" onClick={clearFilters}>
        Clear filters
      </Button>
    </EmptyListWrapper>
  );
};

const RolesCell = ({ roles }: { roles: string[] | null }) => {
  const { colors } = useTheme<MaterializeTheme>();

  if (!roles || roles.length === 0) {
    return (
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        No roles assigned
      </Text>
    );
  }

  return <Text textStyle="text-ui-reg">{roles.join(", ")}</Text>;
};

const UserRowMenu = ({
  userName,
  currentRoles,
  onEditRoles,
}: {
  userName: string;
  currentRoles: string[];
  onEditRoles: () => void;
}) => {
  return (
    <Menu>
      <MenuButton
        as={IconButton}
        aria-label="More actions"
        icon={<OverflowMenuIcon />}
        variant="ghost"
        size="sm"
        onClick={(e) => e.stopPropagation()}
      />
      <MenuList>
        <MenuItem onClick={onEditRoles}>Edit roles</MenuItem>
      </MenuList>
    </Menu>
  );
};

export const UsersListPage = () => {
  const { colors } = useTheme<MaterializeTheme>();
  const { data, isLoading, isError } = useUsersList();
  const { nameFilter, roleFilter } = useUserRoleFilters();

  const { isOpen, onOpen, onClose } = useDisclosure();
  const [selectedUser, setSelectedUser] = useState<{
    userName: string;
    currentRoles: string[];
  } | null>(null);

  const handleEditRoles = (userName: string, currentRoles: string[]) => {
    setSelectedUser({ userName, currentRoles });
    onOpen();
  };

  const handleModalClose = () => {
    onClose();
    setSelectedUser(null);
  };

  const filteredUsers = useMemo(() => {
    if (!data?.rows) return [];

    return data.rows.filter((user) => {
      const matchesSearch =
        !nameFilter.name ||
        user.user_name.toLowerCase().includes(nameFilter.name.toLowerCase());

      const matchesRole =
        roleFilter.selected.id === ALL_ROLES_FILTER_ID ||
        (user.roles_granted &&
          user.roles_granted.includes(roleFilter.selected.id));

      return matchesSearch && matchesRole;
    });
  }, [data?.rows, nameFilter.name, roleFilter.selected.id]);

  if (isLoading) {
    return (
      <MainContentContainer>
        <LoadingContainer />
      </MainContentContainer>
    );
  }

  if (isError) {
    return (
      <MainContentContainer>
        <Alert
          variant="error"
          message="Failed to load users. Please try refreshing the page."
        />
      </MainContentContainer>
    );
  }

  const users = data?.rows ?? [];

  if (users.length === 0) {
    return (
      <MainContentContainer>
        <UsersEmptyState />
      </MainContentContainer>
    );
  }

  const hasFilters =
    !!nameFilter.name || roleFilter.selected.id !== ALL_ROLES_FILTER_ID;

  return (
    <MainContentContainer>
      <VStack alignItems="flex-start" width="100%" spacing={4}>
        <PageHeader boxProps={{ mb: 0, px: 0, width: "100%" }}>
          <HStack width="100%" justifyContent="space-between">
            <PageHeading>Users</PageHeading>
            <HStack spacing="4">
              <SearchInput
                name="user"
                value={nameFilter.name}
                onChange={(e) => nameFilter.setName(e.target.value)}
                placeholder="Search users"
              />
              <RoleFilter
                roleOptions={roleFilter.roleOptions}
                selected={roleFilter.selected}
                setSelected={roleFilter.setSelected}
                containerWidth="180px"
              />
            </HStack>
          </HStack>
        </PageHeader>

        <Text textStyle="text-small" color={colors.foreground.secondary}>
          {filteredUsers.length} of {users.length} users
        </Text>

        {filteredUsers.length === 0 && hasFilters ? (
          <UsersEmptyFilteredState
            clearFilters={() => {
              nameFilter.setName(undefined);
              roleFilter.setSelected(undefined);
            }}
          />
        ) : (
          <Table variant="linkable" data-testid="users-list-table">
            <Thead>
              <Tr>
                <Th>Email</Th>
                <Th>Roles</Th>
                <Th width="50px"></Th>
              </Tr>
            </Thead>
            <Tbody>
              {filteredUsers.map((user) => (
                <Tr key={user.id}>
                  <Td>
                    <Text textStyle="text-ui-med">{user.user_name}</Text>
                  </Td>
                  <Td>
                    <RolesCell roles={user.roles_granted} />
                  </Td>
                  <Td>
                    <UserRowMenu
                      userName={user.user_name}
                      currentRoles={user.roles_granted || []}
                      onEditRoles={() =>
                        handleEditRoles(
                          user.user_name,
                          user.roles_granted || [],
                        )
                      }
                    />
                  </Td>
                </Tr>
              ))}
            </Tbody>
          </Table>
        )}

        {selectedUser && (
          <EditRolesModal
            isOpen={isOpen}
            onClose={handleModalClose}
            userName={selectedUser.userName}
            currentRoles={selectedUser.currentRoles}
          />
        )}
      </VStack>
    </MainContentContainer>
  );
};

export default UsersListPage;
