// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Avatar as ChakraAvatar,
  ButtonProps,
  HStack,
  Menu,
  MenuButton,
  MenuDivider,
  MenuGroup,
  MenuItem,
  MenuList,
  Tag,
  Text,
  useBreakpointValue,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { Link as RouterLink } from "react-router-dom";

import { useSegment } from "~/analytics/segment";
import { getCurrentTenant, useCurrentOrganization } from "~/api/auth";
import { logoutAndRedirectOrThrow } from "~/api/materialize/auth";
import ThemeSwitcher from "~/components/ThemeSwitcher";
import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import {
  AdminPortal,
  AuthActions,
  AuthState,
  type User,
} from "~/external-library-wrappers/frontegg";
import { AUTH_ROUTES } from "~/fronteggRoutes";
import { NAV_HORIZONTAL_SPACING, NAV_HOVER_STYLES } from "~/layouts/constants";
import docUrls from "~/mz-doc-urls.json";
import { MaterializeTheme } from "~/theme";

const UserInfoMenuItem = () => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <AppConfigSwitch
      cloudConfigElement={({ runtimeConfig }) => {
        if (runtimeConfig.isImpersonating) return null;
        const { user, auth, authActions } = runtimeConfig;
        return (
          <>
            <VStack
              px="3"
              pt="3"
              pb="2"
              align="left"
              lineHeight="1.3"
              spacing="0"
            >
              <Text fontWeight="semibold">{user?.name}</Text>
              <Text mt="1" fontSize="xs" color={colors.foreground.secondary}>
                {user?.email}
              </Text>
            </VStack>
            <MenuDivider />
            <OrganizationMenuGroup
              user={user}
              authState={auth}
              authActions={authActions}
            />
            <MenuDivider />
          </>
        );
      }}
    />
  );
};

const PricingMenuItem = () => (
  <MenuItem
    as={RouterLink}
    to="https://materialize.com/s/pricing"
    fontWeight="medium"
    target="_blank"
    title="See Materialize pricing"
  >
    Pricing
  </MenuItem>
);

const SignOutMenuItem = () => {
  return (
    <AppConfigSwitch
      cloudConfigElement={({ runtimeConfig: { isImpersonating } }) => {
        if (isImpersonating) return null;
        return (
          <>
            <MenuDivider />
            <MenuItem
              fontWeight="medium"
              as={RouterLink}
              to={AUTH_ROUTES.logoutPath}
            >
              Sign out
            </MenuItem>
          </>
        );
      }}
      selfManagedConfigElement={({ appConfig }) => {
        if (appConfig.authMode === "None") {
          return null;
        }
        return (
          <>
            <MenuDivider />
            <MenuItem fontWeight="medium" onClick={logoutAndRedirectOrThrow}>
              Sign out
            </MenuItem>
          </>
        );
      }}
    />
  );
};

const DefaultAvatar = () => {
  return <ChakraAvatar size="xs" />;
};

const Avatar = () => {
  return (
    <AppConfigSwitch
      cloudConfigElement={({ runtimeConfig }) => {
        if (runtimeConfig.isImpersonating) {
          return <DefaultAvatar />;
        }
        const { user } = runtimeConfig;
        return (
          <ChakraAvatar
            size="xs"
            src={user.profilePictureUrl ?? undefined}
            name={user.name}
          />
        );
      }}
      selfManagedConfigElement={() => {
        return <DefaultAvatar />;
      }}
    />
  );
};

const OrganizationName = () => {
  const { colors } = useTheme<MaterializeTheme>();

  const { organization } = useCurrentOrganization();

  if (!organization) {
    return null;
  }

  return (
    <Text
      textStyle="text-small"
      fontWeight="500"
      color={colors.foreground.secondary}
      minW="100%"
      maxW="160px"
      textAlign="left"
      noOfLines={1}
      // noOfLines sets display, so this has to come after it
      display={{ base: "none", lg: "-webkit-box" }}
    >
      {organization.name}
    </Text>
  );
};

const ProfileDropdown = ({
  isCollapsed,
  ...props
}: ButtonProps & { isCollapsed: boolean }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const menuPlacement = useBreakpointValue({
    base: "bottom" as const,
    lg: "top-end" as const,
  });

  return (
    <Menu gutter={1} placement={menuPlacement}>
      <MenuButton
        aria-label="Profile"
        title="Profile"
        px={NAV_HORIZONTAL_SPACING}
        py={1}
        borderRadius={{ base: "lg", lg: "sm" }}
        _hover={{ lg: NAV_HOVER_STYLES }}
        _active={NAV_HOVER_STYLES}
        borderWidth={{ base: "1px", lg: "0" }}
        borderColor={colors.border.secondary}
        {...props}
      >
        <HStack spacing={2}>
          <Avatar />
          {!isCollapsed && (
            <VStack alignItems="flex-start" spacing={0} width="100%">
              <AppConfigSwitch
                cloudConfigElement={({ runtimeConfig }) => {
                  const userName = runtimeConfig.isImpersonating
                    ? "Impersonation user"
                    : runtimeConfig.user.name;
                  return (
                    <>
                      <Text
                        textStyle="text-ui-med"
                        minW="100%"
                        maxW="160px"
                        noOfLines={1}
                        textAlign="left"
                        color={colors.foreground.primary}
                      >
                        {userName}
                      </Text>
                      <OrganizationName />
                    </>
                  );
                }}
                selfManagedConfigElement={
                  <Text
                    textStyle="text-ui-med"
                    color={colors.foreground.primary}
                  >
                    Settings
                  </Text>
                }
              />
            </VStack>
          )}
        </HStack>
      </MenuButton>
      <MenuList pb={2}>
        <UserInfoMenuItem />
        <ThemeSwitcher />
        <AppConfigSwitch
          cloudConfigElement={({ runtimeConfig }) => {
            if (runtimeConfig.isImpersonating) return null;
            return (
              <>
                <MenuDivider />
                <ProfileMenuItems />
              </>
            );
          }}
        />
        <MenuDivider />
        <CommunityMenuItems />
        <SignOutMenuItem />
      </MenuList>
    </Menu>
  );
};

const OrganizationMenuGroup = ({
  user,
  authState,
  authActions,
}: {
  user: User;
  authState: AuthState;
  authActions: AuthActions;
}) => {
  const { tenantsState } = authState;
  const { switchTenant } = authActions;
  const tenantSwitchingEnabled = tenantsState.tenants.length > 1;
  const currentTenant = getCurrentTenant(user, tenantsState.tenants);

  const handleTenantClick = async (tenantId: string) => {
    if (tenantSwitchingEnabled && tenantId !== currentTenant?.tenantId) {
      await switchTenant({ tenantId });
      // We want to force a full page reload to clear the query params and reset all in-memory JS state
      // after the tenant switch.
      // eslint-disable-next-line react-compiler/react-compiler
      window.location.href = window.location.href.split("?")[0];
    }
  };

  return (
    <MenuGroup title="Organization">
      {tenantsState.tenants
        .filter((tenant) => tenant && tenant.name)
        .sort((t1, t2) =>
          // always show orgs in the same order
          t1.name.toLowerCase() < t2.name.toLowerCase() ? -1 : 1,
        )
        .map((tenant) => (
          <MenuItem
            key={`org-${tenant.tenantId}`}
            isDisabled={
              !tenantSwitchingEnabled ||
              currentTenant?.tenantId === tenant.tenantId
            }
            title={
              tenantSwitchingEnabled &&
              currentTenant?.tenantId !== tenant.tenantId
                ? "Set as active organization"
                : "Current organization"
            }
            justifyContent="space-between"
            gap="var(--ck-space-2)"
            _disabled={{
              opacity: 1,
              cursor: "default",
              background: "none",
            }}
            _active={{
              // clicking flashes a background when disabled without this
              background: "none",
            }}
            _hover={{
              background:
                currentTenant?.tenantId === tenant.tenantId
                  ? "none"
                  : "default",
              cursor:
                currentTenant?.tenantId === tenant.tenantId
                  ? "default"
                  : "pointer",
            }}
            onClick={() => handleTenantClick(tenant.tenantId)}
          >
            {tenant.name}{" "}
            {currentTenant?.tenantId === tenant.tenantId && (
              <Tag size="sm" colorScheme="lavender">
                active
              </Tag>
            )}
          </MenuItem>
        ))}
    </MenuGroup>
  );
};

export const ProfileMenuItems = () => {
  return (
    <VStack spacing={0} width="100%">
      <MenuItem fontWeight="medium" onClick={() => AdminPortal.show()}>
        Account settings
      </MenuItem>
      <PricingMenuItem />
    </VStack>
  );
};

const CommunityMenuItems = () => {
  const { track } = useSegment();

  return (
    <VStack spacing={0} width="100%">
      <MenuItem
        as={RouterLink}
        to={docUrls["/docs/"]}
        onClick={() => {
          track("Link Click", {
            label: "Docs",
            href: docUrls["/docs/"],
          });
        }}
        fontWeight="medium"
        target="_blank"
        title="View Materialize documentation"
      >
        Documentation
      </MenuItem>
      <MenuItem
        as={RouterLink}
        to="https://materialize.com/s/chat"
        fontWeight="medium"
        target="_blank"
      >
        Join us on Slack
      </MenuItem>
      <MenuItem
        as={RouterLink}
        to={docUrls["/docs/support/"]}
        fontWeight="medium"
        target="_blank"
      >
        Help Center
      </MenuItem>
    </VStack>
  );
};

export default ProfileDropdown;
