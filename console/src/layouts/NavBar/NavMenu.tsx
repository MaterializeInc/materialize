// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  HStack,
  Spacer,
  StackProps,
  Text,
  Tooltip,
  useDisclosure,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { Location, useLocation } from "react-router-dom";

import { useCanViewUsage } from "~/api/auth";
import ConnectModal from "~/components/ConnectModal";
import FreeTrialNotice from "~/components/FreeTrialNotice";
import { AppConfigSwitch, CloudRuntimeConfig } from "~/config/AppConfigSwitch";
import { useFlags } from "~/hooks/useFlags";
import { useIsSuperUser } from "~/hooks/useIsSuperUser";
import { shellPath } from "~/platform/routeHelpers";
import { useRegionSlug } from "~/store/environments";
import { MonitorIcon } from "~/svg/Monitor";
import AdminIcon from "~/svg/nav/AdminIcon";
import { ClustersIcon } from "~/svg/nav/ClustersIcon";
import { DataIcon } from "~/svg/nav/DataIcon";
import GalleryIcon from "~/svg/nav/GalleryIcon";
import { MonitoringIcon } from "~/svg/nav/MonitoringIcon";
import { ShellIcon } from "~/svg/nav/ShellIcon";
import { MaterializeTheme } from "~/theme";

import { NAV_HOVER_STYLES } from "../constants";
import { HideIfEnvironmentDisabled, NavItem } from "./NavItem";

export type NavItemType = {
  href: string;
  state?: object;
  icon?: JSX.Element;
  label: string;
  navItems?: NavItemType[];
  onClick?: () => void;
  forceShow?: boolean;
};

const getNavItems = ({
  regionSlug,
  flags,
  canViewUsage,
  canViewQueryHistory,
  canViewAppPasswords,
  canViewLicenseKeys,
  canViewRoles,
  isRbacEnabled,
  location,
}: {
  regionSlug: string;
  flags: ReturnType<typeof useFlags>;
  canViewUsage: boolean;
  canViewQueryHistory: boolean;
  canViewAppPasswords: boolean;
  canViewLicenseKeys: boolean;
  canViewRoles: boolean;
  isRbacEnabled: boolean;
  location: Location;
}): NavItemType[] => {
  return [
    {
      href: shellPath(regionSlug),
      icon: <ShellIcon />,
      label: "SQL Shell",
    },
    {
      icon: <DataIcon />,
      label: "Data",
      href: `/regions/${regionSlug}/objects`,
    },
    {
      href: `/regions/${regionSlug}/clusters`,
      icon: <ClustersIcon />,
      label: "Clusters",
    },
    {
      href: `/regions/${regionSlug}/integrations`,
      icon: <GalleryIcon />,
      label: "Integrations",
    },
    {
      href: flags["environment-overview-2855"]
        ? `/regions/${regionSlug}/environment-overview`
        : `/regions/${regionSlug}/query-history`,
      icon: <MonitoringIcon />,
      label: "Monitoring",
      navItems: [
        ...(flags["environment-overview-2855"]
          ? [
              {
                label: "Environment Overview",
                href: `/regions/${regionSlug}/environment-overview`,
              },
            ]
          : []),
        ...(canViewQueryHistory
          ? [
              {
                label: "Query History",
                href: `/regions/${regionSlug}/query-history`,
              },
            ]
          : []),
        {
          label: "Sources",
          href: `/regions/${regionSlug}/sources`,
        },
        {
          label: "Sinks",
          href: `/regions/${regionSlug}/sinks`,
        },
      ],
    },
    {
      href: canViewAppPasswords
        ? "/access/app-passwords"
        : canViewLicenseKeys
          ? "/license"
          : "/access/app-passwords",
      icon: <AdminIcon />,
      label: "Admin",
      forceShow: true,
      navItems: [
        ...(canViewAppPasswords
          ? [
              {
                label: "App Passwords",
                href: "/access/app-passwords",
                forceShow: true,
              },
            ]
          : []),
        ...(canViewLicenseKeys
          ? [
              {
                label: "License",
                href: "/license",
                forceShow: true,
              },
            ]
          : []),
        ...(canViewRoles && isRbacEnabled
          ? [
              {
                label: "Roles And Users",
                href: "/roles",
              },
            ]
          : []),

        ...(canViewUsage
          ? [
              {
                label: "Usage & Billing",
                href: "/usage",
                state: { from: location.pathname + location.search },
                forceShow: true,
              },
            ]
          : []),
      ],
    },
  ];
};

export const NavMenuContainer = (props: StackProps) => {
  return (
    <VStack
      display={{ base: "none", lg: "flex" }}
      mx={4}
      spacing={2}
      flex="2"
      alignSelf="stretch"
      alignItems="stretch"
      mt={0}
      mb={{ base: 0, lg: 4 }}
      {...props}
    />
  );
};

const useCloudNavMenuItems = ({
  runtimeConfig,
}: {
  runtimeConfig: CloudRuntimeConfig;
}) => {
  const flags = useFlags();
  const location = useLocation();
  const regionSlug = useRegionSlug();
  const canViewUsage = useCanViewUsage({ runtimeConfig });
  const canViewLicenseKeys = flags["license-keys-3833"];
  const isRbacEnabled = flags["rbac-ui-9904"];
  const { isSuperUser } = useIsSuperUser();

  return getNavItems({
    regionSlug,
    flags,
    canViewUsage,
    location,
    canViewQueryHistory: true,
    canViewAppPasswords: true,
    canViewLicenseKeys,
    canViewRoles: Boolean(isSuperUser),
    isRbacEnabled,
  });
};

const useSelfManagedNavMenuItems = () => {
  const flags = useFlags();
  const location = useLocation();
  const regionSlug = useRegionSlug();
  const canViewLicenseKeys = flags["license-keys-3833"];
  const isRbacEnabled = flags["rbac-ui-9904"];
  const { isSuperUser } = useIsSuperUser();

  return getNavItems({
    regionSlug,
    flags,
    canViewUsage: false,
    location,
    // TODO (SangJunBak): Remove guard once we want to re-enable Query History
    //  for self managed, see <https://github.com/MaterializeInc/cloud/issues/10755>
    canViewQueryHistory: false,
    canViewAppPasswords: false,
    canViewLicenseKeys,
    canViewRoles: Boolean(isSuperUser),
    isRbacEnabled,
  });
};

const NavMenu = (props: { isCollapsed?: boolean; items: NavItemType[] }) => {
  return (
    <NavMenuContainer>
      {props.items.map((item) => (
        <HideIfEnvironmentDisabled key={item.label} forceShow={item.forceShow}>
          <NavItem key={item.label} {...item} isCollapsed={props.isCollapsed} />
        </HideIfEnvironmentDisabled>
      ))}
    </NavMenuContainer>
  );
};

const NavMenuMobile = (props: {
  closeMenu: () => void;
  offsetY: number | undefined;
  items: NavItemType[];
}) => {
  const {
    isOpen: isConnectModalOpen,
    onClose: onCloseConnectModal,
    onOpen: onOpenConnectModal,
  } = useDisclosure();

  return (
    <VStack
      spacing={4}
      flex="2"
      alignSelf="stretch"
      alignItems="stretch"
      height={`calc(100vh - ${props.offsetY}px)`}
      overflowY="auto"
    >
      <VStack px="4" py="6">
        {props.items.map((item) => (
          <HideIfEnvironmentDisabled
            key={item.label}
            forceShow={item.forceShow}
          >
            <NavItem key={item.label} closeMenu={props.closeMenu} {...item} />
          </HideIfEnvironmentDisabled>
        ))}
      </VStack>
      <Spacer />
      <VStack align="stretch">
        <FreeTrialNotice />
        <VStack>
          <AppConfigSwitch
            cloudConfigElement={({ runtimeConfig }) =>
              runtimeConfig.isImpersonating ? null : (
                <HideIfEnvironmentDisabled>
                  <ConnectMenuItem
                    width="100%"
                    onClick={onOpenConnectModal}
                    mb={{ base: 0, lg: 6 }}
                  />
                  <ConnectModal
                    onClose={onCloseConnectModal}
                    isOpen={isConnectModalOpen}
                    user={runtimeConfig.user}
                  />
                </HideIfEnvironmentDisabled>
              )
            }
          />
        </VStack>
      </VStack>
    </VStack>
  );
};

type BaseCloudNavMenuProps = {
  runtimeConfig: CloudRuntimeConfig;
};

type MobileNavMenuProps = {
  isMobile: true;
  closeMenu: () => void;
  offsetY: number | undefined;
};

type DesktopNavMenuProps = {
  isMobile: false;
  isCollapsed?: boolean;
};

export const CloudNavMenu = (
  props:
    | (BaseCloudNavMenuProps & DesktopNavMenuProps)
    | (BaseCloudNavMenuProps & MobileNavMenuProps),
) => {
  const items = useCloudNavMenuItems({
    runtimeConfig: props.runtimeConfig,
  });

  if (props.isMobile) {
    return <NavMenuMobile {...props} items={items} />;
  }

  return <NavMenu {...props} items={items} />;
};

export const SelfManagedNavMenu = (
  props: DesktopNavMenuProps | MobileNavMenuProps,
) => {
  const items = useSelfManagedNavMenuItems();

  if (props.isMobile) {
    return <NavMenuMobile {...props} items={items} />;
  }

  return <NavMenu {...props} items={items} />;
};

export const ConnectMenuItem = ({
  isCollapsed,
  ...props
}: StackProps & { isCollapsed?: boolean }) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <HStack
      as="button"
      spacing="3"
      px={4}
      py={2}
      cursor="pointer"
      _hover={NAV_HOVER_STYLES}
      _active={NAV_HOVER_STYLES}
      aria-label="Connect"
      title="Connect"
      data-testid="connect-menu-button"
      borderRadius="sm"
      borderColor={colors.border.secondary}
      justifyContent={isCollapsed ? "center" : undefined}
      {...props}
    >
      {isCollapsed ? (
        <Tooltip label="Connect">
          <MonitorIcon w="4" h="4" />
        </Tooltip>
      ) : (
        <>
          <MonitorIcon w="4" h="4" />
          <Text textStyle="text-ui-med" color={colors.foreground.secondary}>
            Connect
          </Text>
        </>
      )}
    </HStack>
  );
};
