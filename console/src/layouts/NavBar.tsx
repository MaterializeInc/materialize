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
  Flex,
  forwardRef,
  HStack,
  IconButton,
  Spacer,
  Spinner,
  useBreakpointValue,
  useDisclosure,
  usePopper,
  useTheme,
} from "@chakra-ui/react";
import * as React from "react";
import { Link as RouterLink } from "react-router-dom";
import useResizeObserver from "use-resize-observer";

import ConnectModal from "~/components/ConnectModal";
import FreeTrialNotice from "~/components/FreeTrialNotice";
import { MaterializeLogo } from "~/components/MaterializeLogo";
import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import EnvironmentSelectField from "~/layouts/EnvironmentSelect";
import ProfileDropdown from "~/layouts/ProfileDropdown";
import { CloseIcon } from "~/svg/CloseIcon";
import { HamburgerIcon } from "~/svg/HamburgerIcon";
import { MaterializeTheme } from "~/theme";

import {
  NAV_HORIZONTAL_SPACING,
  NAV_MIN_HEIGHT_PX,
  NAV_MIN_WIDTH_PX,
} from "./constants";
import { CreateObjectButton } from "./NavBar/CreateObjectButton";
import { HideIfEnvironmentDisabled } from "./NavBar/NavItem";
import {
  CloudNavMenu,
  ConnectMenuItem,
  SelfManagedNavMenu,
} from "./NavBar/NavMenu";
import { NAVBAR_Z_INDEX } from "./zIndex";

export { NavMenuContainer } from "./NavBar/NavMenu";

export const NavBarContainer = forwardRef<
  React.PropsWithChildren<{ isCollapsed?: boolean }>,
  "div"
>((props, ref) => {
  const { colors } = useTheme<MaterializeTheme>();
  const lgWidth = props.isCollapsed ? "72px" : `${NAV_MIN_WIDTH_PX}px`;

  return (
    <Flex
      ref={ref}
      direction={{ base: "row", lg: "column" }}
      justify="flex-start"
      align={{ base: "center", lg: "stretch" }}
      alignItems={props.isCollapsed ? "center" : undefined}
      px={{ base: 6, lg: 0 }}
      bg={colors.background.secondary}
      color={colors.foreground.primary}
      minH={{ base: `${NAV_MIN_HEIGHT_PX}px`, lg: "full" }}
      borderRightWidth={{ base: 0, lg: 1 }}
      borderBottomWidth={{ base: 1, lg: 0 }}
      borderColor={colors.border.primary}
      width={{
        base: "100%",
        lg: lgWidth,
      }}
      minWidth={{ lg: lgWidth }}
      zIndex={NAVBAR_Z_INDEX}
    >
      {props.children}
    </Flex>
  );
});

export const NavBarHeader = (
  props: React.PropsWithChildren<{ isCollapsed?: boolean; isMobile?: boolean }>,
) => {
  const isSmallBreakpoint = useBreakpointValue(
    {
      base: true,
      md: false,
    },
    { ssr: false },
  );
  const markOnly = isSmallBreakpoint || props.isCollapsed;
  const centerLogo = markOnly && !props.isMobile;

  return (
    <HStack
      px={{ base: 0, lg: NAV_HORIZONTAL_SPACING }}
      flexGrow="0"
      flexShrink="0"
      justifyContent="flex-start"
    >
      {props.children}
      <HStack
        as={RouterLink}
        to="/"
        ml={props.isCollapsed ? 0 : 2}
        flexGrow="0"
        flexShrink="0"
        width="full"
        justifyContent={centerLogo ? "center" : "flex-start"}
        py={{ lg: 6, sm: 2 }}
        mt="3px"
      >
        <MaterializeLogo markOnly={isSmallBreakpoint || props.isCollapsed} />
      </HStack>
    </HStack>
  );
};

export const NavBarEnvironmentSelect = () => {
  return (
    <AppConfigSwitch
      cloudConfigElement={({ runtimeConfig }) => {
        if (runtimeConfig.isImpersonating) return null;

        return (
          <Flex
            alignItems="flex-start"
            justifyContent="stretch"
            flexShrink="0"
            px={NAV_HORIZONTAL_SPACING}
            mb={{ base: 0, lg: 4 }}
          >
            <React.Suspense fallback={<Spinner />}>
              <EnvironmentSelectField user={runtimeConfig.user} />
            </React.Suspense>
          </Flex>
        );
      }}
    />
  );
};

export interface NavBarProps {
  isCollapsed: boolean;
}

export const NavBar = ({ isCollapsed }: NavBarProps) => {
  const {
    isOpen: isConnectModalOpen,
    onClose: onCloseConnectModal,
    onOpen: onOpenConnectModal,
  } = useDisclosure();
  const {
    isOpen: isMobileNavOpen,
    onToggle: toggleMobileNav,
    onClose: closeMobileNav,
  } = useDisclosure();
  const navBarContainerRef = React.useRef<HTMLElement | null>(null);
  const { height: navBarContainerHeight } = useResizeObserver<HTMLElement>({
    ref: navBarContainerRef,
  });
  const { referenceRef, popperRef } = usePopper({
    gutter: 0,
    placement: "bottom-start",
  });
  const { colors } = useTheme<MaterializeTheme>();
  const isMobile = useBreakpointValue(
    {
      base: true,
      lg: false,
    },
    { ssr: false },
  );

  if (!isMobile && isMobileNavOpen) {
    closeMobileNav();
  }

  return (
    <NavBarContainer
      isCollapsed={isCollapsed}
      ref={(el) => {
        referenceRef(el);
        navBarContainerRef.current = el;
      }}
    >
      <Flex
        alignItems={{ base: "center", lg: "stretch" }}
        direction={{ base: "row", lg: "column" }}
        flexGrow="1"
        overflow="auto"
      >
        <NavBarHeader isCollapsed={isCollapsed} isMobile={isMobile}>
          {isMobile && (
            <IconButton
              aria-label="Open navigation menu"
              icon={isMobileNavOpen ? <CloseIcon /> : <HamburgerIcon />}
              onClick={toggleMobileNav}
              variant="inline"
            />
          )}
        </NavBarHeader>
        {isMobile && <Spacer />}
        {!isCollapsed && <NavBarEnvironmentSelect />}
        {isMobileNavOpen && (
          <Box
            background={colors.background.secondary}
            borderColor={colors.border.primary}
            borderRightWidth={1}
            ref={popperRef}
            width="272px"
          >
            <AppConfigSwitch
              cloudConfigElement={({ runtimeConfig }) => (
                <CloudNavMenu
                  closeMenu={closeMobileNav}
                  offsetY={navBarContainerHeight}
                  runtimeConfig={runtimeConfig}
                  isMobile={true}
                />
              )}
              selfManagedConfigElement={
                <SelfManagedNavMenu
                  closeMenu={closeMobileNav}
                  offsetY={navBarContainerHeight}
                  isMobile={true}
                />
              }
            />
          </Box>
        )}
        <CreateObjectButton isCollapsed={isCollapsed} />
        <AppConfigSwitch
          cloudConfigElement={({ runtimeConfig }) => (
            <CloudNavMenu
              isCollapsed={isCollapsed}
              runtimeConfig={runtimeConfig}
              isMobile={false}
            />
          )}
          selfManagedConfigElement={
            <SelfManagedNavMenu isCollapsed={isCollapsed} isMobile={false} />
          }
        />
        {!isMobile && <Spacer />}
        {!isMobile && !isCollapsed && (
          <FreeTrialNotice mb="4" mx={{ lg: "4" }} />
        )}
      </Flex>
      <Flex
        direction={{ base: "row", lg: "column" }}
        align={{ base: "center", lg: "stretch" }}
        gap={{ base: "4", lg: "0" }}
        py={{ base: 0, lg: 2 }}
      >
        {!isMobile && (
          <AppConfigSwitch
            cloudConfigElement={({ runtimeConfig }) =>
              runtimeConfig.isImpersonating ? null : (
                <HideIfEnvironmentDisabled>
                  <ConnectMenuItem
                    isCollapsed={isCollapsed}
                    width="100%"
                    onClick={onOpenConnectModal}
                  />

                  <ConnectModal
                    user={runtimeConfig.user}
                    onClose={onCloseConnectModal}
                    isOpen={isConnectModalOpen}
                  />
                </HideIfEnvironmentDisabled>
              )
            }
          />
        )}
        <ProfileDropdown
          display="flex"
          width={{ base: "auto", lg: "100%" }}
          isCollapsed={isCollapsed}
        />
      </Flex>
    </NavBarContainer>
  );
};
