// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Flex, FlexProps, Tooltip, useTheme } from "@chakra-ui/react";
import { Box, HStack, VStack } from "@chakra-ui/react";
import { useAtom, useAtomValue } from "jotai";
import React from "react";
import { Link as RouterLink, useLocation } from "react-router-dom";

import {
  currentEnvironmentState,
  isEnvironmentReady,
} from "~/store/environments";
import { isCurrentOrganizationBlockedAtom } from "~/store/organization";
import { MaterializeTheme } from "~/theme";

import { NAV_HOVER_STYLES } from "../constants";
import { NavItemType } from "./NavMenu";
import { isSubroute } from "./utils";

export interface NavItemProps extends NavItemType {
  nested?: boolean;
  isCollapsed?: boolean;
  closeMenu?: () => void;
}

export const NavItem = (props: NavItemProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const location = useLocation();
  const active = isSubroute(props.href, location.pathname);
  const hasChildren = props.navItems && props.navItems.length > 0;
  const showActiveStyle = props.isCollapsed || !hasChildren;

  return (
    <VStack
      width={props.isCollapsed ? undefined : "100%"}
      alignItems={props.isCollapsed ? "center" : "start"}
      spacing={0}
    >
      <NavLink
        href={props.href}
        onClick={() => {
          props.closeMenu?.();
          props.onClick?.();
        }}
        width={props.isCollapsed ? undefined : "100%"}
        state={props.state}
      >
        <HStack
          width={props.isCollapsed ? undefined : "100%"}
          aria-current={active ? "page" : undefined}
          spacing="2"
          px={2}
          py={props.nested ? 1.5 : 1}
          ml={props.nested ? 6 : 0}
          transition="all 0.2s"
          borderRadius={{ lg: "lg", md: "none" }}
          color={colors.foreground.primary}
          _hover={props.href ? NAV_HOVER_STYLES : undefined}
          _activeLink={
            showActiveStyle
              ? {
                  ...NAV_HOVER_STYLES,
                  // slightly more opaque than colors.background.accent
                  bg: "rgba(90, 52, 203, 0.2)",
                }
              : undefined
          }
        >
          {props.isCollapsed ? (
            <Tooltip label={props.label}>{props.icon}</Tooltip>
          ) : (
            <>
              {props.icon}
              <Box
                textStyle={props.nested ? "text-small-heavy" : "text-ui-med"}
              >
                {props.label}
              </Box>
            </>
          )}
        </HStack>
      </NavLink>
      {!props.isCollapsed &&
        props.navItems?.map(({ forceShow, ...itemProps }: NavItemType) => (
          <HideIfEnvironmentDisabled
            key={itemProps.label}
            forceShow={forceShow}
          >
            <NavItem
              {...itemProps}
              nested
              isCollapsed={props.isCollapsed}
              closeMenu={props.closeMenu}
            />
          </HideIfEnvironmentDisabled>
        ))}
    </VStack>
  );
};

export const NavLink = ({
  href,
  state,
  ...props
}: React.PropsWithChildren<FlexProps & { href: string; state?: object }>) => {
  if (href.search("//") === -1) {
    return <Flex as={RouterLink} state={state} to={href} {...props} />;
  }
  // React router Link doesn't support external links
  return (
    <Flex as="a" href={href} target="_blank" rel="noreferrer" {...props} />
  );
};

export const HideIfEnvironmentDisabled = ({
  children,
  forceShow,
}: {
  children?: React.ReactNode;
  forceShow?: boolean;
}) => {
  const [currentEnvironment] = useAtom(currentEnvironmentState);
  const isOrganizationBlocked = useAtomValue(isCurrentOrganizationBlockedAtom);

  if (
    !forceShow &&
    (!isEnvironmentReady(currentEnvironment) || isOrganizationBlocked)
  ) {
    return null;
  }

  return <>{children}</>;
};
