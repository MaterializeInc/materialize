// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Button,
  Flex,
  forwardRef,
  Popover,
  PopoverContent,
  PopoverTrigger,
  Text,
  Tooltip,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { Link, LinkProps } from "react-router-dom";

import { useSegment } from "~/analytics/segment";
import useCanCreateCluster from "~/api/materialize/cluster/useCanCreateCluster";
import useCanCreateObjects from "~/api/materialize/useCanCreateObjects";
import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import { regionPath } from "~/platform/routeHelpers";
import { NEW_SOURCE_BUTTON_DISABLED_MESSAGE } from "~/platform/sources/constants";
import { useRegionSlug } from "~/store/environments";
import { PlusIcon } from "~/svg/PlusIcon";
import { MaterializeTheme } from "~/theme";

import { NAV_HOVER_STYLES } from "../constants";

export interface CreateObjectButtonProps {
  isCollapsed: boolean;
}

export const CreateObjectButton = (props: CreateObjectButtonProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { track } = useSegment();
  const regionSlug = useRegionSlug();
  const { results: canCreateCluster } = useCanCreateCluster();
  const { results: canCreate } = useCanCreateObjects();
  const initialFocusRef = React.useRef<HTMLAnchorElement>(null);

  const newClusterPath = regionPath(regionSlug) + "/clusters/new";
  const newSourcePath = regionPath(regionSlug) + "/sources/new";

  return (
    <Flex
      borderBottomWidth={{ base: 0, lg: "1px" }}
      borderColor={colors.border.primary}
      mb={{ base: 0, lg: 4 }}
      pb={{ base: 0, lg: 2 }}
      mr={{ base: 4, lg: 4 }}
      ml={{ base: 0, lg: 4 }}
    >
      <Popover
        gutter={2}
        placement="bottom-start"
        initialFocusRef={initialFocusRef}
        closeOnBlur
      >
        <PopoverTrigger>
          <Button
            _hover={NAV_HOVER_STYLES}
            justifyContent="flex-start"
            px="2"
            size="sm"
            variant="ghost"
            width="100%"
          >
            {props.isCollapsed ? (
              <Tooltip label="Create new">
                <PlusIcon _hover={NAV_HOVER_STYLES} />
              </Tooltip>
            ) : (
              <>
                <PlusIcon _hover={NAV_HOVER_STYLES} mr="1" />
                Create New
              </>
            )}
          </Button>
        </PopoverTrigger>
        <PopoverContent
          background={colors.components.card.background}
          py="2"
          width="240px"
          motionProps={{
            animate: false,
          }}
        >
          <VStack alignItems="flex-start" spacing="2" width="100%">
            <Text
              color={colors.foreground.tertiary}
              textStyle="text-small-heavy"
              mx="2"
            >
              Create a new...
            </Text>
            {canCreateCluster && (
              <CreateObjectLink
                to={newClusterPath}
                onClick={() =>
                  track("New Cluster Clicked", {
                    source: "Navbar create button",
                  })
                }
              >
                Cluster
              </CreateObjectLink>
            )}
            {canCreate && (
              <CreateObjectLink
                ref={initialFocusRef}
                to={newSourcePath}
                state={{ previousPage: location.pathname }}
                onClick={() =>
                  track("New Source Clicked", {
                    source: "Navbar create button",
                  })
                }
              >
                Source
              </CreateObjectLink>
            )}
            <AppConfigSwitch
              cloudConfigElement={({ runtimeConfig }) =>
                runtimeConfig.isImpersonating ? null : (
                  <CreateObjectLink
                    state={{ new: true }}
                    to="/access/app-passwords"
                  >
                    App Password
                  </CreateObjectLink>
                )
              }
            />
          </VStack>
        </PopoverContent>
      </Popover>
    </Flex>
  );
};

export const CreateObjectLink = forwardRef<
  LinkProps & { isDisabled?: boolean },
  "a"
>(
  (
    { children, to, state, isDisabled }: LinkProps & { isDisabled?: boolean },
    ref,
  ) => {
    const buttonProps = {
      justifyContent: "flex-start",
      px: "4",
      variant: "ghost",
      width: "100%",
      borderRadius: "none",
    };
    if (isDisabled) {
      return (
        <Tooltip label={NEW_SOURCE_BUTTON_DISABLED_MESSAGE}>
          <Button {...buttonProps} isDisabled>
            <Text color="foreground.secondary" textStyle="text-ui-med">
              {children}
            </Text>
          </Button>
        </Tooltip>
      );
    }
    return (
      <Button {...buttonProps} as={Link} ref={ref} to={to} state={state}>
        <Text color="foreground.secondary" textStyle="text-ui-med">
          {children}
        </Text>
      </Button>
    );
  },
);
