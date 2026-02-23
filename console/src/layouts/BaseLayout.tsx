// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * @module
 * Base layout and supporting components, like page headers.
 */

import { ChevronDownIcon, ChevronUpIcon } from "@chakra-ui/icons";
import {
  Box,
  BoxProps,
  Button,
  Center,
  Flex,
  FlexProps,
  forwardRef,
  Heading,
  HeadingProps,
  HStack,
  Menu,
  MenuButtonProps,
  MenuList,
  Portal,
  Spinner,
  StyleProps,
  useMenuButton,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { Link } from "react-router-dom";
import { NavLink, NavLinkProps } from "react-router-dom";

import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import ImpersonationAlert from "~/components/ImpersonationAlert";
import { MfaAlert } from "~/components/MfaAlert";
import WelcomeDialog from "~/components/WelcomeDialog/WelcomeDialog";
import { NavBar } from "~/layouts/NavBar";
import PageFooter from "~/layouts/PageFooter";
import { useTrackPageHeaderHeight } from "~/store/stickyHeader";
import SlashIcon from "~/svg/SlashIcon";
import { MaterializeTheme } from "~/theme";

import { OrgTag } from "./footerTagComponents";
import { FIXED_TOP_BAR_Z_INDEX, MAIN_CONTENT_Z_INDEX } from "./zIndex";

export interface BaseLayoutProps {
  children?: React.ReactNode;
  containerProps?: FlexProps;
  navBarOverride?: React.FunctionComponent;
  sectionNav?: React.ReactNode;
}

export const MAIN_CONTENT_MARGIN = 10;

/**
 * The base layout for logged-in users, containing the navigation bar at the
 * top of the screen and a sticky footer.
 *
 * Pages with the standard page header should be wrapped in MainContentContainer.
 *
 * <PageHeader variant="compact" /> should be full width, not wrapped in
 * MainContentContainer, though the container can still be used to wrap the rest of the
 * page content.
 *
 * ```
 * <BaseLayout>
 *   <MainContentContainer>
 *     <PageHeader>
 *       <PageHeading>Example heading</PageHeading>
 *     </PageHeader>
 *     {children}
 *   </MainContentContainer>
 *   {or}
 *   <PageHeader variant="compact">
 *     <PageBreadcrumbs crumbs={["page", "to", "page"]} />
 *     <PageTabStrip />
 *   </PageHeader>
 *   <MainContentContainer>
 *     {children}
 *   </MainContentContainer>
 * </BaseLayout>
 * ```
 */
export const BaseLayout = (props: BaseLayoutProps) => {
  const NavigationBar = props.navBarOverride ? props.navBarOverride : NavBar;
  return (
    <Flex
      direction="column"
      height="100vh"
      data-testid="page-layout"
      {...props.containerProps}
    >
      <WelcomeDialog />
      <MfaAlert />
      <ImpersonationAlert />
      <Flex
        direction={{ base: "column", lg: "row" }}
        flexGrow="1"
        minHeight="0"
      >
        <NavigationBar isCollapsed={Boolean(props.sectionNav)} />
        <HStack
          alignItems="stretch"
          flexGrow="1"
          minHeight="0"
          minWidth="0"
          spacing="0"
        >
          {props.sectionNav}
          <VStack
            alignItems="stretch"
            flexGrow="1"
            minHeight="0"
            minWidth="0"
            spacing="0"
            zIndex={MAIN_CONTENT_Z_INDEX}
          >
            <Flex
              as="main"
              direction="column"
              flex="1"
              bg="background.primary"
              minHeight="0"
              overflow="auto"
            >
              <AppErrorBoundary>
                <React.Suspense
                  fallback={
                    <Center flex={1} css={{ height: "100%" }}>
                      <Spinner />
                    </Center>
                  }
                >
                  {props.children}
                </React.Suspense>
              </AppErrorBoundary>
            </Flex>
            <PageFooter>
              <OrgTag />
            </PageFooter>
          </VStack>
        </HStack>
      </Flex>
    </Flex>
  );
};

export const MainContentContainer = ({ ...props }: BoxProps) => {
  return (
    <Flex
      data-testid="main-content"
      flex="1"
      height="100%"
      flexDirection="column"
      maxW="100%"
      px={MAIN_CONTENT_MARGIN}
      pb="4"
      bg="background.primary"
      mt={6}
      {...props}
    />
  );
};

/**
 * A container for the header block at the top of a page.
 *
 * Variants:
 *   standard - includes vertical margin. This is the default
 *   compact - no vertical margin.
 * */
export const PageHeader = ({
  boxProps,
  children,
  variant,
  sticky = false,
}: {
  boxProps?: BoxProps;
  children: React.ReactNode;
  variant?: "standard" | "compact";
  sticky?: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const headerRef = useTrackPageHeaderHeight();

  return (
    <Flex
      ref={headerRef}
      position={sticky ? "sticky" : "initial"}
      top="0"
      background={colors.background.primary}
      mb={variant === "compact" ? 0 : 6}
      flexDirection="row"
      alignItems="flex-start"
      justifyContent="space-between"
      width="100%"
      zIndex={FIXED_TOP_BAR_Z_INDEX}
      {...boxProps}
    >
      {variant === "compact" ? (
        <VStack spacing={0} alignItems="start" width="100%">
          {children}
        </VStack>
      ) : (
        children
      )}
    </Flex>
  );
};

export interface PageHeadingProps extends HeadingProps {
  children?: React.ReactNode;
}

/**
 * A heading at the top of the page.
 *
 * This component should be used inside of a `PageHeader`.
 */
export const PageHeading = ({ children, ...props }: PageHeadingProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Heading
      fontSize="2xl"
      lineHeight="32px"
      color={colors.foreground.primary}
      fontWeight="500"
      mt={0}
      {...props}
    >
      {children}
    </Heading>
  );
};

export interface Breadcrumb {
  title: string;
  href?: string;
}

export interface PageBreadcrumbsProps {
  crumbs: Breadcrumb[];
  children?: React.ReactNode;
  rightSideChildren?: React.ReactNode;
  contextMenuChildren?: React.ReactNode;
}

/**
 * A container for breadcrumbs.
 * This goes inside a PageHeader for a header that is a series of paths.
 */
export const PageBreadcrumbs = ({
  crumbs,
  children,
  rightSideChildren,
  contextMenuChildren,
}: PageBreadcrumbsProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  // Render a space if no children so that we take up the right amount of space
  // on pages that don't have breadcrumbs.
  return (
    <HStack
      borderBottom="solid 1px"
      borderColor={colors.border.primary}
      justifyContent="space-between"
      width="100%"
      px="5"
    >
      <HStack my="2" width="100%" spacing="0" height="8" alignItems="center">
        {crumbs.map((crumb, i: number) => {
          const isLast = i === crumbs.length - 1;
          return (
            <React.Fragment key={crumb.title}>
              <Flex
                borderRadius="4"
                alignItems="center"
                textStyle="text-ui-med"
                color={isLast ? "default" : colors.foreground.secondary}
                fontWeight={500}
                px={2}
                py={1}
                _hover={{
                  ...(crumb.href
                    ? {
                        background: colors.background.secondary,
                      }
                    : undefined),
                }}
              >
                {crumb.href ? (
                  <Link to={crumb.href}>{crumb.title}</Link>
                ) : (
                  crumb.title
                )}
              </Flex>
              {isLast ? null : <SlashIcon height="100%" />}
            </React.Fragment>
          );
        })}
        {contextMenuChildren && (
          <Flex alignItems="center">
            <ContextMenu>{contextMenuChildren}</ContextMenu>
          </Flex>
        )}
        {children}
      </HStack>
      {rightSideChildren}
    </HStack>
  );
};

/**
 * Icon button that opens a context menu, used in PageBreadcrumbs.
 * Renders the menu in a portal to avoid button nesting errors.
 */
export const ContextMenu = ({ children }: { children: React.ReactNode }) => {
  return (
    <Menu>
      <ContextMenuButton />
      <Portal>
        <MenuList>{children}</MenuList>
      </Portal>
    </Menu>
  );
};

const ContextMenuButton = forwardRef<MenuButtonProps, "button">(
  (props, ref) => {
    const buttonProps = useMenuButton(props, ref);

    return (
      <Button
        {...buttonProps}
        aria-label="Navigation actions"
        variant="ghost"
        p="0"
        minW="6"
        height="6"
        _hover={{}}
        _active={{}}
      >
        <Button
          // So the clickable area can be larger than the visual button
          as="div"
          variant="ghost"
          p="0"
          minW="4"
          height="4"
          borderRadius="4"
        >
          <ChevronDownIcon />
        </Button>
      </Button>
    );
  },
);

export type Tab = { label: string; href: string; end?: boolean };
export interface PageTabStripProps {
  tabData: Tab[];
}

export const PageTabStrip = ({ tabData }: PageTabStripProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const [tabBoundingBox, setTabBoundingBox] = React.useState<DOMRect | null>(
    null,
  );
  const [wrapperBoundingBox, setWrapperBoundingBox] =
    React.useState<DOMRect | null>(null);
  const [highlightedTab, setHighlightedTab] = React.useState<Tab | null>(null);
  const [isHoveredFromNull, setIsHoveredFromNull] = React.useState(true);

  const wrapperRef = React.useRef<HTMLDivElement>(null);
  const highlightRef = React.useRef(null);

  const repositionHighlight = (
    e: React.MouseEvent<HTMLAnchorElement, MouseEvent>,
    tab: Tab,
  ) => {
    setTabBoundingBox((e.target as HTMLElement).getBoundingClientRect());
    setWrapperBoundingBox(wrapperRef.current!.getBoundingClientRect());
    setIsHoveredFromNull(!highlightedTab);
    setHighlightedTab(tab);
  };

  const resetHighlight = () => setHighlightedTab(null);

  const highlightStyles = {} as StyleProps;

  if (tabBoundingBox && wrapperBoundingBox) {
    highlightStyles.transitionDuration = isHoveredFromNull ? "0ms" : "150ms";
    highlightStyles.opacity = highlightedTab ? 1 : 0;
    highlightStyles.width = `${tabBoundingBox.width}px`;
    highlightStyles.transform = `translate(${
      tabBoundingBox.left - wrapperBoundingBox.left
    }px)`;
  }

  return (
    <Box
      width="100%"
      boxSizing="border-box"
      boxShadow={`inset 0px -1px 0px 0px ${colors.border.primary}`}
      px={4}
      overflowX="scroll"
      overflowY="visible"
      sx={{
        scrollbarWidth: "none",
        "&::-webkit-scrollbar": {
          display: "none",
        },
      }}
    >
      <Flex
        ref={wrapperRef}
        onMouseLeave={resetHighlight}
        position="relative"
        gap={2}
        width="fit-content"
      >
        <Box
          ref={highlightRef}
          background={colors.background.secondary}
          position="absolute"
          top={2}
          left={0}
          borderRadius="4px"
          height="32px"
          transition="0.15ms ease"
          transitionProperty="opacity, width, transform"
          {...highlightStyles}
        />
        {tabData.map((tab) => (
          <PageTab
            to={tab.href}
            end={tab.end}
            key={tab.label}
            onMouseOver={(e) => repositionHighlight(e, tab)}
          >
            {tab.label}
          </PageTab>
        ))}
      </Flex>
    </Box>
  );
};

export type PageTabProps = NavLinkProps & {
  children: React.ReactNode;
  tabProps?: BoxProps;
};
export const PageTab = (props: PageTabProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { children, tabProps, ...navLinkProps } = props;

  return (
    <NavLink
      style={({ isActive }) =>
        isActive
          ? {
              boxSizing: "border-box",
              borderBottom: `solid 1px ${colors.accent.purple}`,
            }
          : undefined
      }
      {...navLinkProps}
    >
      {({ isActive }) => (
        <Box
          {...tabProps}
          color={
            isActive ? colors.foreground.primary : colors.foreground.secondary
          }
          py={3}
          px={3}
          display="inline-block"
          position="relative"
          cursor="pointer"
          transition="color 250ms"
          textStyle="text-ui-med"
          width="fit-content"
          boxSizing="border-box"
          whiteSpace="nowrap"
        >
          {children}
        </Box>
      )}
    </NavLink>
  );
};

export type ExpandablePanelProps = BoxProps & {
  text: string;
  children: React.ReactNode;
};

export const ExpandablePanel = ({
  text,
  children,
  ...boxProps
}: ExpandablePanelProps) => {
  const [show, setShow] = React.useState(false);

  return (
    <Box width="100%">
      <Box
        color="accent.brightPurple"
        fontSize="xs"
        cursor="pointer"
        userSelect="none"
        onClick={() => setShow(!show)}
        {...boxProps}
      >
        {text}
        {show ? <ChevronUpIcon /> : <ChevronDownIcon />}
      </Box>
      {show && children}
    </Box>
  );
};
