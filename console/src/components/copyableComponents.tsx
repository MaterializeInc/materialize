// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { IconProps } from "@chakra-ui/icons";
import { Box, BoxProps, Flex, HStack, Spacer } from "@chakra-ui/layout";
import {
  chakra,
  HTMLChakraProps,
  IconButton,
  IconButtonProps,
  StackProps,
  Text,
  Tooltip,
  useTheme,
} from "@chakra-ui/react";
import React, { useState } from "react";

import ReadOnlyCommandBlock from "~/components/CommandBlock/ReadOnlyCommandBlock";
import { useCopyableText } from "~/hooks/useCopyableText";
import CheckmarkIcon from "~/svg/CheckmarkIcon";
import CopyIcon from "~/svg/CopyIcon";
import EyeClosedIcon from "~/svg/EyeClosedIcon";
import EyeOpenIcon from "~/svg/EyeOpenIcon";
import { MaterializeTheme } from "~/theme";

export const CopyStateIcon: React.FC<
  React.PropsWithChildren<{ copied: boolean } & IconProps>
> = ({ copied }) => {
  const { colors } = useTheme<MaterializeTheme>();
  if (copied)
    return (
      <CheckmarkIcon
        color={colors.accent.green}
        data-testid="copyable-checkicon"
        aria-label="Text has been copied"
      />
    );
  return <CopyIcon data-testid="copyable-copyicon" aria-label="Copy text" />;
};

export const CopyButton: React.FC<
  React.PropsWithChildren<
    { contents: string; onCopy?: () => void } & Omit<
      IconButtonProps,
      "aria-label"
    >
  >
> = ({ contents, size = "md", onCopy: onCopyHandler, ...props }) => {
  const { onCopy, copied } = useCopyableText(contents);
  const title = copied ? "Copied" : "Copy text";
  const handleCopy = () => {
    if (copied) return;
    onCopy();
    onCopyHandler && onCopyHandler();
  };

  return (
    <Box
      as={IconButton}
      data-testid="copyable"
      title={title}
      aria-label={title}
      background="transparent"
      color={copied ? "green.400" : "gray.500"}
      _hover={{
        background: "transparent",
        color: copied ? "default" : "gray.600",
      }}
      _active={{
        color: copied ? "default" : "gray.700",
      }}
      onClick={handleCopy}
      flex={0}
      size={size}
      {...props}
    >
      <Tooltip
        label={title}
        placement="bottom"
        fontSize="xs"
        isOpen={copied}
        top={-1}
      >
        <Box>
          <CopyStateIcon copied={copied} />
        </Box>
      </Tooltip>
    </Box>
  );
};

export interface CopyableBoxProps extends BoxProps {
  contents: string;
  variant?: "default" | "compact";
  maxHeight?: string;
  onCopy?: () => void;
}
/** Copyable component with a bg box but no line breaks  */
export const CopyableBox: React.FC<CopyableBoxProps> = ({
  contents,
  variant = "default",
  maxHeight,
  onCopy,
  ...props
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack
      alignItems="center"
      spacing={0}
      borderRadius="lg"
      bg={colors.background.secondary}
      w="full"
      fontSize="sm"
      {...props}
    >
      <Box
        as="pre"
        fontFamily="mono"
        pl={4}
        py={2}
        flex={1}
        lineHeight="4"
        whiteSpace="nowrap"
        overflow="hidden"
        textOverflow="ellipsis"
        minWidth={0}
        {...(maxHeight && {
          maxHeight,
          overflow: "auto",
        })}
      >
        {props.children ?? contents}
      </Box>
      <CopyButton
        fontSize="md"
        contents={contents}
        size={variant === "compact" ? "sm" : "md"}
        minH="full"
        onCopy={onCopy}
      />
    </HStack>
  );
};

export interface SecretCopyableBoxProps extends StackProps {
  contents: string;
  obfuscatedContent: string;
  variant?: "default" | "embedded";
  maxHeight?: string;
  label?: string;
}

/** Copyable component for secrets. Display obfuscated value by default and allows the user to toggle the visibility of the secret value  */
export const SecretCopyableBox: React.FC<SecretCopyableBoxProps> = ({
  contents,
  label,
  obfuscatedContent,
  variant = "default",
  maxHeight,
  ...props
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [showContent, setShowContent] = useState<boolean>(false);
  return (
    <HStack
      alignItems="center"
      spacing={0}
      borderRadius={variant === "default" ? "lg" : "0"}
      bg={variant === "default" ? colors.background.secondary : "transparent"}
      w="full"
      fontSize="sm"
      {...props}
    >
      <Box
        aria-label={label}
        overflowX="auto"
        sx={{
          "&::-webkit-scrollbar": {
            display: "none",
          },
        }}
        as="pre"
        fontFamily="mono"
        pl={4}
        py={2}
        flex={1}
        minWidth={0}
        {...(variant === "embedded" && {
          py: "4",
          pl: "6",
        })}
        {...(maxHeight && {
          maxHeight,
          overflow: "auto",
        })}
      >
        {showContent ? contents : obfuscatedContent}
      </Box>
      <IconButton
        variant="transparent"
        size="md"
        aria-label="visibility"
        icon={showContent ? <EyeOpenIcon /> : <EyeClosedIcon />}
        onClick={() => setShowContent(!showContent)}
      />
      <CopyButton
        fontSize="md"
        contents={contents}
        size={variant === "embedded" ? "sm" : "md"}
        minH="full"
      />
    </HStack>
  );
};
interface CustomTab {
  title: string;
  children: React.ReactNode;
  icon?: React.ReactNode;
}

interface CodeBlockTab {
  title: string;
  /** The code to display. */
  contents: string;
  icon?: React.ReactNode;
}

type Tab = CodeBlockTab | CustomTab;

type CodeBlockExtraProps = {
  /** Whether to display line numbers. */
  lineNumbers?: boolean;
  /** Whether to force-wrap long lines. */
  wrap?: boolean;
  headingIcon?: React.ReactNode;
};

type TabbedCodeBlockProps = CodeBlockExtraProps & {
  tabs: Tab[];
};

type CodeBlockProps = CodeBlockTab & CodeBlockExtraProps;

/**
 * A nicely-formatted block of code.
 *
 * Code blocks render their contents in monospace and present a "copy to
 * clipboard" button when hovered over. They are offset from their surroundings
 * by a small margin, but style properties are passed through to their
 * container.
 *
 * By default, code blocks are the width of their parent and any overflow
 * results in scroll bars. Setting `wrap` to `true` will cause long lines to
 * wrap instead.
 */
export const TabbedCodeBlock: React.FC<
  React.PropsWithChildren<TabbedCodeBlockProps & BoxProps>
> = ({
  tabs,
  lineNumbers,
  wrap,
  headingIcon,
  ...props
}: TabbedCodeBlockProps & BoxProps) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();
  const [activeTabTitle, setActiveTabTitle] = React.useState(
    tabs[0]?.title || "",
  );

  if (tabs.length === 0) return null;

  const preProps: HTMLChakraProps<"pre"> = {};

  if (wrap !== false) {
    preProps.whiteSpace = "pre-wrap";
  }

  const currentTab = tabs.find((tab) => tab.title === activeTabTitle);
  if (!currentTab) return null;

  const isCustomTab = "children" in currentTab;

  return (
    <Box
      bg={colors.background.primary}
      role="group"
      position="relative"
      borderRadius="8px"
      w="full"
      textAlign="left"
      shadow={shadows.level2}
      overflow="hidden"
      {...props}
    >
      <Flex
        borderBottom="1px"
        bg={colors.background.secondary}
        borderColor={colors.border.secondary}
        w="full"
        alignItems="stretch"
        justifyContent="flex-start"
        pl="2"
      >
        {tabs.length > 1 ? (
          <>
            {tabs.map(({ title, icon }) => (
              <CodeBlockHeading
                key={`codeblock-tab-${title}`}
                as="button"
                onClick={() => setActiveTabTitle(title)}
                borderBottom="1px solid"
                borderColor={
                  title === activeTabTitle
                    ? colors.foreground.primary
                    : "transparent"
                }
                textColor={
                  title === activeTabTitle
                    ? colors.foreground.primary
                    : colors.foreground.secondary
                }
                _hover={{
                  bg: colors.background.tertiary,
                }}
              >
                {icon}
                <Text as="span" fontWeight="500">
                  {title}
                </Text>
              </CodeBlockHeading>
            ))}
          </>
        ) : (
          <CodeBlockHeading>
            <Text as="span" fontWeight="500">
              {tabs[0].title}
            </Text>
          </CodeBlockHeading>
        )}
        <Spacer />
        {headingIcon
          ? headingIcon
          : !isCustomTab && (
              <CopyButton
                contents={currentTab.contents}
                flex={0}
                px="4"
                py="0px"
                h="auto"
                fontSize="sm"
                borderTopRightRadius="sm"
                _hover={{
                  bg: "whiteAlpha.400",
                }}
              />
            )}
      </Flex>
      {isCustomTab ? (
        currentTab.children
      ) : (
        <chakra.pre
          fontSize="sm"
          py={2}
          pl={2}
          pr={8}
          fontFamily="mono"
          overflow="auto"
          {...preProps}
        >
          <ReadOnlyCommandBlock
            lineNumbers={lineNumbers}
            value={currentTab.contents}
            lineWrap
          />
        </chakra.pre>
      )}
    </Box>
  );
};

const CodeBlockHeading = ({ children, ...props }: BoxProps) => {
  return (
    <HStack
      fontSize="sm"
      fontWeight="400"
      flex={0}
      px={2}
      py={2}
      mb="-1px"
      textAlign="left"
      whiteSpace="nowrap"
      spacing="2"
      {...props}
    >
      {children}
    </HStack>
  );
};

export const CodeBlock: React.FC<
  React.PropsWithChildren<CodeBlockProps & BoxProps>
> = ({ title, contents, ...props }) => {
  return <TabbedCodeBlock tabs={[{ title, contents }]} {...props} />;
};
