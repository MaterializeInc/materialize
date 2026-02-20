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
  Text,
  TextProps,
  useTheme,
  VStack,
  Wrap,
  WrapItem,
} from "@chakra-ui/react";
import * as React from "react";

import TextLink from "~/components/TextLink";
import { MaterializeTheme } from "~/theme";

export type DocsLink = {
  label: string;
  href: string;
  icon?: React.ReactElement;
};

export interface DocsCalloutProps {
  title?: string;
  description: string;
  docsLinks: DocsLink[];
  titleProps?: TextProps;
  textProps?: TextProps;
}

export interface SingleDocsLinkProps {
  docsLink: DocsLink;
}

export interface MultiDocsLinkProps {
  docsLinks: DocsLink[];
}

export const SingleDocsLink = ({ docsLink }: SingleDocsLinkProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  // If there's an icon, render as a button (similar to MultiDocsLink)
  if (docsLink.icon) {
    return (
      <Button
        as="a"
        variant="outline"
        size="sm"
        height="10"
        px="4"
        leftIcon={docsLink.icon}
        href={docsLink.href}
        target="_blank"
      >
        {docsLink.label}
      </Button>
    );
  }

  // Otherwise, render as a text link (original behavior)
  return (
    <TextLink
      fontSize="14px"
      lineHeight="16px"
      fontWeight={500}
      color={colors.accent.brightPurple}
      sx={{
        fontFeatureSettings: '"calt"',
        textDecoration: "none",
      }}
      href={docsLink.href}
      target="_blank"
      rel="noopener"
    >
      {docsLink.label} -&gt;
    </TextLink>
  );
};

export const MultiDocsLink = ({ docsLinks }: MultiDocsLinkProps) => {
  return (
    <Wrap spacing="2">
      {docsLinks.map(({ label, href, icon }) => (
        <WrapItem key={label}>
          <Button
            as="a"
            variant="outline"
            size="sm"
            height="10"
            px="4"
            leftIcon={icon}
            href={href}
            target="_blank"
          >
            {label}
          </Button>
        </WrapItem>
      ))}
    </Wrap>
  );
};

export const DocsCallout = ({
  title,
  description,
  docsLinks,
  titleProps,
  textProps,
}: DocsCalloutProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <VStack align="start" spacing={4}>
      <VStack align="start" spacing={2}>
        {title && (
          <Text
            textStyle="text-ui-med"
            color={colors.foreground.primary}
            {...titleProps}
          >
            {title}
          </Text>
        )}
        <Text
          textStyle="text-base"
          color={colors.foreground.secondary}
          {...textProps}
        >
          {description}
        </Text>
      </VStack>
      {docsLinks.length === 1 && !docsLinks[0].icon ? (
        <SingleDocsLink docsLink={docsLinks[0]} />
      ) : (
        <MultiDocsLink docsLinks={docsLinks} />
      )}
    </VStack>
  );
};
