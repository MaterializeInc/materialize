// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, HStack, VStack } from "@chakra-ui/react";
import React from "react";

import { CommandBlockProps } from "~/components/CommandBlock/ReadOnlyCommandBlock";
import { CopyButton } from "~/components/copyableComponents";

import CommandChevron from "./CommandChevron";
import SyntaxHighlightedBlock from "./SyntaxHighlightedBlock";

// In order to match the left gutter, we take the chevron width + the stack gap
const LEFT_GUTTER = 5 + 2;

const styles = {
  copyButtonOpacity: {
    "--copy-button-opacity": 0,
  },
  copyButtonOpacityOnHover: {
    "--copy-button-opacity": 1,
  },
  copyButton: {
    opacity: "var(--copy-button-opacity)",
    visibility: "visible",
    transition: "opacity 0.2s ease-in-out",
  },
};

export const CommandBlockOutputContainer = ({
  command,
  commandBlockContainerProps,
  children,
}: {
  command: string;
  children: React.ReactNode;
  commandBlockContainerProps?: CommandBlockProps["containerProps"];
}) => {
  return (
    <HStack alignItems="flex-start" width="100%" pr={LEFT_GUTTER} gap="8px">
      <CommandChevron />
      <VStack spacing="3" alignItems="flex-start" flex="1" minWidth="0">
        <Box
          width="100%"
          position="relative"
          sx={{
            ...styles.copyButtonOpacity,
            "&:hover": styles.copyButtonOpacityOnHover,
          }}
        >
          <SyntaxHighlightedBlock
            value={command}
            containerProps={commandBlockContainerProps}
          />
          <CopyButton
            contents={command}
            sx={styles.copyButton}
            position="absolute"
            bottom="-2"
            right="3"
            size="md"
            zIndex={2}
            aria-label="Copy query"
          />
        </Box>
        {children}
      </VStack>
    </HStack>
  );
};
