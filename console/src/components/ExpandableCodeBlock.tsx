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
  ButtonProps,
  HStack,
  useDisclosure,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import Alert from "~/components/Alert";
import { useCopyableText } from "~/hooks/useCopyableText";
import MaximizeIcon from "~/svg/MaximizeIcon";
import MinimizeIcon from "~/svg/MinimizeIcon";
import { MaterializeTheme } from "~/theme";

import ReadOnlyCommandBlock from "./CommandBlock/ReadOnlyCommandBlock";
import { CopyStateIcon } from "./copyableComponents";

const LINES_UNTIL_EXPAND = 10;
const _TEXT_LINE_HEIGHT_PX = 21;
const MAX_COLLAPSED_HEIGHT_PX = _TEXT_LINE_HEIGHT_PX * LINES_UNTIL_EXPAND;

export const ExpandableCodeBlock = ({
  text,
  errorMessage,
}: {
  text: string;
  errorMessage?: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  const { onCopy, copied } = useCopyableText(text);
  const { isOpen: isExpanded, onToggle: onExpandButtonClick } = useDisclosure();

  const numLines = React.useMemo(() => text.split("\n").length, [text]);

  const isExpandable = numLines > LINES_UNTIL_EXPAND;

  return (
    <VStack spacing={4} width="100%">
      <Box
        position="relative"
        width="100%"
        borderRadius="lg"
        backgroundColor={colors.background.secondary}
        paddingBottom="4"
      >
        <HStack
          position="sticky"
          zIndex="1"
          paddingX="4"
          paddingTop="2"
          borderTopLeftRadius="lg"
          borderTopRightRadius="lg"
          backgroundColor={colors.background.secondary}
          justifyContent="flex-end"
          width="100%"
          top="0"
        >
          {isExpandable && (
            <ExpandableCodeBlockButton
              leftIcon={isExpanded ? <MinimizeIcon /> : <MaximizeIcon />}
              onClick={onExpandButtonClick}
            >
              {isExpanded ? "Collapse" : "Expand"}
            </ExpandableCodeBlockButton>
          )}

          <ExpandableCodeBlockButton
            leftIcon={<CopyStateIcon copied={copied} />}
            onClick={() => !copied && onCopy()}
          >
            {copied ? "Copied" : "Copy"}
          </ExpandableCodeBlockButton>
        </HStack>

        <Box
          position="relative"
          width="100%"
          overflowX="auto"
          overflowY="hidden"
          height={
            isExpandable && !isExpanded ? MAX_COLLAPSED_HEIGHT_PX : "auto"
          }
        >
          <ReadOnlyCommandBlock
            lineNumbers
            value={text}
            containerProps={{
              paddingLeft: "6",
              paddingRight: "6",
              paddingBottom: "4",
            }}
          />
        </Box>
        {isExpandable && !isExpanded && (
          <Box
            width="100%"
            zIndex="1"
            h="40px"
            position="absolute"
            bottom="0"
            background={`linear-gradient(180deg, rgba(247, 247, 248, 0.00) 0%, ${colors.foreground.inverse} 100%)`}
            pointerEvents="none"
            borderRadius="lg"
          />
        )}
      </Box>
      {errorMessage && (
        <Alert
          variant="error"
          label="Query error"
          message={errorMessage}
          width="100%"
        />
      )}
    </VStack>
  );
};

const ExpandableCodeBlockButton = (props: ButtonProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Button
      size="sm"
      color={colors.foreground.secondary}
      {...props}
      backgroundColor={colors.background.secondary}
      fontSize="12px"
    />
  );
};
