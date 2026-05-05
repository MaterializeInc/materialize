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
  HStack,
  IconButton,
  StackProps,
  Switch,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tooltip,
  Tr,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import copyToClipboard from "copy-to-clipboard";
import { markdownTable } from "markdown-table";
import Papa from "papaparse";
import React, { ChangeEvent, useId } from "react";
import stringWidth from "string-width";

import { Column } from "~/api/materialize/types";
import { useToast } from "~/hooks/useToast";
import { PauseIcon, PlayIcon } from "~/icons";
import ChevronLeftIcon from "~/svg/ChevronLeftIcon";
import ChevronRightIcon from "~/svg/ChevronRightIcon";
import CopyIcon from "~/svg/CopyIcon";
import DownloadIcon from "~/svg/DownloadIcon";
import { MaterializeTheme } from "~/theme";
import { downloadFile } from "~/utils/downloadFile";

import formatRows from "./formatRows";

export const TablePagination = ({
  totalPages,
  totalNumRows,
  onNextPage,
  onPrevPage,
  pageSize,
  currentPage,
  startIndex,
  endIndex,
  prevEnabled,
  isFollowing,
  onToggleFollow,
  nextEnabled,
  ...rest
}: {
  totalPages: number;
  totalNumRows: number;
  onNextPage: () => void;
  onPrevPage: () => void;
  pageSize: number;
  currentPage: number;
  startIndex: number;
  endIndex: number;
  prevEnabled: boolean;
  nextEnabled: boolean;
  isFollowing: boolean | null;
  onToggleFollow: () => void;
} & StackProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const from = startIndex + 1;
  const to = endIndex;

  if (totalNumRows === 0) {
    return null;
  }

  return (
    <HStack {...rest}>
      <Text
        flexGrow="1"
        textStyle="text-ui-reg"
        color={colors.foreground.secondary}
      >
        {from.toLocaleString("en-US")}-{to.toLocaleString("en-US")} of{" "}
        {totalNumRows.toLocaleString("en-US")}
      </Text>
      {totalPages > 1 && (
        <HStack flexGrow="0" spacing="2">
          {isFollowing !== null && (
            <IconButton
              icon={
                isFollowing ? (
                  <PauseIcon height="4" width="4" />
                ) : (
                  <PlayIcon height="4" width="4" />
                )
              }
              aria-label={isFollowing ? "Unfollow" : "Follow"}
              title={isFollowing ? "Unfollow" : "Follow"}
              onClick={onToggleFollow}
              variant="secondary"
              minW="8"
              height="8"
            />
          )}
          <IconButton
            icon={<ChevronLeftIcon height="4" width="4" />}
            aria-label="Previous page"
            onClick={onPrevPage}
            isDisabled={!prevEnabled}
            variant="secondary"
            minW="8"
            height="8"
          />
          <IconButton
            isDisabled={!nextEnabled}
            icon={<ChevronRightIcon height="4" width="4" />}
            aria-label="Next page"
            onClick={onNextPage}
            variant="secondary"
            minW="8"
            height="8"
          />
        </HStack>
      )}
    </HStack>
  );
};

const SqlSelectTable = ({
  colNames,
  cols,
  paginatedRows,
  onSubscribeChange,
  isSubscribeManager,
  isStreamingResult,
  rows,
  ...rest
}: StackProps & {
  cols?: Column[];
  colNames: string[];
  paginatedRows: string[][];
  isSubscribeManager?: boolean;
  isStreamingResult?: boolean;
  onSubscribeChange?: (e: ChangeEvent<HTMLInputElement>) => void;
  rows?: unknown[][];
}) => {
  const toast = useToast();
  const tableId = useId();

  const downloadCsv = () => {
    const formattedRows = formatRows(cols ?? [], rows ?? []);
    const csv = Papa.unparse([colNames, ...(formattedRows ?? [])]);
    downloadFile("materialize_shell_export.csv", csv, { mimeType: "text/csv" });
    toast({ description: "Results saved to CSV" });
  };

  const copyMarkdown = () => {
    const formattedRows = formatRows(cols ?? [], rows ?? []);
    const markdown = markdownTable([colNames, ...(formattedRows ?? [])], {
      // We use stringWidth to account for each character's "visible" size. I.e. emojis have a large visible size than most characters.
      stringLength: stringWidth,
    });
    // Clipboards can generally accept one of two formats: plaintext and HTML.
    // Anything else will result in blank contents (and you having pulled out
    // your hair). We can't use navigator.clipboard.writeText because it
    // won't work in Safari for http domains.
    copyToClipboard(markdown, { format: "text/plain" });
    toast({ description: "Results copied to clipboard" });
  };

  const { colors } = useTheme<MaterializeTheme>();
  isSubscribeManager = isSubscribeManager ?? false;
  isStreamingResult = isStreamingResult ?? false;

  const hasResults = paginatedRows.length > 0;

  if (colNames.length === 0) {
    return (
      <Flex
        px="4"
        py="2"
        borderWidth="1px"
        borderColor={colors.border.secondary}
        borderRadius="lg"
      >
        <Text textStyle="monospace">No results</Text>
      </Flex>
    );
  }

  return (
    <VStack
      borderWidth="1px"
      borderColor={colors.border.secondary}
      borderRadius="lg"
      spacing="0"
      alignItems="flex-start"
      pb={1}
      {...rest}
    >
      <HStack justifyContent="space-between" paddingX="4" py="2" width="100%">
        <Flex alignItems="center">
          <Text textStyle="monospace">Results</Text>
        </Flex>
        <HStack>
          {isStreamingResult && (
            <>
              <Text
                as="label"
                htmlFor={`diff-toggle-${tableId}`}
                textStyle="text-ui-med"
                userSelect="none"
                _hover={{
                  cursor: "pointer",
                }}
              >
                Show diffs
              </Text>
              <Switch
                id={`diff-toggle-${tableId}`}
                isChecked={isSubscribeManager}
                onChange={onSubscribeChange}
              />
            </>
          )}
          {rows && hasResults && (
            <HStack spacing="0">
              <Tooltip label="Copy as Markdown" fontSize="xs">
                <IconButton
                  icon={<CopyIcon />}
                  aria-label="Copy as Markdown"
                  onClick={copyMarkdown}
                  variant="inline"
                  minW="8"
                  height="8"
                />
              </Tooltip>
              <Tooltip label="Download as CSV" fontSize="xs">
                <IconButton
                  icon={
                    <DownloadIcon
                      height="4"
                      width="4"
                      color={colors.foreground.secondary}
                    />
                  }
                  aria-label="Download as CSV"
                  onClick={downloadCsv}
                  variant="inline"
                  minW="8"
                  height="8"
                />
              </Tooltip>
            </HStack>
          )}
        </HStack>
      </HStack>
      <Box width="100%" overflowX="auto">
        <Table variant="shell">
          <Thead>
            <Tr>
              {colNames.map((column, idx) => (
                <Th key={idx}>
                  <Text as="span" textStyle="monospace">
                    {column}
                  </Text>
                </Th>
              ))}
            </Tr>
          </Thead>
          <Tbody>
            {hasResults ? (
              paginatedRows.map((row, rowIdx) => (
                <Tr key={rowIdx}>
                  {row.map((cell, cellIdx) => {
                    return (
                      <Td key={cellIdx}>
                        <Text as="pre" my="1" textStyle="monospace">
                          {/*
                            Empty cells are collapsed to a zero height.
                            Attempting to size them results in funky table
                            layout issues. Sidestep the whole thing by rendering
                            a zero-width space (\u200b) for an empty cell.
                            */}
                          {cell === "" ? "\u200b" : cell}
                        </Text>
                      </Td>
                    );
                  })}
                </Tr>
              ))
            ) : (
              <Tr>
                <Td colSpan={colNames.length}>
                  <Text as="pre" my="1" textStyle="monospace">
                    No results
                  </Text>
                </Td>
              </Tr>
            )}
          </Tbody>
        </Table>
      </Box>
    </VStack>
  );
};

export default SqlSelectTable;
