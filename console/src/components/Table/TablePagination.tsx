// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, IconButton, Text, useTheme } from "@chakra-ui/react";
import React from "react";

import { ChevronLeftIcon, ChevronRightIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";

import { TablePaginationProps } from "./tableTypes";

export interface PaginationControlsProps {
  /** Zero-based. */
  pageIndex: number;
  pageSize: number;
  pageCount: number;
  totalItems: number;
  itemLabel?: React.ReactNode;
  onPrevious: () => void;
  onNext: () => void;
}

/** "Showing 1-30 of 120" with previous and next buttons. Hidden for one page. */
export const PaginationControls = ({
  pageIndex,
  pageSize,
  pageCount,
  totalItems,
  itemLabel = "results",
  onPrevious,
  onNext,
}: PaginationControlsProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  if (pageCount <= 1) return null;

  const from = pageIndex * pageSize + 1;
  const to = Math.min((pageIndex + 1) * pageSize, totalItems);

  return (
    <HStack spacing={2} justify="space-between" px={4} py={2}>
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        Showing {from.toLocaleString("en-US")}-{to.toLocaleString("en-US")} of{" "}
        {totalItems.toLocaleString("en-US")} {itemLabel}
      </Text>
      <HStack spacing={2}>
        <IconButton
          aria-label="Previous page"
          icon={<ChevronLeftIcon height="4" width="4" />}
          variant="secondary"
          minW="8"
          height="8"
          isDisabled={pageIndex === 0}
          onClick={onPrevious}
        />
        <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
          page {pageIndex + 1} of {pageCount}
        </Text>
        <IconButton
          aria-label="Next page"
          icon={<ChevronRightIcon height="4" width="4" />}
          variant="secondary"
          minW="8"
          height="8"
          isDisabled={pageIndex >= pageCount - 1}
          onClick={onNext}
        />
      </HStack>
    </HStack>
  );
};

export const TablePagination = <TData,>({
  table,
  itemLabel,
}: TablePaginationProps<TData>) => {
  const { pageIndex, pageSize } = table.getState().pagination;
  return (
    <PaginationControls
      pageIndex={pageIndex}
      pageSize={pageSize}
      pageCount={table.getPageCount()}
      totalItems={table.getFilteredRowModel().rows.length}
      itemLabel={itemLabel}
      onPrevious={() => table.previousPage()}
      onNext={() => table.nextPage()}
    />
  );
};
