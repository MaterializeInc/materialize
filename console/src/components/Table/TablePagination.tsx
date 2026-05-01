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

export const TablePagination = <TData,>({
  table,
  itemLabel = "results",
}: TablePaginationProps<TData>) => {
  const { colors } = useTheme<MaterializeTheme>();

  const pageCount = table.getPageCount();
  const totalRows = table.getFilteredRowModel().rows.length;
  const { pageIndex, pageSize } = table.getState().pagination;

  if (pageCount <= 1) return null;

  const from = pageIndex * pageSize + 1;
  const to = Math.min((pageIndex + 1) * pageSize, totalRows);

  return (
    <HStack spacing={2} justify="space-between" px={4} py={2}>
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        Showing {from.toLocaleString("en-US")}-{to.toLocaleString("en-US")} of{" "}
        {totalRows.toLocaleString("en-US")} {itemLabel}
      </Text>
      <HStack spacing={2}>
        <IconButton
          aria-label="Previous page"
          icon={<ChevronLeftIcon height="4" width="4" />}
          variant="secondary"
          minW="8"
          height="8"
          isDisabled={!table.getCanPreviousPage()}
          onClick={() => table.previousPage()}
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
          isDisabled={!table.getCanNextPage()}
          onClick={() => table.nextPage()}
        />
      </HStack>
    </HStack>
  );
};
