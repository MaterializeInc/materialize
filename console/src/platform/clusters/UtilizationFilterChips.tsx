// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Tag, TagCloseButton, TagLabel } from "@chakra-ui/react";
import { Table } from "@tanstack/react-table";
import React from "react";

import { utilizationFilterLabel } from "./utilizationFilters";

export interface UtilizationFilterChipsProps<TData> {
  table: Table<TData>;
  /** The filterable columns, in the order their chips should appear. */
  columns: readonly { id: string; header: string }[];
}

/**
 * The utilization filters in force, each removable.
 *
 * A column's filter trigger signals only that it is active, by its colour, and
 * says what it filters by only once its popover is open. The chips put every
 * condition in one place, readable and removable without hunting across
 * headers. Renders nothing when no filter is applied.
 */
export const UtilizationFilterChips = <TData,>({
  table,
  columns,
}: UtilizationFilterChipsProps<TData>) => {
  const chips = columns.flatMap(({ id, header }) => {
    const column = table.getColumn(id);
    const value = column?.getFilterValue() as number | undefined;
    if (!column || !value) return [];
    return [
      {
        id,
        label: utilizationFilterLabel(header, value),
        onRemove: () => column.setFilterValue(undefined),
      },
    ];
  });

  if (chips.length === 0) return null;

  return (
    <HStack spacing="2" flexWrap="wrap">
      {chips.map((chip) => (
        <Tag key={chip.id} size="md" borderRadius="md" px="3" py="1">
          <TagLabel>{chip.label}</TagLabel>
          <TagCloseButton
            aria-label={`Remove ${chip.label}`}
            onClick={chip.onRemove}
            ml="2"
          />
        </Tag>
      ))}
    </HStack>
  );
};
