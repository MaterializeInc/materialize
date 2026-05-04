// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Tag, TagCloseButton, TagLabel } from "@chakra-ui/react";
import { Column, Table } from "@tanstack/react-table";
import React from "react";

import { MaintainedObjectType } from "~/api/materialize/maintained-objects/constants";

import {
  FRESHNESS_THRESHOLD_OPTIONS,
  HYDRATION_LABELS,
  HydrationBucket,
} from "./filters";
import { MaintainedObjectListItem } from "./queries";

interface Chip {
  key: string;
  label: string;
  onRemove: () => void;
}

type AnyColumn = Column<MaintainedObjectListItem, unknown>;

/** Per-value remover for a multi-select column: drops `value` from the
 *  current filter array and clears the filter when the array empties. */
const removeMultiValue =
  <T,>(column: AnyColumn, value: T) =>
  () => {
    const current = (column.getFilterValue() as T[] | undefined) ?? [];
    const next = current.filter((v) => v !== value);
    column.setFilterValue(next.length ? next : undefined);
  };

const multiSelectChips = <T,>(
  column: AnyColumn,
  prefix: string,
  itemLabel: (value: T) => string,
): Chip[] => {
  const values = (column.getFilterValue() as T[] | undefined) ?? [];
  return values.map((value) => ({
    key: `${column.id}:${String(value)}`,
    label: `${prefix}: ${itemLabel(value)}`,
    onRemove: removeMultiValue(column, value),
  }));
};

interface ChipSource {
  columnId: string;
  build: (column: AnyColumn) => Chip[];
}

const CHIP_SOURCES: ChipSource[] = [
  {
    columnId: "clusterName",
    build: (column) => multiSelectChips<string>(column, "Cluster", (c) => c),
  },
  {
    columnId: "objectType",
    build: (column) =>
      multiSelectChips<MaintainedObjectType>(column, "Type", (t) => t),
  },
  {
    columnId: "freshness",
    build: (column) => {
      const value = column.getFilterValue();
      if (typeof value !== "number") return [];
      const label =
        FRESHNESS_THRESHOLD_OPTIONS[String(value)] ?? `Freshness: ${value}s`;
      return [
        {
          key: column.id,
          label,
          onRemove: () => column.setFilterValue(undefined),
        },
      ];
    },
  },
  {
    columnId: "hydration",
    build: (column) =>
      multiSelectChips<HydrationBucket>(
        column,
        "Hydration",
        (b) => HYDRATION_LABELS[b],
      ),
  },
];

export interface FilterChipsProps {
  table: Table<MaintainedObjectListItem>;
}

export const FilterChips = ({ table }: FilterChipsProps) => {
  const chips: Chip[] = [];
  for (const source of CHIP_SOURCES) {
    const column = table.getColumn(source.columnId);
    if (!column) continue;
    chips.push(...source.build(column));
  }

  if (chips.length === 0) return null;

  return (
    <HStack spacing="2" flexWrap="wrap">
      {chips.map((chip) => (
        <Tag key={chip.key} size="md" borderRadius="md" px="3" py="1">
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
