// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  HStack,
  Tag,
  TagCloseButton,
  TagLabel,
  useTheme,
} from "@chakra-ui/react";
import { Table } from "@tanstack/react-table";
import React from "react";

import { MaintainedObjectType } from "~/api/materialize/maintained-objects/constants";
import { MaterializeTheme } from "~/theme";

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

const summarizeMulti = <T,>(
  prefix: string,
  values: readonly T[],
  itemLabel: (value: T) => string,
): string =>
  values.length === 1
    ? `${prefix}: ${itemLabel(values[0])}`
    : `${prefix}: ${values.length}`;

type ChipBuilder<TValue> = (value: TValue) => string | null;

interface ChipSource<TValue> {
  columnId: string;
  build: ChipBuilder<TValue>;
}

const isPresent = <T,>(v: T[] | undefined): v is T[] =>
  Array.isArray(v) && v.length > 0;

const CHIP_SOURCES: ChipSource<unknown>[] = [
  {
    columnId: "clusterName",
    build: (value) =>
      isPresent(value as string[] | undefined)
        ? summarizeMulti("Cluster", value as string[], (c) => c)
        : null,
  } as ChipSource<unknown>,
  {
    columnId: "objectType",
    build: (value) =>
      isPresent(value as MaintainedObjectType[] | undefined)
        ? summarizeMulti("Type", value as MaintainedObjectType[], (t) => t)
        : null,
  } as ChipSource<unknown>,
  {
    columnId: "freshness",
    build: (value) => {
      if (typeof value !== "number") return null;
      return (
        FRESHNESS_THRESHOLD_OPTIONS[String(value)] ?? `Freshness: ${value}s`
      );
    },
  } as ChipSource<unknown>,
  {
    columnId: "hydration",
    build: (value) =>
      isPresent(value as HydrationBucket[] | undefined)
        ? summarizeMulti(
            "Hydration",
            value as HydrationBucket[],
            (b) => HYDRATION_LABELS[b],
          )
        : null,
  } as ChipSource<unknown>,
];

export interface FilterChipsProps {
  table: Table<MaintainedObjectListItem>;
}

export const FilterChips = ({ table }: FilterChipsProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const chips: Chip[] = [];
  for (const source of CHIP_SOURCES) {
    const column = table.getColumn(source.columnId);
    if (!column) continue;
    const label = source.build(column.getFilterValue());
    if (label === null) continue;
    chips.push({
      key: source.columnId,
      label,
      onRemove: () => column.setFilterValue(undefined),
    });
  }

  if (chips.length === 0) return null;

  return (
    <HStack spacing="2" flexWrap="wrap">
      {chips.map((chip) => (
        <Tag
          key={chip.key}
          size="md"
          variant="subtle"
          bg={colors.background.secondary}
          color={colors.foreground.primary}
          borderRadius="full"
        >
          <TagLabel>{chip.label}</TagLabel>
          <TagCloseButton
            aria-label={`Remove ${chip.key} filter`}
            onClick={chip.onRemove}
          />
        </Tag>
      ))}
    </HStack>
  );
};
