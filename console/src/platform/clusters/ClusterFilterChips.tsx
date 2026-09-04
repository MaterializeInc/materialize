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

import { HydrationBucket } from "~/platform/maintained-objects/filters";

import { HYDRATION_COLUMN_ID, hydrationFilterLabel } from "./hydrationFilters";
import {
  REPLICA_COLUMN_ID,
  replicaCountFilterLabel,
} from "./replicaCountFilters";
import { utilizationFilterLabel } from "./utilizationFilters";

interface Chip {
  key: string;
  label: string;
  onRemove: () => void;
}

/** A utilization threshold: one chip per filtered column. */
const utilizationChips = <TData,>(
  table: Table<TData>,
  columns: readonly { id: string; header: string }[],
): Chip[] =>
  columns.flatMap(({ id, header }) => {
    const column = table.getColumn(id);
    const value = column?.getFilterValue() as number | undefined;
    if (!column || !value) return [];
    return [
      {
        key: id,
        label: utilizationFilterLabel(header, value),
        onRemove: () => column.setFilterValue(undefined),
      },
    ];
  });

/**
 * The hydration selection: one chip per selected bucket, so a single bucket can
 * be dropped without clearing the rest.
 */
const hydrationChips = <TData,>(table: Table<TData>): Chip[] => {
  const column = table.getColumn(HYDRATION_COLUMN_ID);
  if (!column) return [];

  const selected =
    (column.getFilterValue() as HydrationBucket[] | undefined) ?? [];

  return selected.map((bucket) => ({
    key: `${HYDRATION_COLUMN_ID}:${bucket}`,
    label: hydrationFilterLabel(bucket),
    onRemove: () => {
      const next = selected.filter((other) => other !== bucket);
      column.setFilterValue(next.length ? next : undefined);
    },
  }));
};

/**
 * The replica count minimum in force. Removing the chip drops the minimum
 * entirely, which is the only way back to the clusters with no replicas once
 * the default minimum has hidden them.
 */
const replicaCountChips = <TData,>(table: Table<TData>): Chip[] => {
  const column = table.getColumn(REPLICA_COLUMN_ID);
  const minimum = column?.getFilterValue() as number | undefined;
  if (!column || minimum === undefined) return [];

  return [
    {
      key: REPLICA_COLUMN_ID,
      label: replicaCountFilterLabel(minimum),
      onRemove: () => column.setFilterValue(undefined),
    },
  ];
};

export interface ClusterFilterChipsProps<TData> {
  table: Table<TData>;
  /** The utilization columns, in the order their chips should appear. */
  utilizationColumns: readonly { id: string; header: string }[];
}

/**
 * The column filters in force, each removable.
 *
 * A column's filter trigger signals only that it is active, by its colour, and
 * says what it filters by only once its popover is open. The chips put every
 * condition in one place, readable and removable without hunting across
 * headers. Renders nothing when no filter is applied.
 */
export const ClusterFilterChips = <TData,>({
  table,
  utilizationColumns,
}: ClusterFilterChipsProps<TData>) => {
  const chips = [
    ...replicaCountChips(table),
    ...utilizationChips(table, utilizationColumns),
    ...hydrationChips(table),
  ];

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
