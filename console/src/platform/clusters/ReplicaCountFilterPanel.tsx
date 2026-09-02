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
  HStack,
  NumberDecrementStepper,
  NumberIncrementStepper,
  NumberInput,
  NumberInputField,
  NumberInputStepper,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { Column } from "@tanstack/react-table";
import React from "react";

import { MaterializeTheme } from "~/theme";

export interface ReplicaCountFilterPanelProps<TData> {
  /** The column to filter, whose `filterFn` must be `replicaCountFilterFn`. */
  column: Column<TData, unknown>;
}

/**
 * Filters by how many replicas a row's cluster has, for the popover
 * `UniversalTable` anchors on a column header.
 *
 * A minimum of 0 is a real choice rather than an empty one, so it is applied as
 * an absent filter: clearing the panel and applying 0 both leave every row
 * visible.
 */
export const ReplicaCountFilterPanel = <TData,>({
  column,
}: ReplicaCountFilterPanelProps<TData>) => {
  const { colors } = useTheme<MaterializeTheme>();
  const filterValue = column.getFilterValue() as number | undefined;

  const [count, setCount] = React.useState(
    filterValue ? String(filterValue) : "",
  );

  // The panel is remounted on each open, so the seed above is what an opening
  // panel shows. This covers the filter changing while the panel is already
  // open, which is what removing the column's chip does.
  React.useEffect(() => {
    setCount(filterValue ? String(filterValue) : "");
  }, [filterValue]);

  const apply = () => {
    const parsed = parseInt(count, 10);
    column.setFilterValue(parsed > 0 ? parsed : undefined);
  };

  const clearFilter = () => {
    // Reset the draft as well as the filter. With nothing applied, clearing
    // leaves the applied value as it was, `undefined`, so the sync effect has
    // no change to react to and a minimum typed but never applied would stay on
    // screen.
    setCount("");
    column.setFilterValue(undefined);
  };

  return (
    <VStack alignItems="stretch" spacing={0}>
      <HStack spacing={2} px={4} py={3}>
        <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
          Replica Count ≥
        </Text>
        <NumberInput
          size="sm"
          maxW="20"
          min={0}
          value={count}
          focusBorderColor={colors.accent.brightPurple}
          onChange={(next) => setCount(next)}
        >
          <NumberInputField
            placeholder="0"
            aria-label="Minimum replica count"
            onKeyDown={(e) => {
              if (e.key === "Enter") apply();
            }}
          />
          <NumberInputStepper>
            <NumberIncrementStepper aria-label="Increase minimum replica count" />
            <NumberDecrementStepper aria-label="Decrease minimum replica count" />
          </NumberInputStepper>
        </NumberInput>
      </HStack>
      <HStack
        borderTopWidth="1px"
        borderColor={colors.border.secondary}
        justifyContent="space-between"
        py={2}
        px={4}
      >
        <Button
          size="sm"
          variant="secondary"
          transition="none"
          isDisabled={filterValue === undefined && count === ""}
          onClick={clearFilter}
        >
          Clear
        </Button>
        <Button size="sm" variant="primary" transition="none" onClick={apply}>
          Apply
        </Button>
      </HStack>
    </VStack>
  );
};
