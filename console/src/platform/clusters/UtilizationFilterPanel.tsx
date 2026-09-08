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

export interface UtilizationFilterPanelProps<TData> {
  /** The column to filter, whose `filterFn` must be `utilizationFilterFn`. */
  column: Column<TData, unknown>;
  /** Column heading, shown inside the panel to name what is being filtered. */
  label: string;
}

/**
 * Filters one utilization column by a lowest percentage, for the popover
 * `UniversalTable` anchors on a column header.
 *
 * Every utilization column reads the same way, a fraction of the replica's
 * allocation, so one panel serves all of them.
 */
export const UtilizationFilterPanel = <TData,>({
  column,
  label,
}: UtilizationFilterPanelProps<TData>) => {
  const { colors } = useTheme<MaterializeTheme>();
  const filterValue = column.getFilterValue() as number | undefined;

  const [percent, setPercent] = React.useState(
    filterValue ? String(filterValue) : "",
  );

  // The panel is remounted on each open, so the seed above is what an opening
  // panel shows. This covers the filter changing while the panel is already
  // open, which is what removing the column's chip does.
  React.useEffect(() => {
    setPercent(filterValue ? String(filterValue) : "");
  }, [filterValue]);

  const apply = () => {
    const parsed = parseFloat(percent);
    // NOTE: no upper bound. `heap_percent` reports RAM plus swap against the
    // heap limit and can legitimately exceed 100%.
    column.setFilterValue(parsed > 0 ? parsed : undefined);
  };

  const clearFilter = () => {
    // Reset the draft as well as the filter. With nothing applied, clearing
    // leaves the applied value as it was, `undefined`, so the sync effect has
    // no change to react to and a threshold typed but never applied would stay
    // on screen.
    setPercent("");
    column.setFilterValue(undefined);
  };

  return (
    <VStack alignItems="stretch" spacing={0}>
      <HStack spacing={2} px={4} py={3}>
        <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
          {label} ≥
        </Text>
        <NumberInput
          size="sm"
          maxW="20"
          min={1}
          value={percent}
          focusBorderColor={colors.accent.brightPurple}
          onChange={(next) => setPercent(next)}
        >
          <NumberInputField
            placeholder="0"
            aria-label={`${label} threshold percentage`}
            onKeyDown={(e) => {
              if (e.key === "Enter") apply();
            }}
          />
          <NumberInputStepper>
            <NumberIncrementStepper
              aria-label={`Increase ${label} threshold`}
            />
            <NumberDecrementStepper
              aria-label={`Decrease ${label} threshold`}
            />
          </NumberInputStepper>
        </NumberInput>
        <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
          %
        </Text>
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
          isDisabled={filterValue === undefined && percent === ""}
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
