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
  Select,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { Column } from "@tanstack/react-table";
import React from "react";

import { MaterializeTheme } from "~/theme";

import {
  DEFAULT_COMPARISON,
  UtilizationComparison,
  UtilizationFilterValue,
} from "./utilizationFilters";

export interface UtilizationFilterPanelProps<TData> {
  /** The column to filter, whose `filterFn` must be `utilizationFilterFn`. */
  column: Column<TData, unknown>;
  /** Column heading, shown inside the panel to name what is being filtered. */
  label: string;
}

/**
 * Filters one utilization column by a percentage threshold, for the popover
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
  const value = column.getFilterValue() as UtilizationFilterValue | undefined;

  const [comparison, setComparison] = React.useState<UtilizationComparison>(
    value?.comparison ?? DEFAULT_COMPARISON,
  );
  const [percent, setPercent] = React.useState(
    value ? String(value.percent) : "",
  );

  // The popover keeps its content mounted between opens, so a seed taken once
  // would drift from the filter in force. Following the applied value keeps
  // Clear, and a filter restored from the URL, visible on the next open.
  React.useEffect(() => {
    setComparison(value?.comparison ?? DEFAULT_COMPARISON);
    setPercent(value ? String(value.percent) : "");
  }, [value]);

  const parsed = Number.parseFloat(percent);
  // NOTE: no upper bound. `heap_percent` reports RAM plus swap against the heap
  // limit and can legitimately exceed 100%.
  const canApply = Number.isFinite(parsed) && parsed >= 0;

  const apply = () => {
    if (!canApply) return;
    column.setFilterValue({ comparison, percent: parsed });
  };

  const clearFilter = () => {
    column.setFilterValue(undefined);
  };

  return (
    <VStack alignItems="stretch" spacing={0}>
      <HStack spacing={2} px={4} py={3}>
        <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
          {label}
        </Text>
        <Select
          size="sm"
          maxW="16"
          fontWeight="700"
          aria-label={`${label} comparison`}
          value={comparison}
          focusBorderColor={colors.accent.brightPurple}
          onChange={(e) =>
            setComparison(e.target.value as UtilizationComparison)
          }
        >
          <option value=">">&gt;</option>
          <option value="<">&lt;</option>
        </Select>
        <NumberInput
          size="sm"
          maxW="20"
          min={0}
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
          isDisabled={value === undefined && percent === ""}
          onClick={clearFilter}
        >
          Clear
        </Button>
        <Button
          size="sm"
          variant="primary"
          transition="none"
          isDisabled={!canApply}
          onClick={apply}
        >
          Apply
        </Button>
      </HStack>
    </VStack>
  );
};
