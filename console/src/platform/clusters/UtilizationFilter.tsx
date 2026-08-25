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
  Popover,
  PopoverContent,
  PopoverTrigger,
  Select,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { Column } from "@tanstack/react-table";
import React from "react";

import { ChevronDownIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";
import { viewportOverflowModifier } from "~/theme/components/Popover";

import {
  DEFAULT_COMPARISON,
  UtilizationComparison,
  UtilizationFilterValue,
} from "./utilizationFilters";

/**
 * The trigger's caption: the column name alone, or the condition in force, so
 * an applied filter is readable without opening the panel.
 */
const triggerLabel = (
  label: string,
  value: UtilizationFilterValue | undefined,
) => (value ? `${label} ${value.comparison} ${value.percent}%` : label);

/**
 * The panel's editable copy of the filter. Applied on Apply rather than on
 * every keystroke, so a half-typed threshold never reorders the table.
 *
 * Mounted fresh on each open (the popover unmounts its content when closed), so
 * the draft starts from whatever filter is currently in force.
 */
const UtilizationFilterPanel = <TData,>({
  column,
  label,
  onClose,
}: {
  column: Column<TData, unknown>;
  label: string;
  onClose: () => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const value = column.getFilterValue() as UtilizationFilterValue | undefined;

  const [comparison, setComparison] = React.useState<UtilizationComparison>(
    value?.comparison ?? DEFAULT_COMPARISON,
  );
  const [percent, setPercent] = React.useState(
    value ? String(value.percent) : "",
  );

  const parsed = Number.parseFloat(percent);
  // NOTE: no upper bound. `heap_percent` reports RAM plus swap against the heap
  // limit and can legitimately exceed 100%.
  const canApply = Number.isFinite(parsed) && parsed >= 0;

  const apply = () => {
    if (!canApply) return;
    column.setFilterValue({ comparison, percent: parsed });
    onClose();
  };

  const clearFilter = () => {
    column.setFilterValue(undefined);
    onClose();
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

export interface UtilizationFilterProps<TData> {
  /** The column to filter, whose `filterFn` must be `utilizationFilterFn`. */
  column: Column<TData, unknown>;
  /** Column name, shown on the trigger and inside the panel. */
  label: string;
}

/**
 * Toolbar control filtering one utilization column by a percentage threshold.
 *
 * Every utilization column reads the same way, a fraction of the replica's
 * allocation, so one control serves all of them.
 */
export const UtilizationFilter = <TData,>({
  column,
  label,
}: UtilizationFilterProps<TData>) => {
  const { colors } = useTheme<MaterializeTheme>();
  const value = column.getFilterValue() as UtilizationFilterValue | undefined;
  const isActive = value !== undefined;

  return (
    <Popover
      isLazy
      // Unmount rather than hide, so the panel's draft state is rebuilt from
      // the filter in force each time it opens.
      lazyBehavior="unmount"
      gutter={2}
      modifiers={viewportOverflowModifier}
      variant="dropdown"
      placement="bottom-start"
    >
      {({ onClose }) => (
        <>
          <PopoverTrigger>
            <Button
              size="sm"
              variant="secondary"
              rightIcon={<ChevronDownIcon />}
              color={isActive ? colors.accent.brightPurple : undefined}
              borderColor={isActive ? colors.accent.brightPurple : undefined}
            >
              {triggerLabel(label, value)}
            </Button>
          </PopoverTrigger>
          <PopoverContent motionProps={{ animate: false }}>
            <UtilizationFilterPanel
              column={column}
              label={label}
              onClose={onClose}
            />
          </PopoverContent>
        </>
      )}
    </Popover>
  );
};
