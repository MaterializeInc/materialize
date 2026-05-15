// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Box,
  Button,
  Checkbox,
  HStack,
  Input,
  NumberInput,
  NumberInputField,
  Select,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { Column } from "@tanstack/react-table";
import React from "react";

import {
  MAINTAINED_OBJECT_TYPES,
  MaintainedObjectType,
} from "~/api/materialize/maintained-objects/constants";
import { MaterializeTheme } from "~/theme";
import { DurationUnit, fromSeconds, toSeconds } from "~/utils/format";

import {
  HYDRATION_BUCKETS,
  HYDRATION_LABELS,
  HydrationBucket,
} from "./filters";
import { MaintainedObjectListItem } from "./queries";

type AnyColumn = Column<MaintainedObjectListItem, unknown>;

interface FilterCheckboxRowProps {
  isChecked: boolean;
  onChange: () => void;
  children: React.ReactNode;
}

const FilterCheckboxRow = ({
  isChecked,
  onChange,
  children,
}: FilterCheckboxRowProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Checkbox
      width="100%"
      px="4"
      py="1"
      spacing="0"
      display="flex"
      flexDir="row-reverse"
      justifyContent="space-between"
      alignItems="center"
      _hover={{ bg: colors.background.secondary }}
      isChecked={isChecked}
      onChange={onChange}
    >
      {children}
    </Checkbox>
  );
};

export interface MultiSelectFilterPanelProps<T extends string> {
  column: AnyColumn;
  items: readonly T[];
  getLabel?: (item: T) => React.ReactNode;
  searchable?: boolean;
  searchPlaceholder?: string;
  emptyMessage?: string;
}

/**
 * Multi-select filter for a TanStack column whose filter value is a `T[]`.
 * An empty selection clears the column filter.
 */
export const MultiSelectFilterPanel = <T extends string>({
  column,
  items,
  getLabel,
  searchable = false,
  searchPlaceholder = "Search...",
  emptyMessage = "No results",
}: MultiSelectFilterPanelProps<T>) => {
  const { colors } = useTheme<MaterializeTheme>();
  const selected = (column.getFilterValue() as T[] | undefined) ?? [];
  const [search, setSearch] = React.useState("");

  const visibleItems = React.useMemo(() => {
    if (!searchable || !search) return items;
    const q = search.toLowerCase();
    return items.filter((item) => item.toLowerCase().includes(q));
  }, [items, search, searchable]);

  const toggle = (item: T) => {
    const next = selected.includes(item)
      ? selected.filter((x) => x !== item)
      : [...selected, item];
    column.setFilterValue(next.length ? next : undefined);
  };

  return (
    <VStack
      alignItems="stretch"
      px={searchable ? "4" : "0"}
      py={searchable ? "3" : "2"}
      spacing="2"
    >
      {searchable && (
        <Input
          size="sm"
          placeholder={searchPlaceholder}
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          autoFocus
        />
      )}
      <Box maxHeight={searchable ? "220px" : undefined} overflowY="auto">
        {visibleItems.length === 0 ? (
          <Text
            textStyle="text-ui-reg"
            color={colors.foreground.secondary}
            p="2"
          >
            {emptyMessage}
          </Text>
        ) : (
          <VStack alignItems="stretch" gap={0}>
            {visibleItems.map((item) => (
              <FilterCheckboxRow
                key={item}
                isChecked={selected.includes(item)}
                onChange={() => toggle(item)}
              >
                <Text textStyle="text-ui-reg" noOfLines={1}>
                  {getLabel ? getLabel(item) : item}
                </Text>
              </FilterCheckboxRow>
            ))}
          </VStack>
        )}
      </Box>
    </VStack>
  );
};

export const ClusterFilterPanel = ({
  column,
  clusters,
}: {
  column: AnyColumn;
  clusters: string[];
}) => (
  <MultiSelectFilterPanel
    column={column}
    items={clusters}
    searchable
    searchPlaceholder="Search clusters..."
    emptyMessage="No clusters match"
  />
);

export const ObjectTypeFilterPanel = ({ column }: { column: AnyColumn }) => (
  <MultiSelectFilterPanel<MaintainedObjectType>
    column={column}
    items={MAINTAINED_OBJECT_TYPES}
  />
);

export const HydrationFilterPanel = ({ column }: { column: AnyColumn }) => (
  <MultiSelectFilterPanel<HydrationBucket>
    column={column}
    items={HYDRATION_BUCKETS}
    getLabel={(b) => HYDRATION_LABELS[b]}
  />
);

export const FreshnessFilterPanel = ({ column }: { column: AnyColumn }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const filterValue = column.getFilterValue() as number | undefined;
  const initial = filterValue ? fromSeconds(filterValue) : null;

  const [amount, setAmount] = React.useState(
    initial ? String(initial.amount) : "",
  );
  const [unit, setUnit] = React.useState<DurationUnit>(
    initial?.unit ?? "seconds",
  );

  const apply = () => {
    const n = parseFloat(amount);
    column.setFilterValue(n > 0 ? toSeconds(n, unit) : undefined);
  };

  const clearFilter = () => {
    setAmount("");
    column.setFilterValue(undefined);
  };

  return (
    <VStack alignItems="stretch" spacing={0}>
      <HStack spacing={2} px={4} py={3}>
        <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
          pMAX ≥
        </Text>
        <NumberInput
          size="sm"
          value={amount}
          min={1}
          focusBorderColor={colors.accent.brightPurple}
          onChange={(next) => setAmount(next)}
          maxW="20"
        >
          <NumberInputField
            placeholder="0"
            onKeyDown={(e) => {
              if (e.key === "Enter") apply();
            }}
          />
        </NumberInput>
        <Select
          size="sm"
          value={unit}
          focusBorderColor={colors.accent.brightPurple}
          onChange={(e) => setUnit(e.target.value as DurationUnit)}
          maxW="32"
        >
          <option value="seconds">seconds</option>
          <option value="minutes">minutes</option>
          <option value="hours">hours</option>
        </Select>
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
          isDisabled={filterValue === undefined && amount === ""}
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
