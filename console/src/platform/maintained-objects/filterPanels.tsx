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
  Checkbox,
  HStack,
  Input,
  Radio,
  RadioGroup,
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

import {
  FRESHNESS_THRESHOLD_OPTIONS,
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
  const selected = column.getFilterValue() as number | undefined;

  const setValue = (value: number | undefined) => {
    column.setFilterValue(value);
  };

  return (
    <RadioGroup
      value={selected?.toString() ?? ""}
      onChange={(value) =>
        setValue(value === "" ? undefined : parseInt(value, 10))
      }
    >
      <VStack alignItems="stretch" gap={0} py="2">
        <HStack
          px={4}
          py={1}
          _hover={{ bg: colors.background.secondary }}
          cursor="pointer"
          onClick={() => setValue(undefined)}
        >
          <Radio value="" />
          <Text textStyle="text-ui-reg">All freshness</Text>
        </HStack>
        {Object.entries(FRESHNESS_THRESHOLD_OPTIONS).map(([value, label]) => (
          <HStack
            key={value}
            px={4}
            py={1}
            _hover={{ bg: colors.background.secondary }}
            cursor="pointer"
            onClick={() => setValue(parseInt(value, 10))}
          >
            <Radio value={value} />
            <Text textStyle="text-ui-reg">{label}</Text>
          </HStack>
        ))}
      </VStack>
    </RadioGroup>
  );
};
