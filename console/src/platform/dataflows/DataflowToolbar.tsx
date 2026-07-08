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
  Checkbox,
  FormControl,
  FormLabel,
  HStack,
  Input,
  Menu,
  MenuButton,
  MenuItem,
  MenuList,
  Select,
  Slider,
  SliderFilledTrack,
  SliderThumb,
  SliderTrack,
  Switch,
  Text,
} from "@chakra-ui/react";
import React from "react";

import { type Filters } from "./dataflowGraph";

export interface DataflowToolbarProps {
  filters: Filters;
  onFiltersChange: (next: Filters) => void;
  channelTypes: string[];
  matchCount: number;
  matchIndex: number;
  onJump: (delta: 1 | -1) => void;
}

export const DataflowToolbar = ({
  filters,
  onFiltersChange,
  channelTypes,
  matchCount,
  matchIndex,
  onJump,
}: DataflowToolbarProps) => {
  const [search, setSearch] = React.useState(filters.search);
  // Debounce search input into the filters object.
  React.useEffect(() => {
    if (search === filters.search) return;
    const timeout = setTimeout(
      () => onFiltersChange({ ...filters, search }),
      300,
    );
    return () => clearTimeout(timeout);
  }, [search, filters, onFiltersChange]);
  return (
    <HStack spacing={4} flexShrink={0} flexWrap="wrap">
      <Input
        size="sm"
        width="240px"
        placeholder="Search operators"
        value={search}
        onChange={(e) => setSearch(e.target.value)}
      />
      {filters.search && (
        <HStack spacing={1}>
          <Button
            size="xs"
            onClick={() => onJump(-1)}
            isDisabled={matchCount === 0}
          >
            Prev
          </Button>
          <Button
            size="xs"
            onClick={() => onJump(1)}
            isDisabled={matchCount === 0}
          >
            Next
          </Button>
          <Text fontSize="xs">
            {matchCount === 0 ? "0/0" : `${matchIndex + 1}/${matchCount}`}
          </Text>
        </HStack>
      )}
      <FormControl display="flex" alignItems="center" width="auto">
        <FormLabel fontSize="xs" mb={0}>
          Hide idle
        </FormLabel>
        <Switch
          size="sm"
          isChecked={filters.hideIdle}
          onChange={(e) =>
            onFiltersChange({ ...filters, hideIdle: e.target.checked })
          }
        />
      </FormControl>
      <Select
        size="sm"
        width="180px"
        value={filters.heatmap}
        onChange={(e) =>
          onFiltersChange({
            ...filters,
            heatmap: e.target.value as Filters["heatmap"],
          })
        }
      >
        <option value="off">Heatmap off</option>
        <option value="elapsed">Heat: elapsed</option>
        <option value="size">Heat: arrangement size</option>
      </Select>
      <Slider
        width="120px"
        isDisabled={filters.heatmap === "off"}
        min={0}
        max={1}
        step={0.05}
        value={filters.heatmapThreshold}
        onChange={(v) => onFiltersChange({ ...filters, heatmapThreshold: v })}
      >
        <SliderTrack>
          <SliderFilledTrack />
        </SliderTrack>
        <SliderThumb />
      </Slider>
      <Menu closeOnSelect={false}>
        <MenuButton as={Button} size="sm">
          Channel types
        </MenuButton>
        <MenuList>
          {channelTypes.map((t) => (
            <MenuItem key={t}>
              <Checkbox
                isChecked={
                  filters.channelTypes === null ||
                  filters.channelTypes.includes(t)
                }
                onChange={(e) => {
                  const current = filters.channelTypes ?? channelTypes;
                  const next = e.target.checked
                    ? [...current, t]
                    : current.filter((x) => x !== t);
                  onFiltersChange({
                    ...filters,
                    channelTypes:
                      next.length === channelTypes.length ? null : next,
                  });
                }}
              >
                {t}
              </Checkbox>
            </MenuItem>
          ))}
        </MenuList>
      </Menu>
    </HStack>
  );
};
