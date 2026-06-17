// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ChevronDownIcon } from "@chakra-ui/icons";
import {
  Box,
  Button,
  Flex,
  Input,
  Popover,
  PopoverBody,
  PopoverContent,
  PopoverTrigger,
  Spinner,
  Text,
  useDisclosure,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { useMemo, useRef, useState } from "react";

import type { MaterializeTheme } from "~/theme";

/** Single item shown in the picker's popover. */
export interface BreadcrumbPickerItem {
  /** Stable identifier compared against `selectedId`. */
  id: string;
  /** Text shown in the menu row. */
  label: string;
  /** Optional heading the item is grouped under. Items with the same group are
   *  rendered together under one heading, in input order. */
  group?: string;
}

export interface BreadcrumbPickerProps {
  /** Text shown in the breadcrumb segment when the popover is closed. */
  trigger: React.ReactNode;
  /** Accessible label for the trigger button. */
  ariaLabel: string;
  items: BreadcrumbPickerItem[];
  /** Identifier of the currently selected item, used to highlight it. */
  selectedId?: string;
  onSelect: (item: BreadcrumbPickerItem) => void;
  /** Show a spinner inside the popover body. */
  isLoading?: boolean;
  /** Disable the trigger entirely. */
  isDisabled?: boolean;
  searchPlaceholder?: string;
  /** Fixed width of the popover. */
  popoverWidth?: string;
}

/**
 * Compact breadcrumb-style picker: the trigger looks like inline text with a
 * small chevron, and clicking it opens a Popover with a search input and a
 * (optionally grouped) list of items. Designed for header context selectors
 * (cluster, schema) where the form-control look of a full dropdown is too
 * heavy.
 */
const BreadcrumbPicker = ({
  trigger,
  ariaLabel,
  items,
  selectedId,
  onSelect,
  isLoading,
  isDisabled,
  searchPlaceholder = "Search…",
  popoverWidth = "280px",
}: BreadcrumbPickerProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { isOpen, onOpen, onClose } = useDisclosure();
  const [search, setSearch] = useState("");
  const inputRef = useRef<HTMLInputElement | null>(null);

  const filteredItems = useMemo(() => {
    const needle = search.trim().toLowerCase();
    if (!needle) return items;
    return items.filter((item) => item.label.toLowerCase().includes(needle));
  }, [items, search]);

  const grouped = useMemo(() => {
    const groups = new Map<string, BreadcrumbPickerItem[]>();
    for (const item of filteredItems) {
      const key = item.group ?? "";
      const list = groups.get(key);
      if (list) list.push(item);
      else groups.set(key, [item]);
    }
    return groups;
  }, [filteredItems]);

  const handleOpen = () => {
    setSearch("");
    onOpen();
  };

  return (
    <Popover
      isOpen={isOpen}
      onOpen={handleOpen}
      onClose={onClose}
      placement="bottom-start"
      initialFocusRef={inputRef}
      isLazy
    >
      <PopoverTrigger>
        <Button
          variant="ghost"
          size="sm"
          aria-label={ariaLabel}
          isDisabled={isDisabled}
          fontWeight="normal"
          color={colors.foreground.primary}
          rightIcon={
            <ChevronDownIcon boxSize="3" color={colors.foreground.tertiary} />
          }
          _hover={{ bg: colors.background.secondary }}
          _active={{ bg: colors.background.secondary }}
          px="2"
        >
          {trigger}
        </Button>
      </PopoverTrigger>
      <PopoverContent w={popoverWidth} shadow="lg">
        <Box
          p="2"
          borderBottomWidth="1px"
          borderColor={colors.border.secondary}
        >
          <Input
            ref={inputRef}
            size="sm"
            placeholder={searchPlaceholder}
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </Box>
        <PopoverBody maxH="320px" overflowY="auto" px="0" py="1">
          {isLoading ? (
            <Flex justify="center" py="4">
              <Spinner size="sm" />
            </Flex>
          ) : filteredItems.length === 0 ? (
            <Text
              px="3"
              py="2"
              color={colors.foreground.tertiary}
              fontSize="sm"
            >
              No results
            </Text>
          ) : (
            <VStack align="stretch" spacing="0">
              {Array.from(grouped.entries()).map(([groupName, groupItems]) => (
                <Box key={groupName || "__ungrouped__"}>
                  {groupName && (
                    <Text
                      px="3"
                      pt="2"
                      pb="1"
                      color={colors.foreground.tertiary}
                      fontSize="xs"
                      fontWeight="medium"
                    >
                      {groupName}
                    </Text>
                  )}
                  {groupItems.map((item) => (
                    <Button
                      key={item.id}
                      variant="ghost"
                      justifyContent="flex-start"
                      size="sm"
                      w="100%"
                      borderRadius="0"
                      fontWeight={item.id === selectedId ? "medium" : "normal"}
                      bg={
                        item.id === selectedId
                          ? colors.background.secondary
                          : undefined
                      }
                      onClick={() => {
                        onSelect(item);
                        onClose();
                      }}
                    >
                      {item.label}
                    </Button>
                  ))}
                </Box>
              ))}
            </VStack>
          )}
        </PopoverBody>
      </PopoverContent>
    </Popover>
  );
};

export default BreadcrumbPicker;
