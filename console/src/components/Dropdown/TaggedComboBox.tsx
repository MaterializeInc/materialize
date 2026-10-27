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
  IconButton,
  Input,
  List,
  ListItem,
  Popover,
  PopoverContent,
  PopoverTrigger,
  Tag,
  TagLabel,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import {
  useCombobox,
  UseComboboxProps,
  useMultipleSelection,
  UseMultipleSelectionProps,
} from "downshift";
import React from "react";

import { OptionBase } from "~/components/reactSelectComponents";
import { CloseIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";
import { viewportOverflowModifier } from "~/theme/components/Popover";

export type TaggedComboBoxProps<Item> = {
  // All items/options to select from. Used to render all available options.
  items: Item[];
  // Must use onStateChange to update this state.
  selectedItems: Item[];
  // A callback to get the string to display for the item. Used for search filtering too.
  getItemLabel: (item: Item) => string;
  /**
   * A callback to get the rendered label content depending on the item.
   * If undefined, uses the string provided by getItemLabel.
   */
  formatOptionLabel?: (item: Item) => React.ReactNode;
  // The current input value.
  inputValue: UseComboboxProps<Item>["inputValue"];
  // The placeholder text to display for the search input.
  placeholder?: string;
  /**  Callbacks to update controlled state. */
  multipleSelectionOnStateChange: UseMultipleSelectionProps<Item>["onStateChange"];
  comboBoxOnStateChange: UseComboboxProps<Item>["onStateChange"];
};

function getFilteredItems<Item>({
  items,
  selectedItems,
  inputValue,
  getItemLabel,
}: {
  items: Item[];
  selectedItems: Item[];
  inputValue: string;
  getItemLabel: (item: Item) => string;
}) {
  return items.filter(
    (item) =>
      !selectedItems.some(
        (selectedItem) => getItemLabel(selectedItem) === getItemLabel(item),
      ) && getItemLabel(item).toLowerCase().includes(inputValue.toLowerCase()),
  );
}

/**
 * A dropdown component that allows for multiple selections. The only visual
 * indicator of selected items are checkmarks beside each selected item in the
 * listbox.
 */
const TaggedComboBox = <Item,>({
  formatOptionLabel,
  items,
  multipleSelectionOnStateChange,
  comboBoxOnStateChange,
  inputValue,
  selectedItems,
  placeholder,
  getItemLabel,
}: TaggedComboBoxProps<Item>) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { getSelectedItemProps, getDropdownProps, removeSelectedItem } =
    useMultipleSelection({
      selectedItems,
      onStateChange: multipleSelectionOnStateChange,
    });

  const filteredItems = getFilteredItems({
    items,
    selectedItems,
    inputValue: inputValue ?? "",
    getItemLabel,
  });

  const {
    isOpen,
    getMenuProps,
    getInputProps,
    getItemProps,
    highlightedIndex,
  } = useCombobox({
    items: filteredItems,
    // defaultHighlightedIndex: 0,
    selectedItem: null,
    stateReducer(_, actionAndChanges) {
      const { changes, type } = actionAndChanges;

      switch (type) {
        case useCombobox.stateChangeTypes.InputKeyDownEnter:
        case useCombobox.stateChangeTypes.ItemClick:
          return {
            ...changes,
            isOpen: true, // keep the menu open after selection.
            highlightedIndex: 0, // with the first option highlighted.
          };
        default:
          return changes;
      }
    },
    onStateChange: comboBoxOnStateChange,
  });

  return (
    <VStack gap="2">
      <Popover
        modifiers={viewportOverflowModifier}
        gutter={0}
        variant="dropdown"
        isOpen={isOpen}
        placement="bottom-start"
        autoFocus={false}
        computePositionOnMount
      >
        <PopoverTrigger>
          <Input
            {...getInputProps(getDropdownProps({ preventKeyAction: isOpen }))}
            value={inputValue}
            type="text"
            placeholder={placeholder}
          />
        </PopoverTrigger>
        <PopoverContent
          animate={false}
          // This is a workaround because of a bug when `animate` is false.
          // The bug is that the popover content is visible when you unmount the component when it's open.
          rootProps={{
            sx: {
              opacity: isOpen ? 1 : 0,
            },
          }}
        >
          {filteredItems.length > 0 ? (
            <List {...getMenuProps()} py="1">
              {filteredItems.map((item, index) => {
                return (
                  <ListItem
                    key={getItemLabel(item)}
                    {...getItemProps({ item, index })}
                  >
                    <OptionBase
                      isHighlighted={highlightedIndex === index}
                      containerProps={{
                        paddingRight: "40px",
                      }}
                    >
                      {formatOptionLabel?.(item) ?? getItemLabel(item)}
                    </OptionBase>
                  </ListItem>
                );
              })}
            </List>
          ) : (
            <Text
              paddingY="4"
              paddingX="10"
              color={colors.foreground.secondary}
            >
              No options
            </Text>
          )}
        </PopoverContent>
      </Popover>
      {selectedItems.length > 0 && (
        <HStack
          flexWrap="wrap"
          alignItems="flex-start"
          justifyContent="flex-start"
          gap="2"
          width="100%"
        >
          {selectedItems.map((item) => (
            <Tag
              size="sm"
              key={getItemLabel(item)}
              variant="solid"
              background={colors.accent.indigo}
              color={colors.accent.purple}
              borderRadius="full"
              {...getSelectedItemProps({ selectedItem: item })}
            >
              <TagLabel>
                {formatOptionLabel?.(item) ?? getItemLabel(item)}
              </TagLabel>

              <IconButton
                variant="unstyled"
                aria-label="Remove"
                size="xs"
                sx={{
                  width: "4",
                  height: "4",
                }}
                onClick={(e) => {
                  e.stopPropagation();
                  removeSelectedItem(item);
                }}
              >
                <CloseIcon
                  width="3"
                  height="3"
                  stroke={colors.foreground.primary}
                />
              </IconButton>
            </Tag>
          ))}
        </HStack>
      )}
    </VStack>
  );
};

type TaggedComboBoxWithStateProps<Item> = Omit<
  TaggedComboBoxProps<Item>,
  "multipleSelectionOnStateChange" | "comboBoxOnStateChange" | "inputValue"
> & {
  // The callback to update the selectedItems upstream.
  onChange: (selectedItems: Item[]) => void;
};

/**
 * A wrapper around TaggedComboBox that controls internal state values and alerts
 * updates via onChange. Useful when the consumer doesn't own internal state values such as the current
 * search value.
 */
const TaggedComboBoxWithState = <Item,>({
  onChange,
  ...props
}: TaggedComboBoxWithStateProps<Item>) => {
  const [inputValue, setInputValue] = React.useState("");

  return (
    <TaggedComboBox
      inputValue={inputValue}
      multipleSelectionOnStateChange={({
        selectedItems: newSelectedItems,
        type,
      }) => {
        switch (type) {
          case useMultipleSelection.stateChangeTypes
            .SelectedItemKeyDownBackspace:
          case useMultipleSelection.stateChangeTypes.SelectedItemKeyDownDelete:
          case useMultipleSelection.stateChangeTypes.DropdownKeyDownBackspace:
          case useMultipleSelection.stateChangeTypes.FunctionRemoveSelectedItem:
            onChange(newSelectedItems ?? []);
            break;
          default:
            break;
        }
      }}
      comboBoxOnStateChange={({
        inputValue: newInputValue,
        type,
        selectedItem: newSelectedItem,
      }) => {
        switch (type) {
          case useCombobox.stateChangeTypes.InputKeyDownEnter:
          case useCombobox.stateChangeTypes.ItemClick:
          case useCombobox.stateChangeTypes.InputBlur:
            if (newSelectedItem) {
              const newSelectedItems = [
                ...props.selectedItems,
                newSelectedItem,
              ];
              onChange(newSelectedItems ?? []);
              setInputValue("");
            }
            break;

          case useCombobox.stateChangeTypes.InputChange:
            setInputValue(newInputValue ?? "");
            break;
          default:
            break;
        }
      }}
      {...props}
    />
  );
};

export default TaggedComboBoxWithState;
