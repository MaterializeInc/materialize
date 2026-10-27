// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  ButtonProps,
  InputProps,
  List,
  ListItem,
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@chakra-ui/react";
import { useSelect, UseSelectProps } from "downshift";
import React from "react";

import { OptionBase } from "~/components/reactSelectComponents";
import { viewportOverflowModifier } from "~/theme/components/Popover";

import { DropdownToggleButton } from "./dropdownComponents";

export type CheckedMultiSelectProps<Item> = {
  // All items/options to select from. Used to render all available options.
  items: Item[];
  // Must use onStateChange to update this state.
  selectedItems: Item[];
  // Left icon to render inside the toggle button
  leftIcon?: React.ReactNode;
  // toggleButtonProps to pass to the toggle button
  toggleButtonProps?: ButtonProps;
  // Content to render inside the toggle button
  toggleButtonContent?: React.ReactNode;
  // A callback to get the rendered label/content depending on the item
  getItemLabel: (item: Item) => React.ReactNode;
  // A callback to handle when an item is selected/deselected. Used to update controlled state.
  onSelectedItemChange?: UseSelectProps<Item>["onSelectedItemChange"];
};

/**
 * A dropdown component that allows for multiple selections. The only visual
 * indicator of selected items are checkmarks beside each selected item in the
 * listbox.
 */
const CheckedMultiSelect = <Item,>({
  getItemLabel,
  items,
  leftIcon,
  toggleButtonContent,
  onSelectedItemChange,
  toggleButtonProps,
  ...props
}: CheckedMultiSelectProps<Item>) => {
  const {
    isOpen,
    getToggleButtonProps,
    getMenuProps,
    highlightedIndex,
    getItemProps,
  } = useSelect({
    items,
    selectedItem: null,
    stateReducer: (state, { changes, type }) => {
      switch (type) {
        case useSelect.stateChangeTypes.ToggleButtonKeyDownEnter:
        case useSelect.stateChangeTypes.ToggleButtonKeyDownSpaceButton:
        case useSelect.stateChangeTypes.ItemClick: {
          return {
            ...changes,
            isOpen: true, // keep the menu open after selection.
            highlightedIndex: state.highlightedIndex, // don't change the highlightedIndex after selection.
          };
        }
      }

      return changes;
    },
    onSelectedItemChange,
  });

  const buttonProps = getToggleButtonProps();

  return (
    <>
      <Popover
        modifiers={viewportOverflowModifier}
        gutter={0}
        placement="bottom-end"
        variant="dropdown"
        autoFocus={false}
        isOpen={isOpen}
      >
        <PopoverTrigger>
          <DropdownToggleButton
            inputProps={{
              ...(toggleButtonProps as InputProps),
              ...buttonProps,
            }}
            ref={buttonProps.ref}
            leftIcon={leftIcon}
          >
            {toggleButtonContent}
          </DropdownToggleButton>
        </PopoverTrigger>
        <PopoverContent
          motionProps={{
            animate: false,
          }}
        >
          <List {...getMenuProps()} py="1">
            {items.map((item, index) => {
              const isSelected = props.selectedItems.includes(item);

              return (
                <ListItem key={index} {...getItemProps({ item, index })}>
                  <OptionBase
                    isHighlighted={highlightedIndex === index}
                    isSelected={isSelected}
                  >
                    {getItemLabel(item)}
                  </OptionBase>
                </ListItem>
              );
            })}
          </List>
        </PopoverContent>
      </Popover>
    </>
  );
};

export default CheckedMultiSelect;
