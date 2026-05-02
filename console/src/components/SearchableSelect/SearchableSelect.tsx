// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CloseIcon } from "@chakra-ui/icons";
import { chakra, Grid, HStack, useTheme } from "@chakra-ui/react";
import React, { ComponentProps } from "react";
import ReactSelect, {
  ClearIndicatorProps,
  components as ReactSelectComponents,
  GroupBase,
  OptionsOrGroups,
  Props,
  ValueContainerProps,
} from "react-select";
// Prettier and eslint are fighting over the empty space between the braces.
// eslint-disable-next-line prettier/prettier
import type {} from "react-select/base";
import Select from "react-select/base";

import { DropdownIndicator, Option } from "~/components/reactSelectComponents";
import { MaterializeTheme } from "~/theme";

import { buildSearchableSelectFilterStyles } from "./utils";

export type SelectOptionWithoutId = { name: string };
export type SelectOption = {
  id: string;
  name: string;
};

export type SelectOptionKind = SelectOption | SelectOptionWithoutId;

type AdditionalMenuProps<
  Option extends SelectOptionKind,
  IsMulti extends boolean,
> = {
  /* Label to show on the left of the ValueContainer part. */
  label?: string;
  variant?: "default" | "error";
  /* Width of the control part. */
  containerWidth?: string;
  /* Width of the popover menu part. */
  menuWidth?: string;
  /* Icon to show in the ValueContainer part on the left. */
  leftIcon?: React.ReactNode;
  /* Render prop for the bottom of the menu. */
  renderMenuBottom?: (props: MenuProps<Option, IsMulti>) => React.ReactNode;
  /* Render the menu in a portal to avoid overflow clipping in modals. */
  usePortal?: boolean;
};

declare module "react-select/base" {
  // Extend react select's Select props type with our own.
  // See: https://react-select.com/typescript#custom-select-props
  /* eslint-disable @typescript-eslint/no-empty-interface, @typescript-eslint/no-empty-object-type, @typescript-eslint/no-unused-vars */
  export interface Props<
    Option extends SelectOptionKind,
    IsMulti extends boolean,
    Group extends GroupBase<Option>,
  > extends AdditionalMenuProps<Option, IsMulti> {
    // Props should go on AdditionalMenuProps, not here.
  }
  /* eslint-enable */
}

type MenuProps<
  Option extends SelectOptionKind,
  IsMulti extends boolean,
> = ComponentProps<
  typeof ReactSelectComponents.Menu<Option, IsMulti, GroupBase<Option>>
> & {
  selectProps: AdditionalMenuProps<Option, IsMulti>;
};

type ControlProps<
  Option extends SelectOptionKind,
  IsMulti extends boolean,
> = ComponentProps<
  typeof ReactSelectComponents.Control<Option, IsMulti, GroupBase<Option>>
> & {
  selectProps: AdditionalMenuProps<Option, IsMulti>;
};
export type SearchableSelectProps<
  Option extends SelectOptionKind = SelectOptionKind,
  IsMulti extends boolean = false,
> = Props<Option, IsMulti, GroupBase<Option>> &
  AdditionalMenuProps<Option, IsMulti> & {
    ariaLabel: string;
    options: OptionsOrGroups<Option, GroupBase<Option>>;
  };

const ClearIndicator = <
  Option,
  IsMulti extends boolean,
  Group extends GroupBase<Option> = GroupBase<Option>,
>(
  props: React.PropsWithChildren<ClearIndicatorProps<Option, IsMulti, Group>>,
) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <ReactSelectComponents.ClearIndicator {...props}>
      <CloseIcon height="8px" width="8px" color={colors.foreground.secondary} />
    </ReactSelectComponents.ClearIndicator>
  );
};

const Menu = <Option extends SelectOptionKind, IsMulti extends boolean>(
  props: MenuProps<Option, IsMulti>,
) => {
  const { children, selectProps } = props;

  return (
    <ReactSelectComponents.Menu {...props}>
      {children}
      {selectProps.renderMenuBottom?.(props) ?? null}
    </ReactSelectComponents.Menu>
  );
};

const Control = <Option extends SelectOptionKind, IsMulti extends boolean>({
  children,
  ...props
}: ControlProps<Option, IsMulti>) => {
  const { colors } = useTheme<MaterializeTheme>();
  const value = props.selectProps.value;
  const title =
    value && !Array.isArray(value)
      ? props.selectProps.getOptionLabel(value as Option)
      : "";
  return (
    <ReactSelectComponents.Control {...props}>
      <HStack w="100%" title={title}>
        {props.selectProps.label && (
          <chakra.label
            py="1"
            px="3"
            textStyle="text-ui-med"
            color={colors.foreground.primary}
            borderRight={`1px solid ${colors.border.secondary}`}
            backgroundColor={colors.background.secondary}
          >
            {props.selectProps.label}
          </chakra.label>
        )}
        {children}
      </HStack>
    </ReactSelectComponents.Control>
  );
};

// Unfortunately react-select doesn't offer a nice way to pass props into an
// internal component. Work around this by by building a custom element that
// wraps the provided icon.
const IconValueContainer = <
  Option extends SelectOptionKind,
  IsMulti extends boolean,
>({
  children,
  ...props
}: ValueContainerProps<Option, IsMulti, GroupBase<Option>>) => {
  /**
   * TODO: A workaround for the text overflowing when alignItems is center. Directly caused by
   * the issue here https://github.com/MaterializeInc/console/issues/1178. Remove once
   * the issue is fixed.
   */
  const gridAlignItems = props.selectProps.isSearchable ? "center" : "normal";

  return (
    <ReactSelectComponents.ValueContainer {...props}>
      {props.selectProps.leftIcon ? (
        <HStack minWidth="0">
          {props.selectProps.leftIcon}
          <Grid alignItems={gridAlignItems} minWidth="0">
            {children}
          </Grid>
        </HStack>
      ) : (
        children
      )}
    </ReactSelectComponents.ValueContainer>
  );
};

const SearchableSelect = <
  Option extends SelectOptionKind = SelectOptionKind,
  IsMulti extends boolean = false,
>(
  {
    options,
    ariaLabel,
    components,
    containerWidth,
    menuWidth,
    isMulti,
    variant = "default",
    usePortal,
    ...props
  }: SearchableSelectProps<Option, IsMulti>,
  ref: React.LegacyRef<Select<Option, IsMulti, GroupBase<Option>>>,
) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();

  return (
    <ReactSelect<Option, IsMulti, GroupBase<Option>>
      ref={ref}
      aria-label={ariaLabel}
      components={{
        Control,
        Option,
        DropdownIndicator,
        Menu,
        ClearIndicator: ClearIndicator,
        ValueContainer: IconValueContainer,
        ...components,
      }}
      getOptionLabel={(option) => option.name}
      getOptionValue={(option) => ("id" in option ? option.id : option.name)}
      isMulti={isMulti}
      isSearchable
      options={options}
      menuPortalTarget={usePortal ? document.body : undefined}
      styles={buildSearchableSelectFilterStyles({
        colors,
        shadows,
        isError: variant === "error",
        containerWidth,
        menuWidth,
        usePortal,
      })}
      {...props}
    />
  );
};

const SearchableSelectWrapper = React.forwardRef(SearchableSelect) as <
  Option extends SelectOptionKind = SelectOptionKind,
  IsMulti extends boolean = false,
>(
  p: SearchableSelectProps<Option, IsMulti> & {
    ref?: React.Ref<HTMLDivElement>;
  },
) => React.ReactElement;

export default SearchableSelectWrapper;
