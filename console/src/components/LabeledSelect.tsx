// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { split } from "@chakra-ui/object-utils";
import {
  chakra,
  forwardRef,
  HTMLChakraProps,
  layoutPropNames,
  omitThemingProps,
  PropsOf,
  SelectField,
  SelectProps,
  SystemStyleObject,
  useFormControl,
  useMultiStyleConfig,
  useTheme,
} from "@chakra-ui/react";
import { dataAttr } from "@chakra-ui/utils";
import * as React from "react";

import { MaterializeTheme } from "~/theme";

export type LabeledSelectProps = SelectProps & { label: string };

// Much of this code was copied from
// https://github.com/chakra-ui/chakra-ui/blob/c483d859d015d850bc871cc5156f159a7694e795/packages/components/select/src/select.tsx
// So that I could add a label inside the wrapper component
const LabeledSelect = forwardRef<LabeledSelectProps, "select">((props, ref) => {
  const { colors, radii, shadows } = useTheme<MaterializeTheme>();

  const styles = useMultiStyleConfig("Select", props);

  const {
    rootProps,
    icon,
    color,
    height,
    h,
    minH,
    minHeight,
    iconColor,
    iconSize,
    ...rest
  } = omitThemingProps(props);

  const [layoutProps, otherProps] = split(rest, layoutPropNames as any[]);

  const ownProps = useFormControl(otherProps);

  const rootStyles: SystemStyleObject = {
    // custom styles
    display: "flex",
    height: "32px",
    overflow: "hidden",
    borderRadius: radii.lg,
    // end custom styles
    position: "relative",
    color,
  };

  const fieldStyles: SystemStyleObject = {
    paddingEnd: "2rem",
    ...styles.field,
    // custom styles
    borderRadius: `0 ${radii.lg} ${radii.lg} 0`,
    _focusVisible: {
      boxShadow: "none",
    },
    _focus: {
      zIndex: "unset",
      ...(styles as any).field?.["_focus"],
      outline: "none",
      border: "none",
      outlineOffset: 0,
    },
    // end custom styles
  };

  const focusRing = shadows.input.focus;

  return (
    <chakra.div
      className="chakra-select__wrapper"
      __css={rootStyles}
      {...layoutProps}
      {...rootProps}
      justifyContent="center"
      alignItems="center"
      // custom styles
      borderRadius={radii.lg}
      border={`1px solid ${colors.border.secondary}`}
      boxShadow="
        0px 1px 3px 0px hsla(0, 0%, 0%, 0.06),
        0px 1px 1px 0px hsla(0, 0%, 0%, 0.04),
        0px 0px 0px 0px hsla(0, 0%, 0%, 0)"
      _focusWithin={{
        border: `1px solid ${colors.accent.brightPurple}`,
        boxShadow: `0px 0px 0px 0px hsla(0, 0%, 0%, 0), 0px 0px 0px 0px hsla(0, 0%, 0%, 0), ${focusRing}` /* accent.brightPurple */,
      }}
      // end custom styles
    >
      {/* This is the custom label we add */}
      <chakra.label
        sx={{
          fontSize: "14px",
          fontWeight: 500,
          lineHeight: "16px",
          padding: "8px 12px",
          borderRight: `1px solid ${colors.border.secondary}`,
          backgroundColor: colors.background.secondary,
        }}
      >
        {props.label}
      </chakra.label>
      {/* end custom label*/}
      <SelectField
        ref={ref}
        height={h ?? height}
        minH={minH ?? minHeight}
        {...ownProps}
        __css={fieldStyles}
        border="none"
        _hover={{
          borderColor: "inherit",
        }}
      >
        {props.children}
      </SelectField>

      <SelectIcon
        data-disabled={dataAttr(ownProps.disabled)}
        {...((iconColor || color) && { color: iconColor || color })}
        __css={styles.icon}
        {...(iconSize && { fontSize: iconSize })}
      >
        {icon}
      </SelectIcon>
    </chakra.div>
  );
});

const IconWrapper = chakra("div", {
  baseStyle: {
    position: "absolute",
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    pointerEvents: "none",
    top: "50%",
    transform: "translateY(-50%)",
  },
});

export const DefaultIcon: React.FC<PropsOf<"svg">> = (props) => (
  <svg viewBox="0 0 24 24" {...props}>
    <path
      fill="currentColor"
      d="M16.59 8.59L12 13.17 7.41 8.59 6 10l6 6 6-6z"
    />
  </svg>
);

export type SelectIconProps = HTMLChakraProps<"div">;

const SelectIcon: React.FC<SelectIconProps> = (props) => {
  const { children = <DefaultIcon />, ...rest } = props;

  const clone = React.cloneElement(children as any, {
    role: "presentation",
    className: "chakra-select__icon",
    focusable: false,
    "aria-hidden": true,
    // force icon to adhere to `IconWrapper` styles
    style: {
      width: "1em",
      height: "1em",
      color: "currentColor",
    },
  });

  return (
    <IconWrapper {...rest} className="chakra-select__icon-wrapper">
      {React.isValidElement(children) ? clone : null}
    </IconWrapper>
  );
};

SelectIcon.displayName = "SelectIcon";

export default LabeledSelect;
