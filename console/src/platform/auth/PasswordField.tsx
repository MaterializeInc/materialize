// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  FormControl,
  FormHelperText,
  Input,
  InputProps,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { FieldError, MultipleFieldErrors } from "react-hook-form";

import { LabeledInput } from "~/components/formComponentsV2";
import { MaterializeTheme } from "~/theme";

import { passwordRules } from "./utils";

export const PasswordField = (props: {
  errors: FieldError | undefined;
  label?: string;
  inputProps: InputProps;
}) => {
  return (
    <FormControl isInvalid={!!props.errors}>
      <LabeledInput
        label={props.label ?? "Password"}
        inputBoxProps={{
          maxWidth: "100%",
        }}
      >
        <Input
          {...props.inputProps}
          autoCorrect="off"
          placeholder="Choose a password"
          size="lg"
          type="password"
          variant={props.errors ? "error" : "default"}
        />
      </LabeledInput>
      <FormHelperText>
        <PasswordRules errors={props.errors?.types} />
      </FormHelperText>
    </FormControl>
  );
};

export const PasswordRules = (props: {
  errors: MultipleFieldErrors | undefined;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <VStack mx="4" mt="2" spacing="1" alignItems="flex-start" textAlign="start">
      <Text textStyle="text-small" mt="0">
        Password must:
      </Text>
      <ul>
        {Object.entries(passwordRules).map(([key, message]) => (
          <Text
            key={key}
            data-valid={!props.errors?.[key]}
            as="li"
            textStyle="text-small"
            ml="4"
            mt="0"
            color={
              props.errors?.[key]
                ? colors.accent.red
                : colors.foreground.secondary
            }
          >
            {message}
          </Text>
        ))}
      </ul>
    </VStack>
  );
};
