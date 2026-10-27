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
  BoxProps,
  Button,
  chakra,
  FormControl,
  FormErrorMessage,
  FormLabel,
  HStack,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  Text,
  Textarea,
  useDisclosure,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { forwardRef, PropsWithChildren, useState } from "react";
import {
  ControllerRenderProps,
  FieldError,
  FieldValues,
  useForm,
} from "react-hook-form";
import {
  components,
  GroupBase,
  OptionProps,
  SingleValueProps,
} from "react-select";

import { alreadyExistsError } from "~/api/materialize/parseErrors";
import { Schema } from "~/api/materialize/schemaList";
import { Secret } from "~/api/materialize/secret/useSecretsCreationFlow";
import { Modal } from "~/components/Modal";
import ObjectNameInput from "~/components/ObjectNameInput";
import SearchableSelect, {
  SearchableSelectProps,
} from "~/components/SearchableSelect/SearchableSelect";
import {
  useCreateSecret,
  useSecretsListPage,
} from "~/platform/secrets/queries";
import { MaterializeTheme } from "~/theme";
import { capitalizeSentence } from "~/util";

import Alert from "./Alert";

export type SecretOption = {
  id: Secret["id"];
  name: Secret["name"];
  databaseName: Secret["databaseName"];
  schemaName: Secret["schemaName"];
};

function secretValueErrorMessage(error: FieldError | undefined) {
  if (!error?.type) return error?.message;
  if (error.type === "required") return "Secret value is required.";
}

type SecretFormState = {
  name: string;
  value: string;
};

const NewSecretModal = ({
  schema,
  isOpen,
  onClose,
}: {
  schema: Schema;
  isOpen: boolean;
  onClose: (secretId?: string) => void;
}) => {
  const { reset, formState, register, handleSubmit, setError, setFocus } =
    useForm<SecretFormState>({
      mode: "onTouched",
    });

  const [generalFormError, setGeneralFormError] = useState<string | null>(null);
  const { mutate, isPending: isCreationInFlight } = useCreateSecret();

  const onSubmit = async (formData: SecretFormState) => {
    setGeneralFormError(null);
    const params = {
      name: formData.name,
      value: formData.value,
      databaseName: schema.databaseName,
      schemaName: schema.name,
    };
    mutate(params, {
      onSuccess: () => {
        onClose(formData.name);
        reset();
      },
      onError: (error) => {
        if (alreadyExistsError(error.message)) {
          setError("name", { message: capitalizeSentence(error.message) });
          setFocus("name");
        } else {
          setGeneralFormError(error.message);
        }
      },
    });
  };

  return (
    <Modal isOpen={isOpen} onClose={onClose}>
      <ModalOverlay />
      <ModalContent>
        <ModalHeader>New secret</ModalHeader>
        <ModalCloseButton />
        <chakra.form onSubmit={handleSubmit(onSubmit)}>
          <ModalBody px={6} pb={6}>
            <VStack gap={4}>
              <Text>
                Create a secret to securely store sensitive credentials in
                Materialize&apos;s secret management system.
              </Text>
              {generalFormError && (
                <Alert variant="error" message={generalFormError} />
              )}
              <FormControl isInvalid={!!formState.errors.name} isRequired>
                <FormLabel>Name</FormLabel>
                <ObjectNameInput
                  {...register("name", {
                    required: "Secret name is required.",
                  })}
                  variant={formState.errors.name ? "error" : "default"}
                  placeholder="Secret name"
                  autoFocus
                />
                <FormErrorMessage>
                  {formState.errors.name?.message}
                </FormErrorMessage>
              </FormControl>
              <FormControl isInvalid={!!formState.errors.value} isRequired>
                <FormLabel>Value</FormLabel>
                <Textarea
                  {...register("value", { required: true })}
                  placeholder="Secret value"
                  variant={formState.errors.value ? "error" : "default"}
                  resize="vertical"
                />
                <FormErrorMessage>
                  {secretValueErrorMessage(formState.errors.value)}
                </FormErrorMessage>
              </FormControl>
            </VStack>
          </ModalBody>
          <ModalFooter gap={4} justifyContent="flex-start">
            <Button
              size="sm"
              onClick={() => onClose()}
              isDisabled={isCreationInFlight}
            >
              Cancel
            </Button>
            <Button
              type="submit"
              size="sm"
              variant="primary"
              isDisabled={!formState.isValid || isCreationInFlight}
            >
              Create Secret
            </Button>
          </ModalFooter>
        </chakra.form>
      </ModalContent>
    </Modal>
  );
};

const SingleValue = ({ children, ...props }: SingleValueProps<Secret>) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <components.SingleValue {...props}>
      <chakra.span color={colors.foreground.secondary}>
        {props.data.databaseName}.{props.data.schemaName}
      </chakra.span>
      .{props.data.name}
    </components.SingleValue>
  );
};

const OptionBase = forwardRef(
  (
    {
      isHighlighted,
      isSelected,
      children,
      containerProps,
    }: {
      isHighlighted?: boolean;
      isSelected?: boolean;
      children?: React.ReactNode;
      containerProps?: BoxProps;
    },
    ref,
  ) => {
    const { colors } = useTheme<MaterializeTheme>();
    return (
      <Box
        ref={ref}
        backgroundColor={
          isHighlighted
            ? colors.background.secondary
            : isSelected
              ? colors.background.tertiary
              : undefined
        }
        py="1"
        px="2"
        width="100%"
        cursor="pointer"
        {...containerProps}
      >
        <Text noOfLines={1} textStyle="text-base" userSelect="none">
          {children}
        </Text>
      </Box>
    );
  },
);

const Option = ({
  children,
  ...props
}: PropsWithChildren<OptionProps<Secret, false, GroupBase<Secret>>>) => {
  const { isFocused, innerRef, innerProps, isSelected } = props;
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <OptionBase
      isHighlighted={isFocused}
      isSelected={isSelected}
      containerProps={{ ...innerProps }}
      ref={innerRef}
    >
      <chakra.span color={colors.foreground.secondary}>
        {props.data.databaseName}.{props.data.schemaName}
      </chakra.span>
      .{props.data.name}
    </OptionBase>
  );
};

const SecretSelectionControl = <T extends FieldValues = FieldValues>({
  schema,
  selectField,
  variant,
  selectProps,
}: {
  schema: Schema;
  selectField: ControllerRenderProps<T, any>;
  variant?: "default" | "error";
  selectProps?: Partial<SearchableSelectProps<SecretOption>>;
}) => {
  const {
    isOpen: isModalOpen,
    onOpen: onModalOpen,
    onClose: onCloseModal,
  } = useDisclosure();
  const { data: secrets, isLoading, refetch } = useSecretsListPage({});

  const onClose = async (secretName?: string) => {
    onCloseModal();
    if (!secretName) {
      return;
    }
    const refetched = await refetch();
    if (refetched.error) {
      return;
    }
    const newSecrets = refetched.data?.rows;
    const created = newSecrets?.find((row) => row.name === secretName);
    if (created) {
      selectField.onChange(created);
    }
  };

  return (
    <HStack gap={4}>
      <SearchableSelect<SecretOption>
        ariaLabel="Select an existing secret"
        placeholder="Select an existing secret"
        components={{ Option, SingleValue }}
        options={secrets?.rows ?? []}
        variant={variant}
        containerWidth="100%"
        isDisabled={isLoading}
        {...selectProps}
        {...selectField}
      />
      <Box>or</Box>
      <Box>
        <Button size="sm" onClick={onModalOpen}>
          Create a new secret
        </Button>
        <NewSecretModal
          schema={schema}
          isOpen={isModalOpen}
          onClose={onClose}
        />
      </Box>
    </HStack>
  );
};

export default SecretSelectionControl;
