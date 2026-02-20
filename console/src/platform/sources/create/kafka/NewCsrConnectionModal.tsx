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
  chakra,
  FormControl,
  HStack,
  Input,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  StackDivider,
  Switch,
  VStack,
} from "@chakra-ui/react";
import { useAtom } from "jotai";
import React, { useState } from "react";
import { useController, UseFormReturn } from "react-hook-form";

import { useSegment } from "~/analytics/segment";
import { createCsrConnection } from "~/api/materialize/connection/createCsrConnection";
import { alreadyExistsError } from "~/api/materialize/parseErrors";
import { Schema } from "~/api/materialize/schemaList";
import useSchemas from "~/api/materialize/useSchemas";
import Alert from "~/components/Alert";
import ErrorBox from "~/components/ErrorBox";
import {
  FORM_CONTENT_WIDTH,
  FormSection,
  LabeledInput,
} from "~/components/formComponentsV2";
import { Modal } from "~/components/Modal";
import ObjectNameInput from "~/components/ObjectNameInput";
import SchemaSelect from "~/components/SchemaSelect";
import SecretSelectionControl from "~/components/SecretSelectionControl";
import { ObjectToastDescription } from "~/components/Toast";
import { getSecretFromOption } from "~/forms/secretsFormControlAccessors";
import { useToast } from "~/hooks/useToast";
import { currentEnvironmentState } from "~/store/environments";
import { assert, capitalizeSentence } from "~/util";

import { CsrConnectionFormState, useCsrConnectionForm } from "./forms";

type FormProp = { form: UseFormReturn<CsrConnectionFormState> };

const GeneralSection = ({
  form,
  schemas,
}: { schemas: Schema[] } & FormProp) => {
  const { control, formState, register } = form;
  const { field: schemaField } = useController({
    control,
    name: "schema",
    rules: { required: "Schema is required." },
  });

  return (
    <FormSection title="Configure connection" caption="Step 1">
      <FormControl isInvalid={!!formState.errors.name}>
        <LabeledInput
          label="Name"
          message="The name of the connection to your registry."
          error={formState.errors.name?.message}
        >
          <ObjectNameInput
            {...register("name", {
              required: "Connection name is required.",
            })}
            placeholder="my_csr_connection"
            variant={formState.errors.name ? "error" : "default"}
            autoFocus
          />
        </LabeledInput>
      </FormControl>
      <FormControl
        isInvalid={!!formState.errors.schema}
        data-testid="connection-schema"
      >
        <LabeledInput
          label="Schema"
          message="The database and schema where this connection will be created."
          error={formState.errors.schema?.message}
        >
          <SchemaSelect
            {...schemaField}
            schemas={schemas}
            variant={formState.errors.schema ? "error" : "default"}
          />
        </LabeledInput>
      </FormControl>
      <FormControl isInvalid={!!formState.errors.url}>
        <LabeledInput
          label="Schema Registry URL"
          error={formState.errors.url?.message}
        >
          <Input
            {...register("url", {
              required: "Registry URL is required.",
            })}
            placeholder="registry.example.com"
            autoCorrect="off"
            size="sm"
            variant={formState.errors.url ? "error" : "default"}
          />
        </LabeledInput>
      </FormControl>
    </FormSection>
  );
};

const CredentialSection = ({ form }: FormProp) => {
  const { control, formState, register, watch } = form;
  const watchSchema = watch("schema");
  const watchUseSsl = watch("useSsl");
  const { field: passwordField } = useController({
    control,
    name: "password",
    rules: {
      required: true,
    },
  });
  const { field: sslCertificateAuthorityField } = useController({
    control,
    name: "sslCertificateAuthority",
    rules: {
      required: false,
    },
  });
  const { field: sslKeyField } = useController({
    control,
    name: "sslKey",
    rules: {
      required: watchUseSsl,
    },
  });
  const { field: sslCertificateField } = useController({
    control,
    name: "sslCertificate",
    rules: {
      required: watchUseSsl,
    },
  });
  return (
    <FormSection title="Provide credentials" caption="Step 2">
      <FormControl>
        <HStack>
          <LabeledInput
            label="Use SSL Authentication"
            inputBoxProps={{ mt: 0 }}
          >
            <Switch {...register("useSsl")} />
          </LabeledInput>
        </HStack>
      </FormControl>
      <FormControl isInvalid={!!formState.errors.username}>
        <LabeledInput
          label="Username"
          error={formState.errors.username?.message}
        >
          <Input
            {...register("username", {
              required: "Registry username is required.",
            })}
            placeholder="user"
            autoCorrect="off"
            size="sm"
            variant={formState.errors.username ? "error" : "default"}
          />
        </LabeledInput>
      </FormControl>
      <FormControl
        isInvalid={!!formState.errors.password}
        data-testid="csr-password"
      >
        <LabeledInput
          label="Password"
          inputBoxProps={{ maxWidth: `${FORM_CONTENT_WIDTH}px` }}
          error={formState.errors.password?.message}
        >
          <SecretSelectionControl
            schema={watchSchema}
            selectField={passwordField}
            variant={formState.errors.password ? "error" : "default"}
          />
        </LabeledInput>
      </FormControl>
      {watchUseSsl && (
        <>
          <FormControl
            isInvalid={!!formState.errors.sslKey}
            data-testid="authentication-ssl-key"
          >
            <LabeledInput
              label="SSL Key"
              inputBoxProps={{ maxWidth: `${FORM_CONTENT_WIDTH}px` }}
              error={formState.errors.sslKey?.message}
            >
              <SecretSelectionControl
                schema={watchSchema}
                selectField={sslKeyField}
                variant={formState.errors.sslKey ? "error" : "default"}
              />
            </LabeledInput>
          </FormControl>
          <FormControl
            isInvalid={!!formState.errors.sslCertificate}
            data-testid="authentication-ssl-certificate"
          >
            <LabeledInput
              label="SSL Certificate"
              inputBoxProps={{ maxWidth: `${FORM_CONTENT_WIDTH}px` }}
              error={formState.errors.sslCertificate?.message}
            >
              <SecretSelectionControl
                schema={watchSchema}
                selectField={sslCertificateField}
                variant={formState.errors.sslCertificate ? "error" : "default"}
              />
            </LabeledInput>
          </FormControl>
          <FormControl
            isInvalid={!!formState.errors.sslCertificateAuthority}
            data-testid="authentication-ssl-ca"
          >
            <LabeledInput
              label="SSL Certificate Authority"
              inputBoxProps={{ maxWidth: `${FORM_CONTENT_WIDTH}px` }}
              error={formState.errors.sslCertificateAuthority?.message}
            >
              <SecretSelectionControl
                schema={watchSchema}
                selectField={sslCertificateAuthorityField}
                selectProps={{ isClearable: true }}
                variant={
                  formState.errors.sslCertificateAuthority ? "error" : "default"
                }
              />
            </LabeledInput>
          </FormControl>
        </>
      )}
    </FormSection>
  );
};

export const NewCsrConnectionForm = ({
  form,
  generalFormError,
}: { generalFormError?: string } & FormProp) => {
  const { results: schemas, failedToLoad: schemasFailedToLoad } = useSchemas({
    filterByCreatePrivilege: true,
  });
  const loadingError = schemasFailedToLoad;

  if (loadingError) {
    return <ErrorBox />;
  }
  return (
    <VStack divider={<StackDivider />} gap={6}>
      <GeneralSection form={form} schemas={schemas ?? []} />
      <CredentialSection form={form} />
      {generalFormError && <Alert variant="error" message={generalFormError} />}
    </VStack>
  );
};

const NewCsrConnectionModal = ({
  isOpen,
  onClose,
  onSuccess,
}: {
  isOpen: boolean;
  onClose: () => void;
  onSuccess: (newConnectionId: string) => void;
}) => {
  const { track } = useSegment();
  const [generalFormError, setGeneralFormError] = useState<
    string | undefined
  >();
  const [isPending, setIsPending] = useState(false);
  const form = useCsrConnectionForm();
  const [environment] = useAtom(currentEnvironmentState);
  const toast = useToast();

  const { formState, handleSubmit, setError } = form;

  const handleValidSubmit = async (state: CsrConnectionFormState) => {
    setGeneralFormError(undefined);
    setIsPending(true);
    try {
      assert(environment?.state === "enabled");
      const { error: createConnectionError, data: createConnectionData } =
        await createCsrConnection({
          params: {
            name: state.name,
            url: state.url,
            databaseName: state.schema.databaseName,
            schemaName: state.schema.name,
            username: state.username,
            password: getSecretFromOption(state.password),
            sslKey: state.useSsl
              ? getSecretFromOption(state.sslKey)
              : undefined,
            sslCertificate: state.useSsl
              ? getSecretFromOption(state.sslCertificate)
              : undefined,
            sslCertificateAuthority: state.useSsl
              ? getSecretFromOption(state.sslCertificateAuthority)
              : undefined,
          },
          environmentdHttpAddress: environment.httpAddress,
        });

      if (createConnectionError) {
        if (alreadyExistsError(createConnectionError.errorMessage)) {
          setError("name", {
            message: capitalizeSentence(createConnectionError.errorMessage),
          });
        } else {
          setGeneralFormError(createConnectionError.errorMessage);
        }
        return;
      }

      const { connectionId } = createConnectionData;

      toast({
        description: (
          <ObjectToastDescription
            name={state.name}
            message="created successfully"
          />
        ),
      });
      onSuccess(connectionId);
    } finally {
      setIsPending(false);
    }
  };

  return (
    <Modal isOpen={isOpen} onClose={onClose} size="xl">
      <ModalOverlay />
      <ModalContent>
        <chakra.form onSubmit={handleSubmit(handleValidSubmit)}>
          <ModalHeader textStyle="heading-sm">
            New Schema Registry Connection
          </ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            <NewCsrConnectionForm
              form={form}
              generalFormError={generalFormError}
            />
          </ModalBody>
          <ModalFooter>
            <HStack spacing="2">
              <Button variant="secondary" size="sm" onClick={onClose}>
                Cancel
              </Button>
              <Button
                type="submit"
                variant="primary"
                size="sm"
                isDisabled={!formState.isValid}
                isLoading={isPending}
                onClick={() => track("Create Schema Registry Clicked")}
              >
                Create connection
              </Button>
            </HStack>
          </ModalFooter>
        </chakra.form>
      </ModalContent>
    </Modal>
  );
};

export default NewCsrConnectionModal;
