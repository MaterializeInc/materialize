// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CloseIcon } from "@chakra-ui/icons";
import {
  Box,
  Button,
  FormControl,
  HStack,
  Input,
  Radio,
  RadioGroup,
  Text,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import {
  FieldError,
  useController,
  useFieldArray,
  UseFormReturn,
} from "react-hook-form";
import { useSearchParams } from "react-router-dom";

import { SASL_MECHANISMS } from "~/api/materialize/connection/createKafkaConnection";
import { Connection } from "~/api/materialize/connection/useConnections";
import { Schema } from "~/api/materialize/schemaList";
import useSchemas from "~/api/materialize/useSchemas";
import Alert from "~/components/Alert";
import { DocsCallout, DocsLink } from "~/components/DocsCallout";
import ErrorBox from "~/components/ErrorBox";
import {
  FORM_CONTENT_WIDTH,
  FORM_CONTROL_WIDTH,
  FORM_LABEL_LINE_HEIGHT,
  FormAsideItem,
  FormAsideList,
  FormContainer,
  FormSection,
  LabeledInput,
} from "~/components/formComponentsV2";
import ObjectNameInput from "~/components/ObjectNameInput";
import SchemaSelect from "~/components/SchemaSelect";
import SearchableSelect, {
  SelectOption,
} from "~/components/SearchableSelect/SearchableSelect";
import SecretSelectionControl, {
  SecretOption,
} from "~/components/SecretSelectionControl";
import TextLink from "~/components/TextLink";
import docUrls from "~/mz-doc-urls.json";
import AwsLogoIcon from "~/svg/AwsLogoIcon";
import ConfluentLogoIcon from "~/svg/ConfluentLogoIcon";
import RedpandaLogoIcon from "~/svg/RedpandaLogoIcon";

type Broker = {
  hostPort: string;
  availabilityZone: string;
  port: string;
};

const AUTHENTICATION_MODES = ["sasl", "ssl", "none"] as const;
type AuthenticationMode = (typeof AUTHENTICATION_MODES)[number];

const AUTHENTICATION_DESCRIPTIONS: Record<
  AuthenticationMode,
  { label: string; description: string }
> = {
  sasl: {
    label: "SASL Authentication",
    description:
      "Authenticate using the Simple Authentication and Security Layer",
  },
  ssl: {
    label: "SSL Authentication",
    description: "Authenticate with a private-key/certificate pair.",
  },
  none: {
    label: "None",
    description: "The cluster does not use authentication",
  },
};

export type KafkaConnectionFormState = {
  connectionAction: "existing" | "new";
  connection: Connection;

  name: string;
  schema: Schema;

  networkSecurity: SecretOption;
  brokers: Broker[];

  authenticationMode: AuthenticationMode;
  saslMechanism: SelectOption;
  saslUsername: string;
  saslPassword: SecretOption;

  sslKey: SecretOption;
  sslCertificate: SecretOption;
  sslCertificateAuthority: SecretOption;
};

type FormProp = {
  form: UseFormReturn<KafkaConnectionFormState>;
};

const kafkaConnectionDocs: DocsLink[] = [
  {
    label: "Confluent",
    href: docUrls["/docs/ingest-data/kafka/confluent-cloud/"],
    icon: <ConfluentLogoIcon height="4" width="4" />,
  },
  {
    label: "Redpanda",
    href: docUrls["/docs/ingest-data/redpanda/redpanda-cloud/"],
    icon: <RedpandaLogoIcon height="4" width="4" />,
  },
  {
    label: "Amazon MSK",
    href: docUrls["/docs/ingest-data/kafka/amazon-msk/"],
    icon: <AwsLogoIcon height="4" width="4" />,
  },
];

function brokerErrorMessage(error: FieldError | undefined) {
  if (!error?.type) return error?.message;
  if (error.type === "pattern")
    return "Broker must not include special characters.";
  if (error.type === "required") return "Broker is required.";
  if (error.type === "unique") return "Brokers must be unique.";
}

const ActionSection = ({
  connections,
  form,
}: { connections: Connection[] } & FormProp) => {
  const { control, formState, getValues, register, watch } = form;
  const { field: connectionField } = useController({
    control,
    name: "connection",
    rules: {
      required: getValues("connectionAction") === "existing",
    },
  });

  const watchConnectionAction = watch("connectionAction");
  return (
    <FormSection
      title="Select a connection"
      caption="Step 1"
      description="Select an existing connection or opt to create a new one."
    >
      <FormControl>
        <Box width="100%">
          <RadioGroup
            defaultValue={getValues("connectionAction")}
            data-testid="connection-action"
          >
            <VStack alignItems="flex-start">
              <Radio
                value="existing"
                variant="form"
                inputProps={{ "aria-label": "Existing connection" }}
                {...register("connectionAction")}
              >
                <Text textStyle="text-ui-med">Existing</Text>
                <Text textStyle="text-small">
                  Use an existing Kafka connection.
                </Text>
              </Radio>
              <Radio
                value="new"
                variant="form"
                inputProps={{ "aria-label": "New connection" }}
                {...register("connectionAction")}
              >
                <Text textStyle="text-ui-med">New</Text>
                <Text textStyle="text-small">
                  Create a new Kafka connection.
                </Text>
              </Radio>
            </VStack>
          </RadioGroup>
        </Box>
      </FormControl>
      {watchConnectionAction === "existing" && (
        <FormControl
          isInvalid={!!formState.errors.connection}
          data-testid="connection-selection"
        >
          <LabeledInput label="Existing connection">
            <SearchableSelect
              ariaLabel="Select connection"
              placeholder="Select one"
              options={[{ label: "Kafka connection", options: connections }]}
              {...connectionField}
            />
          </LabeledInput>
        </FormControl>
      )}
    </FormSection>
  );
};

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
    <FormSection title="Configure your connection type" caption="Step 2">
      <FormControl isInvalid={!!formState.errors.name}>
        <LabeledInput
          label="Name"
          message="The name of the connection to your source cluster."
          error={formState.errors.name?.message}
        >
          <ObjectNameInput
            {...register("name", {
              required: "Connection name is required.",
            })}
            placeholder="my_kafka_connection"
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
    </FormSection>
  );
};

const BROKER_INPUT_LABEL_MARGIN = 2;

export const BrokerSection = ({ form }: FormProp) => {
  const { formState, control, register, watch } = form;
  const { append, fields, remove } = useFieldArray({
    control,
    name: "brokers",
  });
  const watchBrokers = watch("brokers");

  const hasEmptyFields = watchBrokers.some((b) => b.hostPort.trim() === "");

  return (
    <FormSection
      title="Configure brokers"
      caption="Step 3"
      description="Specify the Kafka brokers you wish to connect to Materialize."
    >
      <VStack alignItems="flex-start" gap={4} width={FORM_CONTENT_WIDTH}>
        {fields.map((field, index) => (
          <HStack key={field.id} alignItems="flex-start" gap={4} width="100%">
            <FormControl
              isInvalid={!!formState.errors.brokers?.[index]?.hostPort}
              maxWidth={FORM_CONTROL_WIDTH}
            >
              <LabeledInput
                label={`Broker ${index + 1}`}
                error={brokerErrorMessage(
                  formState.errors.brokers?.[index]?.hostPort,
                )}
                inputBoxProps={{ mt: BROKER_INPUT_LABEL_MARGIN }}
              >
                <Input
                  {...register(`brokers.${index}.hostPort` as const, {
                    required: "Broker is required.",
                    validate: {
                      unique: (value, { brokers }) => {
                        const count = brokers.filter(
                          ({ hostPort }) => hostPort === value,
                        ).length;

                        return count <= 1 || "Brokers must be unique.";
                      },
                    },
                  })}
                  aria-label={`Broker host ${index + 1}`}
                  placeholder={`broker${index + 1}:9092`}
                  autoCorrect="off"
                  spellCheck="false"
                  variant={
                    formState.errors.brokers?.[index]?.hostPort
                      ? "error"
                      : "default"
                  }
                />
              </LabeledInput>
            </FormControl>
            <Box
              alignSelf="flex-start"
              marginTop={BROKER_INPUT_LABEL_MARGIN + FORM_LABEL_LINE_HEIGHT}
            >
              <Button
                variant="borderless"
                height="8"
                minWidth="8"
                width="8"
                onClick={() => remove(index)}
                visibility={index > 0 ? "visible" : "hidden"}
                isDisabled={index === 0}
                title="Remove column"
              >
                <CloseIcon height="8px" width="8px" />
              </Button>
            </Box>
          </HStack>
        ))}
        <Box>
          <Button
            size="sm"
            onClick={() =>
              append({ hostPort: "", availabilityZone: "", port: "" })
            }
            mt={4}
            isDisabled={!!formState.errors.brokers || hasEmptyFields}
          >
            Add broker
          </Button>
        </Box>
      </VStack>
    </FormSection>
  );
};

const CredentialSection = ({ form }: FormProp) => {
  const { control, formState, getValues, register, watch } = form;
  const watchSchema = watch("schema");
  const watchAuthenticationMode = watch("authenticationMode");
  const { field: saslMechanismField } = useController({
    control,
    name: "saslMechanism",
  });
  const { field: saslPasswordField } = useController({
    control,
    name: "saslPassword",
    rules: {
      required: watchAuthenticationMode === "sasl" && "Password is required.",
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
      required: watchAuthenticationMode === "ssl",
    },
  });
  const { field: sslCertificateField } = useController({
    control,
    name: "sslCertificate",
    rules: {
      required: watchAuthenticationMode === "ssl",
    },
  });

  return (
    <FormSection title="Provide your Kafka credentials" caption="Step 4">
      <FormControl>
        <LabeledInput
          label="Authentication method"
          inputBoxProps={{ maxWidth: "unset" }}
        >
          <RadioGroup defaultValue={getValues("authenticationMode")}>
            <VStack alignItems="flex-start">
              {AUTHENTICATION_MODES.map((mode) => (
                <Radio
                  key={mode}
                  value={mode}
                  inputProps={{ "aria-label": `Authentication Mode: ${mode}` }}
                  variant="form"
                  {...register("authenticationMode")}
                >
                  <Text textStyle="text-ui-med">
                    {AUTHENTICATION_DESCRIPTIONS[mode].label}
                  </Text>
                  <Text textStyle="text-small">
                    {AUTHENTICATION_DESCRIPTIONS[mode].description}
                  </Text>
                </Radio>
              ))}
            </VStack>
          </RadioGroup>
        </LabeledInput>
      </FormControl>
      {watchAuthenticationMode === "sasl" && (
        <>
          <FormControl data-testid="sasl-mechanism-selection">
            <LabeledInput
              label="Mechanism"
              message={
                <>
                  Choose a SASL mechanism for authentication. To learn more
                  about the options,{" "}
                  <TextLink
                    href={docUrls["/docs/sql/create-connection/"]}
                    target="_blank"
                    rel="noopener"
                  >
                    view the documentation
                  </TextLink>{" "}
                  on SASL mechanisms.
                </>
              }
            >
              <SearchableSelect
                ariaLabel="Select SASL mechanism"
                options={Object.entries(SASL_MECHANISMS).map(([k, v]) => ({
                  id: k,
                  name: v,
                }))}
                isSearchable={false}
                {...saslMechanismField}
                variant={formState.errors.saslMechanism ? "error" : "default"}
              />
            </LabeledInput>
          </FormControl>
          <FormControl isInvalid={!!formState.errors.saslUsername}>
            <LabeledInput
              label="Username"
              error={formState.errors.saslUsername?.message}
            >
              <Input
                {...register("saslUsername", {
                  required: "Database username is required.",
                })}
                placeholder="user"
                autoCorrect="off"
                size="sm"
                variant={formState.errors.saslUsername ? "error" : "default"}
              />
            </LabeledInput>
          </FormControl>
          <FormControl
            isInvalid={!!formState.errors.saslPassword}
            data-testid="sasl-password"
          >
            <LabeledInput
              label="Password"
              inputBoxProps={{ maxWidth: `${FORM_CONTENT_WIDTH}px` }}
              error={formState.errors.saslPassword?.message}
            >
              <SecretSelectionControl
                schema={watchSchema}
                selectField={saslPasswordField}
                variant={formState.errors.saslPassword ? "error" : "default"}
                selectProps={{ menuPlacement: "top" }}
              />
            </LabeledInput>
          </FormControl>
        </>
      )}
      {watchAuthenticationMode === "ssl" && (
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
                selectProps={{ menuPlacement: "top" }}
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
                selectProps={{ menuPlacement: "top" }}
              />
            </LabeledInput>
          </FormControl>
        </>
      )}
      {watchAuthenticationMode !== "none" && (
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
              variant={
                formState.errors.sslCertificateAuthority ? "error" : "default"
              }
              selectProps={{ menuPlacement: "top", isClearable: true }}
            />
          </LabeledInput>
        </FormControl>
      )}
    </FormSection>
  );
};

const Aside = ({
  kafkaConnectionType,
}: {
  kafkaConnectionType: string | null;
}) => (
  <FormAsideList>
    {kafkaConnectionType === "confluent" ? (
      <FormAsideItem
        title="Need help connecting to Confluent Cloud?"
        href={docUrls["/docs/ingest-data/kafka/confluent-cloud/"]}
      >
        Check out our step-by-step guide or reach out to the team for help with
        setting up your Confluent Cloud Kafka connection.
      </FormAsideItem>
    ) : (
      <FormAsideItem title="Need help connecting to Kafka?">
        <DocsCallout
          description="Check out our step-by-step guides or reach out to the team for help with setting up your Kafka connection."
          docsLinks={kafkaConnectionDocs}
          textProps={{ textStyle: "text-small" }}
        />
      </FormAsideItem>
    )}
    <FormAsideItem
      title="Contact support"
      href={docUrls["/docs/support/"]}
      target="_blank"
    >
      We’re here to help! Chat with us if you feel stuck or have any questions.
    </FormAsideItem>
  </FormAsideList>
);

const NewKafkaConnectionForm = ({
  connections,
  form,
  generalFormError,
}: {
  connections: Connection[] | null;
  generalFormError?: string;
} & FormProp) => {
  const [searchParams] = useSearchParams();
  const kafkaConnectionType = searchParams.get("kafkaConnectionType");
  const { results: schemas, failedToLoad: schemasFailedToLoad } = useSchemas({
    filterByCreatePrivilege: true,
  });
  const { watch } = form;
  const watchConnectionAction = watch("connectionAction");
  const loadingError = schemasFailedToLoad;

  if (loadingError) {
    return <ErrorBox />;
  }
  return (
    <FormContainer aside={<Aside kafkaConnectionType={kafkaConnectionType} />}>
      <ActionSection form={form} connections={connections ?? []} />
      {watchConnectionAction === "new" && (
        <GeneralSection form={form} schemas={schemas ?? []} />
      )}
      {watchConnectionAction === "new" && <BrokerSection form={form} />}
      {watchConnectionAction === "new" && <CredentialSection form={form} />}
      {generalFormError && (
        <Alert variant="error" minWidth="100%" message={generalFormError} />
      )}
    </FormContainer>
  );
};

export default NewKafkaConnectionForm;
