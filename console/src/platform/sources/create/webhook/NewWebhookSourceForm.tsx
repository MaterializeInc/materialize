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
  BoxProps,
  Button,
  Checkbox,
  FormControl,
  FormErrorMessage,
  HStack,
  Input,
  Radio,
  RadioGroup,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { forwardRef, useEffect, useRef } from "react";
import {
  ControllerRenderProps,
  FieldError,
  useController,
  useFieldArray,
  UseFormReturn,
} from "react-hook-form";
import scrollIntoView from "scroll-into-view-if-needed";

import useConnectorClusters, {
  Cluster,
} from "~/api/materialize/cluster/useConnectorClusters";
import { Schema } from "~/api/materialize/schemaList";
import {
  BodyFormat,
  HeaderBehavior,
} from "~/api/materialize/source/createWebhookSourceStatement";
import useSchemas from "~/api/materialize/useSchemas";
import { HTTP_HEADER_FIELD_NAME_REGEX } from "~/api/materialize/validation";
import Alert from "~/components/Alert";
import CommandBlock, { CodeMirrorProvider } from "~/components/CommandBlock";
import {
  useGetView,
  useViewDispatch,
} from "~/components/CommandBlock/provider";
import ErrorBox from "~/components/ErrorBox";
import {
  FORM_CONTENT_WIDTH,
  FORM_LABEL_LINE_HEIGHT,
  FormAsideItem,
  FormAsideList,
  FormContainer,
  FormSection,
  LabeledInput,
} from "~/components/formComponentsV2";
import ObjectNameInput from "~/components/ObjectNameInput";
import SchemaSelect from "~/components/SchemaSelect";
import SearchableSelect from "~/components/SearchableSelect/SearchableSelect";
import SecretSelectionControl, {
  SecretOption,
} from "~/components/SecretSelectionControl";
import TextLink from "~/components/TextLink";
import docUrls from "~/mz-doc-urls.json";
import { MaterializeTheme } from "~/theme";

const BODY_FORMATS: Record<BodyFormat, { label: string; description: string }> =
  {
    bytes: {
      label: "Bytes",
      description:
        "Does no parsing of the request, and stores the body of a request as it was received.",
    },
    json: {
      label: "JSON",
      description: "Parses the body of a request as JSON.",
    },
    json_array: {
      label: "JSON Array",
      description:
        "Parses the body of a request as a list of JSON objects, automatically expanding the list of objects to individual rows.",
    },
    text: {
      label: "Text",
      description: "Parses the body of a request as UTF-8 text.",
    },
  } as const;

const HEADER_BEHAVIOR: Record<
  HeaderBehavior,
  { label: string; description: string }
> = {
  all: {
    label: "Include all headers",
    description:
      "Include a column named 'headers' containing the headers of the request.",
  },
  include_specific: {
    label: "Include specific headers",
    description: "Include columns for specific headers of the request.",
  },
  none: {
    label: "Do not include headers",
    description: "",
  },
} as const;

const REQUEST_VALIDATION_BEHAVIOR = {
  validate: {
    label: "Validate incoming requests",
  },
  noValidation: {
    label: "Do not validate incoming requests",
  },
} as const;

export type WebhookFormState = {
  name: string;
  schema: Schema | null;
  cluster: Cluster | null;
  bodyFormat: BodyFormat;
  headerBehavior: HeaderBehavior;
  headerColumns: Array<{ header: string; column: string }>;
  requestValidationBehavior: keyof typeof REQUEST_VALIDATION_BEHAVIOR;
  noValidationConfirmed: boolean;
  requestValidationSecret: SecretOption;
  requestCheckStatement: string;
};

function headerNameErrorMessage(error: FieldError | undefined) {
  if (!error?.type) return error?.message;
  if (error.type === "pattern")
    return "Header name must not include special characters.";
  if (error.type === "required") return "Header name is required.";
  if (error.type === "unique") return "Header names must be unique.";
}

function columnNameErrorMessage(error: FieldError | undefined) {
  if (!error?.type) return error?.message;
  if (error.type === "pattern")
    return "Column name must not include special characters.";
  if (error.type === "required") return "Column name is required.";
  if (error.type === "unique") return "Column names must be unique.";
}

type CustomCheckControlProps = {
  secret: SecretOption;
  boxProps?: BoxProps;
  error: FieldError | undefined;
} & ControllerRenderProps<WebhookFormState, "requestCheckStatement">;

function getCheckStatement(secret: SecretOption): string {
  return `
CHECK (
  WITH (
    -- include the objects to reference
    HEADERS,
    BODY
      AS request_body,
    SECRET "${secret.databaseName}"."${secret.schemaName}"."${secret.name}"
      AS validation_secret
  )
  constant_time_eq(
    -- write your custom statement here
    -- reference the secret with "validation_secret"
  )
)`.trim();
}

const CustomCheckControl = forwardRef<HTMLDivElement, CustomCheckControlProps>(
  ({ secret, onBlur, onChange, value, boxProps, error }, ref) => {
    const { colors, radii } = useTheme<MaterializeTheme>();
    const getView = useGetView();
    const viewDispatch = useViewDispatch();

    useEffect(() => {
      // react-hook-form won't advertise errors with this field until the
      // first time it loses focus. Since this is the last field in the form,
      // and it's starting populated, fire off a dummy blur event on the
      // first change so it's validated from the jump.
      onBlur();
    });

    const handleChange = (newVal: string) => {
      onChange(newVal);
    };

    useEffect(() => {
      const view = getView();
      if (!view) return;
      if (view.state.doc.toString() === value) return;
      viewDispatch({
        changes: { from: 0, to: view.state.doc.length, insert: value },
      });
    }, [getView, viewDispatch, value]);

    const previousSecret = useRef<SecretOption | null>(null);
    const hasCheckStatement = value.trim().length !== 0;
    useEffect(() => {
      if (
        // If first load with an empty check statement...
        (!hasCheckStatement && !previousSecret.current) ||
        // Or if we're reacting to a secret input change...
        (previousSecret.current && secret.id !== previousSecret.current.id)
      ) {
        // ...update the check statement.
        previousSecret.current = secret;
        onChange(getCheckStatement(secret));
        // If first load with a prepopulated check statement (e.g., going back)
      } else if (hasCheckStatement && !previousSecret.current) {
        // ...associate the current secret with the check statement.
        previousSecret.current = secret;
      }
    }, [onChange, secret, hasCheckStatement]);

    return (
      <VStack {...boxProps}>
        <Alert
          variant="info"
          showLabel={false}
          message={
            <>
              <TextLink
                href={`${docUrls["/docs/sql/create-source/webhook/"]}#creating-a-source`}
                target="_blank"
              >
                Reference our documentation
              </TextLink>{" "}
              to get help configuring your source&apos;s check statement to
              properly validate incoming requests.
            </>
          }
        />
        <FormControl isInvalid={!!error}>
          <Box
            width="100%"
            minHeight="200px"
            padding={4}
            background={colors.background.secondary}
            border="1px solid"
            borderColor={error ? colors.border.error : colors.border.secondary}
            borderRadius={radii.lg}
          >
            <CommandBlock
              onChange={handleChange}
              ref={ref}
              containerProps={{
                width: "100%",
                height: "100%",
                overflow: "auto",
                sx: {
                  // Necessary to ensure the sticky gutter doesn't become
                  // unreadable when horizontally scrolling the editor.
                  ".cm-gutters": {
                    backgroundColor: colors.background.secondary,
                  },
                },
              }}
              lineNumbers
            />
          </Box>
          {error && <FormErrorMessage>{error.message}</FormErrorMessage>}
        </FormControl>
      </VStack>
    );
  },
);

const ConfigSection = ({
  clusters,
  schemas,
  form,
}: {
  clusters: Cluster[];
  schemas: Schema[];
  form: UseFormReturn<WebhookFormState>;
}) => {
  const { control, formState, register } = form;
  const { field: schemaField } = useController({
    control,
    name: "schema",
    rules: {
      required: "Schema is required.",
    },
  });
  const { field: clusterField } = useController({
    control,
    name: "cluster",
    rules: {
      required: "Cluster is required.",
    },
  });
  return (
    <FormSection title="Configure your webhook source" caption="Step 1">
      <FormControl isInvalid={!!formState.errors.name}>
        <LabeledInput
          label="Name"
          message="The name of the source you will create in your database."
          error={formState.errors.name?.message}
        >
          <ObjectNameInput
            {...register("name", {
              required: "Source name is required.",
            })}
            placeholder="my_webhook_source"
            variant={formState.errors.name ? "error" : "default"}
            autoFocus
          />
        </LabeledInput>
      </FormControl>
      <FormControl isInvalid={!!formState.errors.schema}>
        <LabeledInput
          label="Schema"
          message="The database and schema where this source will be created."
          error={formState.errors.schema?.message}
        >
          <SchemaSelect
            {...schemaField}
            schemas={schemas}
            variant={formState.errors.schema ? "error" : "default"}
          />
        </LabeledInput>
      </FormControl>
      <FormControl isInvalid={!!formState.errors.cluster}>
        <LabeledInput
          label="Cluster"
          message="The compute cluster that will power this source. Sources can only be created on clusters with a single replica."
          error={formState.errors.cluster?.message}
        >
          <SearchableSelect
            ariaLabel="Select cluster"
            placeholder="Select one"
            {...clusterField}
            options={[{ label: "Select cluster", options: clusters }]}
            variant={formState.errors.cluster ? "error" : "default"}
          />
        </LabeledInput>
      </FormControl>
    </FormSection>
  );
};

const HEADER_INPUT_LABEL_MARGIN = 2;

const HeaderColumnArrayInput = ({
  form,
}: {
  form: UseFormReturn<WebhookFormState>;
}) => {
  const { formState, control, getValues, register, trigger, watch } = form;
  const { append, fields, remove } = useFieldArray({
    control,
    name: "headerColumns",
  });

  const watchedFields = watch("headerColumns");

  const hasEmptyFields = watchedFields.some(
    (f) => f.header.trim() === "" || f.column.trim() === "",
  );

  return (
    <VStack mt={4} alignItems="flex-start" gap={4}>
      {fields.map((field, index) => (
        <HStack key={field.id} alignItems="flex-start" gap={4}>
          <FormControl isInvalid={!!formState.errors.headerColumns?.[index]}>
            <LabeledInput
              label="Header name"
              error={headerNameErrorMessage(
                formState.errors.headerColumns?.[index]?.header,
              )}
              inputBoxProps={{ mt: HEADER_INPUT_LABEL_MARGIN }}
            >
              <Input
                {...register(`headerColumns.${index}.header` as const, {
                  onChange: () => {
                    for (let i = 0; i < fields.length; i++) {
                      trigger(`headerColumns.${i}.header`);
                    }
                  },
                  pattern: HTTP_HEADER_FIELD_NAME_REGEX,
                  validate: {
                    unique: (value) => {
                      const count = getValues()
                        .headerColumns.map((row) => row.header)
                        .filter((header) => header === value).length;
                      return count <= 1;
                    },
                    required: (value) => {
                      const shouldValidate =
                        getValues("headerBehavior") === "include_specific";
                      if (shouldValidate) {
                        return value.trim() !== "";
                      }
                      return true;
                    },
                  },
                })}
                placeholder="header"
                autoCorrect="off"
                spellCheck="false"
                size="sm"
                variant={
                  formState.errors.headerColumns?.[index]?.header
                    ? "error"
                    : "default"
                }
              />
            </LabeledInput>
          </FormControl>
          <FormControl
            isInvalid={!!formState.errors.headerColumns?.[index]?.column}
          >
            <LabeledInput
              label="Column name"
              error={columnNameErrorMessage(
                formState.errors.headerColumns?.[index]?.column,
              )}
              inputBoxProps={{ mt: HEADER_INPUT_LABEL_MARGIN }}
            >
              <Input
                {...register(`headerColumns.${index}.column` as const, {
                  onChange: () => {
                    for (let i = 0; i < fields.length; i++) {
                      trigger(`headerColumns.${i}.column`);
                    }
                  },
                  required: true,
                  validate: {
                    unique: (value) => {
                      const count = getValues()
                        .headerColumns.map((row) => row.column)
                        .filter((column) => column === value).length;
                      return count <= 1;
                    },
                  },
                })}
                placeholder="column"
                autoCorrect="off"
                spellCheck="false"
                size="sm"
                variant={
                  formState.errors.headerColumns?.[index]?.column
                    ? "error"
                    : "default"
                }
              />
            </LabeledInput>
          </FormControl>
          <Box
            alignSelf="flex-start"
            marginTop={HEADER_INPUT_LABEL_MARGIN + FORM_LABEL_LINE_HEIGHT}
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
          onClick={() => append({ column: "", header: "" })}
          mt={4}
          isDisabled={!!formState.errors.headerColumns || hasEmptyFields}
        >
          Add column
        </Button>
      </Box>
    </VStack>
  );
};

const PayloadSection = ({
  form,
}: {
  form: UseFormReturn<WebhookFormState>;
}) => {
  const { formState, getValues, register, watch } = form;
  const watchHeaderBehavior = watch("headerBehavior");
  return (
    <FormSection title="Configure your payload" caption="Step 2">
      <FormControl isInvalid={!!formState.errors.bodyFormat}>
        <LabeledInput
          label="Body format"
          message="Specify the format of the body of the payload."
          error={formState.errors.bodyFormat?.message}
          inputBoxProps={{ maxWidth: "unset" }}
        >
          <RadioGroup defaultValue={getValues("bodyFormat")}>
            <VStack alignItems="flex-start">
              {Object.entries(BODY_FORMATS).map(([formatType, format]) => (
                <Radio
                  key={formatType}
                  {...register("bodyFormat")}
                  value={formatType}
                  variant="form"
                >
                  <Text textStyle="text-ui-med">{format.label}</Text>
                  <Text textStyle="text-small">{format.description}</Text>
                </Radio>
              ))}
            </VStack>
          </RadioGroup>
        </LabeledInput>
      </FormControl>
      <FormControl isInvalid={!!formState.errors.headerBehavior}>
        <LabeledInput
          label="Include headers"
          message="Specify if you would like to include the value of headers for each request."
          error={formState.errors.headerBehavior?.message}
          inputBoxProps={{ maxWidth: "unset" }}
        >
          <RadioGroup defaultValue={getValues("headerBehavior")}>
            <VStack alignItems="flex-start">
              {Object.entries(HEADER_BEHAVIOR).map(([formatType, format]) => (
                <Radio
                  key={formatType}
                  {...register("headerBehavior")}
                  value={formatType}
                  variant="form"
                >
                  <Text textStyle="text-ui-med">{format.label}</Text>
                  <Text textStyle="text-small">{format.description}</Text>
                </Radio>
              ))}
            </VStack>
          </RadioGroup>
          {watchHeaderBehavior === "include_specific" && (
            <HeaderColumnArrayInput form={form} />
          )}
        </LabeledInput>
      </FormControl>
    </FormSection>
  );
};

const ValidationSection = ({
  form,
}: {
  form: UseFormReturn<WebhookFormState>;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { control, formState, getValues, register, watch } = form;

  const { field: secretField } = useController({
    control,
    name: "requestValidationSecret",
    rules: {
      validate: (value) => {
        const shouldValidate =
          getValues("requestValidationBehavior") === "validate";
        if (shouldValidate) {
          return !!value;
        }
        return true;
      },
    },
  });

  const { field: checkField } = useController({
    control,
    name: "requestCheckStatement",
    rules: {
      validate: (value) => {
        const secret = getValues("requestValidationSecret");
        const shouldValidate =
          getValues("requestValidationBehavior") === "validate" && !!secret;
        if (shouldValidate) {
          const defaultValue = getCheckStatement(secret);
          if (!value || value.trim() === "") {
            return "The check statement cannot be empty.";
          } else if (value === defaultValue) {
            return "The check statement must be modified.";
          }
        }
        return true;
      },
    },
  });

  const watchValidationBehavior = watch("requestValidationBehavior");
  const watchSchema = watch("schema");
  const watchSecret = watch("requestValidationSecret");

  const description =
    "It's common for applications using webhooks to provide a method for\
    validating a request is legitimate. You can specify an expression to\
    do this validation for your webhook source using the\
    CHECK clause.";
  const noValidationMessage =
    "Materialize strongly recommends adding request validation to your\
    sources. By choosing not to validate requests, you will be allowing\
    anyone with the URL to write data to your webhook source.";
  return (
    <FormSection
      title="Request validation"
      caption="Step 3"
      description={description}
      gap={4}
    >
      <FormControl isInvalid={!!formState.errors.requestValidationBehavior}>
        <RadioGroup defaultValue={getValues("requestValidationBehavior")}>
          <VStack alignItems="flex-start">
            {Object.entries(REQUEST_VALIDATION_BEHAVIOR).map(
              ([formatType, format]) => (
                <Radio
                  key={formatType}
                  {...register("requestValidationBehavior")}
                  value={formatType}
                  variant="form"
                >
                  <Text textStyle="text-ui-med">{format.label}</Text>
                </Radio>
              ),
            )}
          </VStack>
        </RadioGroup>
      </FormControl>
      {watchValidationBehavior === "validate" && (
        <FormControl
          isInvalid={!!formState.errors.requestValidationSecret}
          isDisabled={!watchSchema}
        >
          <LabeledInput
            label="Include your shared password"
            message="Webhook authentication relies on a shared password to confirm incoming requests. Select or create a secret in Materialize for that password. Make sure you also include it in the request headers in your webhook service."
            inputBoxProps={{ maxWidth: `${FORM_CONTENT_WIDTH}px` }}
          >
            {watchSchema ? (
              <SecretSelectionControl
                schema={watchSchema}
                selectField={secretField}
                variant={
                  formState.errors.requestValidationSecret ? "error" : "default"
                }
                selectProps={{ menuPlacement: "top" }}
              />
            ) : (
              <Alert
                variant="warning"
                showLabel={false}
                message="You must select a schema in order to specify a secret."
              />
            )}
            {watchSecret && (
              <CustomCheckControl
                secret={watchSecret}
                boxProps={{ mt: 4 }}
                error={formState.errors.requestCheckStatement}
                {...checkField}
              />
            )}
          </LabeledInput>
        </FormControl>
      )}
      {watchValidationBehavior === "noValidation" && (
        <Alert
          variant="warning"
          showLabel={false}
          message={noValidationMessage}
          mt={4}
        >
          <Box px="6" mt="-2" pb="4" background={colors.background.warn}>
            <Checkbox
              {...register("noValidationConfirmed", {
                validate: (value) => {
                  const shouldValidate =
                    getValues("requestValidationBehavior") === "noValidation";
                  if (shouldValidate) {
                    return value;
                  }
                  return true;
                },
              })}
            >
              <Text textStyle="text-ui-med">
                Create without request validation
              </Text>
            </Checkbox>
          </Box>
        </Alert>
      )}
    </FormSection>
  );
};

const NewWebhookSourceForm = ({
  form,
  generalFormError,
}: {
  form: UseFormReturn<WebhookFormState>;
  generalFormError: string | undefined;
}) => {
  const { results: schemas, failedToLoad: schemasFailedToLoad } = useSchemas({
    filterByCreatePrivilege: true,
  });
  const { results: clusters, failedToLoad: clustersFailedToLoad } =
    useConnectorClusters();

  const alertRef = useRef();

  const loadingError = schemasFailedToLoad || clustersFailedToLoad;

  useEffect(() => {
    if (alertRef.current && generalFormError && generalFormError !== "") {
      scrollIntoView(alertRef.current);
    }
  }, [generalFormError]);

  if (loadingError) {
    return <ErrorBox />;
  }

  const aside = (
    <FormAsideList>
      <FormAsideItem
        title="Read the docs"
        href={docUrls["/docs/sql/create-source/webhook/"]}
        target="_blank"
      >
        Not sure how what all these options mean? Check out the docs for webhook
        sources for more in-depth explanations and examples.
      </FormAsideItem>
      <FormAsideItem
        title="Contact support"
        href={docUrls["/docs/support/"]}
        target="_blank"
      >
        We’re here to help! Chat with us if you feel stuck or have any
        questions.
      </FormAsideItem>
    </FormAsideList>
  );
  return (
    <CodeMirrorProvider>
      <FormContainer aside={aside}>
        <ConfigSection
          form={form}
          schemas={schemas ?? []}
          clusters={clusters ?? []}
        />
        <PayloadSection form={form} />
        <ValidationSection form={form} />
        {generalFormError && (
          <Alert
            ref={alertRef}
            variant="error"
            message={generalFormError}
            width="100%"
          />
        )}
      </FormContainer>
    </CodeMirrorProvider>
  );
};

export default NewWebhookSourceForm;
