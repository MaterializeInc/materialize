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
  FormControl,
  HStack,
  Input,
  Radio,
  RadioGroup,
  Switch,
  Text,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { useController, UseFormReturn } from "react-hook-form";

import { Connection } from "~/api/materialize/connection/useConnections";
import { Schema } from "~/api/materialize/schemaList";
import useSchemas from "~/api/materialize/useSchemas";
import Alert from "~/components/Alert";
import { DocsCallout, DocsLink } from "~/components/DocsCallout";
import ErrorBox from "~/components/ErrorBox";
import {
  FORM_CONTENT_WIDTH,
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
import { AwsAuroraLogoIcon } from "~/svg/AwsAuroraLogoIcon";
import AwsLogoIcon from "~/svg/AwsLogoIcon";
import { GcpLogoIcon } from "~/svg/GcpLogoIcon";
import { MsftLogoIcon } from "~/svg/MsftLogoIcon";
import MySqlLogoIcon from "~/svg/MySqlLogoIcon";
import PostgresLogoIcon from "~/svg/PostgresLogoIcon";
import SqlServerLogoIcon from "~/svg/SqlServerLogoIcon";

import { DatabaseTypeProp } from "./constants";
import { typeToLabel } from "./utils";

const SSL_MODE_DESCRIPTIONS = {
  require: "Always encrypt the connection.",
  "verify-ca":
    "Always encrypt the connection and verify the server is trusted.",
  "verify-full":
    "Always encrypt the connection, verify the server is trusted, and is the one intended.",
} as const;

// SQL Server uses different SSL mode names
const SQL_SERVER_SSL_MODE_DESCRIPTIONS = {
  required: "Always encrypt the connection.",
  verify: "Always encrypt the connection and verify the server is trusted.",
  verify_ca:
    "Always encrypt the connection and verify the certificate authority.",
} as const;

export type DatabaseConnectionFormState = {
  connectionAction: "existing" | "new";
  connection: Connection;

  name: string;
  schema: Schema;
  host: string;
  sourceDatabaseName: string;
  port: string;

  user: string;
  password?: SecretOption;
  sslMode:
    | keyof typeof SSL_MODE_DESCRIPTIONS
    | keyof typeof SQL_SERVER_SSL_MODE_DESCRIPTIONS;
  sslAuthentication: boolean;
  sslCertificate?: SecretOption;
  sslKey?: SecretOption;
  sslCertificateAuthority?: SecretOption;
};

const postgresConnectionDocs: DocsLink[] = [
  {
    label: "Amazon RDS",
    href: docUrls["/docs/ingest-data/postgres/amazon-rds/"],
    icon: <AwsLogoIcon height="4" width="4" />,
  },
  {
    label: "Amazon Aurora",
    href: docUrls["/docs/ingest-data/postgres/amazon-aurora/"],
    icon: <AwsAuroraLogoIcon height="4" width="4" />,
  },
  {
    label: "Azure DB",
    href: docUrls["/docs/ingest-data/postgres/azure-db/"],
    icon: <MsftLogoIcon height="4" width="4" />,
  },
  {
    label: "Google Cloud SQL",
    href: docUrls["/docs/ingest-data/postgres/cloud-sql/"],
    icon: <GcpLogoIcon height="4" width="4" />,
  },
  {
    label: "Self-hosted PostgreSQL",
    href: docUrls["/docs/ingest-data/postgres/self-hosted/"],
    icon: <PostgresLogoIcon height="4" width="4" />,
  },
];

const mySqlConnectionDocs: DocsLink[] = [
  {
    label: "Amazon RDS",
    href: docUrls["/docs/ingest-data/mysql/amazon-rds/"],
    icon: <AwsLogoIcon height="4" width="4" />,
  },
  {
    label: "Amazon Aurora",
    href: docUrls["/docs/ingest-data/mysql/amazon-aurora/"],
    icon: <AwsAuroraLogoIcon height="4" width="4" />,
  },
  {
    label: "Azure DB",
    href: docUrls["/docs/ingest-data/mysql/azure-db/"],
    icon: <MsftLogoIcon height="4" width="4" />,
  },
  {
    label: "Google Cloud SQL",
    href: docUrls["/docs/ingest-data/mysql/google-cloud-sql/"],
    icon: <GcpLogoIcon height="4" width="4" />,
  },
  {
    label: "Self-hosted MySQL",
    href: docUrls["/docs/ingest-data/mysql/self-hosted/"],
    icon: <MySqlLogoIcon height="4" width="4" />,
  },
];

const sqlServerConnectionDocs: DocsLink[] = [
  {
    label: "Self-hosted SQL Server",
    href: docUrls["/docs/ingest-data/sql-server/self-hosted/"],
    icon: <SqlServerLogoIcon height="4" width="4" />,
  },
];

type FormProp = {
  form: UseFormReturn<DatabaseConnectionFormState>;
};

const ActionSection = ({
  connections,
  databaseType,
  form,
}: {
  connections: Connection[];
} & DatabaseTypeProp &
  FormProp) => {
  const { control, formState, getValues, register, watch } = form;
  const { field: connectionField } = useController({
    control,
    name: "connection",
    rules: {
      required: getValues("connectionAction") === "existing",
    },
  });
  const label = typeToLabel(databaseType);
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
                  Use an existing {label} connection.
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
                  Create a new {label} connection.
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
          isRequired
        >
          <LabeledInput label="Existing connection">
            <SearchableSelect
              ariaLabel="Select connection"
              placeholder="Select one"
              options={[{ label: `${label} connection`, options: connections }]}
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
  databaseType,
}: { schemas: Schema[] } & DatabaseTypeProp & FormProp) => {
  const { control, formState, register } = form;
  const { field: schemaField } = useController({
    control,
    name: "schema",
    rules: { required: "Schema is required." },
  });
  return (
    <FormSection
      title="Configure your connection type"
      caption="Step 2"
      description={
        databaseType === "postgres" ? (
          <Text>
            Ensure logical replication is enabled and a publication is created
            in the upstream database.{" "}
            <TextLink
              href={
                docUrls["/docs/sql/create-source/postgres/"] +
                "#change-data-capture"
              }
              isExternal
            >
              Learn how.
            </TextLink>
          </Text>
        ) : databaseType === "mysql" ? (
          <Text>
            Ensure GTID-based binlog replication is enabled in the upstream
            database.{" "}
            <TextLink
              href={docUrls["/docs/sql/create-source/mysql/"]}
              isExternal
            >
              Learn how.
            </TextLink>
          </Text>
        ) : databaseType === "sql-server" ? (
          <Text>
            Ensure Change Data Capture (CDC) is enabled in the upstream
            database.{" "}
            <TextLink
              href={
                docUrls["/docs/sql/create-source/sql-server/"] +
                "#change-data-capture"
              }
              isExternal
            >
              Learn how.
            </TextLink>
          </Text>
        ) : null
      }
    >
      <FormControl isInvalid={!!formState.errors.name} isRequired>
        <LabeledInput
          label="Name"
          message="The name of the connection to your source database."
          error={formState.errors.name?.message}
        >
          <ObjectNameInput
            {...register("name", {
              required: "Connection name is required.",
            })}
            placeholder={`my_${databaseType}_connection`}
            variant={formState.errors.name ? "error" : "default"}
            autoFocus
          />
        </LabeledInput>
      </FormControl>
      <FormControl
        isInvalid={!!formState.errors.schema}
        data-testid="connection-schema"
        isRequired
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

const ConnectionSection = ({
  form,
  databaseType,
}: DatabaseTypeProp & FormProp) => {
  const { formState, register } = form;
  return (
    <FormSection title="Connection details" caption="Step 3">
      <HStack width="100%" spacing="4" alignItems="flex-start">
        <FormControl isInvalid={!!formState.errors.host} isRequired flex="3">
          <LabeledInput label="Host" error={formState.errors.host?.message}>
            <Input
              {...register("host", {
                required: "Host is required.",
              })}
              placeholder="db.us-east-1.rds.amazonaws.com"
              autoCorrect="off"
              size="sm"
              variant={formState.errors.host ? "error" : "default"}
            />
          </LabeledInput>
        </FormControl>
        <FormControl isInvalid={!!formState.errors.port} flex="2">
          <LabeledInput label="Port" error={formState.errors.port?.message}>
            <Input
              {...register("port")}
              placeholder={
                databaseType === "mysql"
                  ? "3306"
                  : databaseType === "sql-server"
                    ? "1433"
                    : "5432"
              }
              autoCorrect="off"
              size="sm"
              variant={formState.errors.port ? "error" : "default"}
              type="number"
            />
          </LabeledInput>
        </FormControl>
      </HStack>
      {(databaseType === "postgres" || databaseType === "sql-server") && (
        <FormControl
          isInvalid={!!formState.errors.sourceDatabaseName}
          isRequired
        >
          <LabeledInput
            label="Database"
            error={formState.errors.sourceDatabaseName?.message}
          >
            <Input
              {...register("sourceDatabaseName", {
                required:
                  (databaseType === "postgres" ||
                    databaseType === "sql-server") &&
                  "Database name is required.",
              })}
              placeholder="postgres"
              autoCorrect="off"
              size="sm"
              variant={
                formState.errors.sourceDatabaseName ? "error" : "default"
              }
            />
          </LabeledInput>
        </FormControl>
      )}
    </FormSection>
  );
};

const AuthenticationSection = ({
  form,
  databaseType,
}: DatabaseTypeProp & FormProp) => {
  const { control, formState, getValues, register, watch } = form;
  const watchSslAuthentication = watch("sslAuthentication");
  const watchSslCertificate = watch("sslCertificate");
  const watchSchema = watch("schema");
  const watchSslMode = watch("sslMode");

  const sslModeDescriptions =
    databaseType === "sql-server"
      ? SQL_SERVER_SSL_MODE_DESCRIPTIONS
      : SSL_MODE_DESCRIPTIONS;

  const advancedSslModes =
    databaseType === "sql-server"
      ? ["verify", "verify_ca"]
      : ["verify-ca", "verify-full"];
  const isAdvancedSslEnabled = advancedSslModes.includes(watchSslMode);

  const { field: secretField } = useController({
    control,
    name: "password",
  });
  const { field: sslCertificateField } = useController({
    control,
    name: "sslCertificate",
  });

  const { field: sslKeyField } = useController({
    control,
    name: "sslKey",
    rules: {
      required: !!watchSslCertificate,
    },
  });
  const { field: sslCertificateAuthorityField } = useController({
    control,
    name: "sslCertificateAuthority",
    rules: {
      required: isAdvancedSslEnabled,
    },
  });

  return (
    <FormSection title="Authentication" caption="Step 4">
      <FormControl isInvalid={!!formState.errors.user} isRequired>
        <LabeledInput label="User" error={formState.errors.user?.message}>
          <Input
            {...register("user", {
              required: "Database username is required.",
            })}
            placeholder="user"
            autoCorrect="off"
            size="sm"
            variant={formState.errors.user ? "error" : "default"}
          />
        </LabeledInput>
      </FormControl>
      <FormControl
        isInvalid={!!formState.errors.password}
        data-testid="authentication-password"
      >
        <LabeledInput
          label="Password"
          inputBoxProps={{ maxWidth: `${FORM_CONTENT_WIDTH}px` }}
          error={formState.errors.password?.message}
        >
          <SecretSelectionControl
            schema={watchSchema}
            selectField={secretField}
            variant={formState.errors.password ? "error" : "default"}
            selectProps={{
              menuPlacement: watchSslAuthentication ? "bottom" : "top",
              isClearable: true,
            }}
          />
        </LabeledInput>
      </FormControl>
      <FormControl>
        <HStack>
          <LabeledInput label="SSL Authentication" inputBoxProps={{ mt: 0 }}>
            <Switch {...register("sslAuthentication")} />
          </LabeledInput>
        </HStack>
      </FormControl>
      {watchSslAuthentication && (
        <>
          <FormControl>
            <LabeledInput
              label="SSL Mode"
              inputBoxProps={{ maxWidth: "unset" }}
            >
              <RadioGroup defaultValue={getValues("sslMode")}>
                <VStack alignItems="flex-start">
                  {Object.entries(sslModeDescriptions).map(
                    ([mode, description]) => {
                      return (
                        <Radio
                          key={mode}
                          value={mode}
                          inputProps={{ "aria-label": `SSL Mode: ${mode}` }}
                          variant="form"
                          {...register("sslMode")}
                        >
                          <Text textStyle="text-ui-med">{mode}</Text>
                          <Text textStyle="text-small">{description}</Text>
                        </Radio>
                      );
                    },
                  )}
                </VStack>
              </RadioGroup>
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
              tooltipMessage="Client SSL certificate in PEM format."
            >
              <SecretSelectionControl
                schema={watchSchema}
                selectField={sslCertificateField}
                variant={formState.errors.sslCertificate ? "error" : "default"}
                selectProps={{ menuPlacement: "top", isClearable: true }}
              />
            </LabeledInput>
          </FormControl>

          {watchSslCertificate && (
            <FormControl
              isInvalid={!!formState.errors.sslKey}
              data-testid="authentication-ssl-key"
              isRequired
            >
              <LabeledInput
                label="SSL Key"
                inputBoxProps={{ maxWidth: `${FORM_CONTENT_WIDTH}px` }}
                error={formState.errors.sslKey?.message}
                tooltipMessage="Client SSL key in PEM format."
              >
                <SecretSelectionControl
                  schema={watchSchema}
                  selectField={sslKeyField}
                  variant={formState.errors.sslKey ? "error" : "default"}
                  selectProps={{ menuPlacement: "top" }}
                />
              </LabeledInput>
            </FormControl>
          )}
          {isAdvancedSslEnabled && (
            <FormControl
              isInvalid={!!formState.errors.sslCertificateAuthority}
              data-testid="authentication-ssl-ca"
              isRequired
            >
              <LabeledInput
                label="SSL Certificate Authority"
                inputBoxProps={{ maxWidth: `${FORM_CONTENT_WIDTH}px` }}
                error={formState.errors.sslCertificateAuthority?.message}
                tooltipMessage="The certificate authority certificate in PEM format. Used for both SSL client and server authentication."
              >
                <SecretSelectionControl
                  schema={watchSchema}
                  selectField={sslCertificateAuthorityField}
                  variant={
                    formState.errors.sslCertificateAuthority
                      ? "error"
                      : "default"
                  }
                  selectProps={{ menuPlacement: "top" }}
                />
              </LabeledInput>
            </FormControl>
          )}
        </>
      )}
    </FormSection>
  );
};

const NewPostgresConnectionForm = ({
  form,
  generalFormError,
  connections,
  databaseType,
}: {
  generalFormError?: string;
  connections: Connection[] | null;
} & DatabaseTypeProp &
  FormProp) => {
  const { results: schemas, failedToLoad: schemasFailedToLoad } = useSchemas({
    filterByCreatePrivilege: true,
  });
  const { watch } = form;
  const watchConnectionAction = watch("connectionAction");

  const loadingError = schemasFailedToLoad;

  if (loadingError) {
    return <ErrorBox />;
  }

  const label = typeToLabel(databaseType);

  const aside = (
    <FormAsideList>
      <FormAsideItem title={`Need help connecting to ${label}?`}>
        <DocsCallout
          description={`Check out our step-by-step guides or reach out to the team for help with setting up your ${label} connection.`}
          docsLinks={
            databaseType === "mysql"
              ? mySqlConnectionDocs
              : databaseType === "sql-server"
                ? sqlServerConnectionDocs
                : postgresConnectionDocs
          }
          textProps={{ textStyle: "text-small" }}
        />
      </FormAsideItem>
      <FormAsideItem
        title="Contact support"
        href={docUrls["/docs/support/"]}
        target="_blank"
      >
        We&apos;re here to help! Chat with us if you feel stuck or have any
        questions.
      </FormAsideItem>
    </FormAsideList>
  );
  return (
    <FormContainer aside={aside}>
      <ActionSection
        form={form}
        databaseType={databaseType}
        connections={connections ?? []}
      />
      {watchConnectionAction === "new" && (
        <GeneralSection
          form={form}
          databaseType={databaseType}
          schemas={schemas ?? []}
        />
      )}
      {watchConnectionAction === "new" && (
        <ConnectionSection form={form} databaseType={databaseType} />
      )}
      {watchConnectionAction === "new" && (
        <AuthenticationSection form={form} databaseType={databaseType} />
      )}
      {generalFormError && (
        <Alert variant="error" minWidth="100%" message={generalFormError} />
      )}
    </FormContainer>
  );
};

export default NewPostgresConnectionForm;
