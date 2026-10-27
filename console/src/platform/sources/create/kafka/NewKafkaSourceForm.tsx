// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { FormControl } from "@chakra-ui/react";
import React from "react";
import { useController, UseFormReturn } from "react-hook-form";

import useConnectorClusters from "~/api/materialize/cluster/useConnectorClusters";
import { Connection } from "~/api/materialize/connection/useConnections";
import { Schema } from "~/api/materialize/schemaList";
import { formatOptions } from "~/api/materialize/source/createKafkaSourceStatement";
import {
  ENVELOPE_OPTIONS,
  ENVELOPE_OPTIONS_BY_FORMAT,
} from "~/api/materialize/source/createKafkaSourceStatement";
import { Cluster } from "~/api/materialize/types";
import useSchemas from "~/api/materialize/useSchemas";
import Alert from "~/components/Alert";
import ErrorBox from "~/components/ErrorBox";
import {
  FORM_CONTENT_WIDTH,
  FormContainer,
  FormSection,
  LabeledInput,
} from "~/components/formComponentsV2";
import ObjectNameInput from "~/components/ObjectNameInput";
import SchemaSelect from "~/components/SchemaSelect";
import SearchableSelect from "~/components/SearchableSelect/SearchableSelect";

import CsrSelectionControl from "./CsrSelectionControl";

export type KafkaSourceFormState = {
  name: string;
  schema: Schema | null;
  cluster: Cluster | null;
  topic: string;
  keyFormat: (typeof formatOptions)[number];
  valueFormat: (typeof formatOptions)[number];
  csrConnection: Connection | null;
  envelope: (typeof ENVELOPE_OPTIONS)[number];
  useSchemaRegistry: boolean;
};

type FormProp = {
  form: UseFormReturn<KafkaSourceFormState>;
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
    <FormSection title="General" caption="Step 1">
      <FormControl isInvalid={!!formState.errors.name}>
        <LabeledInput label="Name" error={formState.errors.name?.message}>
          <ObjectNameInput
            {...register("name", {
              required: "Source name is required.",
            })}
            placeholder="my_kafka_source"
            variant={formState.errors.name ? "error" : "default"}
            autoFocus
          />
        </LabeledInput>
      </FormControl>
      <FormControl
        isInvalid={!!formState.errors.schema}
        data-testid="source-schema"
      >
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
    </FormSection>
  );
};

const ClusterSection = ({
  form,
  clusters,
}: { clusters: Cluster[] } & FormProp) => {
  const { control, formState } = form;

  const { field: clusterField } = useController({
    control,
    name: "cluster",
    rules: {
      required: "Cluster is required.",
    },
  });
  return (
    <FormSection title="Cluster" caption="Step 2">
      <FormControl
        isInvalid={!!formState.errors.cluster}
        data-testid="cluster-selector"
      >
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

const ConfigurationSection = ({ form }: FormProp) => {
  const { control, formState, register, watch } = form;
  const { field: keyFormatField } = useController({
    control,
    name: "keyFormat",
    rules: {
      required: "Format is required.",
    },
  });
  const { field: valueFormatField } = useController({
    control,
    name: "valueFormat",
    rules: {
      required: "Format is required.",
    },
  });
  const watchKeyFormat = watch("keyFormat");
  const watchValueFormat = watch("valueFormat");
  const requiresSchemaRegistry =
    watchKeyFormat.id === "avro" ||
    watchKeyFormat.id === "protobuf" ||
    watchValueFormat.id === "avro" ||
    watchValueFormat.id === "protobuf";
  const { field: csrConnectionField } = useController({
    control,
    name: "csrConnection",
    rules: {
      validate: {
        required: (value) => {
          if (!value && requiresSchemaRegistry) {
            return "Schema registry connection is required.";
          }
        },
      },
    },
  });
  const { field: envelopeField } = useController({
    control,
    name: "envelope",
    rules: {
      required: "Envelope is required.",
    },
  });

  return (
    <FormSection title="Configure source options" caption="Step 3">
      <FormControl isInvalid={!!formState.errors.topic}>
        <LabeledInput
          label="Topic"
          message="The Kafka topic you would like Materialize to ingest."
          error={formState.errors.topic?.message}
        >
          <ObjectNameInput
            {...register("topic", {
              required: "Topic is required.",
            })}
            placeholder="my_topic"
            autoCorrect="off"
            variant={formState.errors.topic ? "error" : "default"}
          />
        </LabeledInput>
      </FormControl>
      <FormControl
        isInvalid={!!formState.errors.keyFormat}
        data-testid="key-format-selector"
      >
        <LabeledInput
          label="Key Format"
          message="The key format for your source."
          error={formState.errors.keyFormat?.message}
        >
          <SearchableSelect
            ariaLabel="Select key format"
            placeholder="Select one"
            {...keyFormatField}
            options={[
              {
                label: "Select format",
                options: formatOptions ?? [],
              },
            ]}
            menuPlacement="top"
            variant={formState.errors.keyFormat ? "error" : "default"}
          />
        </LabeledInput>
      </FormControl>
      <FormControl
        isInvalid={!!formState.errors.valueFormat}
        data-testid="value-format-selector"
      >
        <LabeledInput
          label="Value format"
          message="The value format for your source."
          error={formState.errors.valueFormat?.message}
        >
          <SearchableSelect
            ariaLabel="Select value format"
            placeholder="Select one"
            {...valueFormatField}
            options={[
              {
                label: "Select format",
                options: formatOptions ?? [],
              },
            ]}
            menuPlacement="top"
            variant={formState.errors.valueFormat ? "error" : "default"}
          />
        </LabeledInput>
      </FormControl>
      <FormControl
        isInvalid={!!formState.errors.envelope}
        data-testid="envelope-selector"
      >
        <LabeledInput
          label="Envelope"
          message="How Materialize should interpret the incoming records."
          error={formState.errors.envelope?.message}
        >
          <SearchableSelect
            ariaLabel="Select envelope"
            placeholder="Select one"
            {...envelopeField}
            options={[
              {
                label: "Select envelope",
                options: ENVELOPE_OPTIONS_BY_FORMAT[watchKeyFormat?.id],
              },
            ]}
            menuPlacement="top"
            variant={formState.errors.envelope ? "error" : "default"}
          />
        </LabeledInput>
      </FormControl>
      {requiresSchemaRegistry && (
        <FormControl
          isInvalid={!!formState.errors.csrConnection}
          data-testid="csr-connection"
        >
          <LabeledInput
            inputBoxProps={{ maxWidth: `${FORM_CONTENT_WIDTH}px` }}
            label="Schema Registry Connection"
            message="Select or create the connection to your schema registry."
            error={formState.errors.csrConnection?.message}
          >
            <CsrSelectionControl
              selectField={csrConnectionField}
              variant={formState.errors.csrConnection ? "error" : "default"}
              selectProps={{ menuPlacement: "top" }}
            />
          </LabeledInput>
        </FormControl>
      )}
    </FormSection>
  );
};

const NewKafkaSourceForm = ({
  form,
  generalFormError,
}: { generalFormError?: string } & FormProp) => {
  const { results: schemas, failedToLoad: schemasFailedToLoad } = useSchemas({
    filterByCreatePrivilege: true,
  });
  const { results: clusters, failedToLoad: clustersFailedToLoad } =
    useConnectorClusters();

  const loadingError = schemasFailedToLoad || clustersFailedToLoad;

  if (loadingError) {
    return <ErrorBox />;
  }

  return (
    <FormContainer>
      <GeneralSection form={form} schemas={schemas ?? []} />
      <ClusterSection form={form} clusters={clusters ?? []} />
      <ConfigurationSection form={form} />
      {generalFormError && (
        <Alert variant="error" minWidth="100%" message={generalFormError} />
      )}
    </FormContainer>
  );
};

export default NewKafkaSourceForm;
