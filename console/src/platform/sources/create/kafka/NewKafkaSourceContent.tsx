// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { chakra, ModalBody } from "@chakra-ui/react";
import * as Sentry from "@sentry/react";
import { useQueryClient } from "@tanstack/react-query";
import { useAtom } from "jotai";
import { useEffect, useState } from "react";
import React from "react";
import { useNavigate, useSearchParams } from "react-router-dom";

import {
  CATALOG_SERVER_CLUSTER,
  mapRowToObject,
  queryBuilder,
  useSqlLazy,
} from "~/api/materialize";
import createKafkaConnection, {
  SASLMechanism,
} from "~/api/materialize/connection/createKafkaConnection";
import {
  Connection,
  ConnectionFiltered,
  useConnectionsFiltered,
} from "~/api/materialize/connection/useConnections";
import { alreadyExistsError } from "~/api/materialize/parseErrors";
import createKafkaSourceStatement from "~/api/materialize/source/createKafkaSourceStatement";
import getSourceByNameStatement, {
  SourceByNameResult,
} from "~/api/materialize/source/getSourceByNameStatement";
import {
  FormBottomBar,
  FormTopBar,
  WizardStep,
} from "~/components/formComponentsV2";
import { ObjectToastDescription } from "~/components/Toast";
import { getSecretFromOption } from "~/forms/secretsFormControlAccessors";
import { useToast } from "~/hooks/useToast";
import { regionPath, useBuildSourcePath } from "~/platform/routeHelpers";
import { currentEnvironmentState, useRegionSlug } from "~/store/environments";
import { assert, capitalizeSentence } from "~/util";

import { sourceQueryKeys } from "../../queries";
import useNormalizedSteps from "../shared/useNormalizedSteps";
import { useConnectionForm, useSourceForm } from "./forms";
import NewKafkaConnectionForm, {
  KafkaConnectionFormState,
} from "./NewKafkaConnectionForm";
import NewKafkaSourceForm, { KafkaSourceFormState } from "./NewKafkaSourceForm";

function getAuthParamFromFormState(values: KafkaConnectionFormState) {
  const sslCertificateAuthority = values.sslCertificateAuthority
    ? getSecretFromOption(values.sslCertificateAuthority)
    : undefined;

  if (values.authenticationMode === "sasl") {
    const saslUsername = values.saslUsername;
    const saslPassword = getSecretFromOption(values.saslPassword);

    assert(saslUsername);
    assert(saslPassword);
    assert(values.saslMechanism);

    return {
      type: "SASL" as const,
      saslMechanism: values.saslMechanism.id as SASLMechanism,
      saslUsername,
      saslPassword,
      sslCertificateAuthority,
    };
  }

  if (values.authenticationMode === "ssl") {
    const sslCertificate = getSecretFromOption(values.sslCertificate);
    const sslKey = getSecretFromOption(values.sslKey);

    assert(sslCertificate);
    assert(sslKey);

    return {
      type: "SSL" as const,
      sslCertificate,
      sslKey,
      sslCertificateAuthority,
    };
  }
}

const SOURCE_STEPS = [
  { id: "connection", label: "Configure connection" },
  { id: "source", label: "Configure source" },
];

function useCreateSource() {
  return useSqlLazy({
    queryBuilder: ({
      connection,
      state,
    }: {
      connection: Connection;
      state: KafkaSourceFormState;
    }) => {
      assert(state.schema);
      assert(state.cluster);
      const compiledGetSourceQuery = getSourceByNameStatement(
        state.name,
        state.schema.databaseName,
        state.schema.name,
      ).compile();
      // new object to narrow the type
      const createQuery = createKafkaSourceStatement({
        ...state,
        cluster: state.cluster,
        databaseName: state.schema.databaseName,
        schemaName: state.schema.name,
        connection: connection,
        keyFormat: state.keyFormat.id,
        valueFormat: state.valueFormat.id,
        formatConnection: state.csrConnection,
        envelope: state.envelope.id,
      }).compile(queryBuilder);
      return {
        queries: [
          {
            query: createQuery.sql,
            params: createQuery.parameters as string[],
          },
          {
            query: compiledGetSourceQuery.sql,
            params: compiledGetSourceQuery.parameters as string[],
          },
        ],
        cluster: CATALOG_SERVER_CLUSTER,
      };
    },
    // Takes mz approximately 30 seconds to decide if it can connect to a Kafka consumer
    timeout: 35_000,
  });
}

const NewKafkaSourceContent = ({
  initialSteps = [],
}: {
  initialSteps?: WizardStep[];
}) => {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const [environment] = useAtom(currentEnvironmentState);
  const [searchParams, setSearchParams] = useSearchParams();
  const sourcePath = useBuildSourcePath();
  const [isPending, setIsPending] = useState(false);
  const [generalFormError, setGeneralFormError] = useState<
    string | undefined
  >();
  const regionSlug = useRegionSlug();
  const toast = useToast();

  const connectionForm = useConnectionForm();
  const sourceForm = useSourceForm();

  const connectionIdParam = searchParams.get("connectionId");
  const { isNormalizedStep, setNormalizedActiveStep, steps, wizardSteps } =
    useNormalizedSteps({
      initialSteps,
      sourceSteps: SOURCE_STEPS,
      indexOffset: connectionIdParam ? 1 : 0,
    });

  const { results: connections, refetch: refetchConnections } =
    useConnectionsFiltered({ type: "kafka" });
  const { runSql: createSource } = useCreateSource();

  const [sourceConnection, setSourceConnection] = useState<Connection | null>(
    null,
  );

  const goToPrevious = () => {
    setGeneralFormError(undefined);
    if (isNormalizedStep(0)) {
      navigate("..");
    } else {
      wizardSteps.goToPrevious();
    }
  };

  useEffect(() => {
    if (sourceConnection?.id === connectionIdParam) {
      return;
    }
    if (connections && connectionIdParam) {
      const selected = connections.find((c) => c.id === connectionIdParam);
      if (selected) {
        setSourceConnection(selected);
      } else {
        setGeneralFormError("Unknown connection. Please specify another one.");
        setNormalizedActiveStep(0);
      }
    }
  }, [
    connections,
    sourceConnection,
    connectionIdParam,
    setGeneralFormError,
    setNormalizedActiveStep,
    setSourceConnection,
  ]);

  const isStepValid = isNormalizedStep(0)
    ? connectionForm.formState.isValid
    : isNormalizedStep(1)
      ? sourceForm.formState.isValid
      : false;

  const registerSourceConnection = (conn: Connection) => {
    setSourceConnection(conn);
    setSearchParams({ connectionId: conn.id });
    wizardSteps.goToNext();
    setGeneralFormError(undefined);
  };

  const handleConnectionSubmit = async (state: KafkaConnectionFormState) => {
    setGeneralFormError(undefined);
    if (state.connectionAction === "existing") {
      registerSourceConnection(state.connection);
      return;
    }
    try {
      setIsPending(true);

      assert(environment?.state === "enabled");
      const schemaName = state.schema.name;
      const databaseName = state.schema.databaseName;

      const brokers = state.brokers.map(({ hostPort }) => ({
        type: "basic" as const,
        hostPort,
      }));

      const { error: createConnectionError, data: createConnectionData } =
        await createKafkaConnection({
          params: {
            name: state.name,
            databaseName,
            schemaName,
            auth: getAuthParamFromFormState(state),
            brokers,
          },
          environmentdHttpAddress: environment.httpAddress,
        });

      if (createConnectionError) {
        if (alreadyExistsError(createConnectionError.errorMessage)) {
          connectionForm.setError("name", {
            message: capitalizeSentence(createConnectionError.errorMessage),
          });
        } else {
          setGeneralFormError(createConnectionError.errorMessage);
        }
        return;
      }

      const { connectionId } = createConnectionData;
      const [conns] = (await refetchConnections()) ?? [];
      const conn = conns.rows
        ?.map((r) => mapRowToObject(r, conns.columns) as ConnectionFiltered)
        .find((c) => c.id === connectionId);
      if (conn) {
        registerSourceConnection(conn);
      } else {
        setGeneralFormError(
          "There was an error loading the connection. Please refresh and select it from the list",
        );
      }
    } catch (e) {
      if (e instanceof Error) {
        setGeneralFormError(e.message);
      } else {
        console.error("Unhandled error", e);
        setGeneralFormError("There was an error creating the connection.");
      }
    } finally {
      setIsPending(false);
    }
  };

  const handleSourceSubmit = (state: KafkaSourceFormState) => {
    setGeneralFormError(undefined);
    assert(sourceConnection);
    setIsPending(true);
    createSource(
      { connection: sourceConnection, state },
      {
        onSuccess: async (response) => {
          try {
            assert(response);
            assert(response.length >= 2);
            const [_, getSourceResults] = response;

            const { columns, rows } = getSourceResults;
            const newSource = mapRowToObject(
              rows[0],
              columns,
            ) as SourceByNameResult;

            // refetch the list to ensure redirect will work
            await queryClient.refetchQueries({
              queryKey: sourceQueryKeys.list(),
            });
            toast({
              description: (
                <ObjectToastDescription
                  name={state.name}
                  message="created successfully"
                />
              ),
            });
            navigate(sourcePath(newSource));
          } catch (e) {
            Sentry.captureException(e);
            navigate(`${regionPath(regionSlug)}/sources/`);
          }
        },
        onError: (errorMessage) => {
          setIsPending(false);
          if (alreadyExistsError(errorMessage)) {
            sourceForm.setError("name", {
              message: capitalizeSentence(errorMessage ?? ""),
            });
          } else {
            setGeneralFormError(errorMessage);
          }
        },
      },
    );
  };

  const handleSubmit = () => {
    if (isNormalizedStep(0)) {
      return connectionForm.handleSubmit(handleConnectionSubmit);
    } else if (isNormalizedStep(1)) {
      return sourceForm.handleSubmit(handleSourceSubmit);
    }
    return () => {
      console.warn("Unhandled form");
    };
  };

  return (
    <chakra.form display="contents" onSubmit={handleSubmit()}>
      <FormTopBar
        title="Create a Kafka source"
        steps={steps}
        {...wizardSteps}
      />
      <ModalBody>
        {isNormalizedStep(0) && (
          <NewKafkaConnectionForm
            generalFormError={generalFormError}
            form={connectionForm}
            connections={connections}
          />
        )}
        {isNormalizedStep(1) && (
          <NewKafkaSourceForm
            generalFormError={generalFormError}
            form={sourceForm}
          />
        )}
      </ModalBody>
      <FormBottomBar
        steps={steps}
        isPending={isPending}
        isValid={isStepValid}
        submitMessage={
          isNormalizedStep(0) ? "Create connection" : "Create source"
        }
        advanceType="submit"
        {...wizardSteps}
        goToPrevious={goToPrevious}
      />
    </chakra.form>
  );
};

export default NewKafkaSourceContent;
