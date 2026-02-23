// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { chakra, ModalBody } from "@chakra-ui/react";
import { useAtom } from "jotai";
import React, { useEffect, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";

import {
  Connection,
  useConnectionsFiltered,
} from "~/api/materialize/connection/useConnections";
import { alreadyExistsError } from "~/api/materialize/parseErrors";
import {
  FormBottomBar,
  FormTopBar,
  WizardStep,
} from "~/components/formComponentsV2";
import { currentEnvironmentState } from "~/store/environments";
import { assert, capitalizeSentence } from "~/util";

import { DatabaseTypeProp } from "./constants";
import { useDatabaseConnectionForm, useDatabaseSourceForm } from "./forms";
import NewDatabaseConnectionForm, {
  DatabaseConnectionFormState,
} from "./NewDatabaseConnectionForm";
import NewDatabaseSourceForm, {
  DatabaseSourceFormState,
} from "./NewDatabaseSourceForm";
import useNormalizedSteps from "./useNormalizedSteps";
import { typeToLabel } from "./utils";

export class CreateConnectionError extends Error {}
export class CreateSourceError extends Error {}

const SOURCE_STEPS = [
  { id: "connection", label: "Configure connection" },
  { id: "source", label: "Configure source" },
];

const NewDatabaseSourceContent = ({
  initialSteps = [],
  onCreateConnection,
  onCreateSource,
  databaseType,
}: {
  initialSteps?: WizardStep[];
  onCreateConnection: (
    state: DatabaseConnectionFormState,
  ) => Promise<Connection>;
  onCreateSource: (
    connection: Connection,
    state: DatabaseSourceFormState,
  ) => Promise<void>;
} & DatabaseTypeProp) => {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const [isPending, setIsPending] = useState(false);
  const [environment] = useAtom(currentEnvironmentState);
  const [generalFormError, setGeneralFormError] = useState<
    string | undefined
  >();
  const { results: connections, refetch: refetchConnections } =
    useConnectionsFiltered({ type: databaseType });

  const [sourceConnection, setSourceConnection] = useState<Connection | null>(
    null,
  );

  const connectionIdParam = searchParams.get("connectionId");
  const { isNormalizedStep, setNormalizedActiveStep, steps, wizardSteps } =
    useNormalizedSteps({
      initialSteps,
      sourceSteps: SOURCE_STEPS,
      indexOffset: connectionIdParam ? 1 : 0,
    });

  const connectionForm = useDatabaseConnectionForm(databaseType);
  const sourceForm = useDatabaseSourceForm(databaseType);

  const registerSourceConnection = (conn: Connection) => {
    setSourceConnection(conn);
    setSearchParams({ connectionId: conn.id });
    wizardSteps.goToNext();
    setGeneralFormError(undefined);
  };

  const handleConnectionSubmit = async (state: DatabaseConnectionFormState) => {
    assert(environment?.state === "enabled");
    setGeneralFormError(undefined);
    if (state.connectionAction === "existing") {
      registerSourceConnection(state.connection);
    } else {
      try {
        setIsPending(true);
        const connection = await onCreateConnection(state);
        await refetchConnections();
        registerSourceConnection(connection);
      } catch (e) {
        if (e instanceof CreateConnectionError) {
          const errorMessage = capitalizeSentence(e.message);
          if (alreadyExistsError(e.message)) {
            connectionForm.setError("name", {
              message: errorMessage,
            });
          } else {
            setGeneralFormError(errorMessage);
          }
          console.error(errorMessage);
        } else if (e instanceof Error) {
          setGeneralFormError(e.message);
        } else {
          console.error("Could not create connection", e);
          setGeneralFormError("There was an error creating the connection.");
        }
      } finally {
        setIsPending(false);
      }
    }
  };

  const handleSourceSubmit = async (state: DatabaseSourceFormState) => {
    assert(sourceConnection);
    setGeneralFormError(undefined);
    setIsPending(true);
    try {
      await onCreateSource(sourceConnection, state);
    } catch (error) {
      if (error instanceof CreateSourceError) {
        const objectName = alreadyExistsError(error.message);
        const message = capitalizeSentence(error.message ?? "");
        if (objectName === state.name) {
          sourceForm.setError("name", { message });
          sourceForm.setFocus("name");
          return;
        }
        const aliasIndex = state.tables.findIndex(
          (t) => t.alias === objectName,
        );
        if (aliasIndex > -1) {
          const name = `tables.${aliasIndex}.alias` as const;
          sourceForm.setError(name, { message });
          sourceForm.setFocus(name);
          return;
        }
        const tableIndex = state.tables.findIndex((t) => t.name === objectName);
        if (tableIndex > -1) {
          const name = `tables.${tableIndex}.name` as const;
          sourceForm.setError(name, { message });
          sourceForm.setFocus(name);
          return;
        }
        setGeneralFormError(message);
      } else if (error instanceof Error) {
        setGeneralFormError(error.message);
      } else {
        console.error("Could not create source", error);
        setGeneralFormError("There was an error creating the source.");
      }
    } finally {
      setIsPending(false);
    }
  };

  const isStepValid = isNormalizedStep(0)
    ? connectionForm.formState.isValid
    : isNormalizedStep(1)
      ? sourceForm.formState.isValid
      : false;

  const handleSubmit = () => {
    if (isNormalizedStep(0)) {
      return connectionForm.handleSubmit(handleConnectionSubmit);
    } else if (isNormalizedStep(1)) {
      return sourceForm.handleSubmit(handleSourceSubmit);
    }
    return () => {
      console.error("Invalid form type");
    };
  };

  const goToPrevious = () => {
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

  return (
    <chakra.form display="contents" onSubmit={handleSubmit()}>
      <FormTopBar
        title={`New ${typeToLabel(databaseType)} source`}
        steps={steps}
        {...wizardSteps}
      />
      <ModalBody>
        {isNormalizedStep(0) && (
          <NewDatabaseConnectionForm
            connections={connections}
            form={connectionForm}
            generalFormError={generalFormError}
            databaseType={databaseType}
          />
        )}
        {isNormalizedStep(1) && (
          <NewDatabaseSourceForm
            form={sourceForm}
            generalFormError={generalFormError}
            databaseType={databaseType}
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

export default NewDatabaseSourceContent;
