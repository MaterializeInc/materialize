// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as Sentry from "@sentry/react";
import { useQueryClient } from "@tanstack/react-query";
import { useAtom } from "jotai";
import React from "react";
import { useNavigate } from "react-router-dom";

import { queryBuilder, useSqlLazy } from "~/api/materialize";
import createMySqlConnection, {
  CreateMySqlConnectionParameters,
} from "~/api/materialize/connection/createMySqlConnection";
import {
  Connection,
  fetchConnections,
} from "~/api/materialize/connection/useConnections";
import createMySqlSourceStatement, {
  CreateSourceParameters,
} from "~/api/materialize/source/createMySqlSourceStatement";
import getSourceByNameStatement from "~/api/materialize/source/getSourceByNameStatement";
import { WizardStep } from "~/components/formComponentsV2";
import { ObjectToastDescription } from "~/components/Toast";
import { getSecretFromOption } from "~/forms/secretsFormControlAccessors";
import { useToast } from "~/hooks/useToast";
import { connectionQueryKeys } from "~/platform/connections/queries";
import { regionPath, useBuildSourcePath } from "~/platform/routeHelpers";
import { currentEnvironmentState, useRegionSlug } from "~/store/environments";
import { assert } from "~/util";

import { sourceQueryKeys } from "../../queries";
import { DatabaseConnectionFormState } from "../shared/NewDatabaseConnectionForm";
import NewDatabaseSourceContent, {
  CreateConnectionError,
  CreateSourceError,
} from "../shared/NewDatabaseSourceContent";
import { DatabaseSourceFormState } from "../shared/NewDatabaseSourceForm";

function useCreateMySqlSource() {
  return useSqlLazy({
    // Materialize has a 30 second timeout for attempting to connect to MySQL
    timeout: 35_000,
    queryBuilder: ({
      connection,
      state,
    }: {
      connection: Connection;
      state: DatabaseSourceFormState;
    }) => {
      assert(state.schema);
      assert(connection);
      assert(state.cluster);
      state.tables.forEach((t) => assert(t.schemaName !== undefined));
      const compiledGetSourceQuery = getSourceByNameStatement(
        state.name,
        state.schema.databaseName,
        state.schema.name,
      ).compile();
      // new object to narrow the type
      const createQuery = createMySqlSourceStatement({
        name: state.name,
        allTables: state.allTables,
        tables: state.tables as CreateSourceParameters["tables"],
        cluster: state.cluster,
        connection: connection,
        databaseName: state.schema.databaseName,
        schemaName: state.schema.name,
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
        cluster: "mz_catalog_server",
      };
    },
  });
}

const NewMySqlSourceContent = ({
  initialSteps,
}: {
  initialSteps?: WizardStep[];
}) => {
  const navigate = useNavigate();
  const [environment] = useAtom(currentEnvironmentState);
  const regionSlug = useRegionSlug();
  const sourcePath = useBuildSourcePath();
  const queryClient = useQueryClient();

  const { runSql: createSource } = useCreateMySqlSource();
  const toast = useToast();

  const handleConnectionSubmit = async (state: DatabaseConnectionFormState) => {
    assert(environment?.state === "enabled");
    const createParams: CreateMySqlConnectionParameters = {
      name: state.name,
      host: state.host,
      port: state.port,
      user: state.user,
      databaseName: state.schema.databaseName,
      schemaName: state.schema.name,
      password: state.password
        ? getSecretFromOption(state.password)
        : undefined,
      sslKey:
        state.sslAuthentication && !!state.sslKey
          ? getSecretFromOption(state.sslKey)
          : undefined,
      sslMode: state.sslAuthentication ? state.sslMode : undefined,
      sslCertificate:
        state.sslAuthentication && !!state.sslCertificate
          ? getSecretFromOption(state.sslCertificate)
          : undefined,
      sslCertificateAuthority:
        state.sslMode !== "require" && !!state.sslCertificateAuthority
          ? getSecretFromOption(state.sslCertificateAuthority)
          : undefined,
    };
    const { error: createConnectionError } = await createMySqlConnection({
      params: createParams,
      environmentdHttpAddress: environment.httpAddress,
    });
    if (createConnectionError) {
      throw new CreateConnectionError(createConnectionError.errorMessage);
    }
    const connFilter = {
      databaseId: state.schema.databaseId,
      schemaId: state.schema.id,
      nameFilter: state.name,
    };
    const created = await fetchConnections({
      filters: connFilter,
      queryKey: connectionQueryKeys.list(connFilter),
    });
    if (created.rows) {
      return created.rows[0];
    }
    throw new CreateConnectionError("Connection was not created");
  };

  const handleSourceSubmit = async (
    connection: Connection,
    state: DatabaseSourceFormState,
  ) => {
    return new Promise<void>((resolve, reject) => {
      createSource(
        { connection, state },
        {
          onSuccess: async (response) => {
            try {
              assert(response);
              const id = response[1].rows[0][0] as string;
              const databaseName = response[1].rows[0][1] as string;
              const schemaName = response[1].rows[0][2] as string;
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
              navigate(
                sourcePath({
                  id,
                  name: state.name,
                  schemaName,
                  databaseName,
                }),
              );
            } catch (e) {
              Sentry.captureException(e);
              navigate(`${regionPath(regionSlug)}/sources/`);
            }
            resolve();
          },
          onError: (errorMessage) => {
            reject(new CreateSourceError(errorMessage));
          },
        },
      );
    });
  };

  return (
    <NewDatabaseSourceContent
      initialSteps={initialSteps}
      onCreateConnection={handleConnectionSubmit}
      onCreateSource={handleSourceSubmit}
      databaseType="mysql"
    />
  );
};

export default NewMySqlSourceContent;
