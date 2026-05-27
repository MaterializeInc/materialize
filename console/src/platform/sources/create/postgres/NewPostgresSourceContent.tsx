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
import createPostgresConnection, {
  CreatePostgresConnectionParameters,
} from "~/api/materialize/connection/createPostgresConnection";
import {
  Connection,
  fetchConnections,
} from "~/api/materialize/connection/useConnections";
import createPostgresSourceStatement from "~/api/materialize/source/createPostgresSourceStatement";
import getSourceByNameStatement from "~/api/materialize/source/getSourceByNameStatement";
import { WizardStep } from "~/components/formComponentsV2";
import { ObjectToastDescription } from "~/components/Toast";
import { getSecretFromOption } from "~/forms/secretsFormControlAccessors";
import { useToast } from "~/hooks/useToast";
import { useWaitForObjectInSubscription } from "~/hooks/useWaitForObjectInSubscription";
import { connectionQueryKeys } from "~/platform/connections/queries";
import { useBuildSourcePath } from "~/platform/routeHelpers";
import { regionPath } from "~/platform/routeHelpers";
import { currentEnvironmentState, useRegionSlug } from "~/store/environments";
import { assert } from "~/util";

import { sourceQueryKeys } from "../../queries";
import { DatabaseConnectionFormState } from "../shared/NewDatabaseConnectionForm";
import NewDatabaseSourceContent, {
  CreateConnectionError,
  CreateSourceError,
} from "../shared/NewDatabaseSourceContent";
import { DatabaseSourceFormState } from "../shared/NewDatabaseSourceForm";

function useCreatePostgresSource() {
  return useSqlLazy({
    // Materialize has a 30 second timeout for attempting to connect to postgres
    timeout: 35_000,
    queryBuilder: ({
      connection,
      state,
    }: {
      connection: Connection;
      state: DatabaseSourceFormState;
    }) => {
      assert(state.schema);
      assert(state.cluster);
      const compiledGetSourceQuery = getSourceByNameStatement(
        state.name,
        state.schema.databaseName,
        state.schema.name,
      ).compile();
      // new object to narrow the type
      const createQuery = createPostgresSourceStatement({
        name: state.name,
        publication: state.publication,
        allTables: state.allTables,
        tables: state.tables,
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

const NewPostgresSourceContent = ({
  initialSteps = [],
}: {
  initialSteps?: WizardStep[];
}) => {
  const navigate = useNavigate();
  const regionSlug = useRegionSlug();
  const toast = useToast();
  const sourcePath = useBuildSourcePath();
  const [environment] = useAtom(currentEnvironmentState);
  const { runSql: createSource } = useCreatePostgresSource();
  const queryClient = useQueryClient();
  const { waitForObject } = useWaitForObjectInSubscription();

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

              // Wait for object to appear in WebSocket subscription to prevent race conditions on navigation
              await waitForObject(id);

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

  const handleConnectionSubmit = async (state: DatabaseConnectionFormState) => {
    assert(environment?.state === "enabled");

    const createParams: CreatePostgresConnectionParameters = {
      name: state.name,
      host: state.host,
      port: state.port,
      pgDatabaseName: state.sourceDatabaseName,
      databaseName: state.schema.databaseName,
      schemaName: state.schema.name,
      user: state.user,
      password: state.password
        ? getSecretFromOption(state.password)
        : undefined,
      sslMode: state.sslAuthentication ? state.sslMode : undefined,
      sslCertificate:
        state.sslAuthentication && !!state.sslCertificate
          ? getSecretFromOption(state.sslCertificate)
          : undefined,
      sslKey:
        state.sslAuthentication && !!state.sslKey
          ? getSecretFromOption(state.sslKey)
          : undefined,
      sslCertificateAuthority:
        state.sslMode !== "require" && !!state.sslCertificateAuthority
          ? getSecretFromOption(state.sslCertificateAuthority)
          : undefined,
    };

    const { error: createConnectionError } = await createPostgresConnection({
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

  return (
    <NewDatabaseSourceContent
      initialSteps={initialSteps}
      onCreateConnection={handleConnectionSubmit}
      onCreateSource={handleSourceSubmit}
      databaseType="postgres"
    />
  );
};

export default NewPostgresSourceContent;
