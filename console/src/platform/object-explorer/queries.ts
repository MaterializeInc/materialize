// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useSuspenseQuery } from "@tanstack/react-query";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import { assertExactlyOneRow } from "~/api/materialize/assertExactlyOneRow";
import {
  ConnectionDependenciesParameters,
  fetchConnectionDependencies,
} from "~/api/materialize/object-explorer/connectionDependencies";
import {
  DatabaseDetailsParameters,
  fetchDatabaseDetails,
} from "~/api/materialize/object-explorer/databaseDetails";
import {
  fetchObjectColumns,
  ObjectExplorerColumnsParameters,
} from "~/api/materialize/object-explorer/objectColumns";
import {
  fetchObjectDetails,
  ObjectExplorerDetailsParameters,
} from "~/api/materialize/object-explorer/objectDetails";
import {
  fetchObjectIndexes,
  ObjectIndexesParameters,
} from "~/api/materialize/object-explorer/objectIndexes";
import {
  fetchSchemaDetails,
  SchemaDetailsParameters,
} from "~/api/materialize/object-explorer/schemaDetails";
import { fetchIsOwner, IsOwnerParameters } from "~/api/materialize/objects";

export const objectExplorerQueryKeys = {
  all: () => buildRegionQueryKey("object-explorer"),
  columns: (params: ObjectExplorerColumnsParameters) =>
    [
      ...objectExplorerQueryKeys.all(),
      buildQueryKeyPart("columns", params),
    ] as const,
  indexes: (params: ObjectIndexesParameters) =>
    [
      ...objectExplorerQueryKeys.all(),
      buildQueryKeyPart("indexes", params),
    ] as const,
  databaseDetails: (params: DatabaseDetailsParameters) =>
    [
      ...objectExplorerQueryKeys.all(),
      buildQueryKeyPart("databaseDetails", params),
    ] as const,
  schemaDetails: (params: SchemaDetailsParameters) =>
    [
      ...objectExplorerQueryKeys.all(),
      buildQueryKeyPart("schemaDetails", params),
    ] as const,
  objectDetails: (params: ObjectExplorerDetailsParameters) =>
    [
      ...objectExplorerQueryKeys.all(),
      buildQueryKeyPart("objectDetails", params),
    ] as const,
  isOwner: (params: IsOwnerParameters) =>
    [
      ...objectExplorerQueryKeys.all(),
      buildQueryKeyPart("isOwner", params),
    ] as const,
  connectionDependencies: (params: ConnectionDependenciesParameters) =>
    [
      ...objectExplorerQueryKeys.all(),
      buildQueryKeyPart("connectionDependencies", params),
    ] as const,
};

export const useDatabaseDetails = (params: DatabaseDetailsParameters) => {
  const { data, ...rest } = useSuspenseQuery({
    queryKey: objectExplorerQueryKeys.databaseDetails(params),
    queryFn: async ({ queryKey, signal }) => {
      const [, parameters] = queryKey;

      const result = await fetchDatabaseDetails({
        queryKey,
        parameters,
        requestOptions: { signal },
      });
      assertExactlyOneRow(result.rows.length, { skipQueryRetry: true });
      return result;
    },
  });
  return { data: data.rows[0], ...rest };
};

export const useSchemaDetails = (params: SchemaDetailsParameters) => {
  const { data, ...rest } = useSuspenseQuery({
    queryKey: objectExplorerQueryKeys.schemaDetails(params),
    queryFn: async ({ queryKey, signal }) => {
      const [, parameters] = queryKey;

      const result = await fetchSchemaDetails({
        queryKey,
        parameters,
        requestOptions: { signal },
      });
      assertExactlyOneRow(result.rows.length, { skipQueryRetry: true });
      return result;
    },
  });
  return { data: data.rows[0], ...rest };
};

export const useObjectDetails = (params: ObjectExplorerDetailsParameters) => {
  const { data, ...rest } = useSuspenseQuery({
    queryKey: objectExplorerQueryKeys.objectDetails(params),
    queryFn: async ({ queryKey, signal }) => {
      const [, parameters] = queryKey;

      const result = await fetchObjectDetails({
        queryKey,
        parameters,
        requestOptions: { signal },
      });
      assertExactlyOneRow(result.rows.length, { skipQueryRetry: true });
      return result;
    },
  });
  return { data: data.rows[0], ...rest };
};

export const useObjectColumns = (params: ObjectExplorerColumnsParameters) => {
  return useSuspenseQuery({
    queryKey: objectExplorerQueryKeys.columns(params),
    queryFn: async ({ queryKey, signal }) => {
      const [, parameters] = queryKey;

      return fetchObjectColumns({
        queryKey,
        parameters,
        requestOptions: { signal },
      });
    },
  });
};

export const useObjectIndexes = (params: ObjectIndexesParameters) => {
  return useSuspenseQuery({
    queryKey: objectExplorerQueryKeys.indexes(params),
    queryFn: async ({ queryKey, signal }) => {
      const [, parameters] = queryKey;

      return fetchObjectIndexes({
        queryKey,
        parameters,
        requestOptions: { signal },
      });
    },
  });
};

export const useIsOwner = (params: IsOwnerParameters) => {
  return useSuspenseQuery({
    queryKey: objectExplorerQueryKeys.isOwner(params),
    queryFn: async ({ queryKey, signal }) => {
      const [, parameters] = queryKey;

      const result = await fetchIsOwner({
        queryKey,
        parameters,
        requestOptions: { signal },
      });
      assertExactlyOneRow(result.rows.length, { skipQueryRetry: true });
      return result.rows[0].isOwner;
    },
  });
};

export const useConnectionDependencies = (
  params: ConnectionDependenciesParameters,
) => {
  return useSuspenseQuery({
    queryKey: objectExplorerQueryKeys.connectionDependencies(params),
    queryFn: async ({ queryKey, signal }) => {
      const [, parameters] = queryKey;

      return fetchConnectionDependencies({
        queryKey,
        parameters,
        requestOptions: { signal },
      });
    },
    select: (data) => data.rows,
  });
};
