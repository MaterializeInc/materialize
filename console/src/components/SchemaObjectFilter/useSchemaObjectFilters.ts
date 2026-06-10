// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useQuery } from "@tanstack/react-query";
import React, { useMemo } from "react";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import { fetchDatabaseList } from "~/api/materialize/databaseList";
import { useAllSchemas } from "~/store/allSchemas";
import { useQueryStringState } from "~/useQueryString";

const namespaceQueryStringKey = "namespace" as const;

export const schemaObjectFilterQueryKeys = {
  all: () => buildRegionQueryKey("schemaObjectFilter"),
  databaseList: () =>
    [
      ...schemaObjectFilterQueryKeys.all(),
      buildQueryKeyPart("databaseList"),
    ] as const,
};

export const useSchemaObjectFilters = (nameFilterKey: string) => {
  const [selectedNamespace, setSelectedNamespace] = useQueryStringState(
    namespaceQueryStringKey,
  );
  const [nameValue, setNameValue] = useQueryStringState(nameFilterKey);

  const { data: databaseList } = useQuery({
    queryKey: schemaObjectFilterQueryKeys.databaseList(),
    queryFn: async ({ queryKey, signal }) => {
      return (
        await fetchDatabaseList({
          queryKey,
          requestOptions: { signal },
        })
      ).rows;
    },
  });

  const [databaseName, schemaName] = React.useMemo(
    () => (selectedNamespace ?? "").split("."),
    [selectedNamespace],
  );
  const selectedDatabase = React.useMemo(
    () =>
      (databaseList && databaseList.find((d) => d.name === databaseName)) ??
      undefined,
    [databaseList, databaseName],
  );
  const setSelectedDatabase = React.useCallback(
    (id: string) => {
      const selected = databaseList && databaseList.find((d) => d.id === id);

      setSelectedNamespace(selected?.name);
    },
    [databaseList, setSelectedNamespace],
  );

  const {
    data: schemas,
    isError: isSchemaError,
    snapshotComplete: schemaSnapshotComplete,
  } = useAllSchemas();

  const schemaList = useMemo(() => {
    if (isSchemaError || !schemaSnapshotComplete) {
      return null;
    }
    return (
      schemas?.filter(
        (s) =>
          selectedDatabase === undefined ||
          s.databaseName === selectedDatabase.name,
      ) ?? null
    );
  }, [isSchemaError, schemaSnapshotComplete, schemas, selectedDatabase]);

  const selectedSchema = React.useMemo(() => {
    return (
      (schemaList &&
        schemaList.find(
          (s) => s.name === schemaName && s.databaseName === databaseName,
        )) ??
      undefined
    );
  }, [databaseName, schemaList, schemaName]);
  const setSelectedSchema = React.useCallback(
    (id: string) => {
      const selected = schemaList && schemaList.find((d) => d.id === id);

      setSelectedNamespace(
        selected ? `${selected.databaseName}.${selected.name}` : undefined,
      );
    },
    [schemaList, setSelectedNamespace],
  );

  return {
    schemaFilter: {
      schemaList,
      selected: selectedSchema,
      setSelectedSchema,
    },
    databaseFilter: {
      databaseList,
      selected: selectedDatabase,
      setSelectedDatabase,
    },
    nameFilter: {
      name: nameValue,
      setName: setNameValue,
    },
  };
};

export type SchemaObjectFilters = ReturnType<typeof useSchemaObjectFilters>;
export type DatabaseFilterState = SchemaObjectFilters["databaseFilter"];
export type SchemaFilterState = SchemaObjectFilters["schemaFilter"];
export type NameFilterState = SchemaObjectFilters["nameFilter"];
