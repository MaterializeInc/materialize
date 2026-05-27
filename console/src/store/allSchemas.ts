// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { atom, useAtomValue } from "jotai";
import React from "react";

import { isSystemId } from "~/api/materialize";
import {
  buildAllSchemaListQuery,
  SchemaWithOptionalDatabase,
} from "~/api/materialize/schemaList";
import { SubscribeState } from "~/api/materialize/SubscribeManager";
import {
  buildSubscribeQuery,
  useGlobalUpsertSubscribe,
} from "~/api/materialize/useSubscribe";

export const allSchemas = atom<SubscribeState<SchemaWithOptionalDatabase>>({
  data: [],
  error: undefined,
  snapshotComplete: false,
});

export function useSubscribeToAllSchemas() {
  const subscribe = React.useMemo(() => {
    return buildSubscribeQuery(buildAllSchemaListQuery(), { upsertKey: "id" });
  }, []);

  return useGlobalUpsertSubscribe({
    atom: allSchemas,
    subscribe,
    select: (row) => row.data,
    upsertKey: (row) => row.data.id,
  });
}

export function useAllSchemas(options?: { includeSystemSchemas?: boolean }) {
  const includeSystemSchemas = options?.includeSystemSchemas ?? true;
  const result = useAtomValue(allSchemas);

  return React.useMemo(() => {
    const data = includeSystemSchemas
      ? result.data
      : result.data.filter((s) => !isSystemId(s.id));
    return {
      ...result,
      data,
      isError: Boolean(result.error),
    };
  }, [includeSystemSchemas, result]);
}
