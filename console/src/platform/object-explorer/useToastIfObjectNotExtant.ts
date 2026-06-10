// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo } from "react";

import { useToast } from "~/hooks/useToast";
import { useAllObjects } from "~/store/allObjects";
import { useRegionSlug } from "~/store/environments";

import { useSchemaObjectParams } from "./useSchemaObjectParams";

export const useToastIfObjectNotExtant = () => {
  const regionSlug = useRegionSlug();
  const queryClient = useQueryClient();
  const params = useSchemaObjectParams();
  const { data: allObjects } = useAllObjects();
  const shouldShowToast = !allObjects.some((object) => object.id === params.id);

  const message = `Object ${params.databaseName}.${params.schemaName}.${params.objectName} does not exist.`;

  const toast = useToast(
    useMemo(
      () => ({
        status: "error",
        description: message,
        isClosable: true,
        duration: null,
      }),
      [message],
    ),
  );

  useEffect(() => {
    if (shouldShowToast) {
      toast();
    }
  }, [regionSlug, queryClient, toast, shouldShowToast]);
};
