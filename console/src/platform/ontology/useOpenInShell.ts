// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useAtomCallback } from "jotai/utils";
import React from "react";
import { useNavigate } from "react-router-dom";

import { shellPath } from "~/platform/routeHelpers";
import { setPromptValue } from "~/platform/shell/store/prompt";
import { useRegionSlug } from "~/store/environments";

/** Puts `sql` in the SQL Shell's prompt and opens the shell. */
export function useOpenInShell() {
  const navigate = useNavigate();
  const regionSlug = useRegionSlug();
  const setPrompt = useAtomCallback(setPromptValue);
  return React.useCallback(
    (sql: string) => {
      setPrompt(sql);
      navigate(shellPath(regionSlug));
    },
    [navigate, regionSlug, setPrompt],
  );
}
