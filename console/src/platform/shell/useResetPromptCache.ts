// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useAtom } from "jotai";
import { useEffect, useRef } from "react";

import { getCache } from "./commandCache";
import { defaultState as defaultPromptState, promptAtom } from "./store/prompt";

/**
 * Reset the prompt and populate the history with cached commands from the region.
 */
const useResetPromptCache = ({
  organizationId,
  regionId,
}: {
  organizationId?: string;
  regionId: string;
}) => {
  const hasHydrated = useRef(false);
  const [, setPastPrompts] = useAtom(promptAtom);
  useEffect(() => {
    if (!hasHydrated.current) {
      const cache = getCache({ organizationId, regionId });
      setPastPrompts({
        // Use the defaultState here to start without historical baggage or
        // untracked generations.
        ...defaultPromptState,
        past: cache.commands.map((value, generation) => ({
          value,
          generation,
          originalValue: value,
        })),
        present: {
          value: "",
          generation: cache.commands.length,
        },
        currentGeneration: cache.commands.length,
      });
      hasHydrated.current = true;
    }
  }, [organizationId, regionId, setPastPrompts]);
};

export default useResetPromptCache;
