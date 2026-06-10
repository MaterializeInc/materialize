// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useAllObjects } from "~/store/allObjects";

export const useWaitForObjectInSubscription = () => {
  const { data: allObjects } = useAllObjects();

  const waitForObject = async (objectId: string): Promise<void> => {
    // Check if object already exists
    if (allObjects.some((obj) => obj.id === objectId)) {
      return;
    }
    // Timeout for WebSocket to sync
    await new Promise((resolve) => setTimeout(resolve, 500));
  };

  return { waitForObject };
};
