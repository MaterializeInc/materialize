// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Dispatch, SetStateAction, useEffect, useState } from "react";

import storageAvailable from "~/utils/storageAvailable";

function useLocalStorage<T>(
  key: string,
  defaultValue: T,
): [T, Dispatch<SetStateAction<T>>] {
  const [value, setValue] = useState(() => {
    let currentValue = defaultValue;

    if (storageAvailable("localStorage")) {
      const localStorageValue = localStorage.getItem(key);
      if (localStorageValue !== null) {
        currentValue = JSON.parse(localStorageValue) as T;
      }
    }

    return currentValue;
  });

  useEffect(() => {
    if (storageAvailable("localStorage")) {
      localStorage.setItem(key, JSON.stringify(value));
    }
  }, [value, key]);

  return [value, setValue];
}

export default useLocalStorage;
