// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useEffect, useRef } from "react";

function useClickAndHold(duration: number, callback: () => void) {
  const isClicked = useRef(false);
  const holdStartRef = useRef<NodeJS.Timeout | null>(null);

  const startHold = () => {
    if (isClicked.current) return;
    isClicked.current = true;
    holdStartRef.current = setTimeout(() => {
      callback();
    }, duration);
  };

  const endHold = () => {
    if (holdStartRef.current) {
      clearTimeout(holdStartRef.current);
      holdStartRef.current = null;
    }
    isClicked.current = false;
  };

  useEffect(() => () => {
    if (holdStartRef.current) {
      clearTimeout(holdStartRef.current);
    }
  });

  return { startHold, endHold };
}

export default useClickAndHold;
