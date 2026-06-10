// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

/**
 * Delays loading for the specified timeout.
 * @returns boolean indicating whether or not to show a loading state
 */
const useDelayedLoading = (loading: boolean, delayMs = 500) => {
  const [showLoading, setShowLoading] = React.useState(false);
  const timeoutRef = React.useRef<NodeJS.Timeout>();

  React.useEffect(() => {
    if (!showLoading && loading) {
      timeoutRef.current = setTimeout(() => {
        setShowLoading(true);
      }, delayMs);
    }
    return () => {
      // clear the timeout if anything changes before it fires
      clearTimeout(timeoutRef.current);
    };
  }, [showLoading, loading, delayMs]);

  if (showLoading && !loading) {
    setShowLoading(false);
  }

  return showLoading;
};

export default useDelayedLoading;
