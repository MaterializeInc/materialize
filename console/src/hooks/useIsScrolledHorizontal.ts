// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useCallback, useState } from "react";

function useIsScrolledHorizontal(threshold = 0) {
  const [isScrolled, setIsScrolled] = useState<boolean>(false);

  const onScroll = useCallback(
    (event: React.UIEvent<HTMLElement>) => {
      event.stopPropagation();
      if (event.currentTarget.scrollLeft > threshold) {
        setIsScrolled(true);
      } else {
        setIsScrolled(false);
      }
    },
    [threshold],
  );

  return { isScrolled, onScroll };
}

export default useIsScrolledHorizontal;
