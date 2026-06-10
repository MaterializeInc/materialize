// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import Alert from "~/components/Alert";

const CreateEnvironmentWarning = () => {
  const message = (
    <>
      New regions include a <code>25cc</code> quickstart cluster. You will be
      billed for this cluster until you connect to the region and drop it.
    </>
  );

  return <Alert variant="warning" message={message} />;
};

export default CreateEnvironmentWarning;
