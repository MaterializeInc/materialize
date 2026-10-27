// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

enum MaterializeErrorCode {
  ORGANIZATION_BLOCKED = "MZ010",
  NETWORK_POLICY_SESSION_DENIED = "MZ011",
}

export default MaterializeErrorCode;
