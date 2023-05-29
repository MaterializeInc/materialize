# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE CLUSTER storage_cluster REPLICAS (
    r1 (
      STORAGECTL ADDRESSES ['clusterd1:2100'],
      STORAGE ADDRESSES ['clusterd1:2103'],
      COMPUTECTL ADDRESSES ['clusterd1:2101'],
      COMPUTE ADDRESSES ['clusterd1:2102'],
      WORKERS 4
    )
  )
