# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# CREATE a view containing LIST

> CREATE MATERIALIZED VIEW list_view AS SELECT LIST[[1,2],[3]] AS f1, LIST[['1','2'],['abc']] AS f2;
