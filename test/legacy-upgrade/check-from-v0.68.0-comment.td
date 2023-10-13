# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT object_type, object_sub_id, comment FROM mz_internal.mz_comments;
"table" "<null>" "foo"
"table" "1" "load_bearing"
"view" "<null>" "subset_of_comment_table"
