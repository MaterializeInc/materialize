# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test exercises JSONB at the boundary (e.g., by sending it through
# pgwire). Operations on JSONB are more thoroughly tested in jsonb.slt.

> VALUES
  ('1'::jsonb),
  ('["a", "b"]'::jsonb),
  ('{"c": ["d"]}'::jsonb),
  ('null'::jsonb),
  (NULL::jsonb)
"1.0"
"[\"a\",\"b\"]"
"{\"c\":[\"d\"]}"
"null"
"<null>"
