# Copyright Materialize, Inc. and contributors. All rights reserved.
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
  ('1.0'::jsonb),
  ('["a", "b"]'::jsonb),
  ('{"c": ["d"]}'::jsonb),
  ('null'::jsonb),
  (NULL::jsonb)
"1"
"1"
"[\"a\",\"b\"]"
"{\"c\":[\"d\"]}"
"null"
"<null>"

# Show that we suggest using jsonb functions when presented with json functions
! SELECT json_build_object('id', 1)
contains:exist
hint:Try using jsonb_build_object

! SELECT json_agg(1)
contains:exist
hint:Try using jsonb_agg

# This is the generic hint, showing we haven't populated it with the jsonb suggestion
! SELECT json_without_corresponding_jsonb(1)
contains:exist
hint:No function matches the given name and argument type
