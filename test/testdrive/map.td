# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT * FROM mz_map_types;

> CREATE TYPE bool AS MAP (key_type=text, value_type='int4');

> CREATE TYPE custom AS MAP (key_type='text', value_type=bool);

# Without qualifiers, should default to builtin bool.
> SELECT mz_internal.mz_classify_object_id(value_id) FROM mz_map_types WHERE name = 'custom'
system

> CREATE TYPE another_custom AS MAP (key_type='text', value_type=public.bool);

# Qualified name should point to user-defined bool.
> SELECT mz_internal.mz_classify_object_id(value_id) FROM mz_map_types WHERE name = 'another_custom'
user
