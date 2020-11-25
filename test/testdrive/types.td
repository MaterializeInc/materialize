# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Without EXTENDED, SHOW TYPES shouldn't have anything to return.
> SHOW TYPES

> SHOW FULL TYPES

> SHOW EXTENDED TYPES
name
----
_bool
_bytea
_date
_float4
_float8
_int4
_int8
_interval
_jsonb
_numeric
_oid
_record
_text
_time
_timestamp
_timestamptz
_uuid
bool
bytea
date
float4
float8
int4
int8
interval
jsonb
list
map
numeric
oid
record
text
time
timestamp
timestamptz
uuid

> SHOW EXTENDED FULL TYPES
name             type
----------------------
_bool            system
_bytea           system
_date            system
_float4          system
_float8          system
_int4            system
_int8            system
_interval        system
_jsonb           system
_numeric         system
_oid             system
_record          system
_text            system
_time            system
_timestamp       system
_timestamptz     system
_uuid            system
bool             system
bytea            system
date             system
float4           system
float8           system
int4             system
int8             system
interval         system
jsonb            system
list             system
map              system
numeric          system
oid              system
record           system
text             system
time             system
timestamp        system
timestamptz      system
uuid             system

# User-defined types

> CREATE TYPE int_list_c AS LIST (element_type=int4);
> CREATE TYPE int_map_c AS MAP (key_type=text, value_type=int4);

# User-defined types can use other user-defined types
> CREATE TYPE int_list_list_c AS LIST (element_type=int_list_c);

> SHOW FULL TYPES
name            type
---------------------
int_list_c      user
int_map_c       user
int_list_list_c user
