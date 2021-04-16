// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs)]

//! Reserved OIDs through Materialized.
pub const TYPE_LIST_OID: u32 = 16_384;
pub const TYPE_MAP_OID: u32 = 16_385;
pub const FUNC_CEIL_F32_OID: u32 = 16_386;
pub const FUNC_CONCAT_AGG_OID: u32 = 16_387;
pub const FUNC_CSV_EXTRACT_OID: u32 = 16_388;
pub const FUNC_CURRENT_TIMESTAMP_OID: u32 = 16_389;
pub const FUNC_FLOOR_F32_OID: u32 = 16_390;
pub const FUNC_INTERNAL_READ_CACHED_DATA_OID: u32 = 16_391;
pub const FUNC_LIST_APPEND_OID: u32 = 16_392;
pub const FUNC_LIST_CAT_OID: u32 = 16_393;
pub const FUNC_LIST_LENGTH_MAX_OID: u32 = 16_394;
pub const FUNC_LIST_LENGTH_OID: u32 = 16_395;
pub const FUNC_LIST_NDIMS_OID: u32 = 16_396;
pub const FUNC_LIST_PREPEND_OID: u32 = 16_397;
pub const FUNC_MAX_BOOL_OID: u32 = 16_398;
pub const FUNC_MIN_BOOL_OID: u32 = 16_399;
pub const FUNC_MZ_ALL_OID: u32 = 16_400;
pub const FUNC_MZ_ANY_OID: u32 = 16_401;
pub const FUNC_MZ_AVG_PROMOTION_DECIMAL_OID: u32 = 16_402;
pub const FUNC_MZ_AVG_PROMOTION_F32_OID: u32 = 16_403;
pub const FUNC_MZ_AVG_PROMOTION_F64_OID: u32 = 16_404;
pub const FUNC_MZ_AVG_PROMOTION_I32_OID: u32 = 16_405;
pub const FUNC_MZ_CLASSIFY_OBJECT_ID_OID: u32 = 16_406;
pub const FUNC_MZ_CLUSTER_ID_OID: u32 = 16_407;
pub const FUNC_MZ_IS_MATERIALIZED_OID: u32 = 16_408;
pub const FUNC_MZ_LOGICAL_TIMESTAMP_OID: u32 = 16_409;
pub const FUNC_MZ_RENDER_TYPEMOD_OID: u32 = 16_410;
pub const FUNC_MZ_VERSION_OID: u32 = 16_411;
pub const FUNC_REGEXP_EXTRACT_OID: u32 = 16_412;
pub const FUNC_REPEAT_OID: u32 = 16_413;
pub const FUNC_ROUND_F32_OID: u32 = 16_434;
pub const FUNC_UNNEST_LIST_OID: u32 = 16_416;
pub const OP_CONCAT_ELEMENY_LIST_OID: u32 = 16_417;
pub const OP_CONCAT_LIST_ELEMENT_OID: u32 = 16_418;
pub const OP_CONCAT_LIST_LIST_OID: u32 = 16_419;
pub const OP_CONTAINED_JSONB_STRING_OID: u32 = 16_420;
pub const OP_CONTAINED_MAP_MAP_OID: u32 = 16_421;
pub const OP_CONTAINED_STRING_JSONB_OID: u32 = 16_422;
pub const OP_CONTAINS_ALL_KEYS_MAP_OID: u32 = 16_423;
pub const OP_CONTAINS_ANY_KEYS_MAP_OID: u32 = 16_424;
pub const OP_CONTAINS_JSONB_STRING_OID: u32 = 16_425;
pub const OP_CONTAINS_KEY_MAP_OID: u32 = 16_426;
pub const OP_CONTAINS_MAP_MAP_OID: u32 = 16_427;
pub const OP_CONTAINS_STRING_JSONB_OID: u32 = 16_428;
pub const OP_GET_VALUE_MAP_OID: u32 = 16_429;
pub const OP_GET_VALUES_MAP_OID: u32 = 16_430;
pub const OP_MOD_F32_OID: u32 = 16_431;
pub const OP_MOD_F64_OID: u32 = 16_432;
pub const OP_UNARY_PLUS_OID: u32 = 16_433;
pub const FUNC_MZ_SLEEP_OID: u32 = 16_434;
pub const FUNC_MZ_SESSION_ID_OID: u32 = 16_435;
pub const FUNC_MZ_UPTIME_OID: u32 = 16_436;
pub const FUNC_MZ_WORKERS_OID: u32 = 16_437;
pub const TYPE_RDN_OID: u32 = 16_438;
