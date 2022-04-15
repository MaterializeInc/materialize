// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Protobuf structs mirroring [`crate::scalar::func`].

use crate::scalar::func::{UnaryFunc, UnmaterializableFunc};
use mz_repr::proto::TryFromProtoError;

include!(concat!(env!("OUT_DIR"), "/scalar.func.rs"));

impl From<&UnmaterializableFunc> for ProtoUnmaterializableFunc {
    fn from(func: &UnmaterializableFunc) -> Self {
        use proto_unmaterializable_func::Kind::*;
        let kind = match func {
            UnmaterializableFunc::CurrentDatabase => CurrentDatabase(()),
            UnmaterializableFunc::CurrentSchemasWithSystem => CurrentSchemasWithSystem(()),
            UnmaterializableFunc::CurrentSchemasWithoutSystem => CurrentSchemasWithoutSystem(()),
            UnmaterializableFunc::CurrentTimestamp => CurrentTimestamp(()),
            UnmaterializableFunc::CurrentUser => CurrentUser(()),
            UnmaterializableFunc::MzClusterId => MzClusterId(()),
            UnmaterializableFunc::MzLogicalTimestamp => MzLogicalTimestamp(()),
            UnmaterializableFunc::MzSessionId => MzSessionId(()),
            UnmaterializableFunc::MzUptime => MzUptime(()),
            UnmaterializableFunc::MzVersion => MzVersion(()),
            UnmaterializableFunc::PgBackendPid => PgBackendPid(()),
            UnmaterializableFunc::PgPostmasterStartTime => PgPostmasterStartTime(()),
            UnmaterializableFunc::Version => Version(()),
        };
        ProtoUnmaterializableFunc { kind: Some(kind) }
    }
}

impl TryFrom<ProtoUnmaterializableFunc> for UnmaterializableFunc {
    type Error = TryFromProtoError;

    fn try_from(func: ProtoUnmaterializableFunc) -> Result<Self, Self::Error> {
        use proto_unmaterializable_func::Kind::*;
        if let Some(kind) = func.kind {
            match kind {
                CurrentDatabase(()) => Ok(UnmaterializableFunc::CurrentDatabase),
                CurrentSchemasWithSystem(()) => Ok(UnmaterializableFunc::CurrentSchemasWithSystem),
                CurrentSchemasWithoutSystem(()) => {
                    Ok(UnmaterializableFunc::CurrentSchemasWithoutSystem)
                }
                CurrentTimestamp(()) => Ok(UnmaterializableFunc::CurrentTimestamp),
                CurrentUser(()) => Ok(UnmaterializableFunc::CurrentUser),
                MzClusterId(()) => Ok(UnmaterializableFunc::MzClusterId),
                MzLogicalTimestamp(()) => Ok(UnmaterializableFunc::MzLogicalTimestamp),
                MzSessionId(()) => Ok(UnmaterializableFunc::MzSessionId),
                MzUptime(()) => Ok(UnmaterializableFunc::MzUptime),
                MzVersion(()) => Ok(UnmaterializableFunc::MzVersion),
                PgBackendPid(()) => Ok(UnmaterializableFunc::PgBackendPid),
                PgPostmasterStartTime(()) => Ok(UnmaterializableFunc::PgPostmasterStartTime),
                Version(()) => Ok(UnmaterializableFunc::Version),
            }
        } else {
            Err(TryFromProtoError::missing_field(
                "`ProtoUnmaterializableFunc::kind`",
            ))
        }
    }
}

impl From<&UnaryFunc> for ProtoUnaryFunc {
    #[allow(unused_variables)]
    #[allow(clippy::todo)]
    fn from(func: &UnaryFunc) -> Self {
        use proto_unary_func::Kind::*;
        let kind = match func {
            UnaryFunc::Not(_) => Not(()),
            UnaryFunc::IsNull(_) => IsNull(()),
            UnaryFunc::IsTrue(_) => IsTrue(()),
            UnaryFunc::IsFalse(_) => IsFalse(()),
            UnaryFunc::BitNotInt16(_) => BitNotInt16(()),
            UnaryFunc::BitNotInt32(_) => BitNotInt32(()),
            UnaryFunc::BitNotInt64(_) => BitNotInt64(()),
            UnaryFunc::NegInt16(_) => NegInt16(()),
            UnaryFunc::NegInt32(_) => NegInt32(()),
            UnaryFunc::NegInt64(_) => NegInt64(()),
            UnaryFunc::NegFloat32(_) => NegFloat32(()),
            UnaryFunc::NegFloat64(_) => NegFloat64(()),
            UnaryFunc::NegNumeric(_) => NegNumeric(()),
            UnaryFunc::NegInterval(_) => NegInterval(()),
            UnaryFunc::SqrtFloat64(_) => SqrtFloat64(()),
            UnaryFunc::SqrtNumeric(_) => SqrtNumeric(()),
            UnaryFunc::CbrtFloat64(_) => CbrtFloat64(()),
            UnaryFunc::AbsInt16(_) => AbsInt16(()),
            UnaryFunc::AbsInt32(_) => AbsInt32(()),
            UnaryFunc::AbsInt64(_) => AbsInt64(()),
            UnaryFunc::AbsFloat32(_) => AbsFloat32(()),
            UnaryFunc::AbsFloat64(_) => AbsFloat64(()),
            UnaryFunc::AbsNumeric(_) => AbsNumeric(()),
            UnaryFunc::CastBoolToString(_) => CastBoolToString(()),
            UnaryFunc::CastBoolToStringNonstandard(_) => CastBoolToStringNonstandard(()),
            UnaryFunc::CastBoolToInt32(_) => CastBoolToInt32(()),
            UnaryFunc::CastInt16ToFloat32(_) => CastInt16ToFloat32(()),
            UnaryFunc::CastInt16ToFloat64(_) => CastInt16ToFloat64(()),
            UnaryFunc::CastInt16ToInt32(_) => CastInt16ToInt32(()),
            UnaryFunc::CastInt16ToInt64(_) => CastInt16ToInt64(()),
            UnaryFunc::CastInt16ToString(_) => CastInt16ToString(()),
            UnaryFunc::CastInt2VectorToArray(_) => CastInt2VectorToArray(()),
            UnaryFunc::CastInt32ToBool(_) => CastInt32ToBool(()),
            UnaryFunc::CastInt32ToFloat32(_) => CastInt32ToFloat32(()),
            UnaryFunc::CastInt32ToFloat64(_) => CastInt32ToFloat64(()),
            UnaryFunc::CastInt32ToOid(_) => CastInt32ToOid(()),
            UnaryFunc::CastInt32ToPgLegacyChar(_) => CastInt32ToPgLegacyChar(()),
            UnaryFunc::CastInt32ToInt16(_) => CastInt32ToInt16(()),
            UnaryFunc::CastInt32ToInt64(_) => CastInt32ToInt64(()),
            UnaryFunc::CastInt32ToString(_) => CastInt32ToString(()),
            UnaryFunc::CastOidToInt32(_) => CastOidToInt32(()),
            UnaryFunc::CastOidToInt64(_) => CastOidToInt64(()),
            UnaryFunc::CastOidToString(_) => CastOidToString(()),
            UnaryFunc::CastOidToRegClass(_) => CastOidToRegClass(()),
            UnaryFunc::CastRegClassToOid(_) => CastRegClassToOid(()),
            UnaryFunc::CastOidToRegProc(_) => CastOidToRegProc(()),
            UnaryFunc::CastRegProcToOid(_) => CastRegProcToOid(()),
            UnaryFunc::CastOidToRegType(_) => CastOidToRegType(()),
            UnaryFunc::CastRegTypeToOid(_) => CastRegTypeToOid(()),
            UnaryFunc::CastInt64ToInt16(_) => CastInt64ToInt16(()),
            UnaryFunc::CastInt64ToInt32(_) => CastInt64ToInt32(()),
            UnaryFunc::CastInt16ToNumeric(func) => CastInt16ToNumeric((&func.0).into()),
            UnaryFunc::CastInt32ToNumeric(func) => CastInt32ToNumeric((&func.0).into()),
            UnaryFunc::CastInt64ToBool(_) => CastInt64ToBool(()),
            UnaryFunc::CastInt64ToNumeric(func) => CastInt64ToNumeric((&func.0).into()),
            UnaryFunc::CastInt64ToFloat32(_) => CastInt64ToFloat32(()),
            UnaryFunc::CastInt64ToFloat64(_) => CastInt64ToFloat64(()),
            UnaryFunc::CastInt64ToOid(_) => CastInt64ToOid(()),
            UnaryFunc::CastInt64ToString(_) => CastInt64ToString(()),
            UnaryFunc::CastFloat32ToInt16(_) => CastFloat32ToInt16(()),
            UnaryFunc::CastFloat32ToInt32(_) => CastFloat32ToInt32(()),
            UnaryFunc::CastFloat32ToInt64(_) => CastFloat32ToInt64(()),
            UnaryFunc::CastFloat32ToFloat64(_) => CastFloat32ToFloat64(()),
            UnaryFunc::CastFloat32ToString(_) => CastFloat32ToString(()),
            UnaryFunc::CastFloat32ToNumeric(func) => CastFloat32ToNumeric((&func.0).into()),
            UnaryFunc::CastFloat64ToNumeric(func) => CastFloat64ToNumeric((&func.0).into()),
            UnaryFunc::CastFloat64ToInt16(_) => CastFloat64ToInt16(()),
            UnaryFunc::CastFloat64ToInt32(_) => CastFloat64ToInt32(()),
            UnaryFunc::CastFloat64ToInt64(_) => CastFloat64ToInt64(()),
            UnaryFunc::CastFloat64ToFloat32(_) => CastFloat64ToFloat32(()),
            UnaryFunc::CastFloat64ToString(_) => CastFloat64ToString(()),
            UnaryFunc::CastNumericToFloat32(_) => CastNumericToFloat32(()),
            UnaryFunc::CastNumericToFloat64(_) => CastNumericToFloat64(()),
            UnaryFunc::CastNumericToInt16(_) => CastNumericToInt16(()),
            UnaryFunc::CastNumericToInt32(_) => CastNumericToInt32(()),
            UnaryFunc::CastNumericToInt64(_) => CastNumericToInt64(()),
            UnaryFunc::CastNumericToString(_) => CastNumericToString(()),
            UnaryFunc::CastStringToBool(_) => CastStringToBool(()),
            UnaryFunc::CastStringToPgLegacyChar(_) => CastStringToPgLegacyChar(()),
            UnaryFunc::CastStringToBytes(_) => CastStringToBytes(()),
            UnaryFunc::CastStringToInt16(_) => CastStringToInt16(()),
            UnaryFunc::CastStringToInt32(_) => CastStringToInt32(()),
            UnaryFunc::CastStringToInt64(_) => CastStringToInt64(()),
            UnaryFunc::CastStringToInt2Vector(_) => CastStringToInt2Vector(()),
            UnaryFunc::CastStringToOid(_) => CastStringToOid(()),
            UnaryFunc::CastStringToFloat32(_) => CastStringToFloat32(()),
            UnaryFunc::CastStringToFloat64(_) => CastStringToFloat64(()),
            UnaryFunc::CastStringToDate(_) => CastStringToDate(()),
            UnaryFunc::CastStringToArray(_) => todo!(),
            UnaryFunc::CastStringToList(_) => todo!(),
            UnaryFunc::CastStringToMap(_) => todo!(),
            UnaryFunc::CastStringToTime(_) => CastStringToTime(()),
            UnaryFunc::CastStringToTimestamp(_) => CastStringToTimestamp(()),
            UnaryFunc::CastStringToTimestampTz(_) => CastStringToTimestampTz(()),
            UnaryFunc::CastStringToInterval(_) => CastStringToInterval(()),
            UnaryFunc::CastStringToNumeric(func) => CastStringToNumeric((&func.0).into()),
            UnaryFunc::CastStringToUuid(_) => CastStringToUuid(()),
            UnaryFunc::CastStringToChar(_) => todo!(),
            UnaryFunc::PadChar(_) => todo!(),
            UnaryFunc::CastStringToVarChar(_) => todo!(),
            UnaryFunc::CastCharToString(_) => CastCharToString(()),
            UnaryFunc::CastVarCharToString(_) => CastVarCharToString(()),
            UnaryFunc::CastDateToTimestamp(_) => CastDateToTimestamp(()),
            UnaryFunc::CastDateToTimestampTz(_) => CastDateToTimestampTz(()),
            UnaryFunc::CastDateToString(_) => CastDateToString(()),
            UnaryFunc::CastTimeToInterval(_) => CastTimeToInterval(()),
            UnaryFunc::CastTimeToString(_) => CastTimeToString(()),
            UnaryFunc::CastIntervalToString(_) => CastIntervalToString(()),
            UnaryFunc::CastIntervalToTime(_) => CastIntervalToTime(()),
            UnaryFunc::CastTimestampToDate(_) => CastTimestampToDate(()),
            UnaryFunc::CastTimestampToTimestampTz(_) => CastTimestampToTimestampTz(()),
            UnaryFunc::CastTimestampToString(_) => CastTimestampToString(()),
            UnaryFunc::CastTimestampToTime(_) => CastTimestampToTime(()),
            UnaryFunc::CastTimestampTzToDate(_) => CastTimestampTzToDate(()),
            UnaryFunc::CastTimestampTzToTimestamp(_) => CastTimestampTzToTimestamp(()),
            UnaryFunc::CastTimestampTzToString(_) => CastTimestampTzToString(()),
            UnaryFunc::CastTimestampTzToTime(_) => CastTimestampTzToTime(()),
            UnaryFunc::CastPgLegacyCharToString(_) => CastPgLegacyCharToString(()),
            UnaryFunc::CastPgLegacyCharToInt32(_) => CastPgLegacyCharToInt32(()),
            UnaryFunc::CastBytesToString(_) => CastBytesToString(()),
            UnaryFunc::CastStringToJsonb(_) => todo!(),
            UnaryFunc::CastJsonbToString(_) => todo!(),
            UnaryFunc::CastJsonbOrNullToJsonb(_) => todo!(),
            UnaryFunc::CastJsonbToInt16(_) => todo!(),
            UnaryFunc::CastJsonbToInt32(_) => todo!(),
            UnaryFunc::CastJsonbToInt64(_) => todo!(),
            UnaryFunc::CastJsonbToFloat32(_) => todo!(),
            UnaryFunc::CastJsonbToFloat64(_) => todo!(),
            UnaryFunc::CastJsonbToNumeric(_) => todo!(),
            UnaryFunc::CastJsonbToBool(_) => todo!(),
            UnaryFunc::CastUuidToString(_) => todo!(),
            UnaryFunc::CastRecordToString { ty } => todo!(),
            UnaryFunc::CastRecord1ToRecord2 {
                return_ty,
                cast_exprs,
            } => todo!(),
            UnaryFunc::CastArrayToString { ty } => todo!(),
            UnaryFunc::CastListToString { ty } => todo!(),
            UnaryFunc::CastList1ToList2 {
                return_ty,
                cast_expr,
            } => todo!(),
            UnaryFunc::CastArrayToListOneDim(_) => todo!(),
            UnaryFunc::CastMapToString { ty } => todo!(),
            UnaryFunc::CastInt2VectorToString => todo!(),
            UnaryFunc::CeilFloat32(_) => todo!(),
            UnaryFunc::CeilFloat64(_) => todo!(),
            UnaryFunc::CeilNumeric(_) => todo!(),
            UnaryFunc::FloorFloat32(_) => todo!(),
            UnaryFunc::FloorFloat64(_) => todo!(),
            UnaryFunc::FloorNumeric(_) => todo!(),
            UnaryFunc::Ascii => todo!(),
            UnaryFunc::BitLengthBytes => todo!(),
            UnaryFunc::BitLengthString => todo!(),
            UnaryFunc::ByteLengthBytes => todo!(),
            UnaryFunc::ByteLengthString => todo!(),
            UnaryFunc::CharLength => todo!(),
            UnaryFunc::Chr(_) => todo!(),
            UnaryFunc::IsLikeMatch(_) => todo!(),
            UnaryFunc::IsRegexpMatch(_) => todo!(),
            UnaryFunc::RegexpMatch(_) => todo!(),
            UnaryFunc::ExtractInterval(_) => todo!(),
            UnaryFunc::ExtractTime(_) => todo!(),
            UnaryFunc::ExtractTimestamp(_) => todo!(),
            UnaryFunc::ExtractTimestampTz(_) => todo!(),
            UnaryFunc::ExtractDate(_) => todo!(),
            UnaryFunc::DatePartInterval(_) => todo!(),
            UnaryFunc::DatePartTime(_) => todo!(),
            UnaryFunc::DatePartTimestamp(_) => todo!(),
            UnaryFunc::DatePartTimestampTz(_) => todo!(),
            UnaryFunc::DateTruncTimestamp(_) => todo!(),
            UnaryFunc::DateTruncTimestampTz(_) => todo!(),
            UnaryFunc::TimezoneTimestamp(_) => todo!(),
            UnaryFunc::TimezoneTimestampTz(_) => todo!(),
            UnaryFunc::TimezoneTime { tz, wall_time } => todo!(),
            UnaryFunc::ToTimestamp(_) => todo!(),
            UnaryFunc::JustifyDays(_) => todo!(),
            UnaryFunc::JustifyHours(_) => todo!(),
            UnaryFunc::JustifyInterval(_) => todo!(),
            UnaryFunc::JsonbArrayLength => todo!(),
            UnaryFunc::JsonbTypeof => todo!(),
            UnaryFunc::JsonbStripNulls => todo!(),
            UnaryFunc::JsonbPretty => todo!(),
            UnaryFunc::RoundFloat32(_) => todo!(),
            UnaryFunc::RoundFloat64(_) => todo!(),
            UnaryFunc::RoundNumeric(_) => todo!(),
            UnaryFunc::TrimWhitespace => todo!(),
            UnaryFunc::TrimLeadingWhitespace => todo!(),
            UnaryFunc::TrimTrailingWhitespace => todo!(),
            UnaryFunc::RecordGet(_) => todo!(),
            UnaryFunc::ListLength => todo!(),
            UnaryFunc::MapLength => todo!(),
            UnaryFunc::Upper => todo!(),
            UnaryFunc::Lower => todo!(),
            UnaryFunc::Cos(_) => todo!(),
            UnaryFunc::Acos(_) => todo!(),
            UnaryFunc::Cosh(_) => todo!(),
            UnaryFunc::Acosh(_) => todo!(),
            UnaryFunc::Sin(_) => todo!(),
            UnaryFunc::Asin(_) => todo!(),
            UnaryFunc::Sinh(_) => todo!(),
            UnaryFunc::Asinh(_) => todo!(),
            UnaryFunc::Tan(_) => todo!(),
            UnaryFunc::Atan(_) => todo!(),
            UnaryFunc::Tanh(_) => todo!(),
            UnaryFunc::Atanh(_) => todo!(),
            UnaryFunc::Cot(_) => todo!(),
            UnaryFunc::Degrees(_) => todo!(),
            UnaryFunc::Radians(_) => todo!(),
            UnaryFunc::Log10(_) => todo!(),
            UnaryFunc::Log10Numeric(_) => todo!(),
            UnaryFunc::Ln(_) => todo!(),
            UnaryFunc::LnNumeric(_) => todo!(),
            UnaryFunc::Exp(_) => todo!(),
            UnaryFunc::ExpNumeric(_) => todo!(),
            UnaryFunc::Sleep(_) => todo!(),
            UnaryFunc::RescaleNumeric(_) => todo!(),
            UnaryFunc::PgColumnSize(_) => todo!(),
            UnaryFunc::MzRowSize(_) => todo!(),
            UnaryFunc::MzTypeName(_) => todo!(),
        };
        ProtoUnaryFunc { kind: Some(kind) }
    }
}

impl TryFrom<ProtoUnaryFunc> for UnaryFunc {
    type Error = TryFromProtoError;

    #[allow(clippy::todo)]
    fn try_from(func: ProtoUnaryFunc) -> Result<Self, Self::Error> {
        use crate::scalar::func::impls;
        use proto_unary_func::Kind::*;
        if let Some(kind) = func.kind {
            match kind {
                Not(()) => Ok(UnaryFunc::Not(impls::Not)),
                IsNull(()) => Ok(UnaryFunc::IsNull(impls::IsNull)),
                IsTrue(()) => Ok(UnaryFunc::IsTrue(impls::IsTrue)),
                IsFalse(()) => Ok(UnaryFunc::IsFalse(impls::IsFalse)),
                BitNotInt16(()) => Ok(UnaryFunc::BitNotInt16(impls::BitNotInt16)),
                BitNotInt32(()) => Ok(UnaryFunc::BitNotInt32(impls::BitNotInt32)),
                BitNotInt64(()) => Ok(UnaryFunc::BitNotInt64(impls::BitNotInt64)),
                NegInt16(()) => Ok(UnaryFunc::NegInt16(impls::NegInt16)),
                NegInt32(()) => Ok(UnaryFunc::NegInt32(impls::NegInt32)),
                NegInt64(()) => Ok(UnaryFunc::NegInt64(impls::NegInt64)),
                NegFloat32(()) => Ok(UnaryFunc::NegFloat32(impls::NegFloat32)),
                NegFloat64(()) => Ok(UnaryFunc::NegFloat64(impls::NegFloat64)),
                NegNumeric(()) => Ok(UnaryFunc::NegNumeric(impls::NegNumeric)),
                NegInterval(()) => Ok(UnaryFunc::NegInterval(impls::NegInterval)),
                SqrtFloat64(()) => Ok(UnaryFunc::SqrtFloat64(impls::SqrtFloat64)),
                SqrtNumeric(()) => Ok(UnaryFunc::SqrtNumeric(impls::SqrtNumeric)),
                CbrtFloat64(()) => Ok(UnaryFunc::CbrtFloat64(impls::CbrtFloat64)),
                AbsInt16(()) => Ok(UnaryFunc::AbsInt16(impls::AbsInt16)),
                AbsInt32(()) => Ok(UnaryFunc::AbsInt32(impls::AbsInt32)),
                AbsInt64(()) => Ok(UnaryFunc::AbsInt64(impls::AbsInt64)),
                AbsFloat32(()) => Ok(UnaryFunc::AbsFloat32(impls::AbsFloat32)),
                AbsFloat64(()) => Ok(UnaryFunc::AbsFloat64(impls::AbsFloat64)),
                AbsNumeric(()) => Ok(UnaryFunc::AbsNumeric(impls::AbsNumeric)),
                CastBoolToString(()) => Ok(UnaryFunc::CastBoolToString(impls::CastBoolToString)),
                CastBoolToStringNonstandard(()) => Ok(UnaryFunc::CastBoolToStringNonstandard(
                    impls::CastBoolToStringNonstandard,
                )),
                CastBoolToInt32(()) => Ok(UnaryFunc::CastBoolToInt32(impls::CastBoolToInt32)),
                CastInt16ToFloat32(()) => {
                    Ok(UnaryFunc::CastInt16ToFloat32(impls::CastInt16ToFloat32))
                }
                CastInt16ToFloat64(()) => {
                    Ok(UnaryFunc::CastInt16ToFloat64(impls::CastInt16ToFloat64))
                }
                CastInt16ToInt32(()) => Ok(UnaryFunc::CastInt16ToInt32(impls::CastInt16ToInt32)),
                CastInt16ToInt64(()) => Ok(UnaryFunc::CastInt16ToInt64(impls::CastInt16ToInt64)),
                CastInt16ToString(()) => Ok(UnaryFunc::CastInt16ToString(impls::CastInt16ToString)),
                CastInt2VectorToArray(()) => Ok(UnaryFunc::CastInt2VectorToArray(
                    impls::CastInt2VectorToArray,
                )),
                CastInt32ToBool(()) => Ok(UnaryFunc::CastInt32ToBool(impls::CastInt32ToBool)),
                CastInt32ToFloat32(()) => {
                    Ok(UnaryFunc::CastInt32ToFloat32(impls::CastInt32ToFloat32))
                }
                CastInt32ToFloat64(()) => {
                    Ok(UnaryFunc::CastInt32ToFloat64(impls::CastInt32ToFloat64))
                }
                CastInt32ToOid(()) => Ok(UnaryFunc::CastInt32ToOid(impls::CastInt32ToOid)),
                CastInt32ToPgLegacyChar(()) => Ok(UnaryFunc::CastInt32ToPgLegacyChar(
                    impls::CastInt32ToPgLegacyChar,
                )),
                CastInt32ToInt16(()) => Ok(UnaryFunc::CastInt32ToInt16(impls::CastInt32ToInt16)),
                CastInt32ToInt64(()) => Ok(UnaryFunc::CastInt32ToInt64(impls::CastInt32ToInt64)),
                CastInt32ToString(()) => Ok(UnaryFunc::CastInt32ToString(impls::CastInt32ToString)),
                CastOidToInt32(()) => Ok(UnaryFunc::CastOidToInt32(impls::CastOidToInt32)),
                CastOidToInt64(()) => Ok(UnaryFunc::CastOidToInt64(impls::CastOidToInt64)),
                CastOidToString(()) => Ok(UnaryFunc::CastOidToString(impls::CastOidToString)),
                CastOidToRegClass(()) => Ok(UnaryFunc::CastOidToRegClass(impls::CastOidToRegClass)),
                CastRegClassToOid(()) => Ok(UnaryFunc::CastRegClassToOid(impls::CastRegClassToOid)),
                CastOidToRegProc(()) => Ok(UnaryFunc::CastOidToRegProc(impls::CastOidToRegProc)),
                CastRegProcToOid(()) => Ok(UnaryFunc::CastRegProcToOid(impls::CastRegProcToOid)),
                CastOidToRegType(()) => Ok(UnaryFunc::CastOidToRegType(impls::CastOidToRegType)),
                CastRegTypeToOid(()) => Ok(UnaryFunc::CastRegTypeToOid(impls::CastRegTypeToOid)),
                CastInt64ToInt16(()) => Ok(UnaryFunc::CastInt64ToInt16(impls::CastInt64ToInt16)),
                CastInt64ToInt32(()) => Ok(UnaryFunc::CastInt64ToInt32(impls::CastInt64ToInt32)),
                CastInt16ToNumeric(max_scale) => Ok(UnaryFunc::CastInt16ToNumeric(
                    impls::CastInt16ToNumeric(max_scale.try_into()?),
                )),
                CastInt32ToNumeric(max_scale) => Ok(UnaryFunc::CastInt32ToNumeric(
                    impls::CastInt32ToNumeric(max_scale.try_into()?),
                )),
                CastInt64ToBool(()) => Ok(UnaryFunc::CastInt64ToBool(impls::CastInt64ToBool)),
                CastInt64ToNumeric(max_scale) => Ok(UnaryFunc::CastInt64ToNumeric(
                    impls::CastInt64ToNumeric(max_scale.try_into()?),
                )),
                CastInt64ToFloat32(()) => {
                    Ok(UnaryFunc::CastInt64ToFloat32(impls::CastInt64ToFloat32))
                }
                CastInt64ToFloat64(()) => {
                    Ok(UnaryFunc::CastInt64ToFloat64(impls::CastInt64ToFloat64))
                }
                CastInt64ToOid(()) => Ok(UnaryFunc::CastInt64ToOid(impls::CastInt64ToOid)),
                CastInt64ToString(()) => Ok(UnaryFunc::CastInt64ToString(impls::CastInt64ToString)),
                CastFloat32ToInt16(()) => {
                    Ok(UnaryFunc::CastFloat32ToInt16(impls::CastFloat32ToInt16))
                }
                CastFloat32ToInt32(()) => {
                    Ok(UnaryFunc::CastFloat32ToInt32(impls::CastFloat32ToInt32))
                }
                CastFloat32ToInt64(()) => {
                    Ok(UnaryFunc::CastFloat32ToInt64(impls::CastFloat32ToInt64))
                }
                CastFloat32ToFloat64(()) => {
                    Ok(UnaryFunc::CastFloat32ToFloat64(impls::CastFloat32ToFloat64))
                }
                CastFloat32ToString(()) => {
                    Ok(UnaryFunc::CastFloat32ToString(impls::CastFloat32ToString))
                }
                CastFloat32ToNumeric(max_scale) => Ok(UnaryFunc::CastFloat32ToNumeric(
                    impls::CastFloat32ToNumeric(max_scale.try_into()?),
                )),
                CastFloat64ToNumeric(max_scale) => Ok(UnaryFunc::CastFloat64ToNumeric(
                    impls::CastFloat64ToNumeric(max_scale.try_into()?),
                )),
                CastFloat64ToInt16(()) => {
                    Ok(UnaryFunc::CastFloat64ToInt16(impls::CastFloat64ToInt16))
                }
                CastFloat64ToInt32(()) => {
                    Ok(UnaryFunc::CastFloat64ToInt32(impls::CastFloat64ToInt32))
                }
                CastFloat64ToInt64(()) => {
                    Ok(UnaryFunc::CastFloat64ToInt64(impls::CastFloat64ToInt64))
                }
                CastFloat64ToFloat32(()) => {
                    Ok(UnaryFunc::CastFloat64ToFloat32(impls::CastFloat64ToFloat32))
                }
                CastFloat64ToString(()) => {
                    Ok(UnaryFunc::CastFloat64ToString(impls::CastFloat64ToString))
                }
                CastNumericToFloat32(()) => {
                    Ok(UnaryFunc::CastNumericToFloat32(impls::CastNumericToFloat32))
                }
                CastNumericToFloat64(()) => {
                    Ok(UnaryFunc::CastNumericToFloat64(impls::CastNumericToFloat64))
                }
                CastNumericToInt16(()) => {
                    Ok(UnaryFunc::CastNumericToInt16(impls::CastNumericToInt16))
                }
                CastNumericToInt32(()) => {
                    Ok(UnaryFunc::CastNumericToInt32(impls::CastNumericToInt32))
                }
                CastNumericToInt64(()) => {
                    Ok(UnaryFunc::CastNumericToInt64(impls::CastNumericToInt64))
                }
                CastNumericToString(()) => {
                    Ok(UnaryFunc::CastNumericToString(impls::CastNumericToString))
                }
                CastStringToBool(()) => Ok(UnaryFunc::CastStringToBool(impls::CastStringToBool)),
                CastStringToPgLegacyChar(()) => Ok(UnaryFunc::CastStringToPgLegacyChar(
                    impls::CastStringToPgLegacyChar,
                )),
                CastStringToBytes(()) => Ok(UnaryFunc::CastStringToBytes(impls::CastStringToBytes)),
                CastStringToInt16(()) => Ok(UnaryFunc::CastStringToInt16(impls::CastStringToInt16)),
                CastStringToInt32(()) => Ok(UnaryFunc::CastStringToInt32(impls::CastStringToInt32)),
                CastStringToInt64(()) => Ok(UnaryFunc::CastStringToInt64(impls::CastStringToInt64)),
                CastStringToInt2Vector(()) => Ok(UnaryFunc::CastStringToInt2Vector(
                    impls::CastStringToInt2Vector,
                )),
                CastStringToOid(()) => Ok(UnaryFunc::CastStringToOid(impls::CastStringToOid)),
                CastStringToFloat32(()) => {
                    Ok(UnaryFunc::CastStringToFloat32(impls::CastStringToFloat32))
                }
                CastStringToFloat64(()) => {
                    Ok(UnaryFunc::CastStringToFloat64(impls::CastStringToFloat64))
                }
                CastStringToDate(()) => Ok(UnaryFunc::CastStringToDate(impls::CastStringToDate)),
                CastStringToArray(()) => todo!(),
                CastStringToList(()) => todo!(),
                CastStringToMap(()) => todo!(),
                CastStringToTime(()) => Ok(UnaryFunc::CastStringToTime(impls::CastStringToTime)),
                CastStringToTimestamp(()) => Ok(UnaryFunc::CastStringToTimestamp(
                    impls::CastStringToTimestamp,
                )),
                CastStringToTimestampTz(()) => Ok(UnaryFunc::CastStringToTimestampTz(
                    impls::CastStringToTimestampTz,
                )),
                CastStringToInterval(()) => {
                    Ok(UnaryFunc::CastStringToInterval(impls::CastStringToInterval))
                }
                CastStringToNumeric(max_scale) => Ok(UnaryFunc::CastStringToNumeric(
                    impls::CastStringToNumeric(max_scale.try_into()?),
                )),
                CastStringToUuid(()) => Ok(UnaryFunc::CastStringToUuid(impls::CastStringToUuid)),
                CastStringToChar(()) => todo!(),
                PadChar(()) => todo!(),
                CastStringToVarChar(()) => todo!(),
                CastCharToString(()) => Ok(UnaryFunc::CastCharToString(impls::CastCharToString)),
                CastVarCharToString(()) => {
                    Ok(UnaryFunc::CastVarCharToString(impls::CastVarCharToString))
                }
                CastDateToTimestamp(()) => {
                    Ok(UnaryFunc::CastDateToTimestamp(impls::CastDateToTimestamp))
                }
                CastDateToTimestampTz(()) => Ok(UnaryFunc::CastDateToTimestampTz(
                    impls::CastDateToTimestampTz,
                )),
                CastDateToString(()) => Ok(UnaryFunc::CastDateToString(impls::CastDateToString)),
                CastTimeToInterval(()) => {
                    Ok(UnaryFunc::CastTimeToInterval(impls::CastTimeToInterval))
                }
                CastTimeToString(()) => Ok(UnaryFunc::CastTimeToString(impls::CastTimeToString)),
                CastIntervalToString(()) => {
                    Ok(UnaryFunc::CastIntervalToString(impls::CastIntervalToString))
                }
                CastIntervalToTime(()) => {
                    Ok(UnaryFunc::CastIntervalToTime(impls::CastIntervalToTime))
                }
                CastTimestampToDate(()) => {
                    Ok(UnaryFunc::CastTimestampToDate(impls::CastTimestampToDate))
                }
                CastTimestampToTimestampTz(()) => Ok(UnaryFunc::CastTimestampToTimestampTz(
                    impls::CastTimestampToTimestampTz,
                )),
                CastTimestampToString(()) => Ok(UnaryFunc::CastTimestampToString(
                    impls::CastTimestampToString,
                )),
                CastTimestampToTime(()) => {
                    Ok(UnaryFunc::CastTimestampToTime(impls::CastTimestampToTime))
                }
                CastTimestampTzToDate(()) => Ok(UnaryFunc::CastTimestampTzToDate(
                    impls::CastTimestampTzToDate,
                )),
                CastTimestampTzToTimestamp(()) => Ok(UnaryFunc::CastTimestampTzToTimestamp(
                    impls::CastTimestampTzToTimestamp,
                )),
                CastTimestampTzToString(()) => Ok(UnaryFunc::CastTimestampTzToString(
                    impls::CastTimestampTzToString,
                )),
                CastTimestampTzToTime(()) => Ok(UnaryFunc::CastTimestampTzToTime(
                    impls::CastTimestampTzToTime,
                )),
                CastPgLegacyCharToString(()) => Ok(UnaryFunc::CastPgLegacyCharToString(
                    impls::CastPgLegacyCharToString,
                )),
                CastPgLegacyCharToInt32(()) => Ok(UnaryFunc::CastPgLegacyCharToInt32(
                    impls::CastPgLegacyCharToInt32,
                )),
                CastBytesToString(()) => Ok(UnaryFunc::CastBytesToString(impls::CastBytesToString)),
                CastStringToJsonb(_) => todo!(),
                CastJsonbToString(_) => todo!(),
                CastJsonbOrNullToJsonb(_) => todo!(),
                CastJsonbToInt16(_) => todo!(),
                CastJsonbToInt32(_) => todo!(),
                CastJsonbToInt64(_) => todo!(),
                CastJsonbToFloat32(_) => todo!(),
                CastJsonbToFloat64(_) => todo!(),
                CastJsonbToNumeric(_) => todo!(),
                CastJsonbToBool(_) => todo!(),
                CastUuidToString(_) => todo!(),
                CastRecordToString(_) => todo!(),
                CastRecord1ToRecord2(_) => todo!(),
                CastArrayToString(_) => todo!(),
                CastListToString(_) => todo!(),
                CastList1ToList2(_) => todo!(),
                CastArrayToListOneDim(_) => todo!(),
                CastMapToString(_) => todo!(),
                CastInt2VectorToString(_) => todo!(),
                CeilFloat32(_) => todo!(),
                CeilFloat64(_) => todo!(),
                CeilNumeric(_) => todo!(),
                FloorFloat32(_) => todo!(),
                FloorFloat64(_) => todo!(),
                FloorNumeric(_) => todo!(),
                Ascii(_) => todo!(),
                BitLengthBytes(_) => todo!(),
                BitLengthString(_) => todo!(),
                ByteLengthBytes(_) => todo!(),
                ByteLengthString(_) => todo!(),
                CharLength(_) => todo!(),
                Chr(_) => todo!(),
                IsLikeMatch(_) => todo!(),
                IsRegexpMatch(_) => todo!(),
                RegexpMatch(_) => todo!(),
                ExtractInterval(_) => todo!(),
                ExtractTime(_) => todo!(),
                ExtractTimestamp(_) => todo!(),
                ExtractTimestampTz(_) => todo!(),
                ExtractDate(_) => todo!(),
                DatePartInterval(_) => todo!(),
                DatePartTime(_) => todo!(),
                DatePartTimestamp(_) => todo!(),
                DatePartTimestampTz(_) => todo!(),
                DateTruncTimestamp(_) => todo!(),
                DateTruncTimestampTz(_) => todo!(),
                TimezoneTimestamp(_) => todo!(),
                TimezoneTimestampTz(_) => todo!(),
                TimezoneTime(_) => todo!(),
                ToTimestamp(_) => todo!(),
                JustifyDays(_) => todo!(),
                JustifyHours(_) => todo!(),
                JustifyInterval(_) => todo!(),
                JsonbArrayLength(_) => todo!(),
                JsonbTypeof(_) => todo!(),
                JsonbStripNulls(_) => todo!(),
                JsonbPretty(_) => todo!(),
                RoundFloat32(_) => todo!(),
                RoundFloat64(_) => todo!(),
                RoundNumeric(_) => todo!(),
                TrimWhitespace(_) => todo!(),
                TrimLeadingWhitespace(_) => todo!(),
                TrimTrailingWhitespace(_) => todo!(),
                RecordGet(_) => todo!(),
                ListLength(_) => todo!(),
                MapLength(_) => todo!(),
                Upper(_) => todo!(),
                Lower(_) => todo!(),
                Cos(_) => todo!(),
                Acos(_) => todo!(),
                Cosh(_) => todo!(),
                Acosh(_) => todo!(),
                Sin(_) => todo!(),
                Asin(_) => todo!(),
                Sinh(_) => todo!(),
                Asinh(_) => todo!(),
                Tan(_) => todo!(),
                Atan(_) => todo!(),
                Tanh(_) => todo!(),
                Atanh(_) => todo!(),
                Cot(_) => todo!(),
                Degrees(_) => todo!(),
                Radians(_) => todo!(),
                Log10(_) => todo!(),
                Log10Numeric(_) => todo!(),
                Ln(_) => todo!(),
                LnNumeric(_) => todo!(),
                Exp(_) => todo!(),
                ExpNumeric(_) => todo!(),
                Sleep(_) => todo!(),
                RescaleNumeric(_) => todo!(),
                PgColumnSize(_) => todo!(),
                MzRowSize(_) => todo!(),
                MzTypeName(_) => todo!(),
            }
        } else {
            Err(TryFromProtoError::missing_field("`ProtoUnaryFunc::kind`"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::proto::protobuf_roundtrip;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn unmaterializable_func_protobuf_roundtrip(expect in any::<UnmaterializableFunc>()) {
            let actual = protobuf_roundtrip::<_, ProtoUnmaterializableFunc>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }

    proptest! {
        #[test]
        fn unary_func_protobuf_roundtrip(expect in any::<UnaryFunc>()) {
            let actual = protobuf_roundtrip::<_, ProtoUnaryFunc>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
