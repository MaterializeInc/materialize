// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generate function documentation from macros.

use std::collections::BTreeMap;

use mz_expr::func::{AddInt16, BinaryFuncKind, FuncDoc, ListLengthMax};
use serde::Serialize;

fn main() {
    let mut categories: BTreeMap<String, Category> = BTreeMap::default();

    for func in binary_func_kinds() {
        let Some(doc) = binary_func_doc_for(func) else {
            continue;
        };
        let function = Function {
            signature: doc.signature.to_string(),
            description: doc.description.to_string(),
            url: None,
            version_added: None,
            unmaterializable: false,
            known_time_zone_limitation_cast: false,
            side_effecting: false,
        };
        categories
            .entry(doc.category.to_string())
            .or_insert_with(|| Category {
                r#type: doc.category.to_string(),
                description: "".to_string(),
                functions: Default::default(),
            })
            .functions
            .push(function);
    }

    for category in categories.values_mut() {
        category.functions.sort();
    }

    let categories = categories.into_values().collect::<Vec<_>>();

    let json = serde_json::to_string_pretty(&categories).expect("can serialize");
    println!("{json}\n");
}

#[derive(Debug, Serialize, Ord, PartialOrd, Eq, PartialEq)]
struct Category {
    r#type: String,
    description: String,
    functions: Vec<Function>,
}

#[derive(Debug, Serialize, Ord, PartialOrd, Eq, PartialEq)]
struct Function {
    signature: String,
    description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version_added: Option<String>,
    unmaterializable: bool,
    known_time_zone_limitation_cast: bool,
    side_effecting: bool,
}

fn binary_func_doc_for(func: &BinaryFuncKind) -> Option<FuncDoc> {
    match func {
        BinaryFuncKind::AddInt16 => return Some(AddInt16::func_doc()),
        BinaryFuncKind::AddInt32 => {}
        BinaryFuncKind::AddInt64 => {}
        BinaryFuncKind::AddUint16 => {}
        BinaryFuncKind::AddUint32 => {}
        BinaryFuncKind::AddUint64 => {}
        BinaryFuncKind::AddFloat32 => {}
        BinaryFuncKind::AddFloat64 => {}
        BinaryFuncKind::AddInterval => {}
        BinaryFuncKind::AddTimestampInterval => {}
        BinaryFuncKind::AddTimestampTzInterval => {}
        BinaryFuncKind::AddDateInterval => {}
        BinaryFuncKind::AddDateTime => {}
        BinaryFuncKind::AddTimeInterval => {}
        BinaryFuncKind::AddNumeric => {}
        BinaryFuncKind::AgeTimestamp => {}
        BinaryFuncKind::AgeTimestampTz => {}
        BinaryFuncKind::BitAndInt16 => {}
        BinaryFuncKind::BitAndInt32 => {}
        BinaryFuncKind::BitAndInt64 => {}
        BinaryFuncKind::BitAndUint16 => {}
        BinaryFuncKind::BitAndUint32 => {}
        BinaryFuncKind::BitAndUint64 => {}
        BinaryFuncKind::BitOrInt16 => {}
        BinaryFuncKind::BitOrInt32 => {}
        BinaryFuncKind::BitOrInt64 => {}
        BinaryFuncKind::BitOrUint16 => {}
        BinaryFuncKind::BitOrUint32 => {}
        BinaryFuncKind::BitOrUint64 => {}
        BinaryFuncKind::BitXorInt16 => {}
        BinaryFuncKind::BitXorInt32 => {}
        BinaryFuncKind::BitXorInt64 => {}
        BinaryFuncKind::BitXorUint16 => {}
        BinaryFuncKind::BitXorUint32 => {}
        BinaryFuncKind::BitXorUint64 => {}
        BinaryFuncKind::BitShiftLeftInt16 => {}
        BinaryFuncKind::BitShiftLeftInt32 => {}
        BinaryFuncKind::BitShiftLeftInt64 => {}
        BinaryFuncKind::BitShiftLeftUint16 => {}
        BinaryFuncKind::BitShiftLeftUint32 => {}
        BinaryFuncKind::BitShiftLeftUint64 => {}
        BinaryFuncKind::BitShiftRightInt16 => {}
        BinaryFuncKind::BitShiftRightInt32 => {}
        BinaryFuncKind::BitShiftRightInt64 => {}
        BinaryFuncKind::BitShiftRightUint16 => {}
        BinaryFuncKind::BitShiftRightUint32 => {}
        BinaryFuncKind::BitShiftRightUint64 => {}
        BinaryFuncKind::SubInt16 => {}
        BinaryFuncKind::SubInt32 => {}
        BinaryFuncKind::SubInt64 => {}
        BinaryFuncKind::SubUint16 => {}
        BinaryFuncKind::SubUint32 => {}
        BinaryFuncKind::SubUint64 => {}
        BinaryFuncKind::SubFloat32 => {}
        BinaryFuncKind::SubFloat64 => {}
        BinaryFuncKind::SubInterval => {}
        BinaryFuncKind::SubTimestamp => {}
        BinaryFuncKind::SubTimestampTz => {}
        BinaryFuncKind::SubTimestampInterval => {}
        BinaryFuncKind::SubTimestampTzInterval => {}
        BinaryFuncKind::SubDate => {}
        BinaryFuncKind::SubDateInterval => {}
        BinaryFuncKind::SubTime => {}
        BinaryFuncKind::SubTimeInterval => {}
        BinaryFuncKind::SubNumeric => {}
        BinaryFuncKind::MulInt16 => {}
        BinaryFuncKind::MulInt32 => {}
        BinaryFuncKind::MulInt64 => {}
        BinaryFuncKind::MulUint16 => {}
        BinaryFuncKind::MulUint32 => {}
        BinaryFuncKind::MulUint64 => {}
        BinaryFuncKind::MulFloat32 => {}
        BinaryFuncKind::MulFloat64 => {}
        BinaryFuncKind::MulNumeric => {}
        BinaryFuncKind::MulInterval => {}
        BinaryFuncKind::DivInt16 => {}
        BinaryFuncKind::DivInt32 => {}
        BinaryFuncKind::DivInt64 => {}
        BinaryFuncKind::DivUint16 => {}
        BinaryFuncKind::DivUint32 => {}
        BinaryFuncKind::DivUint64 => {}
        BinaryFuncKind::DivFloat32 => {}
        BinaryFuncKind::DivFloat64 => {}
        BinaryFuncKind::DivNumeric => {}
        BinaryFuncKind::DivInterval => {}
        BinaryFuncKind::ModInt16 => {}
        BinaryFuncKind::ModInt32 => {}
        BinaryFuncKind::ModInt64 => {}
        BinaryFuncKind::ModUint16 => {}
        BinaryFuncKind::ModUint32 => {}
        BinaryFuncKind::ModUint64 => {}
        BinaryFuncKind::ModFloat32 => {}
        BinaryFuncKind::ModFloat64 => {}
        BinaryFuncKind::ModNumeric => {}
        BinaryFuncKind::RoundNumeric => {}
        BinaryFuncKind::Eq => {}
        BinaryFuncKind::NotEq => {}
        BinaryFuncKind::Lt => {}
        BinaryFuncKind::Lte => {}
        BinaryFuncKind::Gt => {}
        BinaryFuncKind::Gte => {}
        BinaryFuncKind::LikeEscape => {}
        BinaryFuncKind::IsLikeMatchCaseInsensitive => {}
        BinaryFuncKind::IsLikeMatchCaseSensitive => {}
        BinaryFuncKind::IsRegexpMatch => {}
        BinaryFuncKind::ToCharTimestamp => {}
        BinaryFuncKind::ToCharTimestampTz => {}
        BinaryFuncKind::DateBinTimestamp => {}
        BinaryFuncKind::DateBinTimestampTz => {}
        BinaryFuncKind::ExtractInterval => {}
        BinaryFuncKind::ExtractTime => {}
        BinaryFuncKind::ExtractTimestamp => {}
        BinaryFuncKind::ExtractTimestampTz => {}
        BinaryFuncKind::ExtractDate => {}
        BinaryFuncKind::DatePartInterval => {}
        BinaryFuncKind::DatePartTime => {}
        BinaryFuncKind::DatePartTimestamp => {}
        BinaryFuncKind::DatePartTimestampTz => {}
        BinaryFuncKind::DateTruncTimestamp => {}
        BinaryFuncKind::DateTruncTimestampTz => {}
        BinaryFuncKind::DateTruncInterval => {}
        BinaryFuncKind::TimezoneTimestamp => {}
        BinaryFuncKind::TimezoneTimestampTz => {}
        BinaryFuncKind::TimezoneIntervalTimestamp => {}
        BinaryFuncKind::TimezoneIntervalTimestampTz => {}
        BinaryFuncKind::TimezoneIntervalTime => {}
        BinaryFuncKind::TimezoneOffset => {}
        BinaryFuncKind::TextConcat => {}
        BinaryFuncKind::JsonbGetInt64 => {}
        BinaryFuncKind::JsonbGetInt64Stringify => {}
        BinaryFuncKind::JsonbGetString => {}
        BinaryFuncKind::JsonbGetStringStringify => {}
        BinaryFuncKind::JsonbGetPath => {}
        BinaryFuncKind::JsonbGetPathStringify => {}
        BinaryFuncKind::JsonbContainsString => {}
        BinaryFuncKind::JsonbConcat => {}
        BinaryFuncKind::JsonbContainsJsonb => {}
        BinaryFuncKind::JsonbDeleteInt64 => {}
        BinaryFuncKind::JsonbDeleteString => {}
        BinaryFuncKind::MapContainsKey => {}
        BinaryFuncKind::MapGetValue => {}
        BinaryFuncKind::MapContainsAllKeys => {}
        BinaryFuncKind::MapContainsAnyKeys => {}
        BinaryFuncKind::MapContainsMap => {}
        BinaryFuncKind::ConvertFrom => {}
        BinaryFuncKind::Left => {}
        BinaryFuncKind::Position => {}
        BinaryFuncKind::Right => {}
        BinaryFuncKind::RepeatString => {}
        BinaryFuncKind::Normalize => {}
        BinaryFuncKind::Trim => {}
        BinaryFuncKind::TrimLeading => {}
        BinaryFuncKind::TrimTrailing => {}
        BinaryFuncKind::EncodedBytesCharLength => {}
        BinaryFuncKind::ListLengthMax => return Some(ListLengthMax::func_doc()),
        BinaryFuncKind::ArrayContains => {}
        BinaryFuncKind::ArrayContainsArray => {}
        BinaryFuncKind::ArrayContainsArrayRev => {}
        BinaryFuncKind::ArrayLength => {}
        BinaryFuncKind::ArrayLower => {}
        BinaryFuncKind::ArrayRemove => {}
        BinaryFuncKind::ArrayUpper => {}
        BinaryFuncKind::ArrayArrayConcat => {}
        BinaryFuncKind::ListListConcat => {}
        BinaryFuncKind::ListElementConcat => {}
        BinaryFuncKind::ElementListConcat => {}
        BinaryFuncKind::ListRemove => {}
        BinaryFuncKind::ListContainsList => {}
        BinaryFuncKind::ListContainsListRev => {}
        BinaryFuncKind::DigestString => {}
        BinaryFuncKind::DigestBytes => {}
        BinaryFuncKind::MzRenderTypmod => {}
        BinaryFuncKind::Encode => {}
        BinaryFuncKind::Decode => {}
        BinaryFuncKind::LogNumeric => {}
        BinaryFuncKind::Power => {}
        BinaryFuncKind::PowerNumeric => {}
        BinaryFuncKind::GetBit => {}
        BinaryFuncKind::GetByte => {}
        BinaryFuncKind::ConstantTimeEqBytes => {}
        BinaryFuncKind::ConstantTimeEqString => {}
        BinaryFuncKind::RangeContainsElem => {}
        BinaryFuncKind::RangeContainsRange => {}
        BinaryFuncKind::RangeOverlaps => {}
        BinaryFuncKind::RangeAfter => {}
        BinaryFuncKind::RangeBefore => {}
        BinaryFuncKind::RangeOverleft => {}
        BinaryFuncKind::RangeOverright => {}
        BinaryFuncKind::RangeAdjacent => {}
        BinaryFuncKind::RangeUnion => {}
        BinaryFuncKind::RangeIntersection => {}
        BinaryFuncKind::RangeDifference => {}
        BinaryFuncKind::UuidGenerateV5 => {}
        BinaryFuncKind::MzAclItemContainsPrivilege => {}
        BinaryFuncKind::ParseIdent => {}
        BinaryFuncKind::PrettySql => {}
        BinaryFuncKind::RegexpReplace => {}
        BinaryFuncKind::StartsWith => {}
    }
    None
}

fn binary_func_kinds() -> &'static [BinaryFuncKind] {
    &[
        BinaryFuncKind::AddInt16,
        BinaryFuncKind::AddInt32,
        BinaryFuncKind::AddInt64,
        BinaryFuncKind::AddUint16,
        BinaryFuncKind::AddUint32,
        BinaryFuncKind::AddUint64,
        BinaryFuncKind::AddFloat32,
        BinaryFuncKind::AddFloat64,
        BinaryFuncKind::AddInterval,
        BinaryFuncKind::AddTimestampInterval,
        BinaryFuncKind::AddTimestampTzInterval,
        BinaryFuncKind::AddDateInterval,
        BinaryFuncKind::AddDateTime,
        BinaryFuncKind::AddTimeInterval,
        BinaryFuncKind::AddNumeric,
        BinaryFuncKind::AgeTimestamp,
        BinaryFuncKind::AgeTimestampTz,
        BinaryFuncKind::BitAndInt16,
        BinaryFuncKind::BitAndInt32,
        BinaryFuncKind::BitAndInt64,
        BinaryFuncKind::BitAndUint16,
        BinaryFuncKind::BitAndUint32,
        BinaryFuncKind::BitAndUint64,
        BinaryFuncKind::BitOrInt16,
        BinaryFuncKind::BitOrInt32,
        BinaryFuncKind::BitOrInt64,
        BinaryFuncKind::BitOrUint16,
        BinaryFuncKind::BitOrUint32,
        BinaryFuncKind::BitOrUint64,
        BinaryFuncKind::BitXorInt16,
        BinaryFuncKind::BitXorInt32,
        BinaryFuncKind::BitXorInt64,
        BinaryFuncKind::BitXorUint16,
        BinaryFuncKind::BitXorUint32,
        BinaryFuncKind::BitXorUint64,
        BinaryFuncKind::BitShiftLeftInt16,
        BinaryFuncKind::BitShiftLeftInt32,
        BinaryFuncKind::BitShiftLeftInt64,
        BinaryFuncKind::BitShiftLeftUint16,
        BinaryFuncKind::BitShiftLeftUint32,
        BinaryFuncKind::BitShiftLeftUint64,
        BinaryFuncKind::BitShiftRightInt16,
        BinaryFuncKind::BitShiftRightInt32,
        BinaryFuncKind::BitShiftRightInt64,
        BinaryFuncKind::BitShiftRightUint16,
        BinaryFuncKind::BitShiftRightUint32,
        BinaryFuncKind::BitShiftRightUint64,
        BinaryFuncKind::SubInt16,
        BinaryFuncKind::SubInt32,
        BinaryFuncKind::SubInt64,
        BinaryFuncKind::SubUint16,
        BinaryFuncKind::SubUint32,
        BinaryFuncKind::SubUint64,
        BinaryFuncKind::SubFloat32,
        BinaryFuncKind::SubFloat64,
        BinaryFuncKind::SubInterval,
        BinaryFuncKind::SubTimestamp,
        BinaryFuncKind::SubTimestampTz,
        BinaryFuncKind::SubTimestampInterval,
        BinaryFuncKind::SubTimestampTzInterval,
        BinaryFuncKind::SubDate,
        BinaryFuncKind::SubDateInterval,
        BinaryFuncKind::SubTime,
        BinaryFuncKind::SubTimeInterval,
        BinaryFuncKind::SubNumeric,
        BinaryFuncKind::MulInt16,
        BinaryFuncKind::MulInt32,
        BinaryFuncKind::MulInt64,
        BinaryFuncKind::MulUint16,
        BinaryFuncKind::MulUint32,
        BinaryFuncKind::MulUint64,
        BinaryFuncKind::MulFloat32,
        BinaryFuncKind::MulFloat64,
        BinaryFuncKind::MulNumeric,
        BinaryFuncKind::MulInterval,
        BinaryFuncKind::DivInt16,
        BinaryFuncKind::DivInt32,
        BinaryFuncKind::DivInt64,
        BinaryFuncKind::DivUint16,
        BinaryFuncKind::DivUint32,
        BinaryFuncKind::DivUint64,
        BinaryFuncKind::DivFloat32,
        BinaryFuncKind::DivFloat64,
        BinaryFuncKind::DivNumeric,
        BinaryFuncKind::DivInterval,
        BinaryFuncKind::ModInt16,
        BinaryFuncKind::ModInt32,
        BinaryFuncKind::ModInt64,
        BinaryFuncKind::ModUint16,
        BinaryFuncKind::ModUint32,
        BinaryFuncKind::ModUint64,
        BinaryFuncKind::ModFloat32,
        BinaryFuncKind::ModFloat64,
        BinaryFuncKind::ModNumeric,
        BinaryFuncKind::RoundNumeric,
        BinaryFuncKind::Eq,
        BinaryFuncKind::NotEq,
        BinaryFuncKind::Lt,
        BinaryFuncKind::Lte,
        BinaryFuncKind::Gt,
        BinaryFuncKind::Gte,
        BinaryFuncKind::LikeEscape,
        BinaryFuncKind::IsLikeMatchCaseInsensitive,
        BinaryFuncKind::IsLikeMatchCaseSensitive,
        BinaryFuncKind::IsRegexpMatch,
        BinaryFuncKind::ToCharTimestamp,
        BinaryFuncKind::ToCharTimestampTz,
        BinaryFuncKind::DateBinTimestamp,
        BinaryFuncKind::DateBinTimestampTz,
        BinaryFuncKind::ExtractInterval,
        BinaryFuncKind::ExtractTime,
        BinaryFuncKind::ExtractTimestamp,
        BinaryFuncKind::ExtractTimestampTz,
        BinaryFuncKind::ExtractDate,
        BinaryFuncKind::DatePartInterval,
        BinaryFuncKind::DatePartTime,
        BinaryFuncKind::DatePartTimestamp,
        BinaryFuncKind::DatePartTimestampTz,
        BinaryFuncKind::DateTruncTimestamp,
        BinaryFuncKind::DateTruncTimestampTz,
        BinaryFuncKind::DateTruncInterval,
        BinaryFuncKind::TimezoneTimestamp,
        BinaryFuncKind::TimezoneTimestampTz,
        BinaryFuncKind::TimezoneIntervalTimestamp,
        BinaryFuncKind::TimezoneIntervalTimestampTz,
        BinaryFuncKind::TimezoneIntervalTime,
        BinaryFuncKind::TimezoneOffset,
        BinaryFuncKind::TextConcat,
        BinaryFuncKind::JsonbGetInt64,
        BinaryFuncKind::JsonbGetInt64,
        BinaryFuncKind::JsonbGetString,
        BinaryFuncKind::JsonbGetStringStringify,
        BinaryFuncKind::JsonbGetPath,
        BinaryFuncKind::JsonbGetPathStringify,
        BinaryFuncKind::JsonbContainsString,
        BinaryFuncKind::JsonbConcat,
        BinaryFuncKind::JsonbContainsJsonb,
        BinaryFuncKind::JsonbDeleteInt64,
        BinaryFuncKind::JsonbDeleteString,
        BinaryFuncKind::MapContainsKey,
        BinaryFuncKind::MapGetValue,
        BinaryFuncKind::MapContainsAllKeys,
        BinaryFuncKind::MapContainsAnyKeys,
        BinaryFuncKind::MapContainsMap,
        BinaryFuncKind::ConvertFrom,
        BinaryFuncKind::Left,
        BinaryFuncKind::Position,
        BinaryFuncKind::Right,
        BinaryFuncKind::RepeatString,
        BinaryFuncKind::Normalize,
        BinaryFuncKind::Trim,
        BinaryFuncKind::TrimLeading,
        BinaryFuncKind::TrimTrailing,
        BinaryFuncKind::EncodedBytesCharLength,
        BinaryFuncKind::ListLengthMax,
        BinaryFuncKind::ArrayContains,
        BinaryFuncKind::ArrayContainsArray,
        BinaryFuncKind::ArrayContainsArrayRev,
        BinaryFuncKind::ArrayLength,
        BinaryFuncKind::ArrayLower,
        BinaryFuncKind::ArrayRemove,
        BinaryFuncKind::ArrayUpper,
        BinaryFuncKind::ArrayArrayConcat,
        BinaryFuncKind::ListListConcat,
        BinaryFuncKind::ListElementConcat,
        BinaryFuncKind::ElementListConcat,
        BinaryFuncKind::ListRemove,
        BinaryFuncKind::ListContainsList,
        BinaryFuncKind::ListContainsListRev,
        BinaryFuncKind::DigestString,
        BinaryFuncKind::DigestBytes,
        BinaryFuncKind::MzRenderTypmod,
        BinaryFuncKind::Encode,
        BinaryFuncKind::Decode,
        BinaryFuncKind::LogNumeric,
        BinaryFuncKind::Power,
        BinaryFuncKind::PowerNumeric,
        BinaryFuncKind::GetBit,
        BinaryFuncKind::GetByte,
        BinaryFuncKind::ConstantTimeEqBytes,
        BinaryFuncKind::ConstantTimeEqString,
        BinaryFuncKind::RangeContainsElem,
        BinaryFuncKind::RangeContainsRange,
        BinaryFuncKind::RangeOverlaps,
        BinaryFuncKind::RangeAfter,
        BinaryFuncKind::RangeBefore,
        BinaryFuncKind::RangeOverleft,
        BinaryFuncKind::RangeOverright,
        BinaryFuncKind::RangeAdjacent,
        BinaryFuncKind::RangeUnion,
        BinaryFuncKind::RangeIntersection,
        BinaryFuncKind::RangeDifference,
        BinaryFuncKind::UuidGenerateV5,
        BinaryFuncKind::MzAclItemContainsPrivilege,
        BinaryFuncKind::ParseIdent,
        BinaryFuncKind::PrettySql,
        BinaryFuncKind::RegexpReplace,
        BinaryFuncKind::StartsWith,
    ]
}
