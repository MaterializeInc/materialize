// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! JSONB functions.

use repr::adt::jsonb::JsonbRef;
use repr::strconv;
use repr::{Datum, RowArena, RowPacker, ScalarType};

use crate::scalar::func::{FuncProps, Nulls, OutputType};
use crate::scalar::EvalError;

pub const CAST_JSONB_TO_STRING_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::String),
};

pub fn cast_jsonb_to_string<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut buf = String::new();
    strconv::format_jsonb(&mut buf, JsonbRef::from_datum(a));
    Ok(Datum::String(temp_storage.push_string(buf)))
}

pub const CAST_STRING_TO_JSONB_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: true, // TODO(benesch): should error
    },
    output_type: OutputType::Fixed(ScalarType::Jsonb),
};

// TODO(jamii): it would be much more efficient to skip the intermediate
// repr::jsonb::Jsonb.
pub fn cast_string_to_jsonb<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    match strconv::parse_jsonb(a.unwrap_str()) {
        Err(_) => Ok(Datum::Null),
        Ok(jsonb) => Ok(temp_storage.push_row(jsonb.into_row()).unpack_first()),
    }
}

pub const CAST_JSONB_OR_NULL_TO_JSONB_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Never,
    output_type: OutputType::Fixed(ScalarType::Jsonb),
};

pub fn cast_jsonb_or_null_to_jsonb(a: Datum) -> Result<Datum, EvalError> {
    Ok(match a {
        Datum::Null => Datum::JsonNull,
        Datum::Float64(f) => {
            if f.is_finite() {
                a
            } else if f.is_nan() {
                Datum::String("NaN")
            } else if f.is_sign_positive() {
                Datum::String("Infinity")
            } else {
                Datum::String("-Infinity")
            }
        }
        _ => a,
    })
}

pub const CAST_JSONB_TO_FLOAT64_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: false,
        introduces_nulls: true,
    },
    output_type: OutputType::Fixed(ScalarType::Float64),
};

pub fn cast_jsonb_to_float64(a: Datum) -> Result<Datum, EvalError> {
    Ok(match a {
        Datum::Float64(_) => a,
        Datum::String(s) => match s {
            "NaN" => std::f64::NAN.into(),
            "Infinity" => std::f64::INFINITY.into(),
            "-Infinity" => std::f64::NEG_INFINITY.into(),
            _ => Datum::Null,
        },
        _ => Datum::Null,
    })
}

pub const CAST_JSONB_TO_BOOL_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: true,
    },
    output_type: OutputType::Fixed(ScalarType::Bool),
};

pub fn cast_jsonb_to_bool(a: Datum) -> Result<Datum, EvalError> {
    Ok(match a {
        Datum::True | Datum::False => a,
        _ => Datum::Null,
    })
}

pub fn jsonb_get_props(stringify: bool) -> FuncProps {
    FuncProps {
        can_error: false,
        preserves_uniqueness: false,
        nulls: Nulls::Sometimes {
            propagates_nulls: true,
            introduces_nulls: true,
        },
        output_type: OutputType::Fixed(match stringify {
            false => ScalarType::Jsonb,
            true => ScalarType::String,
        }),
    }
}

pub fn jsonb_get_int64<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
    stringify: bool,
) -> Result<Datum<'a>, EvalError> {
    let i = b.unwrap_int64();
    Ok(match a {
        Datum::List(list) => {
            let i = if i >= 0 {
                i
            } else {
                // index backwards from the end
                (list.iter().count() as i64) + i
            };
            match list.iter().nth(i as usize) {
                Some(d) if stringify => jsonb_stringify(d, temp_storage),
                Some(d) => d,
                None => Datum::Null,
            }
        }
        Datum::Dict(_) => Datum::Null,
        _ => {
            if i == 0 || i == -1 {
                // I have no idea why postgres does this, but we're stuck with it
                if stringify {
                    jsonb_stringify(a, temp_storage)
                } else {
                    a
                }
            } else {
                Datum::Null
            }
        }
    })
}

pub fn jsonb_get_string<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
    stringify: bool,
) -> Result<Datum<'a>, EvalError> {
    let k = b.unwrap_str();
    Ok(match a {
        Datum::Dict(dict) => match dict.iter().find(|(k2, _v)| k == *k2) {
            Some((_k, v)) if stringify => jsonb_stringify(v, temp_storage),
            Some((_k, v)) => v,
            None => Datum::Null,
        },
        _ => Datum::Null,
    })
}

pub const JSONB_ARRAY_LENGTH_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: true,
    },
    output_type: OutputType::Fixed(ScalarType::Int64),
};

pub fn jsonb_array_length(a: Datum) -> Result<Datum, EvalError> {
    Ok(match a {
        Datum::List(list) => Datum::Int64(list.iter().count() as i64),
        _ => Datum::Null,
    })
}

pub const JSONB_TYPEOF_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::String),
};

pub fn jsonb_typeof(a: Datum) -> Result<Datum, EvalError> {
    Ok(match a {
        Datum::Dict(_) => Datum::String("object"),
        Datum::List(_) => Datum::String("array"),
        Datum::String(_) => Datum::String("string"),
        Datum::Float64(_) => Datum::String("number"),
        Datum::True | Datum::False => Datum::String("boolean"),
        Datum::JsonNull => Datum::String("null"),
        Datum::Null => Datum::Null,
        _ => panic!("Not jsonb: {:?}", a),
    })
}

pub const JSONB_STRIP_NULLS_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Jsonb),
};

pub fn jsonb_strip_nulls<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    fn strip_nulls(a: Datum, packer: &mut RowPacker) {
        match a {
            Datum::Dict(dict) => packer.push_dict_with(|packer| {
                for (k, v) in dict.iter() {
                    match v {
                        Datum::JsonNull => (),
                        _ => {
                            packer.push(Datum::String(k));
                            strip_nulls(v, packer);
                        }
                    }
                }
            }),
            Datum::List(list) => packer.push_list_with(|packer| {
                for elem in list.iter() {
                    strip_nulls(elem, packer);
                }
            }),
            _ => packer.push(a),
        }
    }
    Ok(temp_storage.make_datum(|packer| strip_nulls(a, packer)))
}

pub const JSONB_PRETTY_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::String),
};

pub fn jsonb_pretty<'a>(a: Datum, temp_storage: &'a RowArena) -> Result<Datum<'a>, EvalError> {
    let mut buf = String::new();
    strconv::format_jsonb_pretty(&mut buf, JsonbRef::from_datum(a));
    Ok(Datum::String(temp_storage.push_string(buf)))
}

pub const JSONB_CONTAINS_STRING_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Bool),
};

pub fn jsonb_contains_string<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let k = b.unwrap_str();
    // https://www.postgresql.org/docs/current/datatype-json.html#JSON-CONTAINMENT
    Ok(match a {
        Datum::List(list) => list.iter().any(|k2| b == k2).into(),
        Datum::Dict(dict) => dict.iter().any(|(k2, _v)| k == k2).into(),
        Datum::String(string) => (string == k).into(),
        _ => false.into(),
    })
}

pub const JSONB_CONTAINS_JSONB_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Bool),
};

// TODO(jamii) nested loops are possibly not the fastest way to do this
pub fn jsonb_contains_jsonb<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    // https://www.postgresql.org/docs/current/datatype-json.html#JSON-CONTAINMENT
    fn contains(a: Datum, b: Datum, at_top_level: bool) -> bool {
        match (a, b) {
            (Datum::JsonNull, Datum::JsonNull) => true,
            (Datum::False, Datum::False) => true,
            (Datum::True, Datum::True) => true,
            (Datum::Float64(a), Datum::Float64(b)) => (a == b),
            (Datum::String(a), Datum::String(b)) => (a == b),
            (Datum::List(a), Datum::List(b)) => b
                .iter()
                .all(|b_elem| a.iter().any(|a_elem| contains(a_elem, b_elem, false))),
            (Datum::Dict(a), Datum::Dict(b)) => b.iter().all(|(b_key, b_val)| {
                a.iter()
                    .any(|(a_key, a_val)| (a_key == b_key) && contains(a_val, b_val, false))
            }),

            // fun special case
            (Datum::List(a), b) => {
                at_top_level && a.iter().any(|a_elem| contains(a_elem, b, false))
            }

            _ => false,
        }
    }
    Ok(contains(a, b, true).into())
}

pub const JSONB_CONCAT_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: true,
    },
    output_type: OutputType::Fixed(ScalarType::Jsonb),
};

pub fn jsonb_concat<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    Ok(match (a, b) {
        (Datum::Dict(dict_a), Datum::Dict(dict_b)) => {
            let mut pairs = dict_b.iter().chain(dict_a.iter()).collect::<Vec<_>>();
            // stable sort, so if keys collide dedup prefers dict_b
            pairs.sort_by(|(k1, _v1), (k2, _v2)| k1.cmp(k2));
            pairs.dedup_by(|(k1, _v1), (k2, _v2)| k1 == k2);
            temp_storage.make_datum(|packer| packer.push_dict(pairs))
        }
        (Datum::List(list_a), Datum::List(list_b)) => {
            let elems = list_a.iter().chain(list_b.iter());
            temp_storage.make_datum(|packer| packer.push_list(elems))
        }
        (Datum::List(list_a), b) => {
            let elems = list_a.iter().chain(Some(b).into_iter());
            temp_storage.make_datum(|packer| packer.push_list(elems))
        }
        (a, Datum::List(list_b)) => {
            let elems = Some(a).into_iter().chain(list_b.iter());
            temp_storage.make_datum(|packer| packer.push_list(elems))
        }
        _ => Datum::Null,
    })
}

pub const JSONB_DELETE_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: true,
    },
    output_type: OutputType::Fixed(ScalarType::Jsonb),
};

pub fn jsonb_delete_int64<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let i = b.unwrap_int64();
    Ok(match a {
        Datum::List(list) => {
            let i = if i >= 0 {
                i
            } else {
                // index backwards from the end
                (list.iter().count() as i64) + i
            } as usize;
            let elems = list
                .iter()
                .enumerate()
                .filter(|(i2, _e)| i != *i2)
                .map(|(_, e)| e);
            temp_storage.make_datum(|packer| packer.push_list(elems))
        }
        _ => Datum::Null,
    })
}

pub fn jsonb_delete_string<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    Ok(match a {
        Datum::List(list) => {
            let elems = list.iter().filter(|e| b != *e);
            temp_storage.make_datum(|packer| packer.push_list(elems))
        }
        Datum::Dict(dict) => {
            let k = b.unwrap_str();
            let pairs = dict.iter().filter(|(k2, _v)| k != *k2);
            temp_storage.make_datum(|packer| packer.push_dict(pairs))
        }
        _ => Datum::Null,
    })
}

pub fn jsonb_stringify<'a>(a: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a> {
    match a {
        Datum::JsonNull => Datum::Null,
        Datum::String(_) => a,
        _ => {
            let mut buf = String::new();
            strconv::format_jsonb(&mut buf, JsonbRef::from_datum(a));
            Datum::String(temp_storage.push_string(buf))
        }
    }
}

pub const JSONB_BUILD_ARRAY_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: false,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Jsonb),
};

pub fn jsonb_build_array<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    if datums.iter().any(|datum| datum.is_null()) {
        // the inputs should all be valid jsonb types, but a casting error might produce a Datum::Null that needs to be propagated
        // TODO(benesch): I don't understand this at all.
        Ok(Datum::Null)
    } else {
        Ok(temp_storage.make_datum(|packer| packer.push_list(datums)))
    }
}

pub const JSONB_BUILD_OBJECT_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: false,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Jsonb),
};

pub fn jsonb_build_object<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    if datums.iter().any(|datum| datum.is_null()) {
        // the inputs should all be valid jsonb types, but a casting error might produce a Datum::Null that needs to be propagated
        // TODO(benesch): I don't understand this at all.
        Ok(Datum::Null)
    } else {
        let mut kvs = datums.chunks(2).collect::<Vec<_>>();
        kvs.sort_by(|kv1, kv2| kv1[0].cmp(&kv2[0]));
        kvs.dedup_by(|kv1, kv2| kv1[0] == kv2[0]);
        Ok(temp_storage.make_datum(|packer| {
            packer.push_dict(kvs.into_iter().map(|kv| (kv[0].unwrap_str(), kv[1])))
        }))
    }
}
