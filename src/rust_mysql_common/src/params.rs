// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{
    collections::{
        hash_map::{Entry, Entry::Occupied},
        HashMap,
    },
    error::Error,
    fmt,
};

use crate::value::{convert::ToValue, Value};

/// `FromValue` conversion error.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct MissingNamedParameterError(pub Vec<u8>);

impl fmt::Display for MissingNamedParameterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Missing named parameter `{}` for statement",
            String::from_utf8_lossy(&self.0)
        )
    }
}

impl Error for MissingNamedParameterError {
    fn description(&self) -> &str {
        "Missing named parameter for statement"
    }
}

/// Representations of parameters of a prepared statement.
#[derive(Clone, PartialEq)]
pub enum Params {
    Empty,
    Named(HashMap<Vec<u8>, Value>),
    Positional(Vec<Value>),
}

impl fmt::Debug for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "Empty"),
            Self::Named(arg0) => {
                let arg0 = arg0
                    .iter()
                    .map(|(k, v)| (String::from_utf8_lossy(k), v))
                    .collect::<HashMap<_, _>>();
                f.debug_tuple("Named").field(&arg0).finish()
            }
            Self::Positional(arg0) => f.debug_tuple("Positional").field(arg0).finish(),
        }
    }
}

impl Params {
    /// Will convert named parameters into positional assuming order passed in `named_params`
    /// attribute.
    pub fn into_positional(
        self,
        named_params: &[Vec<u8>],
    ) -> Result<Params, MissingNamedParameterError> {
        match self {
            Params::Named(mut map) => {
                let mut params: Vec<Value> = Vec::new();
                'params: for (i, name) in named_params.iter().enumerate() {
                    match map.entry(name.clone()) {
                        Occupied(entry) => {
                            let mut x = named_params.len() - 1;
                            while x > i {
                                if *name == named_params[x] {
                                    params.push(entry.get().clone());
                                    continue 'params;
                                }
                                x -= 1;
                            }
                            params.push(entry.remove());
                        }
                        _ => return Err(MissingNamedParameterError(name.clone())),
                    }
                }
                Ok(Params::Positional(params))
            }
            params => Ok(params),
        }
    }
}

impl<'a, T: Into<Params> + Clone> From<&'a T> for Params {
    fn from(x: &'a T) -> Params {
        x.clone().into()
    }
}

impl<T> From<Vec<T>> for Params
where
    Value: From<T>,
{
    fn from(x: Vec<T>) -> Params {
        let mut raw_params: Vec<Value> = Vec::new();
        for v in x.into_iter() {
            raw_params.push(Value::from(v));
        }
        if raw_params.is_empty() {
            Params::Empty
        } else {
            Params::Positional(raw_params)
        }
    }
}

impl<N, V> From<Vec<(N, V)>> for Params
where
    Vec<u8>: From<N>,
    Value: From<V>,
{
    fn from(x: Vec<(N, V)>) -> Params {
        let mut map = HashMap::default();
        for (name, value) in x.into_iter() {
            let name: Vec<u8> = name.into();
            match map.entry(name) {
                Entry::Vacant(entry) => entry.insert(Value::from(value)),
                Entry::Occupied(entry) => {
                    panic!(
                        "Redefinition of named parameter `{}'",
                        String::from_utf8_lossy(entry.key())
                    );
                }
            };
        }
        Params::Named(map)
    }
}

impl<'a> From<&'a [&'a dyn ToValue]> for Params {
    fn from(x: &'a [&'a dyn ToValue]) -> Params {
        let mut raw_params: Vec<Value> = Vec::new();
        for v in x {
            raw_params.push(v.to_value());
        }
        if raw_params.is_empty() {
            Params::Empty
        } else {
            Params::Positional(raw_params)
        }
    }
}

impl From<()> for Params {
    fn from(_: ()) -> Params {
        Params::Empty
    }
}

macro_rules! into_params_impl {
    ($([$A:ident,$a:ident]),*) => (
        impl<$($A: Into<Value>,)*> From<($($A,)*)> for Params {
            fn from(x: ($($A,)*)) -> Params {
                let ($($a,)*) = x;
                Params::Positional(vec![
                    $($a.into(),)*
                ])
            }
        }
    );
}

into_params_impl!([A, a]);
into_params_impl!([A, a], [B, b]);
into_params_impl!([A, a], [B, b], [C, c]);
into_params_impl!([A, a], [B, b], [C, c], [D, d]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g]);
into_params_impl!(
    [A, a],
    [B, b],
    [C, c],
    [D, d],
    [E, e],
    [F, f],
    [G, g],
    [H, h]
);
into_params_impl!(
    [A, a],
    [B, b],
    [C, c],
    [D, d],
    [E, e],
    [F, f],
    [G, g],
    [H, h],
    [I, i]
);
into_params_impl!(
    [A, a],
    [B, b],
    [C, c],
    [D, d],
    [E, e],
    [F, f],
    [G, g],
    [H, h],
    [I, i],
    [J, j]
);
into_params_impl!(
    [A, a],
    [B, b],
    [C, c],
    [D, d],
    [E, e],
    [F, f],
    [G, g],
    [H, h],
    [I, i],
    [J, j],
    [K, k]
);
into_params_impl!(
    [A, a],
    [B, b],
    [C, c],
    [D, d],
    [E, e],
    [F, f],
    [G, g],
    [H, h],
    [I, i],
    [J, j],
    [K, k],
    [L, l]
);
