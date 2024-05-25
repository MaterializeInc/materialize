use std::str::FromStr;

use darling::{util::SpannedValue, FromMeta};
use proc_macro::LexError;
use syn::{parse::Parse, punctuated::Punctuated};

use crate::from_value::enums::attrs::container::Crate;

#[derive(Default, FromMeta)]
pub struct Mysql {
    #[darling(default)]
    pub crate_name: Crate,
    #[darling(default)]
    pub bound: Option<Bound>,
    #[darling(default)]
    pub deserialize_with: Option<SpannedValue<FnPath>>,
    #[darling(default)]
    pub serialize_with: Option<SpannedValue<FnPath>>,
}

#[derive(Debug)]
pub struct FnPath(pub syn::Path);

impl Parse for FnPath {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        syn::Path::parse(input).map(Self)
    }
}

impl FromMeta for FnPath {
    fn from_string(value: &str) -> darling::Result<Self> {
        syn::parse::<syn::Path>(
            FromStr::from_str(value)
                .map_err(|e: LexError| darling::Error::unsupported_format(&e.to_string()))?,
        )
        .map_err(|e| darling::Error::unsupported_format(&e.to_string()))
        .map(Self)
    }
}

pub struct Bound(pub Punctuated<syn::GenericParam, syn::Token![,]>);

impl Parse for Bound {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Punctuated::<syn::GenericParam, syn::Token![,]>::parse_terminated(input).map(Self)
    }
}

impl FromMeta for Bound {
    fn from_string(value: &str) -> darling::Result<Self> {
        syn::parse::<Self>(
            FromStr::from_str(value)
                .map_err(|e: LexError| darling::Error::unsupported_format(&e.to_string()))?,
        )
        .map_err(|e| darling::Error::unsupported_format(&e.to_string()))
    }
}
