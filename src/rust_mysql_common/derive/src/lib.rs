//! Implements [`FromValue`] and [`FromRow`] derive macros.

extern crate proc_macro;

use proc_macro_error::abort;

use crate::error::Error;
type Result<T> = std::result::Result<T, crate::error::Error>;

mod error;
mod warn;

mod from_row;
mod from_value;

/// Derives `FromValue`. See `mysql_common` crate-level docs for more info.
#[proc_macro_derive(FromValue, attributes(mysql))]
#[proc_macro_error::proc_macro_error]
pub fn from_value(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input: syn::DeriveInput = syn::parse(input).unwrap();
    match from_value::impl_from_value(&input) {
        Ok(gen) => gen.into(),
        Err(e) => abort!(e),
    }
}

/// Derives `FromRow`. See `mysql_common` crate-level docs for more info.
#[proc_macro_derive(FromRow, attributes(mysql))]
#[proc_macro_error::proc_macro_error]
pub fn from_row(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input: syn::DeriveInput = syn::parse(input).unwrap();
    match from_row::impl_from_row(&input) {
        Ok(gen) => gen.into(),
        Err(e) => abort!(e),
    }
}
