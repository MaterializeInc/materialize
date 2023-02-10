// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use proc_macro2::TokenStream as TokenStream2;
use syn::parse::{Parse, ParseStream};

use mz_repr::RelationType;

use super::TestCatalog;
use crate::MirRelationExpr;

/// Builds a [MirRelationExpr] from a string.
pub fn try_parse_mir(s: &str, _catalog: &TestCatalog) -> syn::Result<MirRelationExpr> {
    let tokens = TokenStream2::from_str(s)?;
    syn::parse2(tokens)
}

impl Parse for MirRelationExpr {
    fn parse(input: ParseStream) -> syn::Result<MirRelationExpr> {
        let lookahead = input.lookahead1();
        if lookahead.peek(kw::Constant) {
            input.call(parse_constant)
        } else {
            Err(lookahead.error())
        }
    }
}

fn parse_constant(input: ParseStream) -> syn::Result<MirRelationExpr> {
    input.parse::<kw::Constant>()?;

    let is_empty;
    if input.peek(syn::Token![<]) && input.peek2(kw::empty) && input.peek3(syn::Token![>]) {
        input.parse::<syn::Token![<]>()?;
        input.parse::<kw::empty>()?;
        input.parse::<syn::Token![>]>()?;
        is_empty = true;
    } else {
        is_empty = false;
    }

    if is_empty {
        Ok(MirRelationExpr::constant(vec![], RelationType::empty()))
    } else {
        Err(syn::Error::new(
            input.span(),
            "non-empty constants not supported yet",
        ))
    }
}

mod kw {
    syn::custom_keyword!(empty);
    syn::custom_keyword!(Constant);
}
