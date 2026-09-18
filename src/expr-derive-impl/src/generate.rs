// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Expansion shared by every scalar function arity.

use proc_macro2::{Ident, TokenStream};
use quote::quote;

use crate::shape::{Modifier, Shape};
use crate::sqlfunc::Modifiers;

/// One `fn name(&self) -> Ret { expr }` per modifier present in `mods`.
///
/// Generates in the arity table's order rather than the attribute's order, so the
/// generated code is stable against how a call site happens to spell its
/// modifiers.
pub(crate) fn override_methods(shape: Shape, mods: &Modifiers) -> Vec<TokenStream> {
    let present: Vec<_> = mods.iter().collect();
    shape
        .modifiers()
        .iter()
        .filter_map(|(modifier, ret)| {
            let expr = present
                .iter()
                .find(|(candidate, _)| candidate == modifier)
                .map(|(_, expr)| *expr)?;
            let name = Ident::new(modifier.name(), proc_macro2::Span::call_site());
            let ret = ret.to_tokens();
            Some(quote! {
                fn #name(&self) -> #ret {
                    #expr
                }
            })
        })
        .collect()
}

/// Restores a hand-built `introduces_nulls` method to the slot `override_methods`
/// would have produced for it.
///
/// Every generator arm synthesizes `introduces_nulls` itself, either from
/// `output_type` when the modifier is absent or from the modifier directly
/// otherwise, so callers clear `introduces_nulls` on the `Modifiers` passed to
/// `override_methods` to avoid emitting it twice. `mods` here is that same cleared
/// copy: only the modifiers ahead of `IntroducesNulls` in `shape`'s table decide the
/// insertion point, and clearing `introduces_nulls` does not change which of those
/// are present.
pub(crate) fn insert_introduces_nulls(
    methods: &mut Vec<TokenStream>,
    shape: Shape,
    mods: &Modifiers,
    introduces_nulls_fn: TokenStream,
) {
    let index = shape
        .modifiers()
        .iter()
        .take_while(|(modifier, _)| *modifier != Modifier::IntroducesNulls)
        .filter(|(modifier, _)| mods.iter().any(|(present, _)| present == *modifier))
        .count();
    methods.insert(index, introduces_nulls_fn);
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::shape::Shape;

    #[mz_ore::test]
    fn binary_is_monotone_generates_a_pair_return() {
        let mods = crate::sqlfunc::Modifiers::from_tokens(quote! {
            is_monotone = (true, true),
            could_error = false,
        })
        .expect("parses");
        let methods = super::override_methods(Shape::Binary, &mods);
        let rendered = methods
            .iter()
            .map(|m| m.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            rendered.contains("fn is_monotone (& self) -> (bool , bool)"),
            "got:\n{rendered}"
        );
        assert!(
            rendered.contains("fn could_error (& self) -> bool"),
            "got:\n{rendered}"
        );
    }

    #[mz_ore::test]
    fn unary_is_monotone_generates_a_bool_return() {
        let mods =
            crate::sqlfunc::Modifiers::from_tokens(quote! { is_monotone = true }).expect("parses");
        let methods = super::override_methods(Shape::Unary, &mods);
        let rendered = methods[0].to_string();
        assert!(
            rendered.contains("fn is_monotone (& self) -> bool"),
            "got:\n{rendered}"
        );
    }

    #[mz_ore::test]
    fn absent_modifiers_generate_nothing() {
        let mods = crate::sqlfunc::Modifiers::from_tokens(quote! {}).expect("parses");
        assert!(super::override_methods(Shape::Unary, &mods).is_empty());
    }

    #[mz_ore::test]
    fn insert_introduces_nulls_lands_after_could_error() {
        let mods = crate::sqlfunc::Modifiers::from_tokens(quote! {
            could_error = true,
            is_monotone = true,
        })
        .expect("parses");
        let mut methods = super::override_methods(Shape::Unary, &mods);
        assert_eq!(methods.len(), 2, "could_error and is_monotone");
        super::insert_introduces_nulls(
            &mut methods,
            Shape::Unary,
            &mods,
            quote! {
                fn introduces_nulls(&self) -> bool {
                    true
                }
            },
        );
        let rendered: Vec<String> = methods.iter().map(|m| m.to_string()).collect();
        assert!(rendered[0].contains("could_error"), "got:\n{rendered:?}");
        assert!(
            rendered[1].contains("introduces_nulls"),
            "got:\n{rendered:?}"
        );
        assert!(rendered[2].contains("is_monotone"), "got:\n{rendered:?}");
    }

    #[mz_ore::test]
    fn insert_introduces_nulls_leads_when_could_error_absent() {
        let mods =
            crate::sqlfunc::Modifiers::from_tokens(quote! { is_monotone = true }).expect("parses");
        let mut methods = super::override_methods(Shape::Unary, &mods);
        assert_eq!(methods.len(), 1, "is_monotone only");
        super::insert_introduces_nulls(
            &mut methods,
            Shape::Unary,
            &mods,
            quote! {
                fn introduces_nulls(&self) -> bool {
                    true
                }
            },
        );
        let rendered: Vec<String> = methods.iter().map(|m| m.to_string()).collect();
        assert!(
            rendered[0].contains("introduces_nulls"),
            "got:\n{rendered:?}"
        );
        assert!(rendered[1].contains("is_monotone"), "got:\n{rendered:?}");
    }
}
