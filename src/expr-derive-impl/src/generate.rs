// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//! Expansion shared by every scalar function arity.

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::modifiers::{Modifiers, reject_inapplicable};
use crate::shape::{Modifier, Shape};
use crate::signature;
use crate::source::sqlfunc_source;

/// One `fn name(&self) -> Ret { expr }` per modifier present in `mods`, except
/// `introduces_nulls`.
///
/// Generates in the arity table's order rather than the attribute's order, so the
/// generated code is stable against how a call site happens to spell its
/// modifiers.
///
/// `introduces_nulls` is skipped because [`generate`] synthesizes that method from
/// the output type when the modifier is absent, then places it with
/// [`insert_introduces_nulls`].
fn override_methods(shape: Shape, mods: &Modifiers) -> Vec<TokenStream> {
    let present: Vec<_> = mods.iter().collect();
    shape
        .modifiers()
        .iter()
        .filter(|(modifier, _)| *modifier != Modifier::IntroducesNulls)
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

/// Places a hand-built `introduces_nulls` method in the slot `shape`'s table assigns
/// it, among the methods `override_methods` generated.
///
/// Only the modifiers ahead of `IntroducesNulls` in the table decide the insertion
/// point, so whether `mods` itself carries `introduces_nulls` is immaterial.
fn insert_introduces_nulls(
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

/// What the shared expansion needs that is not arity specific.
struct Expansion {
    struct_name: Ident,
    /// True when the struct is defined at the call site, so expansion attaches an
    /// inherent method instead of defining a unit struct.
    has_self: bool,
    sqlname: TokenStream,
    fn_name: Ident,
    /// The registry's record of this function's source, generated as members of the
    /// `FuncName` impl.
    source: TokenStream,
}

/// Wraps an arity's trait impl with the parts every arity shares: the unit-struct
/// definition (unless `has_self`), the `Display` impl, the `FuncName` impl, and the
/// annotated function itself.
fn expand(e: &Expansion, func: &syn::ItemFn, trait_impl: TokenStream) -> TokenStream {
    let Expansion {
        struct_name,
        has_self,
        sqlname,
        fn_name,
        source,
    } = e;

    let display_impl = quote! {
        impl std::fmt::Display for #struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(#sqlname)
            }
        }
    };

    let funcname_impl = quote! {
        impl crate::func::FuncName for #struct_name {
            const NAME: &'static str = stringify!(#fn_name);
            #source
        }
    };

    if *has_self {
        quote! {
            impl #struct_name {
                #func
            }
            #trait_impl
            #display_impl
            #funcname_impl
        }
    } else {
        quote! {
            #[derive(
                Ord, PartialOrd, Clone,
                Debug, Eq, PartialEq, serde::Serialize,
                serde::Deserialize, Hash,
            )]
            #[cfg_attr(any(test, feature = "proptest"), derive(proptest_derive::Arbitrary))]
            pub struct #struct_name;

            #trait_impl
            #display_impl
            #funcname_impl

            #func
        }
    }
}

/// A variadic function's parameter names and their pre-erasure types, skipping the
/// receiver and the trailing `&RowArena`.
fn variadic_params(
    func: &syn::ItemFn,
    arena: bool,
    has_self: bool,
) -> darling::Result<(Vec<syn::Type>, Vec<Ident>)> {
    let start = usize::from(has_self);
    let end = func.sig.inputs.len() - usize::from(arena);
    if end == start {
        return Err(darling::Error::custom(
            "variadic function must have at least one input parameter",
        ));
    }

    let mut types = Vec::new();
    let mut names = Vec::new();
    for param in func.sig.inputs.iter().skip(start).take(end - start) {
        match param {
            syn::FnArg::Typed(pat) => {
                let syn::Pat::Ident(ident) = &*pat.pat else {
                    return Err(
                        darling::Error::custom("unsupported parameter pattern").with_span(&pat.pat)
                    );
                };
                names.push(ident.ident.clone());
                types.push(signature::patch_lifetimes(&pat.ty));
            }
            syn::FnArg::Receiver(_) => {
                return Err(darling::Error::custom("unexpected self parameter"));
            }
        }
    }
    Ok((types, names))
}

/// Generates the trait impl for one `#[sqlfunc]`-annotated function, together with the
/// items [`expand`] wraps around it.
///
/// `struct_ty` names the generated struct when the call site spells one, which only
/// the variadic arity accepts. `has_self` says the annotated function takes a
/// receiver, so the trait's `call` dispatches through it.
pub(crate) fn generate(
    shape: Shape,
    func: &syn::ItemFn,
    mut mods: Modifiers,
    struct_ty: Option<syn::Path>,
    has_self: bool,
    attr: &TokenStream,
) -> darling::Result<TokenStream> {
    reject_inapplicable(shape, &mods)?;

    let fn_name = &func.sig.ident;
    let struct_name = struct_ty
        .as_ref()
        .and_then(|ty| ty.segments.last())
        .map_or_else(|| signature::camel_case(fn_name), |seg| seg.ident.clone());

    // Whether the annotated function itself wants the arena, which is independent of
    // whether the trait's `call` receives one.
    let arena = signature::last_is_arena(func);
    let output_ty_raw = signature::output_type(func)?;
    let generic_params = signature::find_generic_type_params(func);

    // Unary and binary bind their inputs positionally, ignoring how the function
    // spells its parameters, while variadic forwards the real names.
    let (param_types_raw, param_names) = match shape {
        Shape::Unary => (
            vec![signature::arg_type(func, 0)?],
            vec![Ident::new("a", Span::call_site())],
        ),
        Shape::Binary => (
            vec![signature::arg_type(func, 0)?, signature::arg_type(func, 1)?],
            vec![
                Ident::new("a", Span::call_site()),
                Ident::new("b", Span::call_site()),
            ],
        ),
        Shape::Variadic => variadic_params(func, arena, has_self)?,
    };

    let output_type = mods.output_type.take();
    let mut output_type_expr = mods.output_type_expr.take();
    let mut introduces_nulls = mods.introduces_nulls.take();

    // Derive the output type from where the generic parameters land. This reads the
    // pre-erasure types, because erasure is what removes those parameters.
    if !generic_params.is_empty() && output_type.is_none() && output_type_expr.is_none() {
        if let Some(derived) = signature::derive_output_type_for_generics(
            &param_types_raw,
            output_ty_raw,
            &generic_params,
            shape == Shape::Unary,
        )? {
            output_type_expr = Some(syn::parse2(derived)?);
            if introduces_nulls.is_none() {
                let nullable = signature::is_option_wrapped(output_ty_raw);
                introduces_nulls = Some(syn::parse_quote!(#nullable));
            }
        }
    }

    // TODO: these two conflict checks construct their error with `unknown_field`,
    // which renders as "Unknown field: <message>", while every other modifier
    // legality error in the crate uses `Error::custom`. The message text is pinned
    // by a snapshot, so changing the constructor requires updating that snapshot.
    if output_type.is_some() && output_type_expr.is_some() {
        return Err(darling::Error::unknown_field(
            "output_type and output_type_expr cannot be used together",
        ));
    }
    if output_type_expr.is_some() && introduces_nulls.is_none() {
        return Err(darling::Error::unknown_field(
            "output_type_expr requires introduces_nulls",
        ));
    }

    // Erase generic type params → Datum<'a> for use in the trait impl's associated
    // types, where the function's parameters are not in scope.
    let param_types: Vec<syn::Type> = param_types_raw
        .iter()
        .map(|ty| signature::erase_all_generic_params(ty, &generic_params))
        .collect();
    let output_ty = signature::erase_all_generic_params(output_ty_raw, &generic_params);

    // A lone input passes through bare. Several become a tuple.
    let input_ty: syn::Type = if let [ty] = param_types.as_slice() {
        ty.clone()
    } else {
        syn::parse_quote! { (#(#param_types),*) }
    };
    let destructure = if let [name] = param_names.as_slice() {
        quote! { #name }
    } else {
        quote! { (#(#param_names),*) }
    };

    let (mut output_type_code, mut introduces_nulls_fn) = if let Some(output_type) = output_type {
        let introduces_nulls_fn = quote! {
            fn introduces_nulls(&self) -> bool {
                <#output_type as ::mz_repr::OutputDatumType<'_, ()>>::nullable()
            }
        };
        (
            quote! { <#output_type>::as_column_type() },
            Some(introduces_nulls_fn),
        )
    } else {
        (quote! { Self::Output::as_column_type() }, None)
    };

    if let Some(output_type_expr) = output_type_expr {
        output_type_code = quote! { #output_type_expr };
    }

    if let Some(introduces_nulls) = introduces_nulls {
        introduces_nulls_fn = Some(quote! {
            fn introduces_nulls(&self) -> bool {
                #introduces_nulls
            }
        });
    }

    let mut methods = override_methods(shape, &mods);
    if let Some(introduces_nulls_fn) = introduces_nulls_fn {
        insert_introduces_nulls(&mut methods, shape, &mods, introduces_nulls_fn);
    }

    let arena_param = if shape.takes_arena() {
        quote! { , temp_storage: &'a mz_repr::RowArena }
    } else {
        quote! {}
    };
    let arena_arg = if arena {
        quote! { , temp_storage }
    } else {
        quote! {}
    };
    let receiver = if has_self {
        quote! { self. }
    } else {
        quote! {}
    };

    let trait_path = shape.trait_path();
    let (output_method, output_method_param) = shape.output_method();
    let nullability = shape.nullability(&signature::non_nullable_position_checks(&param_types));

    let trait_impl = quote! {
        impl #trait_path for #struct_name {
            type Input<'a> = #input_ty;
            type Output<'a> = #output_ty;

            fn call<'a>(
                &self,
                #destructure: Self::Input<'a>
                #arena_param
            ) -> Self::Output<'a> {
                #receiver #fn_name(#(#param_names),* #arena_arg)
            }

            fn #output_method(
                &self,
                #output_method_param
            ) -> mz_repr::SqlColumnType {
                use mz_repr::AsColumnType;
                let output = #output_type_code;
                let propagates_nulls = #trait_path::propagates_nulls(self);
                let nullable = output.nullable;
                #nullability
            }

            #(#methods)*
        }
    };

    let sqlname = mods
        .sqlname
        .as_ref()
        .map_or_else(|| quote! { stringify!(#fn_name) }, |name| quote! { #name });
    let expansion = Expansion {
        struct_name,
        has_self,
        sqlname,
        fn_name: fn_name.clone(),
        source: sqlfunc_source(attr, func, &param_types_raw, output_ty_raw, &param_types),
    };
    Ok(expand(&expansion, func, trait_impl))
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::shape::Shape;

    #[mz_ore::test]
    fn binary_is_monotone_generates_a_pair_return() {
        let mods = crate::modifiers::Modifiers::from_tokens(quote! {
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
        let mods = crate::modifiers::Modifiers::from_tokens(quote! { is_monotone = true })
            .expect("parses");
        let methods = super::override_methods(Shape::Unary, &mods);
        let rendered = methods[0].to_string();
        assert!(
            rendered.contains("fn is_monotone (& self) -> bool"),
            "got:\n{rendered}"
        );
    }

    #[mz_ore::test]
    fn override_methods_never_generates_introduces_nulls() {
        let mods = crate::modifiers::Modifiers::from_tokens(quote! {
            could_error = true,
            introduces_nulls = true,
        })
        .expect("parses");
        for shape in [Shape::Unary, Shape::Binary, Shape::Variadic] {
            let rendered = super::override_methods(shape, &mods)
                .iter()
                .map(|m| m.to_string())
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                !rendered.contains("introduces_nulls"),
                "{} generated it:\n{rendered}",
                shape.label(),
            );
            assert!(rendered.contains("could_error"), "got:\n{rendered}");
        }
    }

    #[mz_ore::test]
    fn absent_modifiers_generate_nothing() {
        let mods = crate::modifiers::Modifiers::from_tokens(quote! {}).expect("parses");
        assert!(super::override_methods(Shape::Unary, &mods).is_empty());
    }

    #[mz_ore::test]
    fn insert_introduces_nulls_lands_after_could_error() {
        // `introduces_nulls` is present here to pin `insert_introduces_nulls`'s claim
        // that carrying it does not shift the insertion point.
        let mods = crate::modifiers::Modifiers::from_tokens(quote! {
            could_error = true,
            introduces_nulls = true,
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
        let mods = crate::modifiers::Modifiers::from_tokens(quote! { is_monotone = true })
            .expect("parses");
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

    #[mz_ore::test]
    fn unit_struct_expansion_defines_the_struct_and_keeps_the_function() {
        let func: syn::ItemFn = syn::parse_quote! {
            fn some_fn(a: i32) -> i32 { a }
        };
        let e = super::Expansion {
            struct_name: syn::parse_quote!(SomeFn),
            has_self: false,
            sqlname: quote! { "some_fn" },
            fn_name: syn::parse_quote!(some_fn),
            source: quote! {},
        };
        let out = super::expand(&e, &func, quote! { impl Marker for SomeFn {} }).to_string();
        assert!(out.contains("pub struct SomeFn"), "got:\n{out}");
        assert!(
            out.contains("impl std :: fmt :: Display for SomeFn"),
            "got:\n{out}"
        );
        assert!(
            out.contains("impl crate :: func :: FuncName for SomeFn"),
            "got:\n{out}"
        );
        assert!(out.contains("fn some_fn"), "got:\n{out}");
    }

    #[mz_ore::test]
    fn external_struct_expansion_attaches_a_method_and_defines_no_struct() {
        let func: syn::ItemFn = syn::parse_quote! {
            fn some_fn(&self, a: i32) -> i32 { a }
        };
        let e = super::Expansion {
            struct_name: syn::parse_quote!(SomeFn),
            has_self: true,
            sqlname: quote! { "some_fn" },
            fn_name: syn::parse_quote!(some_fn),
            source: quote! {},
        };
        let out = super::expand(&e, &func, quote! { impl Marker for SomeFn {} }).to_string();
        assert!(!out.contains("pub struct SomeFn"), "got:\n{out}");
        assert!(out.contains("impl SomeFn"), "got:\n{out}");
    }
}
