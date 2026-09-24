// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//! Codegen shared by every scalar function arity.

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::modifiers::{Modifiers, reject_inapplicable};
use crate::shape::{Modifier, Shape};
use crate::signature;
use crate::source::sqlfunc_source;

/// One `fn name(&self) -> Ret { expr }` per modifier present in `mods`, in the arity
/// table's order rather than the attribute's order, so the generated code is stable
/// against how a call site happens to spell its modifiers.
///
/// `introduces_nulls_fn` takes the table's `introduces_nulls` slot. [`generate`]
/// synthesizes that method from the output type and has already taken the modifier out
/// of `mods`, so the walk never finds one to build.
fn override_methods(
    shape: Shape,
    mods: &Modifiers,
    mut introduces_nulls_fn: Option<TokenStream>,
) -> Vec<TokenStream> {
    let present: Vec<_> = mods.iter().collect();
    shape
        .modifiers()
        .iter()
        .filter_map(|(modifier, ret)| {
            if *modifier == Modifier::IntroducesNulls {
                return introduces_nulls_fn.take();
            }
            let expr = present
                .iter()
                .find(|(candidate, _)| candidate == modifier)
                .map(|(_, expr)| *expr)?;
            let name = Ident::new(modifier.name(), proc_macro2::Span::call_site());
            let body = ret.body(expr);
            let ret = ret.to_tokens();
            Some(quote! {
                fn #name(&self) -> #ret {
                    #body
                }
            })
        })
        .collect()
}

/// What the shared codegen needs that is not arity specific.
struct Codegen {
    struct_name: Ident,
    /// True when the struct is defined at the call site, so expansion attaches an
    /// inherent method instead of defining a unit struct.
    has_self: bool,
    sqlname: TokenStream,
    /// The registry's record of this function's source, generated as members of the
    /// `FuncName` impl.
    source: TokenStream,
    /// Suppresses the generated `Display` impl, so a call site whose SQL name
    /// depends on struct state can supply its own.
    skip_display: bool,
}

/// Wraps an arity's trait impl with the parts every arity shares: the unit-struct
/// definition (unless `has_self`), the `Display` impl (unless `skip_display`), the
/// `FuncName` impl, and the annotated function itself.
fn expand(e: &Codegen, func: &syn::ItemFn, trait_impl: TokenStream) -> TokenStream {
    let func_name = &func.sig.ident;
    let Codegen {
        struct_name,
        has_self,
        sqlname,
        source,
        skip_display,
    } = e;

    let display_impl = if *skip_display {
        quote! {}
    } else {
        quote! {
            impl std::fmt::Display for #struct_name {
                fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str(#sqlname)
                }
            }
        }
    };

    let funcname_impl = quote! {
        impl crate::func::FuncName for #struct_name {
            const NAME: &'static str = stringify!(#func_name);
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
/// `struct_ty` names the generated struct when the call site spells one. `has_self`
/// says the annotated function takes a receiver, so the trait's `call` dispatches
/// through it.
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
    // spells its parameters, while variadic forwards the real names. A `&self`
    // receiver occupies position 0, so it offsets where the bound arguments start.
    let self_offset = usize::from(has_self);
    let (param_types_raw, param_names) = match shape {
        Shape::Unary => (
            vec![signature::arg_type(func, self_offset)?],
            vec![Ident::new("a", Span::call_site())],
        ),
        Shape::Binary => (
            vec![
                signature::arg_type(func, self_offset)?,
                signature::arg_type(func, self_offset + 1)?,
            ],
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

    // TODO: the conflict checks below construct their error with `unknown_field`,
    // which renders as "Unknown field: <message>", while every other modifier
    // legality error in the crate uses `Error::custom`.
    if output_type.is_some() && output_type_expr.is_some() {
        return Err(darling::Error::unknown_field(
            "output_type and output_type_expr cannot be used together",
        ));
    }
    if mods.skip_display() && mods.sqlname.is_some() {
        return Err(darling::Error::unknown_field(
            "sqlname has no effect with skip_display, which suppresses the only impl that reads it",
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

    let methods = override_methods(shape, &mods, introduces_nulls_fn);

    // Every arity's `call` receives the arena; only `arena` (whether the annotated
    // function itself wants it) varies.
    let arena_param = quote! { , temp_storage: &'a mz_repr::RowArena };
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
    let codegen = Codegen {
        struct_name,
        has_self,
        sqlname,
        source: sqlfunc_source(attr, func, &param_types_raw, output_ty_raw, &param_types),
        skip_display: mods.skip_display(),
    };
    Ok(expand(&codegen, func, trait_impl))
}
