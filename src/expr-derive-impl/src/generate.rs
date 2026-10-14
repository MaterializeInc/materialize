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
/// `introduces_nulls_fn` fills the table's `introduces_nulls` slot, because
/// [`generate`] synthesizes that method from the output type and the modifier together.
/// The walk never builds it from `mods`.
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
            let name = Ident::new(modifier.name(), Span::call_site());
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

/// The SQL arguments' pre-erasure types, and the names `call` binds them to.
///
/// Unary and binary bind their inputs positionally as `a` and `b`, ignoring how the
/// function spells its parameters, while variadic forwards the real names.
fn sql_params(shape: Shape, func: &syn::ItemFn) -> darling::Result<(Vec<syn::Type>, Vec<Ident>)> {
    // `crate::sqlfunc::determine_shape` gives unary one SQL argument and binary two.
    const POSITIONAL: [&str; 2] = ["a", "b"];
    let mut types = Vec::new();
    let mut names = Vec::new();
    for (position, param) in signature::sql_args(func).into_iter().enumerate() {
        let syn::FnArg::Typed(pat) = param else {
            return Err(darling::Error::custom("unexpected self parameter").with_span(param));
        };
        let name = match shape {
            Shape::Unary | Shape::Binary => Ident::new(POSITIONAL[position], Span::call_site()),
            Shape::Variadic => {
                let syn::Pat::Ident(ident) = &*pat.pat else {
                    return Err(
                        darling::Error::custom("unsupported parameter pattern").with_span(&pat.pat)
                    );
                };
                ident.ident.clone()
            }
        };
        types.push(signature::patch_lifetimes(&pat.ty));
        names.push(name);
    }
    Ok((types, names))
}

/// The column type expression the output-type method starts from, and the
/// `introduces_nulls` method if one is generated.
///
/// `output_type`, `output_type_expr` and `introduces_nulls` decide both together. A
/// generic signature that sets neither output modifier derives them from where its
/// type parameters appear.
fn output_type_and_introduces_nulls(
    shape: Shape,
    mods: &Modifiers,
    param_types_raw: &[syn::Type],
    output_ty_raw: &syn::Type,
    generic_params: &[Ident],
) -> darling::Result<(TokenStream, Option<TokenStream>)> {
    let output_type = mods.output_type();
    let mut output_type_expr = mods.output_type_expr().map(|expr| quote! { #expr });
    let mut introduces_nulls = mods.introduces_nulls().map(|expr| quote! { #expr });

    if output_type.is_some() && output_type_expr.is_some() {
        return Err(darling::Error::custom(
            "output_type and output_type_expr cannot be used together",
        ));
    }

    // Derive the output type from where the generic parameters land. This reads the
    // pre-erasure types, because erasure is what removes those parameters.
    if !generic_params.is_empty() && output_type.is_none() && output_type_expr.is_none() {
        if let Some(derived) = signature::derive_output_type_for_generics(
            param_types_raw,
            output_ty_raw,
            generic_params,
            shape == Shape::Unary,
        )? {
            output_type_expr = Some(derived);
            if introduces_nulls.is_none() {
                let nullable = signature::is_option_wrapped(output_ty_raw);
                introduces_nulls = Some(quote! { #nullable });
            }
        }
    }

    let column_type = match (output_type, output_type_expr) {
        (Some(output_type), _) => quote! { <#output_type>::as_column_type() },
        (None, Some(output_type_expr)) => output_type_expr,
        (None, None) => quote! { Self::Output::as_column_type() },
    };
    // Without an `introduces_nulls` method the trait default reads `Self::Output`,
    // which an explicit `output_type` replaces.
    let introduces_nulls = introduces_nulls.or_else(|| {
        output_type.map(|output_type| {
            quote! { <#output_type as ::mz_repr::OutputDatumType<'_, ()>>::nullable() }
        })
    });
    let introduces_nulls_fn = introduces_nulls.map(|introduces_nulls| {
        quote! {
            fn introduces_nulls(&self) -> bool {
                #introduces_nulls
            }
        }
    });
    Ok((column_type, introduces_nulls_fn))
}

/// Generates the trait impl for one `#[sqlfunc]`-annotated function, together with the
/// items [`expand`] wraps around it.
///
/// `struct_ty` names the generated struct when the call site spells one.
pub(crate) fn generate(
    shape: Shape,
    func: &syn::ItemFn,
    mods: Modifiers,
    struct_ty: Option<syn::Path>,
    attr: &TokenStream,
) -> darling::Result<TokenStream> {
    reject_inapplicable(shape, &mods)?;

    let fn_name = &func.sig.ident;
    let struct_name = struct_ty
        .as_ref()
        .and_then(|ty| ty.segments.last())
        .map_or_else(|| signature::camel_case(fn_name), |seg| seg.ident.clone());

    // A receiver makes `call` dispatch through `self` to a method on a struct defined
    // at the call site.
    let has_self = signature::has_receiver(func);
    // Whether the annotated function takes the arena. The trait's `call` always receives
    // one.
    let arena = signature::last_is_arena(func);
    let output_ty_raw = signature::output_type(func)?;
    let generic_params = signature::find_generic_type_params(func);

    let (param_types_raw, param_names) = sql_params(shape, func)?;

    if mods.skip_display() && mods.sqlname().is_some() {
        return Err(darling::Error::custom(
            "sqlname has no effect with skip_display, which suppresses the only impl that reads it",
        ));
    }
    // A hand-written `Display` is only needed when the name depends on struct state,
    // and without `&self` the macro defines a unit struct that has none.
    if mods.skip_display() && !has_self {
        return Err(darling::Error::custom(
            "skip_display requires a &self receiver, since a unit struct has no state for a hand-written Display to read",
        ));
    }

    // Replace generic type params with `Datum<'a>` for use in the trait impl's associated
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

    let (output_type_code, introduces_nulls_fn) = output_type_and_introduces_nulls(
        shape,
        &mods,
        &param_types_raw,
        output_ty_raw,
        &generic_params,
    )?;
    let methods = override_methods(shape, &mods, introduces_nulls_fn);

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
                #destructure: Self::Input<'a>,
                temp_storage: &'a mz_repr::RowArena,
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
        .sqlname()
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
