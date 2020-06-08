// Copyright Materialize, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Code generation.
//!
//! This module processes the IR to generate the `visit` and `visit_mut`
//! modules.

use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::{format_ident, quote};

use crate::ir::{Enum, Ir, Item, Struct, Type};

/// Generates the `visit` module.
pub fn gen_visit(ir: &Ir) -> TokenStream {
    gen_root(&Config { mutable: false }, ir)
}

/// Generates the `visit_mut` module.
pub fn gen_visit_mut(ir: &Ir) -> TokenStream {
    gen_root(&Config { mutable: true }, ir)
}

struct Config {
    mutable: bool,
}

impl Config {
    fn trait_ident(&self) -> Ident {
        match self.mutable {
            false => format_ident!("Visit"),
            true => format_ident!("VisitMut"),
        }
    }

    fn func_ident(&self, name: &str) -> Ident {
        match self.mutable {
            false => format_ident!("visit_{}", snake_case(name)),
            true => format_ident!("visit_{}_mut", snake_case(name)),
        }
    }

    fn borrow(&self) -> TokenStream {
        match self.mutable {
            false => quote!(&),
            true => quote!(&mut),
        }
    }

    fn borrow_for(&self, lifetime: &str) -> TokenStream {
        let lifetime = syn::Lifetime::new(lifetime, Span::call_site());
        match self.mutable {
            false => quote!(& #lifetime),
            true => quote!(& #lifetime mut),
        }
    }
}

fn gen_root(c: &Config, ir: &Ir) -> TokenStream {
    let mut methods = TokenStream::new();
    let mut funcs = TokenStream::new();

    for (name, item) in ir {
        gen_item(c, name, item, &mut methods, &mut funcs);
    }

    let trait_summary = match c.mutable {
        false => "Traverses an immutable AST.",
        true => "Traverses a mutable AST.",
    };
    let trait_ident = c.trait_ident();
    quote! {
        #[doc = #trait_summary]
        ///
        /// See the [module documentation](self) for details.
        pub trait #trait_ident<'ast> {
            #methods
        }

        #funcs
    }
}

fn gen_item(
    c: &Config,
    name: &str,
    item: &Item,
    methods: &mut TokenStream,
    funcs: &mut TokenStream,
) {
    let trait_ident = c.trait_ident();
    let func_ident = c.func_ident(name);
    let ty_ident = format_ident!("{}", name);
    let borrow = c.borrow_for("'ast");
    let visit_impl = match &item {
        Item::Struct(s) => gen_struct(c, s),
        Item::Enum(e) => gen_enum(c, &ty_ident, e),
    };
    methods.extend(quote! {
        fn #func_ident(&mut self, node: #borrow #ty_ident) {
            #func_ident(self, node)
        }
    });
    funcs.extend(quote! {
        pub fn #func_ident<'ast, V>(visitor: &mut V, node: #borrow #ty_ident)
        where
            V: #trait_ident<'ast> + ?Sized,
        {
            #visit_impl
        }
    });
}

fn gen_struct(c: &Config, s: &Struct) -> TokenStream {
    let mut visit_impl = TokenStream::new();
    for (i, f) in s.fields.iter().enumerate() {
        let name = field_name(f.name.as_deref(), i);
        let borrow = c.borrow();
        visit_impl.extend(gen_element(c, quote!(#borrow node.#name), &f.ty));
    }
    visit_impl
}

fn gen_enum(c: &Config, ty_ident: &Ident, e: &Enum) -> TokenStream {
    let mut match_arms = TokenStream::new();
    for v in &e.variants {
        let variant_ident = format_ident!("{}", v.name);
        let mut bindings = TokenStream::new();
        let mut inner = TokenStream::new();
        for (i, f) in v.fields.iter().enumerate() {
            let name = field_name(f.name.as_deref(), i);
            let binding = format_ident!("x{}", i);
            bindings.extend(quote!(#name: #binding,));
            inner.extend(gen_element(c, quote!(#binding), &f.ty));
        }
        match_arms.extend(quote! {
            #ty_ident::#variant_ident { #bindings } => {
                #inner
            }
        })
    }
    quote! {
        match node {
            #match_arms
        }
    }
}

fn gen_element(c: &Config, name: TokenStream, ty: &Type) -> TokenStream {
    match ty {
        Type::Primitive => TokenStream::new(),
        Type::Option(ty) => {
            let inner = gen_element(c, quote!(x), ty);
            quote! {
                if let Some(x) = #name {
                    #inner
                }
            }
        }
        Type::Vec(ty) => {
            let inner = gen_element(c, quote!(x), ty);
            quote! {
                for x in #name {
                    #inner
                }
            }
        }
        Type::Box(ty) => {
            let borrow = c.borrow();
            gen_element(c, quote!(#borrow *#name), ty)
        }
        Type::Local(s) => {
            let func_ident = c.func_ident(s);
            quote! {
                visitor.#func_ident(#name);
            }
        }
    }
}

fn snake_case(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        if c.is_ascii_uppercase() {
            if !out.is_empty() {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

fn field_name(name: Option<&str>, i: usize) -> TokenStream {
    match name {
        Some(name) => {
            let id = format_ident!("{}", name);
            quote!(#id)
        }
        None => {
            let lit = Literal::usize_unsuffixed(i);
            quote!(#lit)
        }
    }
}
