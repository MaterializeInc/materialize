use std::cmp;

use attrs::{
    container::{self, EnumRepr},
    variant,
};
use darling::{FromAttributes, FromMeta};
use num_bigint::BigInt;
use proc_macro2::{Span, TokenStream};
use proc_macro_error::abort;
use quote::{ToTokens, TokenStreamExt};
use syn::spanned::Spanned;

pub mod attrs;
mod misc;

pub fn impl_from_value_for_enum(
    attrs: &[syn::Attribute],
    ident: &proc_macro2::Ident,
    _generics: &syn::Generics,
    data_enum: &syn::DataEnum,
) -> crate::Result<TokenStream> {
    let item_attrs = <container::Mysql as FromAttributes>::from_attributes(attrs).expect("foo");
    let meta = attrs.iter().map(|attr| &attr.meta).collect::<Vec<_>>();

    let repr = meta
        .iter()
        .find(|x| matches!(x, syn::Meta::List(y) if y.path.is_ident("repr")))
        .map(|x| <container::Repr as FromMeta>::from_meta(x))
        .transpose()?
        .unwrap_or_default();

    if matches!(
        repr.0,
        container::EnumRepr::U128(_) | container::EnumRepr::I128(_)
    ) {
        abort!(crate::Error::UnsupportedRepresentation(repr.0.span()));
    }

    if *item_attrs.is_integer && *item_attrs.is_string {
        abort!(crate::Error::ConflictingsAttributes(
            item_attrs.is_string.span(),
            item_attrs.is_integer.span()
        ));
    }

    let mut variants = Vec::new();

    let mut max_discriminant = BigInt::default();
    let mut min_discriminant = BigInt::default();

    let mut next_discriminant = BigInt::default();
    for variant in &data_enum.variants {
        if !matches!(variant.fields, syn::Fields::Unit) {
            abort!(crate::Error::NonUnitVariant(variant.span()));
        }

        let mut variant_attrs =
            <variant::Mysql as FromAttributes>::from_attributes(&variant.attrs).expect("bar");

        let discriminant = variant
            .discriminant
            .as_ref()
            .map(|(_, e)| misc::get_discriminant(e))
            .transpose()?
            .unwrap_or(next_discriminant);

        if discriminant >= BigInt::from(u64::MAX) {
            abort!(crate::Error::UnsupportedRepresentation(variant.span()));
        }

        min_discriminant = cmp::min(min_discriminant, discriminant.clone());
        max_discriminant = cmp::max(max_discriminant, discriminant.clone());

        next_discriminant = &discriminant + BigInt::from(1u8);

        if !*item_attrs.is_string && !*item_attrs.is_integer {
            if !item_attrs.allow_invalid_discriminants && !variant_attrs.allow_invalid_discriminants
            {
                if discriminant < BigInt::default() {
                    crate::warn::print_warning(
                        "negative discriminants for MySql enums are discouraging",
                        format!("#[mysql(allow_invalid_discriminants)]\nenum {} {{", ident),
                        "use the following annotation to suppress this warning",
                    )
                    .unwrap();
                } else if discriminant > BigInt::from(u16::MAX) {
                    crate::warn::print_warning(
                        "MySql only supports up to 65535 distinct elements in an enum",
                        format!("#[mysql(allow_invalid_discriminants)]\nenum {} {{", ident),
                        "use the following annotation to suppress this warning",
                    )
                    .unwrap();
                }
            }
            if discriminant == BigInt::default() {
                if variant_attrs.explicit_invalid {
                    variant_attrs.rename = Some("".into());
                } else {
                    abort!(crate::Error::ExplicitInvalid(variant.span()))
                }
            }
        }

        variants.push(EnumVariant {
            my_attrs: variant_attrs,
            ident: variant.ident.clone(),
            name: variant.ident.to_string(),
            discriminant,
        });
    }

    if !*item_attrs.is_integer && !*item_attrs.is_string {
        if !item_attrs.allow_sparse_enum {
            variants.sort_by_key(|x| x.discriminant.clone());
            let mut prev_discriminant = BigInt::default();
            for variant in variants.iter() {
                let is_next =
                    (variant.discriminant.clone() - BigInt::from(1_u8)) == prev_discriminant;
                let is_invalid = variant.discriminant == prev_discriminant
                    && prev_discriminant == BigInt::default();
                if !is_next && !is_invalid {
                    crate::warn::print_warning(
                format!("Sparse enum variant {}::{}. Consider annotating with #[mysql(is_integer)] or #[mysql(is_string)].", ident, variant.ident),
                format!("#[mysql(is_integer)]\nenum {} {{", ident),
                "use #[mysql(allow_sparse_enum)] to suppress this warning",
            )
            .unwrap();
                }
                prev_discriminant = variant.discriminant.clone();
            }
        }

        if min_discriminant >= BigInt::default()
            && max_discriminant <= BigInt::from(u8::MAX)
            && !item_attrs.allow_suboptimal_repr
        {
            if !matches!(repr.0, EnumRepr::U8(_)) {
                crate::warn::print_warning(
                    "enum representation is suboptimal. Consider the following annotation:",
                    format!("#[repr(u8)]\nenum {} {{", ident),
                    "use #[mysql(allow_suboptimal_repr)] to suppress this warning",
                )
                .unwrap();
            }
        } else if min_discriminant >= BigInt::default()
            && max_discriminant <= BigInt::from(u16::MAX)
            && !matches!(repr.0, EnumRepr::U8(_))
        {
            crate::warn::print_warning(
                "enum representation is suboptimal. Consider the following annotation:",
                format!("#[repr(u16)]\nenum {} {{", ident),
                "use #[mysql(allow_suboptimal_repr)] to suppress this warning",
            )
            .unwrap();
        }
    }

    let derived = Enum {
        item_attrs,
        name: ident.clone(),
        variants,
        repr: repr.0,
    };

    let generated = quote::quote! { #derived };
    Ok(generated)
}

struct Enum {
    item_attrs: container::Mysql,
    name: proc_macro2::Ident,
    variants: Vec<EnumVariant>,
    repr: EnumRepr,
}

impl ToTokens for Enum {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let Self {
            item_attrs,
            name: container_name,
            variants,
            repr,
        } = self;

        let crat = match item_attrs.crate_name {
            container::Crate::NotFound => abort!(crate::Error::NoCrateNameFound),
            container::Crate::Multiple => abort!(crate::Error::MultipleCratesFound),
            container::Crate::Itself => syn::Ident::new("crate", Span::call_site()),
            container::Crate::Found(ref name) => syn::Ident::new(name, Span::call_site()),
        };

        let ir_name = syn::Ident::new(&format!("{container_name}Ir"), Span::call_site());
        let parsed_name = syn::Ident::new(&format!("{container_name}IrParsed"), Span::call_site());

        let branches = variants.iter().map(
            |EnumVariant {
                 my_attrs,
                 name,
                 ident,
                 discriminant,
             }| {
                let mut name = name.clone();
                if let Some(ref rename) = item_attrs.rename_all {
                    name = rename.rename(&name);
                }
                if let Some(ref new_name) = my_attrs.rename {
                    name = new_name.clone();
                }
                let s = syn::LitByteStr::new(name.as_bytes(), Span::call_site());
                let n = syn::LitInt::new(&discriminant.to_string(), Span::call_site());

                if *item_attrs.is_integer {
                    if discriminant < &BigInt::default() {
                        quote::quote!(
                            #crat::Value::Int(#n) => {
                                Ok(#ir_name(#parsed_name::Ready(#container_name::#ident)))
                            }
                        )
                    } else {
                        quote::quote!(
                            #crat::Value::Int(#n) | #crat::Value::UInt(#n) => {
                                Ok(#ir_name(#parsed_name::Ready(#container_name::#ident)))
                            }
                        )
                    }
                } else if *item_attrs.is_string {
                    quote::quote!(
                        #crat::Value::Bytes(ref x) if x == #s => {
                            Ok(#ir_name(#parsed_name::Parsed(#container_name::#ident, v)))
                        }
                    )
                } else if discriminant < &BigInt::default() {
                    quote::quote!(
                        #crat::Value::Bytes(ref x) if x == #s => {
                            Ok(#ir_name(#parsed_name::Parsed(#container_name::#ident, v)))
                        }
                        #crat::Value::Int(#n) => {
                            Ok(#ir_name(#parsed_name::Ready(#container_name::#ident)))
                        }
                    )
                } else {
                    quote::quote!(
                        #crat::Value::Bytes(ref x) if x == #s => {
                            Ok(#ir_name(#parsed_name::Parsed(#container_name::#ident, v)))
                        }
                        #crat::Value::Int(#n) | #crat::Value::UInt(#n) => {
                            Ok(#ir_name(#parsed_name::Ready(#container_name::#ident)))
                        }
                    )
                }
            },
        );

        let to_value = if *item_attrs.is_string {
            let branches = variants.iter().map(
                |EnumVariant {
                     my_attrs,
                     name,
                     ident,
                     ..
                 }| {
                    let mut name = name.clone();
                    if let Some(ref rename) = item_attrs.rename_all {
                        name = rename.rename(&name);
                    }
                    if let Some(ref new_name) = my_attrs.rename {
                        name = new_name.clone();
                    }
                    let s = syn::LitByteStr::new(name.as_bytes(), Span::call_site());
                    quote::quote!(
                        #container_name::#ident => #crat::Value::Bytes(#s.to_vec())
                    )
                },
            );
            quote::quote!(
                impl std::convert::From<#container_name> for #crat::Value {
                    fn from(x: #container_name) -> Self {
                        match x {
                            #( #branches ),*
                        }
                    }
                }
            )
        } else if *item_attrs.is_integer {
            quote::quote!(
                impl std::convert::From<#container_name> for #crat::Value {
                    fn from(x: #container_name) -> Self {
                        let repr = x as #repr;
                        match <i64 as std::convert::TryFrom<#repr>>::try_from(repr) {
                            Ok(x) => #crat::Value::Int(x),
                            _ => #crat::Value::UInt(repr as u64),
                        }
                    }
                }
            )
        } else {
            quote::quote!(
                impl std::convert::From<#container_name> for #crat::Value {
                    fn from(x: #container_name) -> Self {
                        #crat::Value::Int(x as #repr as i64)
                    }
                }
            )
        };

        let new_tokens = quote::quote!(
            pub struct #ir_name(#parsed_name);

            enum #parsed_name {
                /// Type instance is ready without parsing.
                Ready(#container_name),
                /// Type instance is successfully parsed from this value.
                Parsed(#container_name, #crat::Value),
            }

            impl #parsed_name {
                fn commit(self) -> #container_name {
                    match self {
                        Self::Ready(t) | Self::Parsed(t, _) => t,
                    }
                }

                fn rollback(self) -> #crat::Value
                {
                    match self {
                        Self::Ready(t) => t.into(),
                        Self::Parsed(_, v) => v,
                    }
                }
            }

            impl std::convert::TryFrom<#crat::Value> for #ir_name {
                type Error = #crat::FromValueError;

                fn try_from(v: #crat::Value) -> std::result::Result<Self, Self::Error> {
                    match v {
                        #( #branches )*
                        v => Err(#crat::FromValueError(v)),
                    }
                }
            }

            impl std::convert::From<#ir_name> for #container_name {
                fn from(value: #ir_name) -> Self {
                    value.0.commit()
                }
            }

            impl std::convert::From<#ir_name> for #crat::Value {
                fn from(value: #ir_name) -> Self {
                    value.0.rollback()
                }
            }

            impl #crat::prelude::FromValue for #container_name {
                type Intermediate = #ir_name;
            }

            #to_value
        );

        tokens.append_all(new_tokens);
    }

    fn to_token_stream(&self) -> TokenStream {
        let mut tokens = TokenStream::new();
        self.to_tokens(&mut tokens);
        tokens
    }

    fn into_token_stream(self) -> TokenStream
    where
        Self: Sized,
    {
        self.to_token_stream()
    }
}

struct EnumVariant {
    pub my_attrs: variant::Mysql,
    pub name: String,
    pub ident: syn::Ident,
    pub discriminant: BigInt,
}

#[cfg(test)]
mod tests {
    #[test]
    fn derive_enum() {
        let code = r#"
            #[derive(FromValue)]
            #[mysql(rename_all = "kebab-case", crate_name = "mysql", allow_sparse_enum, allow_suboptimal_repr)]
            #[repr(u32)]
            enum Size {
                #[mysql(explicit_invalid)]
                Invalid,
                XSmall = 1,
                Small = 3,
                Medium,
                Large,
                #[mysql(rename = "XL")]
                XLarge,
            }
        "#;
        let input = syn::parse_str::<syn::DeriveInput>(code).unwrap();
        let derived = super::super::impl_from_value(&input).unwrap();
        eprintln!("{}", derived);
    }
}
