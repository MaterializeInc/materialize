use darling::FromAttributes;
use proc_macro2::{Span, TokenStream};
use proc_macro_error::abort;
use quote::{ToTokens, TokenStreamExt};
use syn::ext::IdentExt;
use syn::spanned::Spanned;

use crate::from_value::enums::attrs::container::Crate;

mod attrs;

pub fn impl_from_row_for_struct(
    attrs: &[syn::Attribute],
    ident: &proc_macro2::Ident,
    generics: &syn::Generics,
    data_struct: &syn::DataStruct,
) -> crate::Result<TokenStream> {
    let fields = match &data_struct.fields {
        syn::Fields::Named(fields) => fields,
        syn::Fields::Unnamed(_) => {
            return Err(crate::Error::StructsWithUnnamedFieldsNotSupported(
                data_struct.struct_token.span,
            ))
        }
        syn::Fields::Unit => {
            return Err(crate::Error::UnitStructsNotSupported(
                data_struct.struct_token.span,
            ))
        }
    };

    let item_attrs = <attrs::container::Mysql as FromAttributes>::from_attributes(attrs)?;

    let derived = GenericStruct {
        ident,
        fields,
        item_attrs,
        generics,
    };
    Ok(quote::quote! { #derived })
}

struct GenericStruct<'a> {
    ident: &'a proc_macro2::Ident,
    item_attrs: attrs::container::Mysql,
    fields: &'a syn::FieldsNamed,
    generics: &'a syn::Generics,
}

impl ToTokens for GenericStruct<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let Self {
            ident,
            item_attrs,
            fields,
            generics,
        } = self;

        let crat = match self.item_attrs.crate_name {
            Crate::NotFound => abort!(crate::Error::NoCrateNameFound),
            Crate::Multiple => abort!(crate::Error::MultipleCratesFound),
            Crate::Itself => syn::Ident::new("crate", Span::call_site()),
            Crate::Found(ref name) => syn::Ident::new(name, Span::call_site()),
        };

        let impl_generics = (!generics.params.is_empty()).then(|| {
            let generics = self.generics.params.iter();
            quote::quote!(< #(#generics,)* >)
        });

        let ident_generics = (!generics.params.is_empty()).then(|| {
            let generics = self.generics.params.iter().map(|g| match g {
                syn::GenericParam::Type(x) => {
                    let ident = &x.ident;
                    quote::quote!(#ident)
                }
                syn::GenericParam::Lifetime(x) => {
                    let lifetime = &x.lifetime;
                    quote::quote!(#lifetime)
                }
                syn::GenericParam::Const(x) => {
                    let ident = &x.ident;
                    quote::quote!(#ident)
                }
            });
            quote::quote!(< #(#generics,)* >)
        });

        let bounds = item_attrs.bound.as_ref().map(|bound| {
            let bound = bound.0.iter();
            quote::quote!(where #(#bound,)*)
        });

        let table_name_constant = item_attrs.table_name.as_ref().map(|name| {
            let lit = syn::LitStr::new(name, name.span());
            quote::quote!(const TABLE_NAME: &'static str = #lit;)
        });

        let fields_attrs = fields
            .named
            .iter()
            .map(|f| <attrs::field::Mysql as FromAttributes>::from_attributes(&f.attrs))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e: darling::Error| abort!(crate::Error::from(e)))
            .unwrap();

        let fields_names = fields
            .named
            .iter()
            .zip(&fields_attrs)
            .map(|(f, attrs)| {
                let mut name = f.ident.as_ref().unwrap().unraw().to_string();

                if let Some(ref r) = item_attrs.rename_all {
                    name = r.rename(&name);
                }

                if let Some(ref n) = attrs.rename {
                    name = n.clone();
                }

                name
            })
            .collect::<Vec<_>>();

        let field_ident = fields
            .named
            .iter()
            .map(|f| f.ident.as_ref().unwrap())
            .collect::<Vec<_>>();

        let filed_name_constant = fields.named.iter().zip(&fields_names).map(|(f, name)| {
            let ident = f.ident.as_ref().unwrap().unraw();
            let lit = syn::LitStr::new(name, f.span());
            let const_name = syn::Ident::new(
                &format!("{}_FIELD", heck::AsShoutySnakeCase(ident.to_string())),
                f.span(),
            );

            quote::quote!(const #const_name: &'static str = #lit;)
        });

        let take_field = fields
            .named
            .iter()
            .zip(&fields_attrs)
            .zip(&fields_names)
            .enumerate()
            .map(|(i, ((f, attrs), name))| {
                let ident = f.ident.as_ref().unwrap();
                let ty = &f.ty;
                let lit = syn::LitStr::new(name, ident.span());

                let place = field_ident
                    .iter()
                    .zip(&fields_attrs)
                    .zip(&fields_names)
                    .take(i)
                    .map(|((f, attrs), name)| {
                        let lit = syn::LitStr::new(name, f.span());
                        if attrs.json {
                            quote::quote!(
                                row.place(*indexes.get(#lit).unwrap(), #f.rollback())
                            )
                        } else {
                            quote::quote!(
                                row.place(*indexes.get(#lit).unwrap(), #f.into())
                            )
                        }
                    });

                let intermediate_ty = if attrs.json {
                    quote::quote!(<#crat::Deserialized<#ty> as FromValue>::Intermediate)
                } else {
                    quote::quote!(<#ty as FromValue>::Intermediate)
                };

                let try_from = if let Some(ref path) = attrs.with {
                    let path = &path.0;
                    quote::quote!( #path(x) )
                } else {
                    quote::quote!( <#intermediate_ty as std::convert::TryFrom<Value>>::try_from(x) )
                };

                quote::quote!(
                    let #ident = {
                        let val = match row.take_opt::<Value, &str>(#lit) {
                            Some(Ok(x)) => match #try_from {
                                Ok(x) => Some(x),
                                Err(e) => {
                                    row.place(*indexes.get(#lit).unwrap(), e.0);
                                    None
                                }
                            },
                            Some(_) => unreachable!("unable to convert Value to Value"),
                            None => None,
                        };

                        if let Some(val) = val {
                            val
                        } else {
                            #(#place;)*
                            return Err(FromRowError(row));
                        }
                    }
                )
            })
            .collect::<Vec<_>>();

        let set_field = fields
            .named
            .iter()
            .zip(&fields_attrs)
            .map(|(f, attrs)| {
                let ident = f.ident.as_ref().unwrap();
                let ty = &f.ty;
                if attrs.json {
                    quote::quote!(#ident: #ident.commit().0)
                } else if attrs.with.is_some() {
                    quote::quote!( #ident )
                } else {
                    quote::quote!(#ident: <<#ty as FromValue>::Intermediate as std::convert::Into<#ty>>::into(#ident))
                }
            })
            .collect::<Vec<_>>();

        let new_tokens = quote::quote!(
            impl #impl_generics #ident #ident_generics {
                #table_name_constant
                #(#filed_name_constant)*
            }

            impl #impl_generics #crat::prelude::FromRow for #ident #ident_generics
            #bounds {
                fn from_row_opt(
                    mut row: #crat::Row,
                ) -> std::result::Result<Self, #crat::FromRowError>
                where
                    Self: Sized,
                {
                    use #crat::prelude::*;
                    use #crat::Value;
                    use #crat::FromRowError;

                    let columns = row.columns();
                    let indexes = columns.iter().enumerate().fold(
                        std::collections::HashMap::new(),
                        |mut acc, (i, col)| {
                            acc.insert(col.name_str(), i);
                            acc
                        },
                    );

                    #(#take_field;)*

                    Ok(Self {
                        #(#set_field,)*
                    })
                }
            }
        );

        tokens.append_all(new_tokens);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn derive_from_row_named_struct() {
        let code = r#"
            #[derive(FromRow)]
            #[mysql(crate_name = "mysql_common")]
            struct Foo {
                id: u64,
                #[mysql(rename = "def", json)]
                definition: serde_json::Value,
                child: Option<u64>,
            }
        "#;
        let input = syn::parse_str::<syn::DeriveInput>(code).unwrap();
        let derived = super::super::impl_from_row(&input).unwrap();
        eprintln!("{}", derived);
    }

    #[test]
    fn derive_struct_with_raw_identifiers() {
        let code = r#"
            #[derive(FromRow)]
            #[mysql(crate_name = "mysql_common")]
            struct Foo {
                r#type: u64,
            }
        "#;
        let input = syn::parse_str::<syn::DeriveInput>(code).unwrap();
        let derived = super::super::impl_from_row(&input).unwrap();
        eprintln!("{}", derived);
    }
}
