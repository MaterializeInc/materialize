// Copyright Materialize, Inc., and other contributors (if applicable)
//
// Use of this software is governed by the Apache License, Version 2.0

///
/// Derive decoders for Rust structs from Avro values.
/// Currently, only the simplest possible case is supported:
/// decoding an Avro record into a struct, each of whose fields
/// is named the same as the corresponding Avro record field
/// and which is in turn decodeable without external state.
///
/// Example:
///
/// ```
/// fn make_complicated_decoder() -> impl AvroDecode<Out = SomeComplicatedType> {
///     unimplemented!()
/// }
/// #[derive(AvroDecodeable)]
/// struct MyType {
///     x: i32,
///     y: u64,
///     #[decoder_factory(make_complicated_decoder)]
///     z: SomeComplicatedType
/// }
/// ```
///
/// This will create an Avro decoder that expects a record with fields "x", "y", and "z"
/// (and possibly others), where "x" and "y" are of Avro type Int or Long and their
/// values fit in an `i32` or `u64` respectively,
/// and where "z" can be decoded by the decoder returned from `make_complicated_decoder`.
///
/// This crate currently works by generating a struct named (following the example above)
/// MyType_DECODER which is used internally by the `AvroDecodeable` implementation.
/// It also requires that the `mz-avro` crate be linked under its default name.
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse_macro_input;
use syn::ItemStruct;

#[proc_macro_derive(AvroDecodeable, attributes(decoder_factory))]
pub fn derive_decodeable(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);
    let name = input.ident;
    let fields: Vec<_> = input
        .fields
        .iter()
        .map(|f| {
            // The type of the field,
            // which must itself be AvroDecodeable so that we can recursively
            // decode it.
            let ty = &f.ty;
            let id = f.ident.as_ref().unwrap();
            quote! {
                #id: Option<#ty>
            }
        })
        .collect();

    let decode_blocks: Vec<_> = input
        .fields
        .iter()
        .map(|f| {
            // The type of the field,
            // which must itself be AvroDecodeable so that we can recursively
            // decode it.
            let ty = &f.ty;
            let id = f.ident.as_ref().unwrap();
            let id_str = id.to_string();
            let found_twice = format!("field `{}` found twice", id);
            let make_decoder =
                if let Some(decoder_factory) = f.attrs.iter().find(|a| {
                    &a.path.get_ident().as_ref().unwrap().to_string() == "decoder_factory"
                }) {
                    let toks = &decoder_factory.tokens;
                    quote! {
                        #toks()
                    }
                } else {
                    quote! {
                        <#ty as ::mz_avro::AvroDecodeable>::new_decoder()
                    }
                };
            quote! {
                #id_str => {
                    if self.#id.is_some() {
                        ::anyhow::bail!(#found_twice);
                    }
                    let decoder = #make_decoder;
                    self.#id = Some(field.decode_field(decoder)?);
                }
            }
        })
        .collect();
    let check_blocks: Vec<_> = input
        .fields
        .iter()
        .map(|f| {
            let id = f.ident.as_ref().unwrap();
            let not_found = format!("field `{}` not found", id);
            quote! {
                let #id = if let Some(#id) = self.#id.take() {
                    #id
                } else {
                    ::anyhow::bail!(#not_found);
                };
            }
        })
        .collect();
    let return_fields: Vec<_> = input
        .fields
        .iter()
        .map(|f| f.ident.as_ref().unwrap())
        .collect();
    let decoder_name = format_ident!("{}_DECODER", name);
    let out = quote! {
        #[derive(Debug, Default)]
        #[allow(non_camel_case_types)]
        struct #decoder_name {
            #(#fields),*
        }
        impl ::mz_avro::AvroDecode for #decoder_name {
            type Out = #name;
            fn record<R: ::mz_avro::AvroRead, A: ::mz_avro::AvroRecordAccess<R>>(
                mut self,
                a: &mut A,
            ) -> ::anyhow::Result<#name> {
                while let Some((name, _idx, field)) = a.next_field()? {
                    match name {
                        #(#decode_blocks)*
                        _ => {
                            field.decode_field(::mz_avro::TrivialDecoder)?;
                        }
                    }
                }
                #(#check_blocks)*
                Ok(#name {
                    #(#return_fields),*
                })
            }
        }
        impl ::mz_avro::AvroDecodeable for #name {
            type Decoder = #decoder_name;
            fn new_decoder() -> #decoder_name {
                Default::default()
            }
        }
    };
    TokenStream::from(out)
}
