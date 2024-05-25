use proc_macro2::TokenStream;

pub mod enums;
pub mod structs;

pub fn impl_from_value(input: &syn::DeriveInput) -> crate::Result<TokenStream> {
    match input.data {
        syn::Data::Struct(ref data_struct) => structs::impl_from_value_for_struct(
            &input.attrs,
            &input.ident,
            &input.generics,
            data_struct,
        ),
        syn::Data::Enum(ref data_enum) => {
            enums::impl_from_value_for_enum(&input.attrs, &input.ident, &input.generics, data_enum)
        }
        syn::Data::Union(ref data_union) => {
            impl_from_value_for_union(&input.attrs, &input.ident, &input.generics, data_union)
        }
    }
}

fn impl_from_value_for_union(
    _attrs: &[syn::Attribute],
    _ident: &proc_macro2::Ident,
    _generics: &syn::Generics,
    _data_union: &syn::DataUnion,
) -> crate::Result<TokenStream> {
    Err(crate::Error::UnionsNotSupported(_ident.span()))
}
