use proc_macro2::TokenStream;

mod structs;

pub fn impl_from_row(input: &syn::DeriveInput) -> crate::Result<TokenStream> {
    match input.data {
        syn::Data::Struct(ref data_struct) => structs::impl_from_row_for_struct(
            &input.attrs,
            &input.ident,
            &input.generics,
            data_struct,
        ),
        syn::Data::Enum(_) => Err(crate::Error::EnumsNotSupported(input.ident.span())),
        syn::Data::Union(_) => Err(crate::Error::UnionsNotSupported(input.ident.span())),
    }
}
