use proc_macro2::Span;
use proc_macro_error::{Diagnostic, Level};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("expected a struct with a single unnamed field")]
    NotANewTypeStruct(Span),
    #[error("structs with named fields are not supported")]
    NamedFieldsNotSupported(Span),
    #[error("unit structs are not supported")]
    UnitStructsNotSupported(Span),
    #[error("structs with unnamed fields are not supported")]
    StructsWithUnnamedFieldsNotSupported(Span),
    #[error("unions are not supported")]
    UnionsNotSupported(Span),
    #[error("enums are not supported")]
    EnumsNotSupported(Span),
    #[error("non-unit variants are not supported")]
    NonUnitVariant(Span),
    #[error("unsupported discriminant")]
    UnsupportedDiscriminant(Span),
    #[error("add #[mysql(explicit_invalid)] attribute to allow")]
    ExplicitInvalid(Span),
    #[error("no suitable crate found, use #[mysql(crate = \"..\")] to specify the crate name")]
    NoCrateNameFound,
    #[error("multiple crates found, use #[mysql(crate = \"..\")] to specify the particular name")]
    MultipleCratesFound,
    #[error(transparent)]
    Syn(#[from] syn::Error),
    #[error(transparent)]
    Darling(#[from] darling::error::Error),
    #[error("conflicting attributes")]
    ConflictingsAttributes(Span, Span),
    #[error("representation won't fit into MySql integer")]
    UnsupportedRepresentation(Span),
    #[error("this attribute requires `{}` attribute", 0)]
    AttributeRequired(Span, &'static str),
}

impl From<Error> for Diagnostic {
    fn from(x: Error) -> Diagnostic {
        match x {
            Error::UnionsNotSupported(span)
            | Error::EnumsNotSupported(span)
            | Error::NonUnitVariant(span)
            | Error::UnsupportedDiscriminant(span)
            | Error::ExplicitInvalid(span)
            | Error::NotANewTypeStruct(span)
            | Error::NamedFieldsNotSupported(span)
            | Error::UnitStructsNotSupported(span)
            | Error::UnsupportedRepresentation(span)
            | Error::StructsWithUnnamedFieldsNotSupported(span) => {
                Diagnostic::spanned(span, Level::Error, format!("FromValue: {x}"))
            }
            Error::Syn(ref e) => {
                Diagnostic::spanned(e.span(), Level::Error, format!("FromValue: {x}"))
            }
            Error::Darling(ref e) => {
                Diagnostic::spanned(e.span(), Level::Error, format!("FromValue: {x}"))
            }
            Error::NoCrateNameFound => Diagnostic::new(Level::Error, format!("FromValue: {x}")),
            Error::MultipleCratesFound => Diagnostic::new(Level::Error, format!("FromValue: {x}")),
            Error::ConflictingsAttributes(s1, s2) => {
                Diagnostic::spanned(s1, Level::Error, format!("FromValue: {x}"))
                    .span_error(s2, "conflicting attribute".into())
            }
            Error::AttributeRequired(s, _) => {
                Diagnostic::spanned(s, Level::Error, format!("FromValue: {x}"))
            }
        }
    }
}
