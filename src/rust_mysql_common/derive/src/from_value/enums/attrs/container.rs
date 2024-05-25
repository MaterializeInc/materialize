use darling::{util::SpannedValue, FromAttributes, FromMeta};
use proc_macro2::{Span, TokenStream};
use proc_macro_crate::{crate_name, FoundCrate};
use quote::TokenStreamExt;

#[derive(Debug, Default, FromAttributes)]
#[darling(attributes(mysql))]
pub struct Mysql {
    #[darling(default)]
    pub crate_name: Crate,
    #[darling(default)]
    pub rename_all: Option<RenameAll>,
    #[darling(default)]
    pub allow_invalid_discriminants: bool,
    #[darling(default)]
    pub allow_sparse_enum: bool,
    #[darling(default)]
    pub allow_suboptimal_repr: bool,
    #[darling(default)]
    pub is_integer: SpannedValue<bool>,
    #[darling(default)]
    pub is_string: SpannedValue<bool>,
}

#[derive(Debug)]
pub enum Crate {
    NotFound,
    Multiple,
    Itself,
    Found(String),
}

impl Default for Crate {
    fn default() -> Self {
        let mysql = crate_name("mysql");
        let mysql_async = crate_name("mysql_async");
        let mysql_common = crate_name("mysql_common");
        match (mysql, mysql_async, mysql_common) {
            (Ok(_), Ok(_), _) => Self::Multiple,
            (Ok(FoundCrate::Name(x)), _, _)
            | (_, Ok(FoundCrate::Name(x)), _)
            | (_, _, Ok(FoundCrate::Name(x))) => Self::Found(x),
            (Ok(FoundCrate::Itself), _, _)
            | (_, Ok(FoundCrate::Itself), _)
            | (_, _, Ok(FoundCrate::Itself)) => Self::Itself,
            (Err(_), Err(_), Err(_)) => Self::NotFound,
        }
    }
}

impl FromMeta for Crate {
    fn from_string(value: &str) -> darling::Result<Self> {
        Ok(Self::Found(value.into()))
    }
}

#[derive(Debug)]
pub enum RenameAll {
    Lowercase,
    Uppercase,
    UpperCamelCase,
    LowerCamelCase,
    SnakeCase,
    KebabCase,
    ShoutySnakeCase,
    ShoutyKebabCase,
}
impl RenameAll {
    pub fn rename(&self, name: &str) -> String {
        match self {
            RenameAll::Lowercase => name.to_lowercase(),
            RenameAll::Uppercase => name.to_uppercase(),
            RenameAll::UpperCamelCase => heck::AsUpperCamelCase(name).to_string(),
            RenameAll::LowerCamelCase => heck::AsLowerCamelCase(name).to_string(),
            RenameAll::SnakeCase => heck::AsSnakeCase(name).to_string(),
            RenameAll::KebabCase => heck::AsKebabCase(name).to_string(),
            RenameAll::ShoutySnakeCase => heck::AsShoutySnakeCase(name).to_string(),
            RenameAll::ShoutyKebabCase => heck::AsShoutyKebabCase(name).to_string(),
        }
    }
}

impl FromMeta for RenameAll {
    fn from_string(value: &str) -> darling::Result<Self> {
        match value {
            "lowercase" => Ok(Self::Lowercase),
            "UPPERCASE" => Ok(Self::Uppercase),
            "PascalCase" => Ok(Self::UpperCamelCase),
            "camelCase" => Ok(Self::LowerCamelCase),
            "snake_case" => Ok(Self::SnakeCase),
            "kebab-case" => Ok(Self::KebabCase),
            "SCREAMING_SNAKE_CASE" => Ok(Self::ShoutySnakeCase),
            "SCREAMING-KEBAB-CASE" => Ok(Self::ShoutyKebabCase),
            _ => Err(darling::Error::unknown_value(value)),
        }
    }
}

#[derive(Default, FromMeta)]
#[darling(allow_unknown_fields)]
pub struct Repr(pub EnumRepr);

pub enum EnumRepr {
    I8(Span),
    U8(Span),
    I16(Span),
    U16(Span),
    I32(Span),
    U32(Span),
    I64(Span),
    U64(Span),
    I128(Span),
    U128(Span),
    ISize(Span),
    USize(Span),
}

impl EnumRepr {
    const I8_IDENT: &'static str = "i8";
    const U8_IDENT: &'static str = "u8";
    const I16_IDENT: &'static str = "i16";
    const U16_IDENT: &'static str = "u16";
    const I32_IDENT: &'static str = "i32";
    const U32_IDENT: &'static str = "u32";
    const I64_IDENT: &'static str = "i64";
    const U64_IDENT: &'static str = "u64";
    const I128_IDENT: &'static str = "i128";
    const U128_IDENT: &'static str = "u128";
    const ISIZE_IDENT: &'static str = "isize";
    const USIZE_IDENT: &'static str = "usize";

    pub fn span(&self) -> Span {
        match self {
            EnumRepr::I8(x) => *x,
            EnumRepr::U8(x) => *x,
            EnumRepr::I16(x) => *x,
            EnumRepr::U16(x) => *x,
            EnumRepr::I32(x) => *x,
            EnumRepr::U32(x) => *x,
            EnumRepr::I64(x) => *x,
            EnumRepr::U64(x) => *x,
            EnumRepr::I128(x) => *x,
            EnumRepr::U128(x) => *x,
            EnumRepr::ISize(x) => *x,
            EnumRepr::USize(x) => *x,
        }
    }

    const fn ident(&self) -> &'static str {
        match self {
            EnumRepr::I8(_) => "i8",
            EnumRepr::U8(_) => "u8",
            EnumRepr::I16(_) => "i16",
            EnumRepr::U16(_) => "u16",
            EnumRepr::I32(_) => "i32",
            EnumRepr::U32(_) => "u32",
            EnumRepr::I64(_) => "i64",
            EnumRepr::U64(_) => "u64",
            EnumRepr::I128(_) => "i128",
            EnumRepr::U128(_) => "u128",
            EnumRepr::ISize(_) => "isize",
            EnumRepr::USize(_) => "usize",
        }
    }

    fn from_ident(ident: &syn::Ident) -> Option<Self> {
        match ident.to_string().as_str() {
            Self::I8_IDENT => Some(EnumRepr::I8(ident.span())),
            Self::U8_IDENT => Some(EnumRepr::U8(ident.span())),
            Self::I16_IDENT => Some(EnumRepr::I16(ident.span())),
            Self::U16_IDENT => Some(EnumRepr::U16(ident.span())),
            Self::I32_IDENT => Some(EnumRepr::I32(ident.span())),
            Self::U32_IDENT => Some(EnumRepr::U32(ident.span())),
            Self::I64_IDENT => Some(EnumRepr::I64(ident.span())),
            Self::U64_IDENT => Some(EnumRepr::U64(ident.span())),
            Self::I128_IDENT => Some(EnumRepr::I128(ident.span())),
            Self::U128_IDENT => Some(EnumRepr::U128(ident.span())),
            Self::ISIZE_IDENT => Some(EnumRepr::ISize(ident.span())),
            Self::USIZE_IDENT => Some(EnumRepr::USize(ident.span())),
            _ => None,
        }
    }
}

impl quote::ToTokens for EnumRepr {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.append(syn::Ident::new(self.ident(), Span::call_site()))
    }
}

impl Default for EnumRepr {
    fn default() -> Self {
        Self::ISize(Span::call_site())
    }
}

impl FromMeta for EnumRepr {
    fn from_list(items: &[darling::ast::NestedMeta]) -> darling::Result<Self> {
        Ok(items
            .iter()
            .filter_map(|x| match x {
                darling::ast::NestedMeta::Meta(syn::Meta::Path(path)) => Some(path),
                _ => None,
            })
            .filter_map(|x| x.get_ident())
            .find_map(Self::from_ident)
            .unwrap_or_default())
    }
}
