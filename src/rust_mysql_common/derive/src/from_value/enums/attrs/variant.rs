use darling::FromAttributes;

#[derive(Debug, Default, FromAttributes)]
#[darling(attributes(mysql))]
pub struct Mysql {
    #[darling(default)]
    pub rename: Option<String>,
    #[darling(default)]
    pub explicit_invalid: bool,
    #[darling(default)]
    pub allow_invalid_discriminants: bool,
}
