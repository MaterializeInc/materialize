use darling::{util::SpannedValue, FromAttributes};

use crate::from_value::structs::attrs::container::FnPath;

#[derive(Debug, Default, FromAttributes)]
#[darling(attributes(mysql))]
pub struct Mysql {
    #[darling(default)]
    pub json: bool,
    #[darling(default)]
    pub rename: Option<String>,
    #[darling(default)]
    pub with: Option<SpannedValue<FnPath>>,
}
