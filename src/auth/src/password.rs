use std::fmt::{Debug, Display};

use static_assertions::assert_not_impl_all;

///Password is a String wrapper type that does not implement Display or Debug
#[derive(Clone, PartialEq, Eq)]
pub struct Password(String);

assert_not_impl_all!(Password: Display);

impl From<String> for Password {
    fn from(password: String) -> Self {
        Password(password)
    }
}

impl ToString for Password {
    fn to_string(&self) -> String {
        self.0.clone()
    }
}

impl Debug for Password {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Password(****)")
    }
}
