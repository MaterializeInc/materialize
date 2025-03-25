use serde::{Deserialize, Serialize};

/// Whether to use the constraint-based timestamp selection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConstraintBasedTimestampSelection {
    Enabled,
    Disabled,
    Verify,
}

impl std::default::Default for ConstraintBasedTimestampSelection {
    fn default() -> Self {
        Self::Verify
    }
}

impl ConstraintBasedTimestampSelection {
    pub const fn const_default() -> Self {
        Self::Verify
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "enabled" => Self::Enabled,
            "disabled" => Self::Disabled,
            "verify" => Self::Verify,
            _ => {
                tracing::error!("invalid value for ConstraintBasedTimestampSelection: {}", s);
                ConstraintBasedTimestampSelection::default()
            }
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
            Self::Verify => "verify",
        }
    }
}
