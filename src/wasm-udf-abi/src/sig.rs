// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Signature strings and their encoding into export names.

use std::fmt;

use base64::Engine;
use base64::alphabet::Alphabet;
use base64::engine::GeneralPurpose;
use base64::engine::general_purpose::NO_PAD;

use crate::types::UdfType;

/// The export-name prefix for scalar and table functions.
pub(crate) const FUNCTION_PREFIX: &str = "arrowudf_";

/// The export-name prefix for struct type declarations.
pub(crate) const TYPE_PREFIX: &str = "arrowudt_";

/// Standard base64 with `$` and `_` in place of `+` and `/`, so that encoded
/// strings are valid symbol names. This must match the arrow-udf macros.
const SYMBOL_ENGINE: GeneralPurpose = GeneralPurpose::new(
    &match Alphabet::new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789$_") {
        Ok(alphabet) => alphabet,
        Err(_) => panic!("valid alphabet"),
    },
    NO_PAD,
);

/// Encodes a signature or type declaration into the symbol form arrow-udf
/// uses in export names.
pub fn encode_symbol(input: &str) -> String {
    SYMBOL_ENGINE.encode(input)
}

/// Decodes a symbol produced by [`encode_symbol`], or `None` if it is not
/// valid symbol base64 or does not decode to UTF-8.
pub fn decode_symbol(symbol: &str) -> Option<String> {
    let bytes = SYMBOL_ENGINE.decode(symbol).ok()?;
    String::from_utf8(bytes).ok()
}

/// The signature of a scalar function, as it appears in an arrow-udf export
/// name.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScalarSignature {
    /// The guest-side function name.
    pub name: String,
    pub args: Vec<UdfType>,
    pub ret: UdfType,
}

impl ScalarSignature {
    /// The name of the export that implements this signature.
    pub fn export_name(&self) -> String {
        function_export_name(&self.to_string())
    }
}

/// The name of the export that implements the function with the given
/// signature string.
pub fn function_export_name(signature: &str) -> String {
    format!("{FUNCTION_PREFIX}{}", encode_symbol(signature))
}

impl fmt::Display for ScalarSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        for (i, arg) in self.args.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{arg}")?;
        }
        write!(f, ")->{}", self.ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn symbol_round_trip() {
        let sig = "gcd(int32,int32)->int32";
        let symbol = encode_symbol(sig);
        assert!(
            symbol
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '$' || c == '_'),
            "symbol {symbol} contains characters outside the symbol alphabet",
        );
        assert_eq!(decode_symbol(&symbol).as_deref(), Some(sig));
    }

    #[mz_ore::test]
    fn export_name_matches_arrow_udf() {
        // The export name the arrow-udf 0.10 macro generates for
        // `#[function("gcd(int, int) -> int")]`.
        let sig = ScalarSignature {
            name: "gcd".into(),
            args: vec![UdfType::Int32, UdfType::Int32],
            ret: UdfType::Int32,
        };
        assert_eq!(sig.to_string(), "gcd(int32,int32)->int32");
        assert_eq!(
            sig.export_name(),
            "arrowudf_Z2NkKGludDMyLGludDMyKS0$aW50MzI"
        );
    }
}
