use coord::catalog::builtin::{
    Builtin, BuiltinFunc, BuiltinLog, BuiltinTable, BuiltinView, BUILTINS,
};
use expr::GlobalId;
use std::collections::BTreeSet;

#[test]
/// Ensures that all our builtins' fields are assigned globally-unique IDs.
fn builtins_have_unique_ids() {
    let mut encountered = BTreeSet::<GlobalId>::new();
    let mut encounter =
        move |kind_name: &str, field_name: &str, identifier: &str, id: &GlobalId| {
            assert!(
                encountered.insert(id.to_owned()),
                "{} for {} {:?} is already used as a global ID: {:?}",
                field_name,
                kind_name,
                identifier,
                id,
            );
        };

    use Builtin::*;
    for (id, b) in BUILTINS.iter() {
        match b {
            Type(_) => {
                let name = format!("with ID {:?}", id);
                encounter("type", "id", &name, id);
            }
            Log(BuiltinLog { name, index_id, .. }) => {
                encounter("type", "id", name, id);
                encounter("type", "index_id", name, index_id);
            }
            Table(BuiltinTable { index_id, name, .. }) => {
                encounter("builtin table", "id", name, id);
                encounter("builtin table", "index_id", name, index_id);
            }
            View(BuiltinView { name, .. }) => {
                encounter("view", "id", name, id);
            }
            Func(BuiltinFunc { name, .. }) => {
                encounter("function", "id", name, id);
            }
        }
    }
}
