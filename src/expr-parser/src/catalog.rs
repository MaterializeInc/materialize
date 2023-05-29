// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mz_ore::cast::CastFrom;
use mz_repr::explain::{DummyHumanizer, ExprHumanizer};
use mz_repr::{GlobalId, RelationType, ScalarType};

/// A catalog that holds types of objects previously created for the unit test.
///
/// This is for the purpose of allowing `MirRelationExpr`s to refer to them
/// later.
#[derive(Debug, Default)]
pub struct TestCatalog {
    objects: BTreeMap<String, (GlobalId, RelationType)>,
    names: BTreeMap<GlobalId, String>,
}

impl<'a> TestCatalog {
    /// Registers an object in the catalog.
    ///
    /// Specifying `transient` as true allows the object to be deleted by
    /// [Self::remove_transient_objects].
    ///
    /// Returns the GlobalId assigned by the catalog to the object.
    ///
    /// Errors if an object of the same name is already in the catalog.
    pub fn insert(
        &mut self,
        name: &str,
        typ: RelationType,
        transient: bool,
    ) -> Result<GlobalId, String> {
        if self.objects.contains_key(name) {
            return Err(format!("Object {name} already exists in catalog"));
        }
        let id = if transient {
            GlobalId::Transient(u64::cast_from(self.objects.len()))
        } else {
            GlobalId::User(u64::cast_from(self.objects.len()))
        };
        self.objects.insert(name.to_string(), (id, typ));
        self.names.insert(id, name.to_string());
        Ok(id)
    }

    pub fn get(&'a self, name: &str) -> Option<&'a (GlobalId, RelationType)> {
        self.objects.get(name)
    }

    /// Looks up the name of the object referred to as `id`.
    pub fn get_source_name(&'a self, id: &GlobalId) -> Option<&'a String> {
        self.names.get(id)
    }

    /// Clears all transient objects from the catalog.
    pub fn remove_transient_objects(&mut self) {
        self.objects
            .retain(|_, (id, _)| !matches!(id, GlobalId::Transient(_)));
        self.names
            .retain(|k, _| !matches!(k, GlobalId::Transient(_)));
    }
}

impl ExprHumanizer for TestCatalog {
    fn humanize_id(&self, id: GlobalId) -> Option<String> {
        self.names.get(&id).map(|s| s.to_string())
    }

    fn humanize_id_unqualified(&self, id: GlobalId) -> Option<String> {
        self.names.get(&id).map(|s| s.to_string())
    }

    fn humanize_scalar_type(&self, ty: &ScalarType) -> String {
        DummyHumanizer.humanize_scalar_type(ty)
    }
}
