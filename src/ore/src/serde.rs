// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Serde utilities.

/// Used to serialize fields of Maps whose key type is not a native string. Annotate the field with
/// `#[serde(serialize_with = mz_ore::serde::map_key_to_string)]`.
pub fn map_key_to_string<'a, I, K, V, S>(map: I, serializer: S) -> Result<S::Ok, S::Error>
where
    I: IntoIterator<Item = (&'a K, &'a V)>,
    K: std::fmt::Display + 'a,
    V: serde::Serialize + 'a,
    S: serde::Serializer,
{
    use serde::ser::SerializeMap;

    let mut s = serializer.serialize_map(None)?;
    for (key, value) in map {
        s.serialize_entry(&key.to_string(), value)?;
    }
    s.end()
}

/// Used to deserialize fields of [`std::collections::BTreeMap`] whose key type is not a native
/// string. Annotate the field with
/// `#[serde(deserialize_with = "mz_ore::serde::string_key_to_btree_map")]`.
pub fn string_key_to_btree_map<'de, K, V, D>(
    deserializer: D,
) -> Result<std::collections::BTreeMap<K, V>, D::Error>
where
    K: std::str::FromStr + Ord + std::fmt::Debug,
    K::Err: std::fmt::Display,
    V: serde::Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    struct BTreeMapVisitor<K, V> {
        _phantom: std::marker::PhantomData<(K, V)>,
    }

    impl<'de, K, V> serde::de::Visitor<'de> for BTreeMapVisitor<K, V>
    where
        K: std::str::FromStr + Ord,
        V: serde::Deserialize<'de>,
        K::Err: std::fmt::Display,
    {
        type Value = std::collections::BTreeMap<K, V>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "a map with keys that implement FromStr")
        }

        fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            let mut map = std::collections::BTreeMap::new();

            while let Some((key, value)) = access.next_entry::<String, V>()? {
                let key = K::from_str(&key).map_err(serde::de::Error::custom)?;
                map.insert(key, value);
            }

            Ok(map)
        }
    }

    deserializer.deserialize_map(BTreeMapVisitor {
        _phantom: Default::default(),
    })
}
