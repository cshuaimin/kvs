use serde::de::{Deserialize, MapAccess, Visitor};
use serde::export::PhantomData;
use serde::ser::{Serialize, SerializeMap, Serializer};
use serde::Deserializer;
use std::fmt;
use std::ops::Deref;

#[derive(Debug)]
pub(crate) struct SkipMap<K: Ord, V>(crossbeam_skiplist::SkipMap<K, V>);

impl<K, V> SkipMap<K, V>
where
    K: Ord + Send + 'static,
    V: Send + 'static,
{
    pub(crate) fn new() -> SkipMap<K, V> {
        SkipMap(crossbeam_skiplist::SkipMap::new())
    }
}

impl<K, V> Deref for SkipMap<K, V>
where
    K: Ord + Send + 'static,
    V: Send + 'static,
{
    type Target = crossbeam_skiplist::SkipMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, V> Default for SkipMap<K, V>
where
    K: Ord + Send + 'static,
    V: Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) struct SkipMapVisitor<K, V>
where
    K: Ord + Send + 'static,
    V: Send + 'static,
{
    marker: PhantomData<fn() -> SkipMap<K, V>>,
}

impl<K, V> SkipMapVisitor<K, V>
where
    K: Ord + Send + 'static,
    V: Send + 'static,
{
    fn new() -> Self {
        SkipMapVisitor {
            marker: PhantomData,
        }
    }
}

impl<'de, K, V> Visitor<'de> for SkipMapVisitor<K, V>
where
    K: Deserialize<'de> + Ord + Send + 'static,
    V: Deserialize<'de> + Send + 'static,
{
    type Value = SkipMap<K, V>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a SkipMap")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let map = SkipMap::new();

        while let Some((key, value)) = access.next_entry()? {
            map.insert(key, value);
        }

        Ok(map)
    }
}

impl<'de, K, V> Deserialize<'de> for SkipMap<K, V>
where
    K: Deserialize<'de> + Ord + Send + 'static,
    V: Deserialize<'de> + Send + 'static,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(SkipMapVisitor::<K, V>::new())
    }
}

impl<K, V> Serialize for SkipMap<K, V>
where
    K: Serialize + Ord + Send + 'static,
    V: Serialize + Send + 'static,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for entry in self.iter() {
            map.serialize_entry(entry.key(), entry.value())?;
        }
        map.end()
    }
}
