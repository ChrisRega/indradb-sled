use std::io::Cursor;
use std::u8;

use indradb::{Identifier, Json, util};
use serde_json::Value as JsonValue;
use sled::{IVec, Tree};
use uuid::Uuid;

use errors::map_err;

pub type OwnedPropertyItem = ((Uuid, Identifier), JsonValue);

pub struct VertexPropertyManager<'tree> {
    pub tree: &'tree Tree,
    pub value_index_tree: &'tree Tree,
}

impl<'tree> VertexPropertyManager<'tree> {
    pub fn new(tree: &'tree Tree, value_index_tree: &'tree Tree) -> Self {
        VertexPropertyManager { tree, value_index_tree }
    }

    fn key(&self, vertex_id: Uuid, name: Identifier) -> Vec<u8> {
        util::build(&[util::Component::Uuid(vertex_id), util::Component::Identifier(name)])
    }

    fn key_value_index(vertex_id: &Uuid, value: &JsonValue, property_name: Identifier) -> Vec<u8> {
        util::build(&[
            util::Component::Identifier(property_name),
            util::Component::Json(&Json::new(value.clone())),
            util::Component::Uuid(*vertex_id),
        ])
    }

    fn read_key_value_index(buf: IVec) -> (Identifier, u64, Uuid) {
        let mut cursor = Cursor::new(buf.as_ref());
        let name = util::read_identifier(&mut cursor);
        let value = util::read_u64(&mut cursor);
        let uuid = util::read_uuid(&mut cursor);
        (name, value, uuid)
    }

    pub fn iterate_for_property_name(
        &self,
        name: Identifier,
    ) -> indradb::Result<impl Iterator<Item = indradb::Result<OwnedPropertyItem>> + '_> {
        let prefix = util::build(&[util::Component::Identifier(name)]);
        let iterator = self.value_index_tree.scan_prefix(&prefix);

        Ok(iterator.map(move |item| -> indradb::Result<OwnedPropertyItem> {
            let (k, v) = map_err(item)?;
            let (n, _, vid) = Self::read_key_value_index(k);
            let value = serde_json::from_slice(&v)?;
            Ok(((vid, n), value))
        }))
    }

    pub fn iterate_for_property_name_and_value(
        &self,
        name: Identifier,
        value: &JsonValue,
    ) -> indradb::Result<impl Iterator<Item = indradb::Result<OwnedPropertyItem>> + '_> {
        let prefix = util::build(&[
            util::Component::Identifier(name),
            util::Component::Json(&Json::new(value.clone())),
        ]);
        let iterator = self.value_index_tree.scan_prefix(&prefix);

        Ok(iterator.map(move |item| -> indradb::Result<OwnedPropertyItem> {
            let (k, v) = map_err(item)?;
            let (n, _, vid) = Self::read_key_value_index(k);
            let value = serde_json::from_slice(&v)?;
            Ok(((vid, n), value))
        }))
    }

    pub fn iterate_for_owner(
        &self,
        vertex_id: Uuid,
    ) -> indradb::Result<impl Iterator<Item = indradb::Result<OwnedPropertyItem>> + '_> {
        let prefix = util::build(&[util::Component::Uuid(vertex_id)]);
        let iterator = self.tree.scan_prefix(&prefix);

        Ok(iterator.map(move |item| -> indradb::Result<OwnedPropertyItem> {
            let (k, v) = map_err(item)?;
            let mut cursor = Cursor::new(k);
            let owner_id = util::read_uuid(&mut cursor);
            debug_assert_eq!(vertex_id, owner_id);
            let name = util::read_identifier(&mut cursor);
            let value = serde_json::from_slice(&v)?;
            Ok(((owner_id, name), value))
        }))
    }

    pub fn get(&self, vertex_id: Uuid, name: Identifier) -> indradb::Result<Option<JsonValue>> {
        let key = self.key(vertex_id, name);

        match map_err(self.tree.get(&key))? {
            Some(value_bytes) => Ok(Some(serde_json::from_slice(&value_bytes)?)),
            None => Ok(None),
        }
    }

    pub fn set(&self, vertex_id: Uuid, name: Identifier, value: &JsonValue) -> indradb::Result<()> {
        let key = self.key(vertex_id, name);
        let value_json = serde_json::to_vec(value)?;
        map_err(self.tree.insert(key.as_slice(), value_json.as_slice()))?;
        let value_index_key = Self::key_value_index(&vertex_id, value, name);
        map_err(self.value_index_tree.insert(value_index_key, value_json.as_slice()))?;
        Ok(())
    }

    pub fn delete(&self, vertex_id: Uuid, name: Identifier) -> indradb::Result<()> {
        map_err(self.tree.remove(&self.key(vertex_id, name)))?;
        let prefix = util::build(&[util::Component::Identifier(name)]);
        let items = self.value_index_tree.scan_prefix(prefix);
        for item in items {
            if let Ok((key, _)) = item {
                let (_n, _v, vid) = Self::read_key_value_index(key.clone());
                if vertex_id == vid {
                    map_err(self.value_index_tree.remove(key))?;
                }
            }
        }
        Ok(())
    }
}
#[cfg(test)]
mod test {
    use serde_json::json;
    use uuid::{Context, Timestamp};

    use super::*;

    #[test]
    fn test_index_key_and_reco() {
        let context = Context::new(24);
        let uuid = Uuid::new_v1(Timestamp::now(context), &[1, 2, 3, 4, 5, 6]);
        let name = Identifier::new("_changesetID").unwrap();
        let value = json! {"Changesets/25dfc1e7-fdd1-4027-9e98-48a8429a9c70"};
        let key = VertexPropertyManager::key_value_index(&uuid, &value, name);

        let (n, _v, id) = VertexPropertyManager::read_key_value_index(key.into());
        assert_eq!(n, name);
        assert_eq!(uuid, id);
    }
}
