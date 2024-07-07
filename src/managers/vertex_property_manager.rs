use std::io::Cursor;
use std::u8;

use indradb::{Json, util};
use serde_json::Value as JsonValue;
use sled::{IVec, Tree};
use uuid::Uuid;

use errors::map_err;

// TODO: add a second tree here that prefix-indexes all values like key = [PropName,PropValue,Vertex-UUID]

pub type OwnedPropertyItem = ((Uuid, String), JsonValue);

pub struct VertexPropertyManager<'tree> {
    pub tree: &'tree Tree,
    pub value_index_tree: &'tree Tree,
}

impl<'tree> VertexPropertyManager<'tree> {
    pub fn new(tree: &'tree Tree, value_index_tree: &'tree Tree) -> Self {
        VertexPropertyManager { tree, value_index_tree }
    }

    fn key(&self, vertex_id: Uuid, name: &str) -> Vec<u8> {
        util::build(&[
            util::Component::Uuid(vertex_id),
            util::Component::FixedLengthString(name),
        ])
    }

    fn key_value_index(&self, vertex_id: &Uuid, value: &JsonValue, property_name: &str) -> Vec<u8> {
        util::build(&[
            util::Component::FixedLengthString(property_name),
            util::Component::Json(&Json::new(value.clone())),
            util::Component::Uuid(*vertex_id)
        ])
    }

    fn read_key_value_index(buf: IVec) -> (String, JsonValue, Uuid) {
        let mut cursor = Cursor::new(buf.as_ref());
        let name = util::read_fixed_length_string(&mut cursor);
        let value = serde_json::from_reader(cursor.clone()).unwrap();
        let uuid = util::read_uuid(&mut cursor);
        (name, value, uuid)
    }

    pub fn iterate_for_owner(&self, vertex_id: Uuid) -> indradb::Result<impl Iterator<Item=indradb::Result<OwnedPropertyItem>> + '_> {
        let prefix = util::build(&[util::Component::Uuid(vertex_id)]);
        let iterator = self.tree.scan_prefix(&prefix);

        Ok(iterator.map(move |item| -> indradb::Result<OwnedPropertyItem> {
            let (k, v) = map_err(item)?;
            let mut cursor = Cursor::new(k);
            let owner_id = util::read_uuid(&mut cursor);
            debug_assert_eq!(vertex_id, owner_id);
            let name = util::read_fixed_length_string(&mut cursor);
            let value = serde_json::from_slice(&v)?;
            Ok(((owner_id, name), value))
        }))
    }

    pub fn get(&self, vertex_id: Uuid, name: &str) -> indradb::Result<Option<JsonValue>> {
        let key = self.key(vertex_id, name);

        match map_err(self.tree.get(&key))? {
            Some(value_bytes) => Ok(Some(serde_json::from_slice(&value_bytes)?)),
            None => Ok(None),
        }
    }

    pub fn set(&self, vertex_id: Uuid, name: &str, value: &JsonValue) -> indradb::Result<()> {
        let key = self.key(vertex_id, name);
        let value_json = serde_json::to_vec(value)?;
        map_err(self.tree.insert(key.as_slice(), value_json.as_slice()))?;
        let value_index_key = self.key_value_index(&vertex_id, value, name);
        map_err(self.value_index_tree.insert(value_index_key, &[]))?;
        Ok(())
    }

    pub fn delete(&self, vertex_id: Uuid, name: &str) -> indradb::Result<()> {
        map_err(self.tree.remove(&self.key(vertex_id, name)))?;
        let prefix = util::build(&[util::Component::FixedLengthString(name)]);
        let items = self.value_index_tree.scan_prefix(prefix);
        for item in items {
            if let Ok((key, _)) = item {
                let (n, v, vid) = Self::read_key_value_index(key);
                if vertex_id == vid {
                    let key = self.key_value_index(&vid, &v, n.as_str());
                    map_err(self.value_index_tree.remove(key))?;
                }
            }
        }
        Ok(())
    }
}