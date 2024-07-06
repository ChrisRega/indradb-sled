use std::io::Cursor;
use std::u8;

use indradb::{Identifier, util};
use serde_json::Value as JsonValue;
use sled::Tree;
use uuid::Uuid;

use errors::map_err;

// TODO: add a second tree here that prefix-indexes all values like key = [PropName,PropValue,(EdgeIdentifier)]

pub type EdgePropertyItem = ((Uuid, Identifier, Uuid, String), JsonValue);

pub struct EdgePropertyManager<'tree> {
    pub tree: &'tree Tree,
}

impl<'tree> EdgePropertyManager<'tree> {
    pub fn new(tree: &'tree Tree) -> Self {
        EdgePropertyManager { tree }
    }

    fn key(&self, outbound_id: Uuid, t: &Identifier, inbound_id: Uuid, name: &str) -> Vec<u8> {
        util::build(&[
            util::Component::Uuid(outbound_id),
            util::Component::Identifier(t.clone()),
            util::Component::Uuid(inbound_id),
            util::Component::FixedLengthString(name),
        ])
    }

    pub fn iterate_for_owner<'a>(
        &'a self,
        outbound_id: Uuid,
        t: &'a Identifier,
        inbound_id: Uuid,
    ) -> indradb::Result<Box<dyn Iterator<Item=indradb::Result<EdgePropertyItem>> + 'a>> {
        let prefix = util::build(&[
            util::Component::Uuid(outbound_id),
            util::Component::Identifier(t.clone()),
            util::Component::Uuid(inbound_id),
        ]);

        let iterator = self.tree.scan_prefix(&prefix);

        let mapped = iterator.map(move |item| -> indradb::Result<EdgePropertyItem> {
            let (k, v) = map_err(item)?;
            let mut cursor = Cursor::new(k);

            let edge_property_outbound_id = util::read_uuid(&mut cursor);
            debug_assert_eq!(edge_property_outbound_id, outbound_id);

            let edge_property_t = util::read_identifier(&mut cursor);
            debug_assert_eq!(&edge_property_t, t);

            let edge_property_inbound_id = util::read_uuid(&mut cursor);
            debug_assert_eq!(edge_property_inbound_id, inbound_id);

            let edge_property_name = util::read_fixed_length_string(&mut cursor);

            let value = serde_json::from_slice(&v)?;
            Ok((
                (
                    edge_property_outbound_id,
                    edge_property_t,
                    edge_property_inbound_id,
                    edge_property_name,
                ),
                value,
            ))
        });

        Ok(Box::new(mapped))
    }

    pub fn get(&self, outbound_id: Uuid, t: &Identifier, inbound_id: Uuid, name: &str) -> indradb::Result<Option<JsonValue>> {
        let key = self.key(outbound_id, t, inbound_id, name);

        match map_err(self.tree.get(&key))? {
            Some(ref value_bytes) => Ok(Some(serde_json::from_slice(value_bytes)?)),
            None => Ok(None),
        }
    }

    pub fn set(&self, outbound_id: Uuid, t: &Identifier, inbound_id: Uuid, name: &str, value: &JsonValue) -> indradb::Result<()> {
        let key = self.key(outbound_id, t, inbound_id, name);
        let value_json = serde_json::to_vec(value)?;
        map_err(self.tree.insert(key.as_slice(), value_json.as_slice()))?;
        Ok(())
    }

    pub fn delete(&self, outbound_id: Uuid, t: &Identifier, inbound_id: Uuid, name: &str) -> indradb::Result<()> {
        map_err(self.tree.remove(&self.key(outbound_id, t, inbound_id, name)))?;
        Ok(())
    }
}