use std::io::Cursor;

use ecow::EcoVec;
use indradb::{Edge, Identifier, Json, util};
use serde_json::{Value as JsonValue, Value};
use sled::{IVec, Tree};
use uuid::Uuid;

use errors::map_err;

pub type EdgePropertyItem = ((Edge, Identifier), JsonValue);

pub struct EdgePropertyManager<'tree> {
    pub tree: &'tree Tree,
    pub value_index_tree: &'tree Tree,
}

impl<'tree> EdgePropertyManager<'tree> {
    pub fn new(tree: &'tree Tree, value_index_tree: &'tree Tree) -> Self {
        EdgePropertyManager { tree, value_index_tree }
    }

    fn key(&self, outbound_id: Uuid, t: Identifier, inbound_id: Uuid, name: Identifier) -> Vec<u8> {
        util::build(&[
            util::Component::Uuid(outbound_id),
            util::Component::Identifier(t),
            util::Component::Uuid(inbound_id),
            util::Component::Identifier(name),
        ])
    }

    pub fn iterate_for_property_name(
        &self,
        name: Identifier,
    ) -> indradb::Result<impl Iterator<Item = indradb::Result<Edge>> + '_> {
        let prefix = util::build(&[util::Component::Identifier(name)]);
        let iterator = self.value_index_tree.scan_prefix(prefix);

        Ok(iterator.map(move |item| -> indradb::Result<Edge> {
            let (k, _v) = map_err(item)?;
            let (_p, _, edge) = Self::read_key_value_index(k);
            Ok(edge)
        }))
    }

    pub fn iterate_for_property_name_and_value(
        &'tree self,
        name: Identifier,
        value: &JsonValue,
    ) -> indradb::Result<impl Iterator<Item = indradb::Result<Edge>> + 'tree> {
        let value = value.clone();
        let prefix = util::build(&[util::Component::Identifier(name)]);
        let iterator = self.value_index_tree.scan_prefix(prefix);

        Ok(iterator
            .map(move |item| -> indradb::Result<(Edge, Value)> {
                let (k, v) = map_err(item)?;

                let (_p, _, edge) = Self::read_key_value_index(k);
                let val = serde_json::from_slice(&v)?;
                Ok((edge, val))
            })
            .filter_map(move |item| {
                item.map_or_else(
                    |e| Some(Err(e)),
                    |(edge, val)| {
                        if val == value {
                            Some(Ok(edge))
                        } else {
                            None
                        }
                    },
                )
            }))
    }

    pub fn iterate_for_owner<'a>(
        &'a self,
        edge: &Edge,
    ) -> indradb::Result<Box<dyn Iterator<Item = indradb::Result<EdgePropertyItem>> + 'a>> {
        let prefix = util::build(&[
            util::Component::Uuid(edge.outbound_id),
            util::Component::Identifier(edge.t),
            util::Component::Uuid(edge.inbound_id),
        ]);

        let iterator = self.tree.scan_prefix(prefix);

        let mapped = iterator.map(move |item| -> indradb::Result<EdgePropertyItem> {
            let (k, v) = map_err(item)?;
            let mut cursor = Cursor::new(k);

            let edge_property_outbound_id = util::read_uuid(&mut cursor);

            let edge_property_t = util::read_identifier(&mut cursor);

            let edge_property_inbound_id = util::read_uuid(&mut cursor);

            let edge_property_name = util::read_identifier(&mut cursor);

            let value = serde_json::from_slice(&v)?;
            Ok((
                (
                    Edge {
                        outbound_id: edge_property_outbound_id,
                        t: edge_property_t,
                        inbound_id: edge_property_inbound_id,
                    },
                    edge_property_name,
                ),
                value,
            ))
        });

        Ok(Box::new(mapped))
    }

    pub fn get(
        &self,
        outbound_id: Uuid,
        t: Identifier,
        inbound_id: Uuid,
        name: Identifier,
    ) -> indradb::Result<Option<JsonValue>> {
        let key = self.key(outbound_id, t, inbound_id, name);

        match map_err(self.tree.get(key))? {
            Some(ref value_bytes) => Ok(Some(serde_json::from_slice(value_bytes)?)),
            None => Ok(None),
        }
    }

    fn key_value_index(edge: Edge, value: &JsonValue, property_name: Identifier) -> Vec<u8> {
        util::build(&[
            util::Component::Identifier(property_name),
            util::Component::Json(&Json::new(value.clone())),
            util::Component::Uuid(edge.outbound_id),
            util::Component::Identifier(edge.t),
            util::Component::Uuid(edge.inbound_id),
        ])
    }

    fn read_key_value_index(buf: IVec) -> (Identifier, u64, Edge) {
        let mut cursor = Cursor::new(buf.as_ref());
        let name = util::read_identifier(&mut cursor);
        let value = util::read_u64(&mut cursor);
        let outbound_id = util::read_uuid(&mut cursor);
        let t = util::read_identifier(&mut cursor);
        let inbound_id = util::read_uuid(&mut cursor);
        (
            name,
            value,
            Edge {
                outbound_id,
                t,
                inbound_id,
            },
        )
    }

    pub fn set(
        &self,
        outbound_id: Uuid,
        t: Identifier,
        inbound_id: Uuid,
        name: Identifier,
        value: &JsonValue,
    ) -> indradb::Result<()> {
        let key = self.key(outbound_id, t, inbound_id, name);
        let value_json = serde_json::to_vec(value)?;
        map_err(self.tree.insert(key.as_slice(), value_json.as_slice()))?;
        let value_key = Self::key_value_index(
            Edge {
                outbound_id,
                t,
                inbound_id,
            },
            value,
            name,
        );
        map_err(
            self.value_index_tree
                .insert(value_key.as_slice(), value_json.as_slice()),
        )?;
        Ok(())
    }

    pub fn delete(&self, outbound_id: Uuid, t: Identifier, inbound_id: Uuid, name: Identifier) -> indradb::Result<()> {
        let owner = Edge {
            outbound_id,
            t,
            inbound_id,
        };
        map_err(self.tree.remove(self.key(outbound_id, t, inbound_id, name)))?;
        let prefix = util::build(&[util::Component::Identifier(t)]);
        let mut edges_to_delete = EcoVec::new();
        for edge in self.value_index_tree.scan_prefix(prefix) {
            let (k, _) = map_err(edge)?;
            let (_n, _, vid) = Self::read_key_value_index(k.clone());
            if vid == owner {
                edges_to_delete.push(k);
            }
        }
        for edge in edges_to_delete {
            map_err(self.value_index_tree.remove(&edge))?;
        }
        Ok(())
    }
}
