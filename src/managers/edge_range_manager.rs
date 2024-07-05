use std::io::Cursor;
use std::u8;

use indradb::{Edge, Identifier, util};
use sled::{Iter as DbIterator, Tree};
use uuid::Uuid;

use datastore::SledHolder;
use errors::map_err;
use managers;

pub type EdgeRangeItem = (Uuid, Identifier, Uuid);

pub struct EdgeRangeManager<'tree> {
    pub tree: &'tree Tree,
}

impl<'tree> EdgeRangeManager<'tree> {
    pub fn new<'db: 'tree>(ds: &'db SledHolder) -> Self {
        EdgeRangeManager { tree: &ds.edge_ranges }
    }

    pub fn new_reversed<'db: 'tree>(ds: &'db SledHolder) -> Self {
        EdgeRangeManager {
            tree: &ds.reversed_edge_ranges,
        }
    }

    fn key(&self, first_id: Uuid, t: &Identifier, second_id: Uuid) -> Vec<u8> {
        util::build(&[
            util::Component::Uuid(first_id),
            util::Component::Identifier(t.clone()),
            util::Component::Uuid(second_id),
        ])
    }

    pub(crate) fn contains(&self, edge: &Edge) -> indradb::Result<bool> {
        let key = self.key(edge.outbound_id, &edge.t, edge.inbound_id);
        map_err(self.tree.contains_key(key))
    }

    fn iterate<'it>(&self, iterator: DbIterator, prefix: Vec<u8>) -> impl Iterator<Item=indradb::Result<EdgeRangeItem>> + 'it {
        let filtered = managers::take_while_prefixed(iterator, prefix);
        filtered.map(move |item| -> indradb::Result<EdgeRangeItem> {
            let (k, _) = map_err(item)?;
            let mut cursor = Cursor::new(k);
            let first_id = util::read_uuid(&mut cursor);
            let t = util::read_identifier(&mut cursor);

            let second_id = util::read_uuid(&mut cursor);
            Ok((first_id, t, second_id))
        })
    }

    pub fn iterate_for_range<'iter, 'trans: 'iter>(
        &'trans self,
        id: Uuid,
        t: Option<&Identifier>,
    ) -> indradb::Result<Box<dyn Iterator<Item=indradb::Result<EdgeRangeItem>> + 'iter>> {
        match t {
            Some(t) => {
                let prefix = util::build(&[util::Component::Uuid(id), util::Component::Identifier(t.clone())]);
                let low_key = util::build(&[
                    util::Component::Uuid(id),
                    util::Component::Identifier(t.clone()),
                ]);
                let low_key_bytes: &[u8] = low_key.as_ref();
                let iterator = self.tree.range(low_key_bytes..);
                Ok(Box::new(self.iterate(iterator, prefix)))
            }
            None => {
                let prefix = util::build(&[util::Component::Uuid(id)]);
                let prefix_bytes: &[u8] = prefix.as_ref();
                let iterator = self.tree.range(prefix_bytes..);
                let mapped = self.iterate(iterator, prefix);
                Ok(Box::new(mapped))
            }
        }
    }

    pub fn iterate_for_owner<'iter, 'trans: 'iter>(
        &'trans self,
        id: Uuid,
    ) -> impl Iterator<Item=indradb::Result<EdgeRangeItem>> + 'iter {
        let prefix: Vec<u8> = util::build(&[util::Component::Uuid(id)]);
        let iterator = self.tree.scan_prefix(&prefix);
        self.iterate(iterator, prefix)
    }

    pub fn set(&self, first_id: Uuid, t: &Identifier, second_id: Uuid) -> indradb::Result<()> {
        let key = self.key(first_id, t, second_id);
        map_err(self.tree.insert(&key, &[]))?;
        Ok(())
    }

    pub fn delete(&self, first_id: Uuid, t: &Identifier, second_id: Uuid) -> indradb::Result<()> {
        map_err(self.tree.remove(&self.key(first_id, t, second_id)))?;
        Ok(())
    }
}