use std::u8;

use indradb::{Identifier, util};
use sled::{IVec, Tree};
use uuid::Uuid;

use datastore::SledHolder;
use errors::map_err;

use crate::managers::edge_property_manager::EdgePropertyManager;
use crate::managers::edge_range_manager::EdgeRangeManager;

pub struct EdgeManager<'db: 'tree, 'tree> {
    pub holder: &'db SledHolder,
    pub tree: &'tree Tree,
}

impl<'db, 'tree> EdgeManager<'db, 'tree> {
    pub fn new(ds: &'db SledHolder) -> Self {
        EdgeManager {
            holder: ds,
            tree: &ds.edges,
        }
    }

    fn key(&self, outbound_id: Uuid, t: &Identifier, inbound_id: Uuid) -> Vec<u8> {
        util::build(&[
            util::Component::Uuid(outbound_id),
            util::Component::Identifier(t.clone()),
            util::Component::Uuid(inbound_id),
        ])
    }

    pub fn count(&self) -> u64 {
        self.tree.iter().count() as u64
    }

    pub fn set(&self, outbound_id: Uuid, t: &Identifier, inbound_id: Uuid) -> indradb::Result<()> {
        let edge_range_manager = EdgeRangeManager::new(self.holder);
        let reversed_edge_range_manager = EdgeRangeManager::new_reversed(self.holder);

        let key = self.key(outbound_id, t, inbound_id);
        map_err(self.tree.insert(key, IVec::default()))?;
        edge_range_manager.set(outbound_id, t, inbound_id)?;
        reversed_edge_range_manager.set(inbound_id, t, outbound_id)?;
        Ok(())
    }

    pub fn delete(&self, outbound_id: Uuid, t: &Identifier, inbound_id: Uuid) -> indradb::Result<()> {
        map_err(self.tree.remove(&self.key(outbound_id, t, inbound_id)))?;

        let edge_range_manager = EdgeRangeManager::new(self.holder);
        edge_range_manager.delete(outbound_id, t, inbound_id)?;

        let reversed_edge_range_manager = EdgeRangeManager::new_reversed(self.holder);
        reversed_edge_range_manager.delete(inbound_id, t, outbound_id)?;

        let edge_property_manager =
            EdgePropertyManager::new(&self.holder.edge_properties, &self.holder.edge_property_values);

        for item in edge_property_manager.iterate_for_owner(outbound_id, t, inbound_id)? {
            let ((edge, id), _) = item?;
            edge_property_manager.delete(edge.outbound_id, &edge.t, edge.inbound_id, id)?;
        }
        Ok(())
    }
}
