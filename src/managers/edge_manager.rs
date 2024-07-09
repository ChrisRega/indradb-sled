use indradb::{Edge, util};
use sled::{IVec, Tree};

use datastore::SledHolder;
use errors::map_err;
use reverse_edge;

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

    fn key(&self, edge: Edge) -> Vec<u8> {
        util::build(&[
            util::Component::Uuid(edge.outbound_id),
            util::Component::Identifier(edge.t),
            util::Component::Uuid(edge.inbound_id),
        ])
    }

    pub fn count(&self) -> u64 {
        self.tree.iter().count() as u64
    }

    pub fn set(&self, edge: &Edge) -> indradb::Result<()> {
        let edge_range_manager = EdgeRangeManager::new(self.holder);
        let reversed_edge_range_manager = EdgeRangeManager::new_reversed(self.holder);

        let key = self.key(edge.clone());
        map_err(self.tree.insert(key, IVec::default()))?;
        edge_range_manager.set(edge)?;
        reversed_edge_range_manager.set(&reverse_edge(&edge))?;
        Ok(())
    }

    pub fn delete(&self, edge: &Edge) -> indradb::Result<()> {
        map_err(self.tree.remove(self.key(edge.clone())))?;

        let edge_range_manager = EdgeRangeManager::new(self.holder);
        edge_range_manager.delete(edge)?;

        let reversed_edge_range_manager = EdgeRangeManager::new_reversed(self.holder);
        reversed_edge_range_manager.delete(&reverse_edge(&edge))?;

        let edge_property_manager =
            EdgePropertyManager::new(&self.holder.edge_properties, &self.holder.edge_property_values);

        for item in edge_property_manager.iterate_for_owner(edge)? {
            let ((edge, id), _) = item?;
            edge_property_manager.delete(&edge, id)?;
        }
        Ok(())
    }
}
