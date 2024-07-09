use indradb::{DynIter, Edge, Error, Identifier, Json, Transaction, Vertex};
use uuid::Uuid;

use datastore::SledHolder;
use managers::edge_manager::EdgeManager;
use managers::edge_property_manager::EdgePropertyManager;
use managers::edge_range_manager::EdgeRangeManager;
use managers::vertex_manager::VertexManager;
use managers::vertex_property_manager::VertexPropertyManager;

/// A transaction that is backed by Sled.
pub struct SledTransaction<'a> {
    pub(crate) holder: &'a SledHolder,
    pub(crate) vertex_manager: VertexManager<'a, 'a>,
    pub(crate) edge_manager: EdgeManager<'a, 'a>,
    pub(crate) edge_property_manager: EdgePropertyManager<'a>,
    pub(crate) vertex_property_manager: VertexPropertyManager<'a>,
    pub(crate) edge_range_manager: EdgeRangeManager<'a>,
    pub(crate) edge_range_manager_rev: EdgeRangeManager<'a>,
}

impl<'a> Transaction<'a> for SledTransaction<'a> {
    fn vertex_count(&self) -> u64 {
        let vertex_manager = VertexManager::new(self.holder);
        vertex_manager.count()
    }

    fn all_vertices(&'a self) -> indradb::Result<DynIter<'a, Vertex>> {
        let iterator = self.vertex_manager.iterate_for_range(Uuid::default());
        let mapped = iterator.map(move |item| {
            let (id, t) = item?;
            let vertex = Vertex::with_id(id, t);
            Ok::<Vertex, Error>(vertex)
        });

        Ok(Box::new(mapped))
    }

    fn range_vertices(&'a self, offset: Uuid) -> indradb::Result<DynIter<'a, Vertex>> {
        let iter = self
            .vertex_manager
            .iterate_for_range(offset)
            .map(|e| e.map(|v| Vertex::with_id(v.0, v.1)));
        Ok(Box::new(iter))
    }

    fn specific_vertices(&'a self, ids: Vec<Uuid>) -> indradb::Result<DynIter<'a, Vertex>> {
        let iter = ids.into_iter().filter_map(move |id| {
            let v = self.vertex_manager.get(id).transpose();
            v.map(|v| v.map(|v| Vertex::with_id(id, v)))
        });
        Ok(Box::new(iter))
    }

    fn vertex_ids_with_property(&'a self, name: Identifier) -> indradb::Result<Option<DynIter<'a, Uuid>>> {
        let iter = self.vertex_property_manager.iterate_for_property_name(name)?;
        let iter = iter.map(|r| r.map(|(id, _)| id.0));
        Ok(Some(Box::new(iter)))
    }

    fn vertex_ids_with_property_value(
        &'a self,
        name: Identifier,
        value: &Json,
    ) -> indradb::Result<Option<DynIter<'a, Uuid>>> {
        let iter = self
            .vertex_property_manager
            .iterate_for_property_name_and_value(name, value)?;
        let iter = iter.map(|r| r.map(|(id, _)| id.0));
        Ok(Some(Box::new(iter)))
    }

    fn edge_count(&self) -> u64 {
        let edge_manager = EdgeManager::new(self.holder);
        edge_manager.count()
    }

    fn all_edges(&'a self) -> indradb::Result<DynIter<'a, Edge>> {
        let iter = self.edge_range_manager.iterate_for_all();

        Ok(Box::new(iter))
    }

    fn range_edges(&'a self, offset: Edge) -> indradb::Result<DynIter<'a, Edge>> {
        let iter = self.edge_range_manager.iterate_for_range(&offset);

        Ok(Box::new(iter))
    }

    fn range_reversed_edges(&'a self, offset: Edge) -> indradb::Result<DynIter<'a, Edge>> {
        let iter = self.edge_range_manager_rev.iterate_for_range(&offset);

        Ok(Box::new(iter))
    }

    fn specific_edges(&'a self, edges: Vec<Edge>) -> indradb::Result<DynIter<'a, Edge>> {
        let iter: Vec<_> = edges
            .into_iter()
            .filter(|e| {
                let r = self.edge_range_manager.contains(e);
                if let Ok(r) = r {
                    r
                } else {
                    false
                }
            })
            .map(Ok)
            .collect();
        Ok(Box::new(iter.into_iter()))
    }

    fn edges_with_property(&'a self, name: Identifier) -> indradb::Result<Option<DynIter<'a, Edge>>> {
        let iter = self.edge_property_manager.iterate_for_property_name(name)?;
        Ok(Some(Box::new(iter)))
    }

    fn edges_with_property_value(
        &'a self,
        name: Identifier,
        value: &Json,
    ) -> indradb::Result<Option<DynIter<'a, Edge>>> {
        let iter = self
            .edge_property_manager
            .iterate_for_property_name_and_value(name, value)?;
        Ok(Some(Box::new(iter)))
    }

    fn vertex_property(&self, vertex: &Vertex, name: Identifier) -> indradb::Result<Option<Json>> {
        let r = self.vertex_property_manager.get(vertex.id, name)?;
        Ok(r.map(|v| v.into()))
    }

    fn all_vertex_properties_for_vertex(&'a self, vertex: &Vertex) -> indradb::Result<DynIter<'a, (Identifier, Json)>> {
        let iter = self.vertex_property_manager.iterate_for_owner(vertex.id)?;
        let iter = iter.map(|r| r.map(|((_, name), val)| (name, Json::new(val))));
        Ok(Box::new(iter))
    }

    fn edge_property(&self, edge: &Edge, name: Identifier) -> indradb::Result<Option<Json>> {
        let result = self
            .edge_property_manager
            .get(edge.outbound_id, edge.t, edge.inbound_id, name)?;
        Ok(result.map(Json::new))
    }

    fn all_edge_properties_for_edge(&'a self, edge: &Edge) -> indradb::Result<DynIter<'a, (Identifier, Json)>> {
        let iter = self.edge_property_manager.iterate_for_owner(edge)?;
        let iter = iter.map(|e| e.map(|((_, id), val)| (id, Json::new(val))));
        Ok(Box::new(iter))
    }

    fn delete_vertices(&mut self, vertices: Vec<Vertex>) -> indradb::Result<()> {
        for v in vertices {
            self.vertex_manager.delete(v.id)?
        }
        Ok(())
    }

    fn delete_edges(&mut self, edges: Vec<Edge>) -> indradb::Result<()> {
        for item in edges.iter() {
            if self.vertex_manager.get(item.outbound_id)?.is_some() {
                self.edge_manager.delete(item)?;
            };
        }

        Ok(())
    }

    fn delete_vertex_properties(&mut self, props: Vec<(Uuid, Identifier)>) -> indradb::Result<()> {
        for (id, prop) in props {
            self.vertex_property_manager.delete(id, prop)?
        }
        Ok(())
    }

    fn delete_edge_properties(&mut self, props: Vec<(Edge, Identifier)>) -> indradb::Result<()> {
        for (edge, prop) in props {
            self.edge_property_manager
                .delete(edge.outbound_id, edge.t, edge.inbound_id, prop)?;
        }
        Ok(())
    }

    fn create_vertex(&mut self, vertex: &Vertex) -> indradb::Result<bool> {
        self.vertex_manager.create(vertex)
    }

    fn create_edge(&mut self, edge: &Edge) -> indradb::Result<bool> {
        let outbound_exists = self.vertex_manager.exists(edge.outbound_id)?;
        let inbound_exists = self.vertex_manager.exists(edge.inbound_id)?;

        if !outbound_exists || !inbound_exists {
            Ok(false)
        } else {
            self.edge_manager.set(edge)?;
            Ok(true)
        }
    }

    fn index_property(&mut self, _name: Identifier) -> indradb::Result<()> {
        // oh boy we really want this i guess
        Ok(())
    }

    fn set_vertex_properties(&mut self, vertices: Vec<Uuid>, name: Identifier, value: &Json) -> indradb::Result<()> {
        for v in vertices {
            self.vertex_property_manager.set(v, name, value)?;
        }
        Ok(())
    }

    fn set_edge_properties(&mut self, edges: Vec<Edge>, name: Identifier, value: &Json) -> indradb::Result<()> {
        for edge in edges {
            self.edge_property_manager
                .set(edge.outbound_id, edge.t, edge.inbound_id, name, value)?;
        }
        Ok(())
    }
}
