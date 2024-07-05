use std::path::Path;
use std::sync::Arc;
use std::u64;

use chrono::offset::Utc;
use indradb::{Datastore, DynIter, Edge, Error, Identifier, Json, Result, Transaction, Vertex};
use sled::{Config, Db, Tree};
use uuid::Uuid;

use super::errors::map_err;
use super::managers::*;

#[derive(Copy, Clone, Default, Debug)]
pub struct SledConfig {
    use_compression: bool,
    compression_factor: Option<i32>,
}

impl SledConfig {
    /// Creates a new sled config with zstd compression enabled.
    ///
    /// # Arguments
    /// * `factor`: The zstd compression factor to use. If unspecified, this
    ///   will default to 5.
    pub fn with_compression(factor: Option<i32>) -> SledConfig {
        SledConfig {
            use_compression: true,
            compression_factor: factor,
        }
    }

    /// Creates a new sled datastore.
    pub fn open<P: AsRef<Path>>(self, path: P) -> Result<SledDatastore> {
        Ok(SledDatastore {
            holder: Arc::new(SledHolder::new(path, self)?),
        })
    }
}

/// The meat of a Sled datastore
pub struct SledHolder {
    pub(crate) db: Arc<Db>, // Derefs to Tree, holds the vertices
    pub(crate) edges: Tree,
    pub(crate) edge_ranges: Tree,
    pub(crate) reversed_edge_ranges: Tree,
    pub(crate) vertex_properties: Tree,
    pub(crate) edge_properties: Tree,
}

impl<'ds> SledHolder {
    /// The meat of a Sled datastore.
    ///
    /// # Arguments
    /// * `path`: The file path to the Sled database.
    /// * `opts`: Sled options to pass in.
    pub fn new<P: AsRef<Path>>(path: P, opts: SledConfig) -> Result<SledHolder> {
        let mut config = Config::default().path(path);

        if opts.use_compression {
            config = config.use_compression(true);
        }

        if let Some(compression_factor) = opts.compression_factor {
            config = config.compression_factor(compression_factor);
        }

        let db = map_err(config.open())?;

        Ok(SledHolder {
            edges: map_err(db.open_tree("edges"))?,
            edge_ranges: map_err(db.open_tree("edge_ranges"))?,
            reversed_edge_ranges: map_err(db.open_tree("reversed_edge_ranges"))?,
            vertex_properties: map_err(db.open_tree("vertex_properties"))?,
            edge_properties: map_err(db.open_tree("edge_properties"))?,
            db: Arc::new(db),
        })
    }
}

/// A datastore that is backed by Sled.
pub struct SledDatastore {
    pub(crate) holder: Arc<SledHolder>,
}

impl<'ds> SledDatastore {
    /// Creates a new Sled datastore.
    ///
    /// # Arguments
    /// * `path`: The file path to the Sled database.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<SledDatastore> {
        Ok(SledDatastore {
            holder: Arc::new(SledHolder::new(path, SledConfig::default())?),
        })
    }
}

impl Datastore for SledDatastore {
    type Transaction<'a> = SledTransaction;

    fn transaction(&self) -> Self::Transaction<'_> {
        SledTransaction::new(self.holder.clone())
    }
}


/// A transaction that is backed by Sled.
pub struct SledTransaction {
    holder: Arc<SledHolder>,
}

impl SledTransaction {
    fn new(holder: Arc<SledHolder>) -> Self {
        SledTransaction { holder }
    }
}

impl<'a> Transaction<'a> for SledTransaction {
    fn vertex_count(&self) -> u64 {
        let vertex_manager = VertexManager::new(&self.holder);
        vertex_manager.count().into()
    }

    fn all_vertices(&'a self) -> Result<DynIter<'a, Vertex>> {
        let vertex_manager = VertexManager::new(&self.holder);
        let iterator = vertex_manager.iterate_for_range(Uuid::default());
        let mapped = iterator.map(move |item| {
            let (id, t) = item?;
            let vertex = Vertex::with_id(id, t);
            Ok::<Vertex, Error>(vertex)
        });

        Ok(Box::new(mapped))
    }

    fn range_vertices(&'a self, offset: Uuid) -> Result<DynIter<'a, Vertex>> {
        let vertex_manager = VertexManager::new(&self.holder);
        let iter = vertex_manager.iterate_for_range(offset).map(|e| e.map(|v| Vertex::with_id(v.0, v.1)));
        Ok(Box::new(iter))
    }

    fn specific_vertices(&'a self, ids: Vec<Uuid>) -> Result<DynIter<'a, Vertex>> {
        let vertex_manager = VertexManager::new(&self.holder);
        let iter = ids.into_iter().filter_map(move |id| {
            let v = vertex_manager.get(id).transpose();
            v.map(|v| v.map(|v| Vertex::with_id(id, v)))
        });
        Ok(Box::new(iter))
    }

    fn vertex_ids_with_property(&'a self, name: Identifier) -> Result<Option<DynIter<'a, Uuid>>> {
        let prop_manager = VertexPropertyManager::new(&self.holder.vertex_properties);
        let iter = self.all_vertices()?
            .filter(|v|
            {
                let property_result = v.as_ref().and_then(|v| prop_manager.get(v.id, name.as_str()).as_ref());
                if let Ok(v) = property_result {
                    return v.is_some();
                }
                return false;
            }).map(|v| v.map(|v| v.id));

        Ok(Some(Box::new(iter)))
    }

    fn vertex_ids_with_property_value(&'a self, name: Identifier, value: &Json) -> Result<Option<DynIter<'a, Uuid>>> {
        let prop_manager = VertexPropertyManager::new(&self.holder.vertex_properties);
        let iter = self.all_vertices()?
            .filter(|v|
            {
                let property_result = v.as_ref().and_then(|v|
                prop_manager.get(v.id, name.as_str()).as_ref());
                if let Ok(Some(v)) = property_result {
                    return *v == *value.0;
                }
                false
            }).map(|v| v.map(|v| v.id));

        Ok(Some(Box::new(iter)))
    }

    fn edge_count(&self) -> u64 {
        let edge_manager = EdgeManager::new(&self.holder);
        edge_manager.count().into()
    }

    fn all_edges(&'a self) -> Result<DynIter<'a, Edge>> {
        let range_manager = EdgeRangeManager::new(&self.holder);
        let iter = range_manager.iterate_for_range(Uuid::default(), None, None).map(|i| i.map(|e| e.map(|(outbound_id, t, _time, inbound_id)| Edge {
            outbound_id,
            t,
            inbound_id,
        })))?;
        Ok(Box::new(iter))
    }

    fn range_edges(&'a self, offset: Edge) -> Result<DynIter<'a, Edge>> {
        let range_manager = EdgeRangeManager::new(&self.holder);
        let iter = range_manager.iterate_for_range(offset.inbound_id, Some(&offset.t), None)?;
        let iter = iter.map(|r| r.map(|(outbound_id, t, _time, inbound_id)| Edge {
            outbound_id,
            t,
            inbound_id,
        }));
        Ok(Box::new(iter))
    }

    fn range_reversed_edges(&'a self, offset: Edge) -> Result<DynIter<'a, Edge>> {
        let range_manager = EdgeRangeManager::new_reversed(&self.holder);
        let iter = range_manager.iterate_for_range(offset.inbound_id, Some(&offset.t), None)?;
        let iter = iter.map(|r| r.map(|(outbound_id, t, _time, inbound_id)| Edge {
            outbound_id,
            t,
            inbound_id,
        }));
        Ok(Box::new(iter))
    }

    fn specific_edges(&'a self, edges: Vec<Edge>) -> Result<DynIter<'a, Edge>> {
        let edge_manager = EdgeManager::new(&self.holder);
        let iter = edges.into_iter().filter(|e| {
            let r = edge_manager.get(e.outbound_id, &e.t, e.inbound_id).transpose();
            if let Some(Ok(_)) = r {
                return true;
            }
            false
        }).map(|e| Ok(e));
        Ok(Box::new(iter))
    }

    fn edges_with_property(&'a self, name: Identifier) -> Result<Option<DynIter<'a, Edge>>> {
        let edge_property_manager = EdgePropertyManager::new(&self.holder.edge_properties);
        let iter = self.all_edges()?.filter(|r| {
            let has_property = r.as_ref().and_then(|e| edge_property_manager.get(e.outbound_id, &e.t, e.inbound_id, name.as_str()).as_ref());
            if let Ok(prop) = has_property {
                return prop.is_some();
            }
            false
        });
        Ok(Some(Box::new(iter)))
    }

    fn edges_with_property_value(&'a self, name: Identifier, value: &Json) -> Result<Option<DynIter<'a, Edge>>> {
        let edge_property_manager = EdgePropertyManager::new(&self.holder.edge_properties);
        let iter = self.all_edges()?.filter(|r| {
            let has_property = r.as_ref().and_then(|e| edge_property_manager.get(e.outbound_id, &e.t, e.inbound_id, name.as_str()).as_ref());
            if let Ok(Some(v)) = has_property {
                return *v == **value;
            }
            false
        });
        Ok(Some(Box::new(iter)))
    }

    fn vertex_property(&self, vertex: &Vertex, name: Identifier) -> Result<Option<Json>> {
        let vertex_property_manager = VertexPropertyManager::new(&self.holder.vertex_properties);
        let r = vertex_property_manager.get(vertex.id, &name)?;
        Ok(r.map(|v| v.into()))
    }

    fn all_vertex_properties_for_vertex(&'a self, vertex: &Vertex) -> Result<DynIter<'a, (Identifier, Json)>> {
        let vertex_property_manager = VertexPropertyManager::new(&self.holder.vertex_properties);
        let iter = vertex_property_manager.iterate_for_owner(vertex.id)?;
        let iter = iter.map(|r| r.and_then(|((_, name), val)| Ok((Identifier::new(name)?, Json::new(val)))));
        Ok(Box::new(iter))
    }

    fn edge_property(&self, edge: &Edge, name: Identifier) -> Result<Option<Json>> {
        let edge_property_manager = EdgePropertyManager::new(&self.holder.edge_properties);
        let result = edge_property_manager.get(edge.outbound_id, &edge.t, edge.inbound_id, name.as_str())?;
        Ok(result.map(|v| Json::new(v)))
    }

    fn all_edge_properties_for_edge(&'a self, edge: &Edge) -> Result<DynIter<'a, (Identifier, Json)>> {
        let edge_property_manager = EdgePropertyManager::new(&self.holder.edge_properties);
        let iter = edge_property_manager.iterate_for_owner(edge.outbound_id, &edge.t, edge.inbound_id)?;
        let iter = iter.map(|e| e.map(|((_, id, _, _), val)| (id, Json::new(val))));
        Ok(Box::new(iter))
    }

    fn delete_vertices(&mut self, vertices: Vec<Vertex>) -> Result<()> {
        let vertex_manager = VertexManager::new(&self.holder);
        for v in vertices {
            vertex_manager.delete(v.id)?
        }
        Ok(())
    }

    fn delete_edges(&mut self, edges: Vec<Edge>) -> Result<()> {
        let edge_manager = EdgeManager::new(&self.holder);
        let vertex_manager = VertexManager::new(&self.holder);

        for item in edges.iter() {
            let edge = edge_manager.get(item.outbound_id, &item.t, item.inbound_id)?;
            if let Some(t) = edge {
                if vertex_manager.get(item.outbound_id)?.is_some() {
                    edge_manager.delete(item.outbound_id, &item.t, item.inbound_id, t)?;
                };
            }
        }
        Ok(())
    }

    fn delete_vertex_properties(&mut self, props: Vec<(Uuid, Identifier)>) -> Result<()> {
        let vertex_properties = VertexPropertyManager::new(&self.holder.vertex_properties);
        for (id, prop) in props {
            vertex_properties.delete(id, prop.as_str())?
        }
        Ok(())
    }

    fn delete_edge_properties(&mut self, props: Vec<(Edge, Identifier)>) -> Result<()> {
        let edge_property_manager = EdgePropertyManager::new(&self.holder.edge_properties);
        for (edge, prop) in props {
            edge_property_manager.delete(edge.outbound_id, &edge.t, edge.inbound_id, prop.as_str())?;
        }
        Ok(())
    }

    fn create_vertex(&mut self, vertex: &Vertex) -> Result<bool> {
        let vertex_manager = VertexManager::new(&self.holder);
        vertex_manager.create(vertex)
    }

    fn create_edge(&mut self, edge: &Edge) -> Result<bool> {
        let vertex_manager = VertexManager::new(&self.holder);

        if !vertex_manager.exists(edge.outbound_id)? || !vertex_manager.exists(edge.inbound_id)? {
            Ok(false)
        } else {
            let edge_manager = EdgeManager::new(&self.holder);
            edge_manager.set(edge.outbound_id, &edge.t, edge.inbound_id, Utc::now())?;
            Ok(true)
        }
    }

    fn index_property(&mut self, _name: Identifier) -> Result<()> {
        // oh boy we really want this i guess
        Ok(())
    }

    fn set_vertex_properties(&mut self, vertices: Vec<Uuid>, name: Identifier, value: &Json) -> Result<()> {
        let vertex_property_manager = VertexPropertyManager::new(&self.holder.vertex_properties);
        for v in vertices {
            vertex_property_manager.set(v, name.as_str(), &value)?;
        }
        Ok(())
    }

    fn set_edge_properties(&mut self, edges: Vec<Edge>, name: Identifier, value: &Json) -> Result<()> {
        let edge_property_manager = EdgePropertyManager::new(&self.holder.edge_properties);
        for edge in edges {
            edge_property_manager.set(edge.outbound_id, &edge.t, edge.inbound_id, name.as_str(), &value)?;
        }
        Ok(())
    }
}


fn remove_nones_from_iterator<I, T>(iter: I) -> impl Iterator<Item=Result<T>>
where
    I: Iterator<Item=Result<Option<T>>>,
{
    iter.filter_map(|item| match item {
        Err(err) => Some(Err(err)),
        Ok(Some(value)) => Some(Ok(value)),
        _ => None,
    })
}
