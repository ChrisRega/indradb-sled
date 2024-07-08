use std::path::Path;
use std::u64;

use indradb::{Datastore, DynIter, Edge, Error, Identifier, Json, Result, Transaction, Vertex};
use sled::{Config, Db, Tree};
use uuid::Uuid;

use managers::edge_manager::EdgeManager;
use managers::edge_range_manager::EdgeRangeManager;
use managers::vertex_property_manager::VertexPropertyManager;

use crate::managers::edge_property_manager::EdgePropertyManager;
use crate::managers::vertex_manager::VertexManager;

use super::errors::map_err;

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
            holder: SledHolder::new(path, self)?,
        })
    }
}

/// The meat of a Sled datastore
pub struct SledHolder {
    pub(crate) db: Db, // Derefs to Tree, holds the vertices
    pub(crate) edges: Tree,
    pub(crate) edge_ranges: Tree,
    pub(crate) reversed_edge_ranges: Tree,
    pub(crate) vertex_properties: Tree,
    pub(crate) edge_properties: Tree,
    // for prop-name -> value -> ID prefix-indexed lookup
    pub(crate) edge_property_values: Tree,
    // for prop-name -> value -> UUID prefix-indexed lookup
    pub(crate) vertex_property_values: Tree,
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
            vertex_property_values: map_err(db.open_tree("vertex_property_values"))?,
            edge_property_values: map_err(db.open_tree("edge_property_values"))?,
            db,
        })
    }
}

/// A datastore that is backed by Sled.
pub struct SledDatastore {
    pub(crate) holder: SledHolder,
}

impl<'ds> SledDatastore {
    /// Creates a new Sled datastore.
    ///
    /// # Arguments
    /// * `path`: The file path to the Sled database.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<SledDatastore> {
        Ok(SledDatastore {
            holder: SledHolder::new(path, SledConfig::default())?,
        })
    }
}

impl Datastore for SledDatastore {
    type Transaction<'a> = SledTransaction<'a>
    where
        Self: 'a;

    fn transaction(&self) -> Self::Transaction<'_> {
        SledTransaction {
            holder: &self.holder,
            vertex_manager: VertexManager::new(&self.holder),
            edge_manager: EdgeManager::new(&self.holder),
            edge_range_manager: EdgeRangeManager::new(&self.holder),
            edge_range_manager_rev: EdgeRangeManager::new_reversed(&self.holder),
            edge_property_manager: EdgePropertyManager::new(&self.holder.edge_properties),
            vertex_property_manager: VertexPropertyManager::new(
                &self.holder.vertex_properties,
                &self.holder.vertex_property_values,
            ),
        }
    }
}

/// A transaction that is backed by Sled.
pub struct SledTransaction<'a> {
    holder: &'a SledHolder,
    vertex_manager: VertexManager<'a, 'a>,
    edge_manager: EdgeManager<'a, 'a>,
    edge_property_manager: EdgePropertyManager<'a>,
    vertex_property_manager: VertexPropertyManager<'a>,
    edge_range_manager: EdgeRangeManager<'a>,
    edge_range_manager_rev: EdgeRangeManager<'a>,
}

impl<'a> Transaction<'a> for SledTransaction<'a> {
    fn vertex_count(&self) -> u64 {
        let vertex_manager = VertexManager::new(&self.holder);
        vertex_manager.count().into()
    }

    fn all_vertices(&'a self) -> Result<DynIter<'a, Vertex>> {
        let iterator = self.vertex_manager.iterate_for_range(Uuid::default());
        let mapped = iterator.map(move |item| {
            let (id, t) = item?;
            let vertex = Vertex::with_id(id, t);
            Ok::<Vertex, Error>(vertex)
        });

        Ok(Box::new(mapped))
    }

    fn range_vertices(&'a self, offset: Uuid) -> Result<DynIter<'a, Vertex>> {
        let iter = self
            .vertex_manager
            .iterate_for_range(offset)
            .map(|e| e.map(|v| Vertex::with_id(v.0, v.1)));
        Ok(Box::new(iter))
    }

    fn specific_vertices(&'a self, ids: Vec<Uuid>) -> Result<DynIter<'a, Vertex>> {
        let iter = ids.into_iter().filter_map(move |id| {
            let v = self.vertex_manager.get(id).transpose();
            v.map(|v| v.map(|v| Vertex::with_id(id, v)))
        });
        Ok(Box::new(iter))
    }

    fn vertex_ids_with_property(&'a self, name: Identifier) -> Result<Option<DynIter<'a, Uuid>>> {
        let iter = self.vertex_property_manager.iterate_for_property_name(name)?;
        let iter = iter.map(|r| r.map(|(id, _)| id.0));
        Ok(Some(Box::new(iter)))
    }

    fn vertex_ids_with_property_value(&'a self, name: Identifier, value: &Json) -> Result<Option<DynIter<'a, Uuid>>> {
        let iter = self
            .vertex_property_manager
            .iterate_for_property_name_and_value(name, value)?;
        let iter = iter.map(|r| r.map(|(id, _)| id.0));
        Ok(Some(Box::new(iter)))
    }

    fn edge_count(&self) -> u64 {
        let edge_manager = EdgeManager::new(&self.holder);
        edge_manager.count().into()
    }

    fn all_edges(&'a self) -> Result<DynIter<'a, Edge>> {
        let iter = self
            .edge_range_manager
            .iterate_for_range(Uuid::default(), None)
            .map(|i| {
                i.map(|e| {
                    e.map(|(outbound_id, t, inbound_id)| Edge {
                        outbound_id,
                        t,
                        inbound_id,
                    })
                })
            })?;
        Ok(Box::new(iter))
    }

    fn range_edges(&'a self, offset: Edge) -> Result<DynIter<'a, Edge>> {
        let iter = self
            .edge_range_manager
            .iterate_for_range(offset.inbound_id, Some(&offset.t))?;
        let iter = iter.map(|r| {
            r.map(|(outbound_id, t, inbound_id)| Edge {
                outbound_id,
                t,
                inbound_id,
            })
        });
        Ok(Box::new(iter))
    }

    fn range_reversed_edges(&'a self, offset: Edge) -> Result<DynIter<'a, Edge>> {
        let iter = self
            .edge_range_manager_rev
            .iterate_for_range(offset.inbound_id, Some(&offset.t))?;
        let iter = iter.map(|r| {
            r.map(|(outbound_id, t, inbound_id)| Edge {
                outbound_id,
                t,
                inbound_id,
            })
        });
        Ok(Box::new(iter))
    }

    fn specific_edges(&'a self, edges: Vec<Edge>) -> Result<DynIter<'a, Edge>> {
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
            .map(|e| Ok(e))
            .collect();
        Ok(Box::new(iter.into_iter()))
    }

    fn edges_with_property(&'a self, name: Identifier) -> Result<Option<DynIter<'a, Edge>>> {
        unimplemented!()
    }

    fn edges_with_property_value(&'a self, name: Identifier, value: &Json) -> Result<Option<DynIter<'a, Edge>>> {
        unimplemented!()
    }

    fn vertex_property(&self, vertex: &Vertex, name: Identifier) -> Result<Option<Json>> {
        let r = self.vertex_property_manager.get(vertex.id, name.clone())?;
        Ok(r.map(|v| v.into()))
    }

    fn all_vertex_properties_for_vertex(&'a self, vertex: &Vertex) -> Result<DynIter<'a, (Identifier, Json)>> {
        let iter = self.vertex_property_manager.iterate_for_owner(vertex.id)?;
        let iter = iter.map(|r| r.and_then(|((_, name), val)| Ok((name, Json::new(val)))));
        Ok(Box::new(iter))
    }

    fn edge_property(&self, edge: &Edge, name: Identifier) -> Result<Option<Json>> {
        let result = self
            .edge_property_manager
            .get(edge.outbound_id, &edge.t, edge.inbound_id, name.as_str())?;
        Ok(result.map(|v| Json::new(v)))
    }

    fn all_edge_properties_for_edge(&'a self, edge: &Edge) -> Result<DynIter<'a, (Identifier, Json)>> {
        let iter: Vec<_> = self
            .edge_property_manager
            .iterate_for_owner(edge.outbound_id, &edge.t, edge.inbound_id)?
            .collect();
        let iter = iter
            .into_iter()
            .map(|e| e.map(|((_, id, _, _), val)| (id, Json::new(val))));
        Ok(Box::new(iter))
    }

    fn delete_vertices(&mut self, vertices: Vec<Vertex>) -> Result<()> {
        for v in vertices {
            self.vertex_manager.delete(v.id)?
        }
        Ok(())
    }

    fn delete_edges(&mut self, edges: Vec<Edge>) -> Result<()> {
        for item in edges.iter() {
            if self.vertex_manager.get(item.outbound_id)?.is_some() {
                self.edge_manager.delete(item.outbound_id, &item.t, item.inbound_id)?;
            };
        }

        Ok(())
    }

    fn delete_vertex_properties(&mut self, props: Vec<(Uuid, Identifier)>) -> Result<()> {
        for (id, prop) in props {
            self.vertex_property_manager.delete(id, prop)?
        }
        Ok(())
    }

    fn delete_edge_properties(&mut self, props: Vec<(Edge, Identifier)>) -> Result<()> {
        for (edge, prop) in props {
            self.edge_property_manager
                .delete(edge.outbound_id, &edge.t, edge.inbound_id, prop.as_str())?;
        }
        Ok(())
    }

    fn create_vertex(&mut self, vertex: &Vertex) -> Result<bool> {
        self.vertex_manager.create(vertex)
    }

    fn create_edge(&mut self, edge: &Edge) -> Result<bool> {
        let outbound_exists = self.vertex_manager.exists(edge.outbound_id)?;
        let inbound_exists = self.vertex_manager.exists(edge.inbound_id)?;

        if !outbound_exists || !inbound_exists {
            Ok(false)
        } else {
            let edge_manager = EdgeManager::new(&self.holder);
            edge_manager.set(edge.outbound_id, &edge.t, edge.inbound_id)?;
            Ok(true)
        }
    }

    fn index_property(&mut self, _name: Identifier) -> Result<()> {
        // oh boy we really want this i guess
        Ok(())
    }

    fn set_vertex_properties(&mut self, vertices: Vec<Uuid>, name: Identifier, value: &Json) -> Result<()> {
        for v in vertices {
            self.vertex_property_manager.set(v, name, &value)?;
        }
        Ok(())
    }

    fn set_edge_properties(&mut self, edges: Vec<Edge>, name: Identifier, value: &Json) -> Result<()> {
        for edge in edges {
            self.edge_property_manager
                .set(edge.outbound_id, &edge.t, edge.inbound_id, name.as_str(), &value)?;
        }
        Ok(())
    }
}

fn remove_nones_from_iterator<I, T>(iter: I) -> impl Iterator<Item = Result<T>>
where
    I: Iterator<Item = Result<Option<T>>>,
{
    iter.filter_map(|item| match item {
        Err(err) => Some(Err(err)),
        Ok(Some(value)) => Some(Ok(value)),
        _ => None,
    })
}
