use std::u8;

use sled::{Iter as DbIterator, IVec};
use sled::Result as SledResult;

pub(crate) mod vertex_manager;
pub(crate) mod edge_manager;
pub(crate) mod edge_range_manager;
pub(crate) mod vertex_property_manager;
pub(crate) mod edge_property_manager;

fn take_while_prefixed(iterator: DbIterator, prefix: Vec<u8>) -> impl Iterator<Item=SledResult<(IVec, IVec)>> {
    iterator.take_while(move |item| -> bool {
        match item {
            Ok((k, _)) => k.starts_with(&prefix),
            Err(_) => false,
        }
    })
}


