//! The Sled datastore implementation.

extern crate chrono;
extern crate ecow;
extern crate indradb;
extern crate serde_json;
extern crate sled;
#[cfg(any(feature = "bench-suite", feature = "test-suite"))]
extern crate tempfile;
extern crate uuid;

pub use self::datastore::{SledConfig, SledDatastore};

mod datastore;
mod errors;
mod managers;
mod transaction;

#[cfg(feature = "bench-suite")]
mod bench {
    use indradb::full_bench_impl;

    use super::*;

    full_bench_impl!({
        let path = tempdir().unwrap().into_path();
        Database::new(SledDatastore::new(path).unwrap())
    });
    full_bench_impl!({
        let path = tempdir().unwrap().into_path();
        Database::new(SledConfig::with_compression(None).open(path).unwrap())
    });
}

#[cfg(test)]
#[cfg(feature = "test-suite")]
mod test {
    use indradb::full_test_impl;
    use tempfile::tempdir;

    use super::*;

    full_test_impl!({
        let path = tempdir().unwrap().into_path();
        Database::new(SledDatastore::new(path).unwrap())
    });

    full_test_impl!({
        let path = tempdir().unwrap().into_path();
        Database::new(SledConfig::with_compression(None).open(path).unwrap())
    });
}
