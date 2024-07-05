//! The Sled datastore implementation.

#![cfg_attr(feature = "bench-suite", feature(test))]

extern crate chrono;
#[cfg(any(feature = "bench-suite", feature = "test-suite"))]
#[macro_use]
extern crate indradb;
#[cfg(not(any(feature = "bench-suite", feature = "test-suite")))]
extern crate indradb;
extern crate serde_json;
extern crate sled;
#[cfg(any(feature = "bench-suite", feature = "test-suite"))]
extern crate tempfile;
extern crate uuid;

pub use self::datastore::{SledConfig, SledDatastore, SledTransaction};

mod datastore;
mod errors;
mod managers;

mod normal_config {
    #[cfg(feature = "bench-suite")]
    full_bench_impl!({
        use super::SledDatastore;
        use tempfile::tempdir;
        let path = tempdir().unwrap().into_path();
        SledDatastore::new(path).unwrap()
    });

    #[cfg(feature = "test-suite")]
    full_test_impl!({
        use super::SledDatastore;
        use tempfile::tempdir;
        let path = tempdir().unwrap().into_path();
        SledDatastore::new(path).unwrap()
    });
}

mod compression_config {
    #[cfg(feature = "bench-suite")]
    full_bench_impl!({
        use super::SledConfig;
        use tempfile::tempdir;
        let path = tempdir().unwrap().into_path();
        SledConfig::with_compression(None).open(path).unwrap()
    });

    #[cfg(feature = "test-suite")]
    full_test_impl!({
        use super::SledConfig;
        use tempfile::tempdir;
        let path = tempdir().unwrap().into_path();
        SledConfig::with_compression(None).open(path).unwrap()
    });
}
