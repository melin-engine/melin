//! Library facet of the bench crate. Exists so integration tests and
//! auxiliary binaries can reuse the calibration pipeline. The bench
//! binary (`src/main.rs`) is self-contained and does not depend on
//! anything declared here.

pub mod calibration;
pub mod generator;
pub mod keys;
