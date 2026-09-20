#[cfg(feature = "cli")]
pub mod registry;

#[cfg(feature = "cli")]
pub use registry::{get, handle, registry, GameCli, GameInfo};

pub mod palworld;
