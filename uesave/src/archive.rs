use std::io::{Read, Seek, Write};

use crate::{Context, Header, Result, StructType, VersionInfo};

pub trait ArchiveReader: Read + Seek {
    fn version(&self) -> &dyn VersionInfo;

    /// Execute a closure with a modified scope for type hint lookups.
    /// The scope is used to track the current property path (e.g., "root.PlayerData.Inventory")
    fn with_scope<F, T>(&mut self, name: &str, f: F) -> T
    where
        F: FnOnce(&mut Self) -> T;

    /// Look up a type hint for the current scope, returning the provided default if not found.
    /// Used to disambiguate struct types in Sets and Maps.
    fn get_type_or(&mut self, default: &StructType) -> Result<StructType>;

    /// Returns true if diagnostic logging is enabled
    fn log(&self) -> bool {
        false
    }
}

pub trait ArchiveWriter: Write + Seek {
    fn version(&self) -> &dyn VersionInfo;

    /// Set version information (typically called after reading/writing the header)
    fn set_version(&mut self, header: Header);

    /// Returns true if diagnostic logging is enabled
    fn log(&self) -> bool {
        false
    }
}

impl<R> ArchiveReader for Context<R>
where
    R: Read + Seek,
{
    fn version(&self) -> &dyn VersionInfo {
        Context::version(self)
    }

    fn with_scope<F, T>(&mut self, name: &str, f: F) -> T
    where
        F: FnOnce(&mut Self) -> T,
    {
        Context::with_scope(self, name, f)
    }

    fn get_type_or(&mut self, default: &StructType) -> Result<StructType> {
        Context::get_type_or(self, default)
    }

    fn log(&self) -> bool {
        Context::log(self)
    }
}
impl<W> ArchiveWriter for Context<W>
where
    W: Write + Seek,
{
    fn version(&self) -> &dyn VersionInfo {
        Context::version(self)
    }

    fn set_version(&mut self, header: Header) {
        Context::set_version(self, header)
    }

    fn log(&self) -> bool {
        Context::log(self)
    }
}
