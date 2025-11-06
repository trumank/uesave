use std::io::{Read, Seek, Write};

use crate::{Header, Result, SaveGameArchive, StructType, VersionInfo};

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

    /// Read a string from the archive
    fn read_string(&mut self) -> Result<String>;

    /// Read a string with trailing bytes from the archive
    fn read_string_trailing(&mut self) -> Result<(String, Vec<u8>)>;

    /// Read an object reference from the archive
    fn read_object_ref(&mut self) -> Result<String>;

    /// Returns true if diagnostic logging is enabled
    fn log(&self) -> bool {
        false
    }
}

pub trait ArchiveWriter: Write + Seek {
    fn version(&self) -> &dyn VersionInfo;

    /// Set version information (typically called after reading/writing the header)
    fn set_version(&mut self, header: Header);

    /// Write a string to the archive
    fn write_string(&mut self, string: &str) -> Result<()>;

    /// Write a string with trailing bytes to the archive
    fn write_string_trailing(&mut self, string: &str, trailing: Option<&[u8]>) -> Result<()>;

    /// Write an object reference to the archive
    fn write_object_ref(&mut self, object_ref: &str) -> Result<()>;

    /// Returns true if diagnostic logging is enabled
    fn log(&self) -> bool {
        false
    }
}

impl<R> ArchiveReader for SaveGameArchive<R>
where
    R: Read + Seek,
{
    fn version(&self) -> &dyn VersionInfo {
        SaveGameArchive::version(self)
    }

    fn with_scope<F, T>(&mut self, name: &str, f: F) -> T
    where
        F: FnOnce(&mut Self) -> T,
    {
        SaveGameArchive::with_scope(self, name, f)
    }

    fn get_type_or(&mut self, default: &StructType) -> Result<StructType> {
        SaveGameArchive::get_type_or(self, default)
    }

    fn read_string(&mut self) -> Result<String> {
        crate::read_string(self)
    }

    fn read_string_trailing(&mut self) -> Result<(String, Vec<u8>)> {
        crate::read_string_trailing(self)
    }

    fn read_object_ref(&mut self) -> Result<String> {
        crate::read_string(self)
    }

    fn log(&self) -> bool {
        SaveGameArchive::log(self)
    }
}
impl<W> ArchiveWriter for SaveGameArchive<W>
where
    W: Write + Seek,
{
    fn version(&self) -> &dyn VersionInfo {
        SaveGameArchive::version(self)
    }

    fn set_version(&mut self, header: Header) {
        SaveGameArchive::set_version(self, header)
    }

    fn write_string(&mut self, string: &str) -> Result<()> {
        crate::write_string(self, string)
    }

    fn write_string_trailing(&mut self, string: &str, trailing: Option<&[u8]>) -> Result<()> {
        crate::write_string_trailing(self, string, trailing)
    }

    fn write_object_ref(&mut self, object_ref: &str) -> Result<()> {
        crate::write_string(self, object_ref)
    }

    fn log(&self) -> bool {
        SaveGameArchive::log(self)
    }
}
