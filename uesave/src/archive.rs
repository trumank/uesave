use std::io::{Read, Seek, Write};

use crate::game::Game;
use crate::{
    Header, Property, PropertyKey, PropertyTagPartial, Result, SaveGameArchive, StructType,
    VersionInfo,
};

/// Defines the type system for an archive format.
///
/// This trait allows different archive types (save games vs assets) to use
/// different representations for object references and other type-specific data.
pub trait ArchiveType: Clone + PartialEq + std::fmt::Debug + Default + serde::Serialize {
    /// The type used to represent object references in this archive format.
    /// - For save games: `String` (object path as string)
    /// - For assets: `FPackageIndex` (index into import/export tables)
    type ObjectRef: Clone
        + PartialEq
        + std::fmt::Debug
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>;

    /// Returns true if the given object reference is the null reference.
    fn is_null_object_ref(object_ref: &Self::ObjectRef) -> bool;

    /// The type used to represent soft object paths in this archive format.
    /// - For save games: `SoftObjectPath` enum with asset paths
    /// - For assets: Could be different representation
    type SoftObjectPath: Clone
        + PartialEq
        + std::fmt::Debug
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>;

    /// The game this archive type is bound to. Determines which game-specific
    /// struct types and read/write hooks are used.
    type Game: crate::game::Game;

    /// Deserialize a schema-aware [`crate::Properties`] map for this archive
    /// type. Game structs that embed nested properties (e.g. Palworld's
    /// `PalCharacterData`) route their `Properties` field through here so the
    /// property tags recorded at `{path}.{field}` in `schemas` are used to
    /// interpret the untyped JSON. Dispatched on `Self` so the concrete
    /// [`crate::Game`] behind `Self::Game` is known.
    fn deserialize_properties<'de, D>(
        path: &str,
        schemas: &crate::PropertySchemas,
        deserializer: D,
    ) -> std::result::Result<crate::Properties<Self>, D::Error>
    where
        D: serde::Deserializer<'de>,
        Self: Sized;
}

pub trait ArchiveReader: Read + Seek {
    type ArchiveType: ArchiveType;

    fn version(&self) -> &dyn VersionInfo;

    /// Get a mutable reference to the scope for tracking the current property path.
    /// Use `scope().push(name)` and `scope().pop()` to manage the scope stack.
    fn scope(&mut self) -> &mut crate::Scope;

    /// Look up a type hint for the current scope, returning the provided default if not found.
    /// Used to disambiguate struct types in Sets and Maps.
    fn get_type_or(&mut self, default: &StructType) -> Result<StructType>;

    /// Read a string from the archive
    fn read_string(&mut self) -> Result<String>;

    /// Read a string with trailing bytes from the archive
    fn read_string_trailing(&mut self) -> Result<(String, Vec<u8>)>;

    /// Read an object reference from the archive
    fn read_object_ref(&mut self) -> Result<<Self::ArchiveType as ArchiveType>::ObjectRef>;

    /// Read a soft object path from the archive
    fn read_soft_object_path(
        &mut self,
    ) -> Result<<Self::ArchiveType as ArchiveType>::SoftObjectPath>;

    /// Record a property schema at the given path
    fn record_schema(&mut self, path: String, tag: PropertyTagPartial);

    /// Get the current property path in the scope hierarchy
    fn path(&self) -> String;

    /// Returns true if diagnostic logging is enabled
    fn log(&self) -> bool {
        false
    }

    /// Returns true if errors during property parsing should produce Raw properties instead of failing.
    /// When false, parsing errors will immediately return an error.
    fn error_to_raw(&self) -> bool {
        false
    }

    /// Post-process a freshly read property, called with the property name pushed on the scope.
    /// Allows archive implementations to convert game-specific embedded data (e.g. Palworld
    /// RawData byte arrays) into typed values. Implementations may update `tag` to reflect the
    /// converted type; the updated tag is what gets recorded in the schemas.
    fn post_process_property(
        &mut self,
        tag: &mut PropertyTagPartial,
        value: Property<Self::ArchiveType>,
    ) -> Result<Property<Self::ArchiveType>> {
        let _ = tag;
        Ok(value)
    }
}

pub trait ArchiveWriter: Write + Seek {
    type ArchiveType: ArchiveType;

    fn version(&self) -> &dyn VersionInfo;

    /// Set version information (typically called after reading/writing the header)
    fn set_version(&mut self, header: Header);

    /// Get a mutable reference to the scope for tracking the current property path.
    /// Use `scope().push(name)` and `scope().pop()` to manage the scope stack.
    fn scope(&mut self) -> &mut crate::Scope;

    /// Write a string to the archive
    fn write_string(&mut self, string: &str) -> Result<()>;

    /// Write a string with trailing bytes to the archive
    fn write_string_trailing(&mut self, string: &str, trailing: Option<&[u8]>) -> Result<()>;

    /// Write an object reference to the archive
    fn write_object_ref(
        &mut self,
        object_ref: &<Self::ArchiveType as ArchiveType>::ObjectRef,
    ) -> Result<()>;

    /// Write a soft object path to the archive
    fn write_soft_object_path(
        &mut self,
        soft_object_path: &<Self::ArchiveType as ArchiveType>::SoftObjectPath,
    ) -> Result<()>;

    /// Get a property schema at the given path
    fn get_schema(&self, path: &str) -> Option<PropertyTagPartial>;

    /// Get the current property path in the scope hierarchy
    fn path(&self) -> String;

    /// Returns true if diagnostic logging is enabled
    fn log(&self) -> bool {
        false
    }

    /// Pre-process a property about to be written, called with the property name pushed on the
    /// scope. Allows archive implementations to convert typed game-specific values (e.g. Palworld
    /// structs) back into their embedded representation (byte arrays). Returning `Some` replaces
    /// both the tag and the property value used for writing.
    #[allow(clippy::type_complexity)]
    fn pre_write_property(
        &mut self,
        key: &PropertyKey,
        tag: &PropertyTagPartial,
        prop: &Property<Self::ArchiveType>,
    ) -> Result<Option<(PropertyTagPartial, Property<Self::ArchiveType>)>> {
        let _ = (key, tag, prop);
        Ok(None)
    }
}

/// Archive type for save games, which use string-based object references
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize)]
#[serde(bound = "")]
pub struct SaveGameArchiveType<G: crate::game::Game = crate::game::NoGame>(
    std::marker::PhantomData<G>,
);

impl<G: crate::game::Game> ArchiveType for SaveGameArchiveType<G> {
    type ObjectRef = String;
    type SoftObjectPath = crate::SoftObjectPath;
    type Game = G;

    fn is_null_object_ref(object_ref: &Self::ObjectRef) -> bool {
        object_ref.is_empty() || object_ref == "None"
    }

    fn deserialize_properties<'de, D>(
        path: &str,
        schemas: &crate::PropertySchemas,
        deserializer: D,
    ) -> std::result::Result<crate::Properties<Self>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        crate::serialization::deserialize_properties_seed::<D, G>(path, schemas, deserializer)
    }
}

impl<R, G> ArchiveReader for SaveGameArchive<R, G>
where
    R: Read + Seek,
    G: Game,
{
    type ArchiveType = SaveGameArchiveType<G>;

    fn version(&self) -> &dyn VersionInfo {
        SaveGameArchive::version(self)
    }

    fn scope(&mut self) -> &mut crate::Scope {
        &mut self.scope
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

    fn read_soft_object_path(&mut self) -> Result<crate::SoftObjectPath> {
        crate::SoftObjectPath::read(self)
    }

    fn record_schema(&mut self, path: String, tag: PropertyTagPartial) {
        self.schemas.borrow_mut().record(path, tag);
    }

    fn path(&self) -> String {
        self.scope.path()
    }

    fn log(&self) -> bool {
        SaveGameArchive::log(self)
    }

    fn error_to_raw(&self) -> bool {
        SaveGameArchive::error_to_raw(self)
    }

    fn post_process_property(
        &mut self,
        tag: &mut PropertyTagPartial,
        value: Property<SaveGameArchiveType<G>>,
    ) -> Result<Property<SaveGameArchiveType<G>>> {
        <<Self::ArchiveType as ArchiveType>::Game>::process_property_for_read(self, tag, value)
    }
}
impl<W, G> ArchiveWriter for SaveGameArchive<W, G>
where
    W: Write + Seek,
    G: Game,
{
    type ArchiveType = SaveGameArchiveType<G>;

    fn version(&self) -> &dyn VersionInfo {
        SaveGameArchive::version(self)
    }

    fn set_version(&mut self, header: Header) {
        SaveGameArchive::set_version(self, header)
    }

    fn scope(&mut self) -> &mut crate::Scope {
        &mut self.scope
    }

    fn write_string(&mut self, string: &str) -> Result<()> {
        crate::write_string(self, string)
    }

    fn write_string_trailing(&mut self, string: &str, trailing: Option<&[u8]>) -> Result<()> {
        crate::write_string_trailing(self, string, trailing)
    }

    fn write_object_ref(&mut self, object_ref: &String) -> Result<()> {
        crate::write_string(self, object_ref)
    }

    fn write_soft_object_path(&mut self, soft_object_path: &crate::SoftObjectPath) -> Result<()> {
        soft_object_path.write(self)
    }

    fn get_schema(&self, path: &str) -> Option<PropertyTagPartial> {
        self.schemas.borrow().get(path).cloned()
    }

    fn path(&self) -> String {
        self.scope.path()
    }

    fn log(&self) -> bool {
        SaveGameArchive::log(self)
    }

    fn pre_write_property(
        &mut self,
        key: &PropertyKey,
        tag: &PropertyTagPartial,
        prop: &Property<SaveGameArchiveType<G>>,
    ) -> Result<Option<(PropertyTagPartial, Property<SaveGameArchiveType<G>>)>> {
        <<Self::ArchiveType as ArchiveType>::Game>::process_property_for_write(self, key, tag, prop)
    }
}
