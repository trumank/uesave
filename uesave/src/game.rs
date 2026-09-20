use std::convert::Infallible;
use std::io::{Read, Write};
use std::marker::PhantomData;

use serde::{Deserialize, Deserializer, Serialize};

use crate::archive::{ArchiveReader, ArchiveType, ArchiveWriter, SaveGameArchiveType};
use crate::{Error, Property, PropertyKey, PropertyTagPartial, Result, SaveGameArchive};

/// A game struct value: read/write against an archive plus native serde.
///
/// Note: only `Serialize` is required (game structs serialize as part of the
/// untagged [`crate::StructValue`] enum). Deserialization is routed through
/// [`Game::deserialize_struct`] by name, which some game structs implement with
/// schema-threading seeds rather than a standalone `Deserialize` impl, so no
/// `Deserialize` supertrait bound is imposed here.
pub trait GameStruct<T: ArchiveType>: Clone + PartialEq + std::fmt::Debug + Serialize {
    fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()>;
}

/// The single extension point: one impl per game. Every method has a default,
/// so a game implements only what it needs.
pub trait Game: Clone + PartialEq + std::fmt::Debug + Default + Serialize + 'static {
    type Struct<T: ArchiveType>: GameStruct<T>;

    /// Does this fully-qualified type path name a game struct parsed from bytes?
    fn is_game_struct_type(_full_path: &str) -> bool {
        false
    }

    /// Read a game struct from the archive given its resolved type name.
    ///
    /// Generic over the archive reader (rather than a concrete
    /// [`SaveGameArchive`]) so it can be dispatched from the generic
    /// `StructValue::read`, where only an [`ArchiveReader`] is in hand.
    fn read_struct<A: ArchiveReader>(
        ar: &mut A,
        name: &str,
    ) -> Result<Self::Struct<A::ArchiveType>>;

    /// Deserialize a game struct from serde given its type name.
    ///
    /// `path` is the property path of the struct being deserialized and
    /// `schemas` the full schema table; game structs that embed nested
    /// [`crate::Properties`] use them (via [`ArchiveType::deserialize_properties`])
    /// to interpret those properties, whose tags were recorded at `{path}.{field}`.
    fn deserialize_struct<'de, D, T: ArchiveType>(
        name: &str,
        path: &str,
        schemas: &crate::PropertySchemas,
        d: D,
    ) -> std::result::Result<Self::Struct<T>, D::Error>
    where
        D: Deserializer<'de>;

    /// Build a game struct from a GVAS-JSON-dialect decoded `RawData` object,
    /// keyed by the property path that produced it.
    ///
    /// Default: `Ok(None)`, meaning this path carries no game struct and the
    /// caller should keep the payload as opaque bytes.
    #[cfg(feature = "gvas-json")]
    fn game_struct_from_gvas_json(
        _path: &str,
        _value: &serde_json::Value,
    ) -> Result<Option<Self::Struct<SaveGameArchiveType<Self>>>> {
        Ok(None)
    }

    /// Re-encode a GVAS-JSON-dialect decoded `RawData` object back to bytes, for
    /// a path that stays opaque bytes rather than becoming a game struct but that
    /// the dialect may still ship decoded.
    ///
    /// Default: `Ok(None)`, meaning this path carries no such byte payload and
    /// the caller should treat the decoded value as an error.
    #[cfg(feature = "gvas-json")]
    fn game_bytes_from_gvas_json(_path: &str, _value: &serde_json::Value) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    /// Post-process a freshly read property (embedded-bytes → typed value).
    fn process_property_for_read<R: Read + std::io::Seek>(
        _ar: &mut SaveGameArchive<R, Self>,
        _tag: &mut PropertyTagPartial,
        value: Property<SaveGameArchiveType<Self>>,
    ) -> Result<Property<SaveGameArchiveType<Self>>> {
        Ok(value)
    }

    /// Pre-process a property about to be written (typed value → embedded bytes).
    #[allow(clippy::type_complexity)]
    fn process_property_for_write<W: Write + std::io::Seek>(
        _ar: &mut SaveGameArchive<W, Self>,
        _key: &PropertyKey,
        _tag: &PropertyTagPartial,
        _prop: &Property<SaveGameArchiveType<Self>>,
    ) -> Result<Option<(PropertyTagPartial, Property<SaveGameArchiveType<Self>>)>> {
        Ok(None)
    }

    /// Decode a wrapped/compressed save into plain GVAS bytes. Default: passthrough.
    fn decompress_save<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        Ok(buf)
    }

    /// Encode plain GVAS bytes into the game's container format. Default: identity.
    fn compress_save(data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }
}

/// Uninhabited game-struct type used by `NoGame`; can never be constructed.
pub enum Never<T: ArchiveType> {
    #[doc(hidden)]
    _Never(Infallible, PhantomData<T>),
}
impl<T: ArchiveType> Clone for Never<T> {
    fn clone(&self) -> Self {
        match *self {
            Never::_Never(inf, _) => match inf {},
        }
    }
}
impl<T: ArchiveType> PartialEq for Never<T> {
    fn eq(&self, _: &Self) -> bool {
        match *self {
            Never::_Never(inf, _) => match inf {},
        }
    }
}
impl<T: ArchiveType> std::fmt::Debug for Never<T> {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Never::_Never(inf, _) => match inf {},
        }
    }
}
impl<T: ArchiveType> Serialize for Never<T> {
    fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
        match *self {
            Never::_Never(inf, _) => match inf {},
        }
    }
}
impl<'de, T: ArchiveType> Deserialize<'de> for Never<T> {
    fn deserialize<D: Deserializer<'de>>(_: D) -> std::result::Result<Self, D::Error> {
        Err(serde::de::Error::custom("NoGame has no game struct types"))
    }
}
impl<T: ArchiveType> GameStruct<T> for Never<T> {
    fn write<A: ArchiveWriter<ArchiveType = T>>(&self, _: &mut A) -> Result<()> {
        match *self {
            Never::_Never(inf, _) => match inf {},
        }
    }
}

/// Core's default game — zero behavior for plain GVAS saves.
#[derive(Default, Clone, PartialEq, Debug, Serialize)]
pub struct NoGame;
impl Game for NoGame {
    type Struct<T: ArchiveType> = Never<T>;
    fn read_struct<A: ArchiveReader>(
        _ar: &mut A,
        name: &str,
    ) -> Result<Self::Struct<A::ArchiveType>> {
        Err(Error::Other(format!(
            "no game loaded for struct type {name:?}"
        )))
    }
    fn deserialize_struct<'de, D, T: ArchiveType>(
        name: &str,
        _path: &str,
        _schemas: &crate::PropertySchemas,
        _d: D,
    ) -> std::result::Result<Self::Struct<T>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Err(serde::de::Error::custom(format!(
            "no game loaded for struct type {name:?}"
        )))
    }
}
