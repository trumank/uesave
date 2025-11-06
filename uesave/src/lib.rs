/*!
A library for reading and writing Unreal Engine save files (commonly referred to
as GVAS).

It has been tested on an extensive set of object structures and can fully read
and write Deep Rock Galactic save files (and likely a lot more).

There is a small binary utility to quickly convert saves to and from a plain
text JSON format which can be used for manual save editing.

# Example

```
use std::fs::File;

use uesave::{Property, PropertyInner, Save};

let save = Save::read(&mut File::open("drg-save-test.sav")?)?;
match save.root.properties["NumberOfGamesPlayed"] {
    Property { inner: PropertyInner::Int(value), .. } => {
        assert_eq!(2173, value);
    }
    _ => {}
}
# Ok::<(), Box<dyn std::error::Error>>(())

```
*/

mod archive;
mod context;
mod error;

#[cfg(test)]
mod tests;

pub use context::Types;
pub use error::{Error, ParseError};

use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use context::SaveGameArchive;
use std::{
    borrow::Cow,
    io::{Cursor, Read, Seek, Write},
    rc::Rc,
};

use serde::{de::Visitor, Deserialize, Deserializer, Serialize, Serializer};

use tracing::instrument;

use crate::{
    archive::{ArchiveReader, ArchiveWriter},
    context::Scope,
};

type Result<T, E = Error> = std::result::Result<T, E>;

struct SeekReader<R: Read> {
    inner: R,
    buffer: Vec<u8>,
    position: usize,
    reached_eof: bool,
}

impl<R: Read> SeekReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            buffer: vec![],
            position: 0,
            reached_eof: false,
        }
    }
    fn position(&self) -> usize {
        self.position
    }
    fn ensure_buffered(&mut self, min_bytes: usize) -> std::io::Result<()> {
        if self.reached_eof {
            return Ok(());
        }

        let available = self.buffer.len().saturating_sub(self.position);
        if available >= min_bytes {
            return Ok(());
        }

        let needed = min_bytes - available;

        // Reserve space for the additional bytes we need to read
        self.buffer.reserve(needed);

        // Read more data from the underlying reader
        let mut temp_buf = vec![0; needed];
        let mut total_read = 0;

        while total_read < needed && !self.reached_eof {
            let bytes_read = self.inner.read(&mut temp_buf[total_read..])?;
            if bytes_read == 0 {
                self.reached_eof = true;
                break;
            }
            total_read += bytes_read;
        }

        // Append the read data to our buffer
        self.buffer.extend_from_slice(&temp_buf[..total_read]);

        Ok(())
    }
}
impl<R: Read> Seek for SeekReader<R> {
    // fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
    //     match pos {
    //         std::io::SeekFrom::Current(0) => Ok(self.read_bytes as u64),
    //         _ => unimplemented!(),
    //     }
    // }
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let new_position = match pos {
            std::io::SeekFrom::Start(offset) => offset as i64,
            std::io::SeekFrom::Current(offset) => self.position as i64 + offset,
            std::io::SeekFrom::End(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "Seeking from end is not supported for non-seekable readers",
                ));
            }
        };

        if new_position < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot seek to a negative position",
            ));
        }

        let new_position = new_position as usize;

        // If seeking within already buffered data, just update position
        if new_position <= self.buffer.len() {
            self.position = new_position;
            return Ok(new_position as u64);
        }

        // If seeking beyond buffered data, we need to read more
        let bytes_needed = new_position - self.buffer.len();
        self.position = self.buffer.len();

        // Read and buffer bytes until we reach the target position
        let mut temp_buf = vec![0; bytes_needed.min(8192)];
        let mut remaining = bytes_needed;

        while remaining > 0 {
            let to_read = remaining.min(temp_buf.len());
            let bytes_read = self.read(&mut temp_buf[..to_read])?;
            if bytes_read == 0 {
                // Hit EOF before reaching target position
                return Ok(self.position as u64);
            }
            remaining -= bytes_read;
        }

        Ok(new_position as u64)
    }
}
impl<R: Read> Read for SeekReader<R> {
    // fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    //     self.reader.read(buf).inspect(|s| self.read_bytes += s)
    // }
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        // Ensure we have data available
        self.ensure_buffered(1)?;

        // Copy data from our buffer to the output buffer
        let available = self.buffer.len() - self.position;
        if available == 0 {
            return Ok(0); // EOF
        }

        let to_copy = buf.len().min(available);
        buf[..to_copy].copy_from_slice(&self.buffer[self.position..self.position + to_copy]);
        self.position += to_copy;

        Ok(to_copy)
    }
}

#[instrument(skip_all)]
fn read_optional_uuid<A: ArchiveReader>(ar: &mut A) -> Result<Option<FGuid>> {
    Ok(if ar.read_u8()? > 0 {
        Some(FGuid::read(ar)?)
    } else {
        None
    })
}
fn write_optional_uuid<A: ArchiveWriter>(ar: &mut A, id: Option<FGuid>) -> Result<()> {
    if let Some(id) = id {
        ar.write_u8(1)?;
        id.write(ar)?;
    } else {
        ar.write_u8(0)?;
    }
    Ok(())
}

#[instrument(skip_all, ret)]
fn read_string<A: ArchiveReader>(ar: &mut A) -> Result<String> {
    let len = ar.read_i32::<LE>()?;
    if len < 0 {
        let chars = read_array((-len) as u32, ar, |r| Ok(r.read_u16::<LE>()?))?;
        let length = chars.iter().position(|&c| c == 0).unwrap_or(chars.len());
        Ok(String::from_utf16(&chars[..length]).unwrap())
    } else {
        let mut chars = vec![0; len as usize];
        ar.read_exact(&mut chars)?;
        let length = chars.iter().position(|&c| c == 0).unwrap_or(chars.len());
        Ok(String::from_utf8_lossy(&chars[..length]).into_owned())
    }
}
#[instrument(skip(ar))]
fn write_string<A: ArchiveWriter>(ar: &mut A, string: &str) -> Result<()> {
    if string.is_empty() {
        ar.write_u32::<LE>(0)?;
    } else {
        write_string_trailing(ar, string, None)?;
    }
    Ok(())
}

#[instrument(skip_all)]
fn read_string_trailing<A: ArchiveReader>(ar: &mut A) -> Result<(String, Vec<u8>)> {
    let len = ar.read_i32::<LE>()?;
    if len < 0 {
        let bytes = (-len) as usize * 2;
        let mut chars = vec![];
        let mut rest = vec![];
        let mut read = 0;
        while read < bytes {
            let next = ar.read_u16::<LE>()?;
            read += 2;
            if next == 0 {
                rest.extend(next.to_le_bytes());
                break;
            } else {
                chars.push(next);
            }
        }
        while read < bytes {
            rest.push(ar.read_u8()?);
            read += 1;
        }
        Ok((String::from_utf16(&chars).unwrap(), rest))
    } else {
        let bytes = len as usize;
        let mut chars = vec![];
        let mut rest = vec![];
        let mut read = 0;
        while read < bytes {
            let next = ar.read_u8()?;
            read += 1;
            if next == 0 {
                rest.push(next);
                break;
            } else {
                chars.push(next);
            }
        }
        while read < bytes {
            rest.push(ar.read_u8()?);
            read += 1;
        }
        Ok((String::from_utf8(chars).unwrap(), rest))
    }
}
#[instrument(skip_all)]
fn write_string_trailing<A: ArchiveWriter>(
    ar: &mut A,
    string: &str,
    trailing: Option<&[u8]>,
) -> Result<()> {
    if string.is_empty() || string.is_ascii() {
        ar.write_u32::<LE>((string.len() + trailing.map(|t| t.len()).unwrap_or(1)) as u32)?;
        ar.write_all(string.as_bytes())?;
        ar.write_all(trailing.unwrap_or(&[0]))?;
    } else {
        let chars: Vec<u16> = string.encode_utf16().collect();
        ar.write_i32::<LE>(-((chars.len() + trailing.map(|t| t.len()).unwrap_or(2) / 2) as i32))?;
        for c in chars {
            ar.write_u16::<LE>(c)?;
        }
        ar.write_all(trailing.unwrap_or(&[0, 0]))?;
    }
    Ok(())
}

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PropertyKey(pub u32, pub String);
impl From<String> for PropertyKey {
    fn from(value: String) -> Self {
        Self(0, value)
    }
}
impl From<&str> for PropertyKey {
    fn from(value: &str) -> Self {
        Self(0, value.to_string())
    }
}

struct PropertyKeyVisitor;
impl Visitor<'_> for PropertyKeyVisitor {
    type Value = PropertyKey;
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str(
            "a property key in the form of key name and index seperated by '_' e.g. property_2",
        )
    }
    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let (name_str, index_str) = value
            .rsplit_once('_')
            .ok_or_else(|| serde::de::Error::custom("property key does not contain a '_'"))?;
        let index: u32 = index_str.parse().map_err(serde::de::Error::custom)?;

        Ok(PropertyKey(index, name_str.to_string()))
    }
}
impl<'de> Deserialize<'de> for PropertyKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(PropertyKeyVisitor)
    }
}
impl Serialize for PropertyKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}_{}", self.1, self.0))
    }
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Properties(pub indexmap::IndexMap<PropertyKey, Property>);
impl Properties {
    fn insert(&mut self, k: impl Into<PropertyKey>, v: Property) -> Option<Property> {
        self.0.insert(k.into(), v)
    }
}
impl<K> std::ops::Index<K> for Properties
where
    K: Into<PropertyKey>,
{
    type Output = Property;
    fn index(&self, index: K) -> &Self::Output {
        self.0.index(&index.into())
    }
}
impl<K> std::ops::IndexMut<K> for Properties
where
    K: Into<PropertyKey>,
{
    fn index_mut(&mut self, index: K) -> &mut Property {
        self.0.index_mut(&index.into())
    }
}
impl<'a> IntoIterator for &'a Properties {
    type Item = <&'a indexmap::IndexMap<PropertyKey, Property> as IntoIterator>::Item;
    type IntoIter = <&'a indexmap::IndexMap<PropertyKey, Property> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[instrument(skip_all)]
fn read_properties_until_none<A: ArchiveReader>(ar: &mut A) -> Result<Properties> {
    let mut properties = Properties::default();
    while let Some((name, prop)) = read_property(ar)? {
        properties.insert(name, prop);
    }
    Ok(properties)
}
#[instrument(skip_all)]
fn write_properties_none_terminated<A: ArchiveWriter>(
    ar: &mut A,
    properties: &Properties,
) -> Result<()> {
    for p in properties {
        write_property(p, ar)?;
    }
    ar.write_string("None")?;
    Ok(())
}

#[instrument(skip_all)]
fn read_property<A: ArchiveReader>(ar: &mut A) -> Result<Option<(PropertyKey, Property)>> {
    if let Some(tag) = PropertyTagFull::read(ar)? {
        let value = ar.with_scope(&tag.name, |ar| Property::read(ar, tag.clone()))?;
        Ok(Some((PropertyKey(tag.index, tag.name.to_string()), value)))
    } else {
        Ok(None)
    }
}
#[instrument(skip_all)]
fn write_property<A: ArchiveWriter>(prop: (&PropertyKey, &Property), ar: &mut A) -> Result<()> {
    let mut tag = prop
        .1
        .tag
        .clone()
        .into_full(&prop.0 .1, 0, prop.0 .0, prop.1);

    // Write tag with placeholder size
    tag.size = 0;
    let tag_start = ar.stream_position()?;
    tag.write(ar)?;
    let data_start = ar.stream_position()?;

    // Write the actual property data
    prop.1.write(ar, &tag)?;
    let data_end = ar.stream_position()?;

    // Calculate actual size
    let size = (data_end - data_start) as u32;
    tag.size = size;

    // Seek back and rewrite the tag with correct size
    ar.seek(std::io::SeekFrom::Start(tag_start))?;
    tag.write(ar)?;

    // Seek to end to continue writing
    ar.seek(std::io::SeekFrom::Start(data_end))?;
    Ok(())
}

#[instrument(skip_all)]
fn read_array<T, F, A: ArchiveReader>(length: u32, ar: &mut A, f: F) -> Result<Vec<T>>
where
    F: Fn(&mut A) -> Result<T>,
{
    (0..length).map(|_| f(ar)).collect()
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FGuid {
    a: u32,
    b: u32,
    c: u32,
    d: u32,
}

impl FGuid {
    pub fn new(a: u32, b: u32, c: u32, d: u32) -> Self {
        Self { a, b, c, d }
    }

    pub fn nil() -> Self {
        Self::default()
    }

    pub fn is_nil(&self) -> bool {
        self.a == 0 && self.b == 0 && self.c == 0 && self.d == 0
    }

    pub fn parse_str(s: &str) -> Result<Self, Error> {
        let s = s.replace("-", "");
        if s.len() != 32 {
            return Err(Error::Other("Invalid GUID string length".into()));
        }

        let parse_hex_u32 = |start: usize| -> Result<u32, Error> {
            u32::from_str_radix(&s[start..start + 8], 16)
                .map_err(|_| Error::Other("Invalid hex in GUID".into()))
        };

        Ok(Self {
            a: parse_hex_u32(0)?,
            b: parse_hex_u32(8)?,
            c: parse_hex_u32(16)?,
            d: parse_hex_u32(24)?,
        })
    }
}

impl std::fmt::Display for FGuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let b = self.b.to_le_bytes();
        let c = self.c.to_le_bytes();

        write!(
            f,
            "{:08x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:08x}",
            self.a, b[3], b[2], b[1], b[0], c[3], c[2], c[1], c[0], self.d,
        )
    }
}

impl Serialize for FGuid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for FGuid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FGuidVisitor;

        impl<'de> Visitor<'de> for FGuidVisitor {
            type Value = FGuid;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a UUID string in format xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                FGuid::parse_str(value).map_err(|e| E::custom(format!("Invalid UUID: {}", e)))
            }
        }

        deserializer.deserialize_str(FGuidVisitor)
    }
}

impl FGuid {
    #[instrument(name = "FGuid_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<FGuid> {
        Ok(Self {
            a: ar.read_u32::<LE>()?,
            b: ar.read_u32::<LE>()?,
            c: ar.read_u32::<LE>()?,
            d: ar.read_u32::<LE>()?,
        })
    }
}
impl FGuid {
    #[instrument(name = "FGuid_write", skip_all)]
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.a)?;
        ar.write_u32::<LE>(self.b)?;
        ar.write_u32::<LE>(self.c)?;
        ar.write_u32::<LE>(self.d)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct PropertyTagFull<'a> {
    name: Cow<'a, str>,
    id: Option<FGuid>,
    size: u32,
    index: u32,
    data: PropertyTagDataFull,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum PropertyTagDataFull {
    Array(std::boxed::Box<PropertyTagDataFull>),
    Struct {
        struct_type: StructType,
        id: FGuid,
    },
    Set {
        key_type: std::boxed::Box<PropertyTagDataFull>,
    },
    Map {
        key_type: std::boxed::Box<PropertyTagDataFull>,
        value_type: std::boxed::Box<PropertyTagDataFull>,
    },
    Byte(Option<String>),
    Enum(String, Option<String>),
    Bool(bool),
    Other(PropertyType),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyTagPartial {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<FGuid>,
    pub data: PropertyTagDataPartial,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PropertyTagDataPartial {
    Array(std::boxed::Box<PropertyTagDataPartial>),
    Struct {
        struct_type: StructType,
        id: FGuid,
    },
    Set {
        key_type: std::boxed::Box<PropertyTagDataPartial>,
    },
    Map {
        key_type: std::boxed::Box<PropertyTagDataPartial>,
        value_type: std::boxed::Box<PropertyTagDataPartial>,
    },
    Byte(Option<String>),
    Enum(String, Option<String>),
    Other(PropertyType),
}
impl PropertyTagDataFull {
    fn into_partial(self) -> PropertyTagDataPartial {
        match self {
            Self::Array(inner) => PropertyTagDataPartial::Array(inner.into_partial().into()),
            Self::Struct { struct_type, id } => PropertyTagDataPartial::Struct { struct_type, id },
            Self::Set { key_type } => PropertyTagDataPartial::Set {
                key_type: key_type.into_partial().into(),
            },
            Self::Map {
                key_type,
                value_type,
            } => PropertyTagDataPartial::Map {
                key_type: key_type.into_partial().into(),
                value_type: value_type.into_partial().into(),
            },
            Self::Byte(a) => PropertyTagDataPartial::Byte(a),
            Self::Enum(a, b) => PropertyTagDataPartial::Enum(a, b),
            Self::Bool(_) => PropertyTagDataPartial::Other(PropertyType::BoolProperty),
            Self::Other(t) => PropertyTagDataPartial::Other(t),
        }
    }
}
impl PropertyTagDataPartial {
    fn into_full(self, prop: &Property) -> PropertyTagDataFull {
        match self {
            Self::Array(inner) => PropertyTagDataFull::Array(inner.into_full(prop).into()),
            Self::Struct { struct_type, id } => PropertyTagDataFull::Struct { struct_type, id },
            Self::Set { key_type } => PropertyTagDataFull::Set {
                key_type: key_type.into_full(prop).into(),
            },
            Self::Map {
                key_type,
                value_type,
            } => PropertyTagDataFull::Map {
                key_type: key_type.into_full(prop).into(),
                value_type: value_type.into_full(prop).into(),
            },
            Self::Byte(a) => PropertyTagDataFull::Byte(a),
            Self::Enum(a, b) => PropertyTagDataFull::Enum(a, b),
            Self::Other(PropertyType::BoolProperty) => {
                PropertyTagDataFull::Bool(match prop.inner {
                    PropertyInner::Bool(value) => value,
                    _ => false,
                })
            }
            Self::Other(t) => PropertyTagDataFull::Other(t),
        }
    }
}

impl PropertyTagDataFull {
    fn basic_type(&self) -> PropertyType {
        match self {
            Self::Array(_) => PropertyType::ArrayProperty,
            Self::Struct { .. } => PropertyType::StructProperty,
            Self::Set { .. } => PropertyType::SetProperty,
            Self::Map { .. } => PropertyType::MapProperty,
            Self::Byte(_) => PropertyType::ByteProperty,
            Self::Enum(_, _) => PropertyType::EnumProperty,
            Self::Bool(_) => PropertyType::BoolProperty,
            Self::Other(property_type) => *property_type,
        }
    }
    fn has_raw_struct(&self) -> bool {
        match self {
            Self::Array(inner) => inner.has_raw_struct(),
            Self::Struct { struct_type, .. } => struct_type.raw(),
            Self::Set { key_type } => key_type.has_raw_struct(),
            Self::Map {
                key_type,
                value_type,
            } => key_type.has_raw_struct() || value_type.has_raw_struct(),
            Self::Byte(_) => false,
            Self::Enum(_, _) => false,
            Self::Bool(_) => false,
            Self::Other(_) => false,
        }
    }
    fn from_type(inner_type: PropertyType, struct_type: Option<StructType>) -> Self {
        match inner_type {
            PropertyType::BoolProperty => Self::Bool(false),
            PropertyType::ByteProperty => Self::Byte(None),
            PropertyType::EnumProperty => Self::Enum("".to_string(), None),
            PropertyType::ArrayProperty => unreachable!("array of array is invalid"),
            PropertyType::SetProperty => unreachable!("array of set is invalid"),
            PropertyType::MapProperty => unreachable!("array of map is invalid"),
            PropertyType::StructProperty => Self::Struct {
                struct_type: struct_type.unwrap_or(StructType::Struct(None)),
                id: Default::default(),
            },
            other => Self::Other(other),
        }
    }
}
bitflags::bitflags! {
    #[derive(Debug, Clone, Copy)]
    struct EPropertyTagFlags : u8 {
        const None = 0x00;
        const HasArrayIndex = 0x01;
        const HasPropertyGuid = 0x02;
        const HasPropertyExtensions = 0x04;
        const HasBinaryOrNativeSerialize = 0x08;
        const BoolTrue = 0x10;
    }
}
impl PropertyTagPartial {
    fn into_full<'a>(
        self,
        name: &'a str,
        size: u32,
        index: u32,
        prop: &Property,
    ) -> PropertyTagFull<'a> {
        PropertyTagFull {
            name: name.into(),
            id: self.id,
            size,
            index,
            data: self.data.into_full(prop),
        }
    }
}
impl PropertyTagFull<'_> {
    fn into_partial(self) -> PropertyTagPartial {
        PropertyTagPartial {
            id: self.id,
            data: self.data.into_partial(),
        }
    }
    #[instrument(name = "PropertyTag_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Option<Self>> {
        let name = ar.read_string()?;
        if name == "None" {
            return Ok(None);
        }
        if ar.version().property_tag() {
            let root_node = read_node(ar)?;

            #[derive(Default, Debug)]
            struct Node {
                name: String,
                inner: Vec<Node>,
            }
            fn read_node<A: ArchiveReader>(ar: &mut A) -> Result<Node> {
                Ok(Node {
                    name: ar.read_string()?,
                    inner: read_array(ar.read_u32::<LE>()?, ar, read_node)?,
                })
            }
            fn read_path(node: &Node) -> Result<String> {
                let name = node;
                assert_eq!(1, name.inner.len());
                let package = &name.inner[0];
                assert_eq!(0, package.inner.len());
                Ok(format!("{}.{}", package.name, name.name))
            }
            fn read_type(node: &Node, flags: EPropertyTagFlags) -> Result<PropertyTagDataFull> {
                Ok(match node.name.as_str() {
                    "ArrayProperty" => {
                        PropertyTagDataFull::Array(read_type(&node.inner[0], flags)?.into())
                    }
                    "StructProperty" => {
                        let raw = flags.contains(EPropertyTagFlags::HasBinaryOrNativeSerialize);
                        let struct_type = StructType::from_full(&read_path(&node.inner[0])?, raw);
                        let id = match node.inner.len() {
                            1 => Default::default(),
                            2 => FGuid::parse_str(&node.inner[1].name)?,
                            _ => unimplemented!(),
                        };
                        PropertyTagDataFull::Struct { struct_type, id }
                    }
                    "SetProperty" => PropertyTagDataFull::Set {
                        key_type: read_type(&node.inner[0], flags)?.into(),
                    },
                    "MapProperty" => PropertyTagDataFull::Map {
                        key_type: read_type(&node.inner[0], flags)?.into(),
                        value_type: read_type(&node.inner[1], flags)?.into(),
                    },
                    "ByteProperty" => {
                        let inner = match node.inner.len() {
                            0 => None,
                            1 => Some(read_path(&node.inner[0])?),
                            _ => unimplemented!(),
                        };
                        PropertyTagDataFull::Byte(inner)
                    }
                    "EnumProperty" => {
                        assert_eq!(2, node.inner.len());
                        let inner = read_path(&node.inner[0])?;
                        let container = &node.inner[1];
                        assert_eq!(0, container.inner.len());
                        PropertyTagDataFull::Enum(inner, Some(container.name.to_owned()))
                    }
                    "BoolProperty" => {
                        PropertyTagDataFull::Bool(flags.contains(EPropertyTagFlags::BoolTrue))
                    }
                    other => {
                        assert_eq!(0, node.inner.len());
                        PropertyTagDataFull::Other(PropertyType::try_from(other)?)
                    }
                })
            }

            let size = ar.read_u32::<LE>()?;

            let flags = EPropertyTagFlags::from_bits(ar.read_u8()?)
                .ok_or_else(|| error::Error::Other("unknown EPropertyTagFlags bits".into()))?;

            let mut tag = Self {
                name: name.into(),
                size,
                index: 0,
                id: None,
                data: read_type(&root_node, flags)?,
            };

            if flags.contains(EPropertyTagFlags::HasArrayIndex) {
                tag.index = ar.read_u32::<LE>()?;
            }
            if flags.contains(EPropertyTagFlags::HasPropertyGuid) {
                tag.id = Some(FGuid::read(ar)?);
            }
            if flags.contains(EPropertyTagFlags::HasPropertyExtensions) {
                unimplemented!();
            }

            Ok(Some(tag))
        } else {
            ar.with_scope(&name.clone(), |ar| {
                let type_ = PropertyType::read(ar)?;
                let size = ar.read_u32::<LE>()?;
                let index = ar.read_u32::<LE>()?;
                let data = match type_ {
                    PropertyType::BoolProperty => {
                        let value = ar.read_u8()? > 0;
                        PropertyTagDataFull::Bool(value)
                    }
                    PropertyType::IntProperty
                    | PropertyType::Int8Property
                    | PropertyType::Int16Property
                    | PropertyType::Int64Property
                    | PropertyType::UInt8Property
                    | PropertyType::UInt16Property
                    | PropertyType::UInt32Property
                    | PropertyType::UInt64Property
                    | PropertyType::FloatProperty
                    | PropertyType::DoubleProperty
                    | PropertyType::StrProperty
                    | PropertyType::ObjectProperty
                    | PropertyType::FieldPathProperty
                    | PropertyType::SoftObjectProperty
                    | PropertyType::NameProperty
                    | PropertyType::TextProperty
                    | PropertyType::DelegateProperty
                    | PropertyType::MulticastDelegateProperty
                    | PropertyType::MulticastInlineDelegateProperty
                    | PropertyType::MulticastSparseDelegateProperty => {
                        PropertyTagDataFull::Other(type_)
                    }
                    PropertyType::ByteProperty => {
                        let enum_type = ar.read_string()?;
                        PropertyTagDataFull::Byte((enum_type != "None").then_some(enum_type))
                    }
                    PropertyType::EnumProperty => {
                        let enum_type = ar.read_string()?;
                        PropertyTagDataFull::Enum(enum_type, None)
                    }
                    PropertyType::ArrayProperty => {
                        let inner_type = PropertyType::read(ar)?;

                        PropertyTagDataFull::Array(std::boxed::Box::new(
                            PropertyTagDataFull::from_type(inner_type, None),
                        ))
                    }
                    PropertyType::SetProperty => {
                        let key_type = PropertyType::read(ar)?;
                        let key_struct_type = match key_type {
                            PropertyType::StructProperty => {
                                Some(ar.get_type_or(&StructType::Guid)?)
                            }
                            _ => None,
                        };

                        let key_type =
                            PropertyTagDataFull::from_type(key_type, key_struct_type.clone())
                                .into();

                        PropertyTagDataFull::Set { key_type }
                    }
                    PropertyType::MapProperty => {
                        let key_type = PropertyType::read(ar)?;
                        let key_struct_type = match key_type {
                            PropertyType::StructProperty => {
                                Some(ar.with_scope("Key", |r| r.get_type_or(&StructType::Guid))?)
                            }
                            _ => None,
                        };
                        let value_type = PropertyType::read(ar)?;
                        let value_struct_type = match value_type {
                            PropertyType::StructProperty => Some(ar.with_scope("Value", |r| {
                                r.get_type_or(&StructType::Struct(None))
                            })?),
                            _ => None,
                        };

                        let key_type =
                            PropertyTagDataFull::from_type(key_type, key_struct_type.clone())
                                .into();
                        let value_type =
                            PropertyTagDataFull::from_type(value_type, value_struct_type.clone())
                                .into();

                        PropertyTagDataFull::Map {
                            key_type,
                            value_type,
                        }
                    }
                    PropertyType::StructProperty => {
                        let struct_type = StructType::read(ar)?;
                        let struct_id = FGuid::read(ar)?;
                        PropertyTagDataFull::Struct {
                            struct_type,
                            id: struct_id,
                        }
                    }
                };
                let id = if ar.version().property_guid() {
                    read_optional_uuid(ar)?
                } else {
                    None
                };
                Ok(Some(Self {
                    name: name.into(),
                    size,
                    index,
                    id,
                    data,
                }))
            })
        }
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_string(&self.name)?;

        if ar.version().property_tag() {
            fn write_node<A: ArchiveWriter>(
                ar: &mut A,
                name: &str,
                inner_count: u32,
            ) -> Result<()> {
                ar.write_string(name)?;
                ar.write_u32::<LE>(inner_count)?;
                Ok(())
            }
            fn write_full_type<A: ArchiveWriter>(ar: &mut A, full_type: &str) -> Result<()> {
                let (a, b) = full_type.split_once('.').unwrap(); // TODO
                write_node(ar, b, 1)?;
                write_node(ar, a, 0)?;
                Ok(())
            }
            fn write_nodes<A: ArchiveWriter>(
                ar: &mut A,
                flags: &mut EPropertyTagFlags,
                data: &PropertyTagDataFull,
            ) -> Result<()> {
                match data {
                    PropertyTagDataFull::Array(inner) => {
                        write_node(ar, "ArrayProperty", 1)?;
                        write_nodes(ar, flags, inner)?;
                    }
                    PropertyTagDataFull::Struct { struct_type, id } => {
                        write_node(ar, "StructProperty", if id.is_nil() { 1 } else { 2 })?;
                        match struct_type {
                            StructType::Struct(Some(_)) => {}
                            _ => *flags |= EPropertyTagFlags::HasBinaryOrNativeSerialize,
                        }
                        write_full_type(ar, struct_type.full_str())?;

                        if !id.is_nil() {
                            write_node(ar, &id.to_string(), 0)?;
                        }
                    }
                    PropertyTagDataFull::Set { key_type } => {
                        write_node(ar, "SetProperty", 1)?;
                        write_nodes(ar, flags, key_type)?;
                    }
                    PropertyTagDataFull::Map {
                        key_type,
                        value_type,
                    } => {
                        write_node(ar, "MapProperty", 2)?;
                        write_nodes(ar, flags, key_type)?;
                        write_nodes(ar, flags, value_type)?;
                    }
                    PropertyTagDataFull::Byte(enum_type) => {
                        write_node(ar, "ByteProperty", if enum_type.is_some() { 1 } else { 0 })?;
                        if let Some(enum_type) = enum_type {
                            write_full_type(ar, enum_type)?;
                        }
                    }
                    PropertyTagDataFull::Enum(enum_type, container) => {
                        write_node(ar, "EnumProperty", 2)?;
                        write_full_type(ar, enum_type)?;
                        write_node(ar, container.as_ref().unwrap(), 0)?;
                    }
                    PropertyTagDataFull::Bool(value) => {
                        if *value {
                            *flags |= EPropertyTagFlags::BoolTrue;
                        }
                        write_node(ar, "BoolProperty", 0)?;
                    }
                    PropertyTagDataFull::Other(property_type) => {
                        write_node(ar, property_type.get_name(), 0)?;
                    }
                }
                Ok(())
            }

            let mut flags = EPropertyTagFlags::empty();
            write_nodes(ar, &mut flags, &self.data)?;

            ar.write_u32::<LE>(self.size)?;

            if self.id.is_some() {
                flags |= EPropertyTagFlags::HasPropertyGuid;
            }

            ar.write_u8(flags.bits())?;
        } else {
            self.data.basic_type().write(ar)?;
            ar.write_u32::<LE>(self.size)?;
            ar.write_u32::<LE>(self.index)?;
            match &self.data {
                PropertyTagDataFull::Array(inner_type) => {
                    inner_type.basic_type().write(ar)?;
                }
                PropertyTagDataFull::Struct { struct_type, id } => {
                    struct_type.write(ar)?;
                    id.write(ar)?;
                }
                PropertyTagDataFull::Set { key_type, .. } => {
                    key_type.basic_type().write(ar)?;
                }
                PropertyTagDataFull::Map {
                    key_type,
                    value_type,
                    ..
                } => {
                    key_type.basic_type().write(ar)?;
                    value_type.basic_type().write(ar)?;
                }
                PropertyTagDataFull::Byte(enum_type) => {
                    ar.write_string(enum_type.as_deref().unwrap_or("None"))?;
                }
                PropertyTagDataFull::Enum(enum_type, _) => {
                    ar.write_string(enum_type)?;
                }
                PropertyTagDataFull::Bool(value) => {
                    ar.write_u8(*value as u8)?;
                }
                PropertyTagDataFull::Other(_) => {}
            }
            if ar.version().property_guid() {
                write_optional_uuid(ar, self.id)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PropertyType {
    IntProperty,
    Int8Property,
    Int16Property,
    Int64Property,
    UInt8Property,
    UInt16Property,
    UInt32Property,
    UInt64Property,
    FloatProperty,
    DoubleProperty,
    BoolProperty,
    ByteProperty,
    EnumProperty,
    ArrayProperty,
    ObjectProperty,
    StrProperty,
    FieldPathProperty,
    SoftObjectProperty,
    NameProperty,
    TextProperty,
    DelegateProperty,
    MulticastDelegateProperty,
    MulticastInlineDelegateProperty,
    MulticastSparseDelegateProperty,
    SetProperty,
    MapProperty,
    StructProperty,
}
impl PropertyType {
    fn get_name(&self) -> &str {
        match &self {
            PropertyType::Int8Property => "Int8Property",
            PropertyType::Int16Property => "Int16Property",
            PropertyType::IntProperty => "IntProperty",
            PropertyType::Int64Property => "Int64Property",
            PropertyType::UInt8Property => "UInt8Property",
            PropertyType::UInt16Property => "UInt16Property",
            PropertyType::UInt32Property => "UInt32Property",
            PropertyType::UInt64Property => "UInt64Property",
            PropertyType::FloatProperty => "FloatProperty",
            PropertyType::DoubleProperty => "DoubleProperty",
            PropertyType::BoolProperty => "BoolProperty",
            PropertyType::ByteProperty => "ByteProperty",
            PropertyType::EnumProperty => "EnumProperty",
            PropertyType::ArrayProperty => "ArrayProperty",
            PropertyType::ObjectProperty => "ObjectProperty",
            PropertyType::StrProperty => "StrProperty",
            PropertyType::FieldPathProperty => "FieldPathProperty",
            PropertyType::SoftObjectProperty => "SoftObjectProperty",
            PropertyType::NameProperty => "NameProperty",
            PropertyType::TextProperty => "TextProperty",
            PropertyType::DelegateProperty => "DelegateProperty",
            PropertyType::MulticastDelegateProperty => "MulticastDelegateProperty",
            PropertyType::MulticastInlineDelegateProperty => "MulticastInlineDelegateProperty",
            PropertyType::MulticastSparseDelegateProperty => "MulticastSparseDelegateProperty",
            PropertyType::SetProperty => "SetProperty",
            PropertyType::MapProperty => "MapProperty",
            PropertyType::StructProperty => "StructProperty",
        }
    }
    #[instrument(name = "PropertyType_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Self::try_from(&ar.read_string()?)
    }
    fn try_from(name: &str) -> Result<Self> {
        match name {
            "Int8Property" => Ok(PropertyType::Int8Property),
            "Int16Property" => Ok(PropertyType::Int16Property),
            "IntProperty" => Ok(PropertyType::IntProperty),
            "Int64Property" => Ok(PropertyType::Int64Property),
            "UInt8Property" => Ok(PropertyType::UInt8Property),
            "UInt16Property" => Ok(PropertyType::UInt16Property),
            "UInt32Property" => Ok(PropertyType::UInt32Property),
            "UInt64Property" => Ok(PropertyType::UInt64Property),
            "FloatProperty" => Ok(PropertyType::FloatProperty),
            "DoubleProperty" => Ok(PropertyType::DoubleProperty),
            "BoolProperty" => Ok(PropertyType::BoolProperty),
            "ByteProperty" => Ok(PropertyType::ByteProperty),
            "EnumProperty" => Ok(PropertyType::EnumProperty),
            "ArrayProperty" => Ok(PropertyType::ArrayProperty),
            "ObjectProperty" => Ok(PropertyType::ObjectProperty),
            "StrProperty" => Ok(PropertyType::StrProperty),
            "FieldPathProperty" => Ok(PropertyType::FieldPathProperty),
            "SoftObjectProperty" => Ok(PropertyType::SoftObjectProperty),
            "NameProperty" => Ok(PropertyType::NameProperty),
            "TextProperty" => Ok(PropertyType::TextProperty),
            "DelegateProperty" => Ok(PropertyType::DelegateProperty),
            "MulticastDelegateProperty" => Ok(PropertyType::MulticastDelegateProperty),
            "MulticastInlineDelegateProperty" => Ok(PropertyType::MulticastInlineDelegateProperty),
            "MulticastSparseDelegateProperty" => Ok(PropertyType::MulticastSparseDelegateProperty),
            "SetProperty" => Ok(PropertyType::SetProperty),
            "MapProperty" => Ok(PropertyType::MapProperty),
            "StructProperty" => Ok(PropertyType::StructProperty),
            _ => Err(Error::UnknownPropertyType(format!("{name:?}"))),
        }
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_string(self.get_name())?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StructType {
    Guid,
    DateTime,
    Timespan,
    Vector2D,
    Vector,
    IntVector,
    Box,
    IntPoint,
    Quat,
    Rotator,
    LinearColor,
    Color,
    SoftObjectPath,
    GameplayTagContainer,
    UniqueNetIdRepl,
    Raw(String),
    Struct(Option<String>),
}
impl From<&str> for StructType {
    fn from(t: &str) -> Self {
        match t {
            "Guid" => StructType::Guid,
            "DateTime" => StructType::DateTime,
            "Timespan" => StructType::Timespan,
            "Vector2D" => StructType::Vector2D,
            "Vector" => StructType::Vector,
            "IntVector" => StructType::IntVector,
            "Box" => StructType::Box,
            "IntPoint" => StructType::IntPoint,
            "Quat" => StructType::Quat,
            "Rotator" => StructType::Rotator,
            "LinearColor" => StructType::LinearColor,
            "Color" => StructType::Color,
            "SoftObjectPath" => StructType::SoftObjectPath,
            "GameplayTagContainer" => StructType::GameplayTagContainer,
            "UniqueNetIdRepl" => StructType::UniqueNetIdRepl,
            "Struct" => StructType::Struct(None),
            _ => StructType::Struct(Some(t.to_owned())),
        }
    }
}
impl From<String> for StructType {
    fn from(t: String) -> Self {
        match t.as_str() {
            "Guid" => StructType::Guid,
            "DateTime" => StructType::DateTime,
            "Timespan" => StructType::Timespan,
            "Vector2D" => StructType::Vector2D,
            "Vector" => StructType::Vector,
            "IntVector" => StructType::IntVector,
            "Box" => StructType::Box,
            "IntPoint" => StructType::IntPoint,
            "Quat" => StructType::Quat,
            "Rotator" => StructType::Rotator,
            "LinearColor" => StructType::LinearColor,
            "Color" => StructType::Color,
            "SoftObjectPath" => StructType::SoftObjectPath,
            "GameplayTagContainer" => StructType::GameplayTagContainer,
            "UniqueNetIdRepl" => StructType::UniqueNetIdRepl,
            "Struct" => StructType::Struct(None),
            _ => StructType::Struct(Some(t)),
        }
    }
}
impl StructType {
    fn from_full(t: &str, raw: bool) -> Self {
        match t {
            "/Script/CoreUObject.Guid" => StructType::Guid,
            "/Script/CoreUObject.DateTime" => StructType::DateTime,
            "/Script/CoreUObject.Timespan" => StructType::Timespan,
            "/Script/CoreUObject.Vector2D" => StructType::Vector2D,
            "/Script/CoreUObject.Vector" => StructType::Vector,
            "/Script/CoreUObject.IntVector" => StructType::IntVector,
            "/Script/CoreUObject.Box" => StructType::Box,
            "/Script/CoreUObject.IntPoint" => StructType::IntPoint,
            "/Script/CoreUObject.Quat" => StructType::Quat,
            "/Script/CoreUObject.Rotator" => StructType::Rotator,
            "/Script/CoreUObject.LinearColor" => StructType::LinearColor,
            "/Script/CoreUObject.Color" => StructType::Color,
            "/Script/CoreUObject.SoftObjectPath" => StructType::SoftObjectPath,
            "/Script/GameplayTags.GameplayTagContainer" => StructType::GameplayTagContainer,
            "/Script/Engine.UniqueNetIdRepl" => StructType::UniqueNetIdRepl,
            "/Script/CoreUObject.Struct" => StructType::Struct(None),
            _ if raw => StructType::Raw(t.to_owned()),
            _ => StructType::Struct(Some(t.to_owned())),
        }
    }
    fn full_str(&self) -> &str {
        match self {
            StructType::Guid => "/Script/CoreUObject.Guid",
            StructType::DateTime => "/Script/CoreUObject.DateTime",
            StructType::Timespan => "/Script/CoreUObject.Timespan",
            StructType::Vector2D => "/Script/CoreUObject.Vector2D",
            StructType::Vector => "/Script/CoreUObject.Vector",
            StructType::IntVector => "/Script/CoreUObject.IntVector",
            StructType::Box => "/Script/CoreUObject.Box",
            StructType::IntPoint => "/Script/CoreUObject.IntPoint",
            StructType::Quat => "/Script/CoreUObject.Quat",
            StructType::Rotator => "/Script/CoreUObject.Rotator",
            StructType::LinearColor => "/Script/CoreUObject.LinearColor",
            StructType::Color => "/Script/CoreUObject.Color",
            StructType::SoftObjectPath => "/Script/CoreUObject.SoftObjectPath",
            StructType::GameplayTagContainer => "/Script/GameplayTags.GameplayTagContainer",
            StructType::UniqueNetIdRepl => "/Script/Engine.UniqueNetIdRepl",
            StructType::Raw(t) => t,
            StructType::Struct(Some(t)) => t,
            _ => unreachable!(),
        }
    }
    fn as_str(&self) -> &str {
        match self {
            StructType::Guid => "Guid",
            StructType::DateTime => "DateTime",
            StructType::Timespan => "Timespan",
            StructType::Vector2D => "Vector2D",
            StructType::Vector => "Vector",
            StructType::IntVector => "IntVector",
            StructType::Box => "Box",
            StructType::IntPoint => "IntPoint",
            StructType::Quat => "Quat",
            StructType::Rotator => "Rotator",
            StructType::LinearColor => "LinearColor",
            StructType::Color => "Color",
            StructType::SoftObjectPath => "SoftObjectPath",
            StructType::GameplayTagContainer => "GameplayTagContainer",
            StructType::UniqueNetIdRepl => "UniqueNetIdRepl",
            StructType::Raw(t) => t,
            StructType::Struct(Some(t)) => t,
            _ => unreachable!(),
        }
    }
    #[instrument(name = "StructType_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(ar.read_string()?.into())
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_string(self.as_str())?;
        Ok(())
    }
    fn raw(&self) -> bool {
        matches!(self, StructType::Raw(_))
    }
}

type DateTime = u64;
type Timespan = i64;
type Int8 = i8;
type Int16 = i16;
type Int = i32;
type Int64 = i64;
type UInt8 = u8;
type UInt16 = u16;
type UInt32 = u32;
type UInt64 = u64;
type Bool = bool;
type Enum = String;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Float(pub f32);
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Double(pub f64);

impl std::fmt::Display for Float {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::fmt::Display for Double {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl From<f32> for Float {
    fn from(value: f32) -> Self {
        Self(value)
    }
}
impl From<f64> for Float {
    fn from(value: f64) -> Self {
        Self(value as f32)
    }
}
impl From<Float> for f32 {
    fn from(val: Float) -> Self {
        val.0
    }
}
impl From<Float> for f64 {
    fn from(val: Float) -> Self {
        val.0 as f64
    }
}
impl From<f32> for Double {
    fn from(value: f32) -> Self {
        Self(value as f64)
    }
}
impl From<f64> for Double {
    fn from(value: f64) -> Self {
        Self(value)
    }
}
impl From<Double> for f32 {
    fn from(val: Double) -> Self {
        val.0 as f32
    }
}
impl From<Double> for f64 {
    fn from(val: Double) -> Self {
        val.0
    }
}
impl<'de> Deserialize<'de> for Float {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FloatVisitor;

        impl serde::de::Visitor<'_> for FloatVisitor {
            type Value = f32;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a float or string representation of NaN/Infinity")
            }
            fn visit_i8<E>(self, value: i8) -> Result<Self::Value, E> {
                Ok(value as f32)
            }
            fn visit_u8<E>(self, value: u8) -> Result<Self::Value, E> {
                Ok(value as f32)
            }
            fn visit_i16<E>(self, value: i16) -> Result<Self::Value, E> {
                Ok(value as f32)
            }
            fn visit_u16<E>(self, value: u16) -> Result<Self::Value, E> {
                Ok(value as f32)
            }
            fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E> {
                Ok(value as f32)
            }
            fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E> {
                Ok(value as f32)
            }
            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                Ok(value as f32)
            }
            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(value as f32)
            }
            fn visit_f32<E>(self, value: f32) -> Result<Self::Value, E> {
                Ok(value)
            }
            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
                Ok(value as f32)
            }
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "NaN" => Ok(f32::NAN),
                    "-NaN" => Ok(-f32::NAN),
                    "Infinity" => Ok(f32::INFINITY),
                    "-Infinity" => Ok(f32::NEG_INFINITY),
                    _ => Err(E::custom(format!(
                        "unxpected string value in place of float '{value}'"
                    ))),
                }
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(&value)
            }
        }

        let value = deserializer.deserialize_any(FloatVisitor)?;
        Ok(Self(value))
    }
}
impl<'de> Deserialize<'de> for Double {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FloatVisitor;

        impl serde::de::Visitor<'_> for FloatVisitor {
            type Value = f64;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a float or string representation of NaN/Infinity")
            }
            fn visit_i8<E>(self, value: i8) -> Result<Self::Value, E> {
                Ok(value as f64)
            }
            fn visit_u8<E>(self, value: u8) -> Result<Self::Value, E> {
                Ok(value as f64)
            }
            fn visit_i16<E>(self, value: i16) -> Result<Self::Value, E> {
                Ok(value as f64)
            }
            fn visit_u16<E>(self, value: u16) -> Result<Self::Value, E> {
                Ok(value as f64)
            }
            fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E> {
                Ok(value as f64)
            }
            fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E> {
                Ok(value as f64)
            }
            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                Ok(value as f64)
            }
            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(value as f64)
            }
            fn visit_f32<E>(self, value: f32) -> Result<Self::Value, E> {
                Ok(value as f64)
            }
            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
                Ok(value)
            }
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "NaN" => Ok(f64::NAN),
                    "-NaN" => Ok(-f64::NAN),
                    "Infinity" => Ok(f64::INFINITY),
                    "-Infinity" => Ok(f64::NEG_INFINITY),
                    _ => Err(E::custom(format!(
                        "unxpected string value in place of float '{value}'"
                    ))),
                }
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(&value)
            }
        }

        let value = deserializer.deserialize_any(FloatVisitor)?;
        Ok(Self(value))
    }
}
impl Serialize for Float {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = self.0;
        let sign = if value.is_sign_negative() { "-" } else { "" };
        if value.is_nan() {
            serializer.serialize_str(&format!("{sign}NaN"))
        } else if value.is_infinite() {
            serializer.serialize_str(&format!("{sign}Infinity"))
        } else {
            serializer.serialize_f32(value)
        }
    }
}
impl Serialize for Double {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = self.0;
        let sign = if value.is_sign_negative() { "-" } else { "" };
        if value.is_nan() {
            serializer.serialize_str(&format!("{sign}NaN"))
        } else if value.is_infinite() {
            serializer.serialize_str(&format!("{sign}Infinity"))
        } else {
            serializer.serialize_f64(value)
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct MapEntry {
    pub key: PropertyValue,
    pub value: PropertyValue,
}
impl MapEntry {
    #[instrument(name = "MapEntry_read", skip_all)]
    fn read<A: ArchiveReader>(
        ar: &mut A,
        key_type: &PropertyTagDataFull,
        value_type: &PropertyTagDataFull,
    ) -> Result<MapEntry> {
        let key = PropertyValue::read(ar, key_type)?;
        let value = PropertyValue::read(ar, value_type)?;
        Ok(Self { key, value })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.key.write(ar)?;
        self.value.write(ar)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct FieldPath {
    path: Vec<String>,
    owner: String,
}
impl FieldPath {
    #[instrument(name = "FieldPath_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            path: read_array(ar.read_u32::<LE>()?, ar, |ar| ar.read_string())?,
            owner: ar.read_string()?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.path.len() as u32)?;
        for p in &self.path {
            ar.write_string(p)?;
        }
        ar.write_string(&self.owner)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Delegate {
    name: String,
    path: String,
}
impl Delegate {
    #[instrument(name = "Delegate_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            name: ar.read_string()?,
            path: ar.read_string()?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_string(&self.name)?;
        ar.write_string(&self.path)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct MulticastDelegate(Vec<Delegate>);
impl MulticastDelegate {
    #[instrument(name = "MulticastDelegate_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self(read_array(ar.read_u32::<LE>()?, ar, Delegate::read)?))
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.0.len() as u32)?;
        for entry in &self.0 {
            entry.write(ar)?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct MulticastInlineDelegate(Vec<Delegate>);
impl MulticastInlineDelegate {
    #[instrument(name = "MulticastInlineDelegate_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self(read_array(ar.read_u32::<LE>()?, ar, Delegate::read)?))
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.0.len() as u32)?;
        for entry in &self.0 {
            entry.write(ar)?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct MulticastSparseDelegate(Vec<Delegate>);
impl MulticastSparseDelegate {
    #[instrument(name = "MulticastSparseDelegate_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self(read_array(ar.read_u32::<LE>()?, ar, Delegate::read)?))
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.0.len() as u32)?;
        for entry in &self.0 {
            entry.write(ar)?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct LinearColor {
    pub r: Float,
    pub g: Float,
    pub b: Float,
    pub a: Float,
}
impl LinearColor {
    #[instrument(name = "LinearColor_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            r: ar.read_f32::<LE>()?.into(),
            g: ar.read_f32::<LE>()?.into(),
            b: ar.read_f32::<LE>()?.into(),
            a: ar.read_f32::<LE>()?.into(),
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_f32::<LE>(self.r.into())?;
        ar.write_f32::<LE>(self.g.into())?;
        ar.write_f32::<LE>(self.b.into())?;
        ar.write_f32::<LE>(self.a.into())?;
        Ok(())
    }
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Quat {
    pub x: Double,
    pub y: Double,
    pub z: Double,
    pub w: Double,
}
impl Quat {
    #[instrument(name = "Quat_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        if ar.version().large_world_coordinates() {
            Ok(Self {
                x: ar.read_f64::<LE>()?.into(),
                y: ar.read_f64::<LE>()?.into(),
                z: ar.read_f64::<LE>()?.into(),
                w: ar.read_f64::<LE>()?.into(),
            })
        } else {
            Ok(Self {
                x: ar.read_f32::<LE>()?.into(),
                y: ar.read_f32::<LE>()?.into(),
                z: ar.read_f32::<LE>()?.into(),
                w: ar.read_f32::<LE>()?.into(),
            })
        }
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        if ar.version().large_world_coordinates() {
            ar.write_f64::<LE>(self.x.into())?;
            ar.write_f64::<LE>(self.y.into())?;
            ar.write_f64::<LE>(self.z.into())?;
            ar.write_f64::<LE>(self.w.into())?;
        } else {
            ar.write_f32::<LE>(self.x.into())?;
            ar.write_f32::<LE>(self.y.into())?;
            ar.write_f32::<LE>(self.z.into())?;
            ar.write_f32::<LE>(self.w.into())?;
        }
        Ok(())
    }
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Rotator {
    pub x: Double,
    pub y: Double,
    pub z: Double,
}
impl Rotator {
    #[instrument(name = "Rotator_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        if ar.version().large_world_coordinates() {
            Ok(Self {
                x: ar.read_f64::<LE>()?.into(),
                y: ar.read_f64::<LE>()?.into(),
                z: ar.read_f64::<LE>()?.into(),
            })
        } else {
            Ok(Self {
                x: ar.read_f32::<LE>()?.into(),
                y: ar.read_f32::<LE>()?.into(),
                z: ar.read_f32::<LE>()?.into(),
            })
        }
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        if ar.version().large_world_coordinates() {
            ar.write_f64::<LE>(self.x.into())?;
            ar.write_f64::<LE>(self.y.into())?;
            ar.write_f64::<LE>(self.z.into())?;
        } else {
            ar.write_f32::<LE>(self.x.into())?;
            ar.write_f32::<LE>(self.y.into())?;
            ar.write_f32::<LE>(self.z.into())?;
        }
        Ok(())
    }
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Color {
    pub r: u8,
    pub g: u8,
    pub b: u8,
    pub a: u8,
}
impl Color {
    #[instrument(name = "Color_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            r: ar.read_u8()?,
            g: ar.read_u8()?,
            b: ar.read_u8()?,
            a: ar.read_u8()?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u8(self.r)?;
        ar.write_u8(self.g)?;
        ar.write_u8(self.b)?;
        ar.write_u8(self.a)?;
        Ok(())
    }
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Vector {
    pub x: Double,
    pub y: Double,
    pub z: Double,
}
impl Vector {
    #[instrument(name = "Vector_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        if ar.version().large_world_coordinates() {
            Ok(Self {
                x: ar.read_f64::<LE>()?.into(),
                y: ar.read_f64::<LE>()?.into(),
                z: ar.read_f64::<LE>()?.into(),
            })
        } else {
            Ok(Self {
                x: ar.read_f32::<LE>()?.into(),
                y: ar.read_f32::<LE>()?.into(),
                z: ar.read_f32::<LE>()?.into(),
            })
        }
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        if ar.version().large_world_coordinates() {
            ar.write_f64::<LE>(self.x.into())?;
            ar.write_f64::<LE>(self.y.into())?;
            ar.write_f64::<LE>(self.z.into())?;
        } else {
            ar.write_f32::<LE>(self.x.into())?;
            ar.write_f32::<LE>(self.y.into())?;
            ar.write_f32::<LE>(self.z.into())?;
        }
        Ok(())
    }
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Vector2D {
    pub x: Double,
    pub y: Double,
}
impl Vector2D {
    #[instrument(name = "Vector2D_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        if ar.version().large_world_coordinates() {
            Ok(Self {
                x: ar.read_f64::<LE>()?.into(),
                y: ar.read_f64::<LE>()?.into(),
            })
        } else {
            Ok(Self {
                x: ar.read_f32::<LE>()?.into(),
                y: ar.read_f32::<LE>()?.into(),
            })
        }
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        if ar.version().large_world_coordinates() {
            ar.write_f64::<LE>(self.x.into())?;
            ar.write_f64::<LE>(self.y.into())?;
        } else {
            ar.write_f32::<LE>(self.x.into())?;
            ar.write_f32::<LE>(self.y.into())?;
        }
        Ok(())
    }
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct IntVector {
    pub x: i32,
    pub y: i32,
    pub z: i32,
}
impl IntVector {
    #[instrument(name = "IntVector_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            x: ar.read_i32::<LE>()?,
            y: ar.read_i32::<LE>()?,
            z: ar.read_i32::<LE>()?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_i32::<LE>(self.x)?;
        ar.write_i32::<LE>(self.y)?;
        ar.write_i32::<LE>(self.z)?;
        Ok(())
    }
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Box {
    pub min: Vector,
    pub max: Vector,
    pub is_valid: bool,
}
impl Box {
    #[instrument(name = "Box_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            min: Vector::read(ar)?,
            max: Vector::read(ar)?,
            is_valid: ar.read_u8()? > 0,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.min.write(ar)?;
        self.max.write(ar)?;
        ar.write_u8(self.is_valid as u8)?;
        Ok(())
    }
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct IntPoint {
    pub x: i32,
    pub y: i32,
}
impl IntPoint {
    #[instrument(name = "IntPoint_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            x: ar.read_i32::<LE>()?,
            y: ar.read_i32::<LE>()?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_i32::<LE>(self.x)?;
        ar.write_i32::<LE>(self.y)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum SoftObjectPath {
    Old {
        asset_path_name: String,
        sub_path_string: String,
    },
    New {
        asset_path_name: String,
        package_name: String,
        asset_name: (String, Vec<u8>),
    },
}
impl SoftObjectPath {
    #[instrument(name = "SoftObjectPath_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(if ar.version().remove_asset_path_fnames() {
            Self::New {
                asset_path_name: ar.read_string()?,
                package_name: ar.read_string()?,
                asset_name: ar.read_string_trailing()?,
            }
        } else {
            Self::Old {
                asset_path_name: ar.read_string()?,
                sub_path_string: ar.read_string()?,
            }
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        match self {
            Self::Old {
                asset_path_name,
                sub_path_string,
            } => {
                ar.write_string(asset_path_name)?;
                ar.write_string(sub_path_string)?;
            }
            Self::New {
                asset_path_name,
                package_name,
                asset_name: (asset_name, trailing),
            } => {
                ar.write_string(asset_path_name)?;
                ar.write_string(package_name)?;
                ar.write_string_trailing(asset_name, Some(trailing))?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct GameplayTag {
    pub name: String,
}
impl GameplayTag {
    #[instrument(name = "GameplayTag_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            name: ar.read_string()?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_string(&self.name)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct GameplayTagContainer {
    pub gameplay_tags: Vec<GameplayTag>,
}
impl GameplayTagContainer {
    #[instrument(name = "GameplayTagContainer_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            gameplay_tags: read_array(ar.read_u32::<LE>()?, ar, GameplayTag::read)?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.gameplay_tags.len() as u32)?;
        for entry in &self.gameplay_tags {
            entry.write(ar)?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct UniqueNetIdRepl {
    pub inner: Option<UniqueNetIdReplInner>,
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct UniqueNetIdReplInner {
    pub size: std::num::NonZeroU32,
    pub type_: String,
    pub contents: String,
}
impl UniqueNetIdRepl {
    #[instrument(name = "UniqueNetIdRepl_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let size = ar.read_u32::<LE>()?;
        let inner = if let Ok(size) = size.try_into() {
            Some(UniqueNetIdReplInner {
                size,
                type_: ar.read_string()?,
                contents: ar.read_string()?,
            })
        } else {
            None
        };
        Ok(Self { inner })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        match &self.inner {
            Some(inner) => {
                ar.write_u32::<LE>(inner.size.into())?;
                ar.write_string(&inner.type_)?;
                ar.write_string(&inner.contents)?;
            }
            None => ar.write_u32::<LE>(0)?,
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct FFormatArgumentData {
    name: String,
    value: FFormatArgumentDataValue,
}
impl FFormatArgumentData {
    #[instrument(name = "FFormatArgumentData_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            name: ar.read_string()?,
            value: FFormatArgumentDataValue::read(ar)?,
        })
    }
}
impl FFormatArgumentData {
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_string(&self.name)?;
        self.value.write(ar)?;
        Ok(())
    }
}
// very similar to FFormatArgumentValue but serializes ints as 32 bits (TODO changes to 64 bit
// again at some later UE version)
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum FFormatArgumentDataValue {
    Int(i32),
    UInt(u32),
    Float(Float),
    Double(Double),
    Text(std::boxed::Box<Text>),
    Gender(u64),
}
impl FFormatArgumentDataValue {
    #[instrument(name = "FFormatArgumentDataValue_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let type_ = ar.read_u8()?;
        match type_ {
            0 => Ok(Self::Int(ar.read_i32::<LE>()?)),
            1 => Ok(Self::UInt(ar.read_u32::<LE>()?)),
            2 => Ok(Self::Float(ar.read_f32::<LE>()?.into())),
            3 => Ok(Self::Double(ar.read_f64::<LE>()?.into())),
            4 => Ok(Self::Text(std::boxed::Box::new(Text::read(ar)?))),
            5 => Ok(Self::Gender(ar.read_u64::<LE>()?)),
            _ => Err(Error::Other(format!(
                "unimplemented variant for FFormatArgumentDataValue 0x{type_:x}"
            ))),
        }
    }
}
impl FFormatArgumentDataValue {
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        match self {
            Self::Int(value) => {
                ar.write_u8(0)?;
                ar.write_i32::<LE>(*value)?;
            }
            Self::UInt(value) => {
                ar.write_u8(1)?;
                ar.write_u32::<LE>(*value)?;
            }
            Self::Float(value) => {
                ar.write_u8(2)?;
                ar.write_f32::<LE>((*value).into())?;
            }
            Self::Double(value) => {
                ar.write_u8(3)?;
                ar.write_f64::<LE>((*value).into())?;
            }
            Self::Text(value) => {
                ar.write_u8(4)?;
                value.write(ar)?;
            }
            Self::Gender(value) => {
                ar.write_u8(5)?;
                ar.write_u64::<LE>(*value)?;
            }
        };
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum FFormatArgumentValue {
    Int(i64),
    UInt(u64),
    Float(Float),
    Double(Double),
    Text(std::boxed::Box<Text>),
    Gender(u64),
}

impl FFormatArgumentValue {
    #[instrument(name = "FFormatArgumentValue_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let type_ = ar.read_u8()?;
        match type_ {
            0 => Ok(Self::Int(ar.read_i64::<LE>()?)),
            1 => Ok(Self::UInt(ar.read_u64::<LE>()?)),
            2 => Ok(Self::Float(ar.read_f32::<LE>()?.into())),
            3 => Ok(Self::Double(ar.read_f64::<LE>()?.into())),
            4 => Ok(Self::Text(std::boxed::Box::new(Text::read(ar)?))),
            5 => Ok(Self::Gender(ar.read_u64::<LE>()?)),
            _ => Err(Error::Other(format!(
                "unimplemented variant for FFormatArgumentValue 0x{type_:x}"
            ))),
        }
    }
}
impl FFormatArgumentValue {
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        match self {
            Self::Int(value) => {
                ar.write_u8(0)?;
                ar.write_i64::<LE>(*value)?;
            }
            Self::UInt(value) => {
                ar.write_u8(1)?;
                ar.write_u64::<LE>(*value)?;
            }
            Self::Float(value) => {
                ar.write_u8(2)?;
                ar.write_f32::<LE>((*value).into())?;
            }
            Self::Double(value) => {
                ar.write_u8(3)?;
                ar.write_f64::<LE>((*value).into())?;
            }
            Self::Text(value) => {
                ar.write_u8(4)?;
                value.write(ar)?;
            }
            Self::Gender(value) => {
                ar.write_u8(5)?;
                ar.write_u64::<LE>(*value)?;
            }
        };
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct FNumberFormattingOptions {
    always_sign: bool,
    use_grouping: bool,
    rounding_mode: i8, // TODO enum ERoundingMode
    minimum_integral_digits: i32,
    maximum_integral_digits: i32,
    minimum_fractional_digits: i32,
    maximum_fractional_digits: i32,
}
impl FNumberFormattingOptions {
    #[instrument(name = "FNumberFormattingOptions_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            always_sign: ar.read_u32::<LE>()? != 0,
            use_grouping: ar.read_u32::<LE>()? != 0,
            rounding_mode: ar.read_i8()?,
            minimum_integral_digits: ar.read_i32::<LE>()?,
            maximum_integral_digits: ar.read_i32::<LE>()?,
            minimum_fractional_digits: ar.read_i32::<LE>()?,
            maximum_fractional_digits: ar.read_i32::<LE>()?,
        })
    }
}
impl FNumberFormattingOptions {
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.always_sign as u32)?;
        ar.write_u32::<LE>(self.use_grouping as u32)?;
        ar.write_i8(self.rounding_mode)?;
        ar.write_i32::<LE>(self.minimum_integral_digits)?;
        ar.write_i32::<LE>(self.maximum_integral_digits)?;
        ar.write_i32::<LE>(self.minimum_fractional_digits)?;
        ar.write_i32::<LE>(self.maximum_fractional_digits)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Text {
    flags: u32,
    variant: TextVariant,
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum TextVariant {
    // -0x1
    None {
        culture_invariant: Option<String>,
    },
    // 0x0
    Base {
        namespace: (String, Vec<u8>),
        key: String,
        source_string: String,
    },
    // 0x3
    ArgumentFormat {
        // aka ArgumentDataFormat
        format_text: std::boxed::Box<Text>,
        arguments: Vec<FFormatArgumentData>,
    },
    // 0x4
    AsNumber {
        source_value: FFormatArgumentValue,
        format_options: Option<FNumberFormattingOptions>,
        culture_name: String,
    },
    // 0x7
    AsDate {
        source_date_time: DateTime,
        date_style: i8, // TODO EDateTimeStyle::Type
        time_zone: String,
        culture_name: String,
    },
    StringTableEntry {
        // 0xb
        table: String,
        key: String,
    },
}

impl Text {
    #[instrument(name = "Text_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let flags = ar.read_u32::<LE>()?;
        let text_history_type = ar.read_i8()?;
        let variant = match text_history_type {
            -0x1 => Ok(TextVariant::None {
                culture_invariant: (ar.read_u32::<LE>()? != 0) // bHasCultureInvariantString
                    .then(|| ar.read_string())
                    .transpose()?,
            }),
            0x0 => Ok(TextVariant::Base {
                namespace: ar.read_string_trailing()?,
                key: ar.read_string()?,
                source_string: ar.read_string()?,
            }),
            0x3 => Ok(TextVariant::ArgumentFormat {
                format_text: std::boxed::Box::new(Text::read(ar)?),
                arguments: read_array(ar.read_u32::<LE>()?, ar, FFormatArgumentData::read)?,
            }),
            0x4 => Ok(TextVariant::AsNumber {
                source_value: FFormatArgumentValue::read(ar)?,
                format_options: (ar.read_u32::<LE>()? != 0) // bHasFormatOptions
                    .then(|| FNumberFormattingOptions::read(ar))
                    .transpose()?,
                culture_name: ar.read_string()?,
            }),
            0x7 => Ok(TextVariant::AsDate {
                source_date_time: ar.read_u64::<LE>()?,
                date_style: ar.read_i8()?,
                time_zone: ar.read_string()?,
                culture_name: ar.read_string()?,
            }),
            0xb => Ok({
                TextVariant::StringTableEntry {
                    table: ar.read_string()?,
                    key: ar.read_string()?,
                }
            }),
            _ => Err(Error::Other(format!(
                "unimplemented variant for FTextHistory 0x{text_history_type:x}"
            ))),
        }?;
        Ok(Self { flags, variant })
    }
}
impl Text {
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.flags)?;
        match &self.variant {
            TextVariant::None { culture_invariant } => {
                ar.write_i8(-0x1)?;
                ar.write_u32::<LE>(culture_invariant.is_some() as u32)?;
                if let Some(culture_invariant) = culture_invariant {
                    ar.write_string(culture_invariant)?;
                }
            }
            TextVariant::Base {
                namespace,
                key,
                source_string,
            } => {
                ar.write_i8(0x0)?;
                // This particular string sometimes includes the trailing null byte and sometimes
                // does not. To preserve byte-for-byte equality we save the trailing bytes (null or
                // not) to the JSON so they can be retored later.
                ar.write_string_trailing(&namespace.0, Some(&namespace.1))?;
                ar.write_string(key)?;
                ar.write_string(source_string)?;
            }
            TextVariant::ArgumentFormat {
                format_text,
                arguments,
            } => {
                ar.write_i8(0x3)?;
                format_text.write(ar)?;
                ar.write_u32::<LE>(arguments.len() as u32)?;
                for a in arguments {
                    a.write(ar)?;
                }
            }
            TextVariant::AsNumber {
                source_value,
                format_options,
                culture_name,
            } => {
                ar.write_i8(0x4)?;
                source_value.write(ar)?;
                ar.write_u32::<LE>(format_options.is_some() as u32)?;
                if let Some(format_options) = format_options {
                    format_options.write(ar)?;
                }
                ar.write_string(culture_name)?;
            }
            TextVariant::AsDate {
                source_date_time,
                date_style,
                time_zone,
                culture_name,
            } => {
                ar.write_i8(0x7)?;
                ar.write_u64::<LE>(*source_date_time)?;
                ar.write_i8(*date_style)?;
                ar.write_string(time_zone)?;
                ar.write_string(culture_name)?;
            }
            TextVariant::StringTableEntry { table, key } => {
                ar.write_i8(0xb)?;
                ar.write_string(table)?;
                ar.write_string(key)?;
            }
        }
        Ok(())
    }
}

/// Just a plain byte, or an enum in which case the variant will be a String
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Byte {
    Byte(u8),
    Label(String),
}
/// Vectorized [`Byte`]
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum ByteArray {
    Byte(Vec<u8>),
    Label(Vec<String>),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum PropertyValue {
    Int(Int),
    Int8(Int8),
    Int16(Int16),
    Int64(Int64),
    UInt16(UInt16),
    UInt32(UInt32),
    Float(Float),
    Double(Double),
    Bool(Bool),
    Byte(Byte),
    Enum(Enum),
    Name(String),
    Str(String),
    SoftObject(SoftObjectPath),
    SoftObjectPath(SoftObjectPath),
    Object(String),
    Struct(StructValue),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum StructValue {
    Guid(FGuid),
    DateTime(DateTime),
    Timespan(Timespan),
    Vector2D(Vector2D),
    Vector(Vector),
    IntVector(IntVector),
    Box(Box),
    IntPoint(IntPoint),
    Quat(Quat),
    LinearColor(LinearColor),
    Color(Color),
    Rotator(Rotator),
    SoftObjectPath(SoftObjectPath),
    GameplayTagContainer(GameplayTagContainer),
    UniqueNetIdRepl(UniqueNetIdRepl),
    /// Raw struct data for other unknown structs serialized with HasBinaryOrNativeSerialize
    Raw(Vec<u8>),
    /// User defined struct which is simply a list of properties
    Struct(Properties),
}

/// Vectorized properties to avoid storing the variant with each value
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum ValueVec {
    Int8(Vec<Int8>),
    Int16(Vec<Int16>),
    Int(Vec<Int>),
    Int64(Vec<Int64>),
    UInt8(Vec<UInt8>),
    UInt16(Vec<UInt16>),
    UInt32(Vec<UInt32>),
    UInt64(Vec<UInt64>),
    Float(Vec<Float>),
    Double(Vec<Double>),
    Bool(Vec<bool>),
    Byte(ByteArray),
    Enum(Vec<Enum>),
    Str(Vec<String>),
    Text(Vec<Text>),
    SoftObject(Vec<(String, String)>),
    Name(Vec<String>),
    Object(Vec<String>),
    Box(Vec<Box>),
}

/// Encapsulates [`ValueVec`] with a special handling of structs. See also: [`ValueSet`]
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum ValueArray {
    Base(ValueVec),
    Struct {
        type_: PropertyType,
        struct_type: StructType,
        id: Option<FGuid>,
        value: Vec<StructValue>,
    },
}
/// Encapsulates [`ValueVec`] with a special handling of structs. See also: [`ValueArray`]
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum ValueSet {
    Base(ValueVec),
    Struct(Vec<StructValue>),
}

impl PropertyValue {
    #[instrument(name = "PropertyValue_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A, t: &PropertyTagDataFull) -> Result<PropertyValue> {
        Ok(match t {
            PropertyTagDataFull::Array(_) => unreachable!(),
            PropertyTagDataFull::Struct { struct_type, .. } => {
                PropertyValue::Struct(StructValue::read(ar, struct_type)?)
            }
            PropertyTagDataFull::Set { .. } => unreachable!(),
            PropertyTagDataFull::Map { .. } => unreachable!(),
            PropertyTagDataFull::Byte(_) => PropertyValue::Byte(Byte::Label(ar.read_string()?)),
            PropertyTagDataFull::Enum(_, _) => PropertyValue::Enum(ar.read_string()?),
            PropertyTagDataFull::Bool(_) => PropertyValue::Bool(ar.read_u8()? > 0),
            PropertyTagDataFull::Other(property_type) => match property_type {
                PropertyType::IntProperty => PropertyValue::Int(ar.read_i32::<LE>()?),
                PropertyType::Int8Property => PropertyValue::Int8(ar.read_i8()?),
                PropertyType::Int16Property => PropertyValue::Int16(ar.read_i16::<LE>()?),
                PropertyType::Int64Property => PropertyValue::Int64(ar.read_i64::<LE>()?),
                PropertyType::UInt16Property => PropertyValue::UInt16(ar.read_u16::<LE>()?),
                PropertyType::UInt32Property => PropertyValue::UInt32(ar.read_u32::<LE>()?),
                PropertyType::FloatProperty => PropertyValue::Float(ar.read_f32::<LE>()?.into()),
                PropertyType::DoubleProperty => PropertyValue::Double(ar.read_f64::<LE>()?.into()),
                PropertyType::NameProperty => PropertyValue::Name(ar.read_string()?),
                PropertyType::StrProperty => PropertyValue::Str(ar.read_string()?),
                PropertyType::SoftObjectProperty => {
                    PropertyValue::SoftObject(SoftObjectPath::read(ar)?)
                }
                PropertyType::ObjectProperty => PropertyValue::Object(ar.read_object_ref()?),
                _ => return Err(Error::Other(format!("unimplemented property {t:?}"))),
            },
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        match &self {
            PropertyValue::Int(v) => ar.write_i32::<LE>(*v)?,
            PropertyValue::Int8(v) => ar.write_i8(*v)?,
            PropertyValue::Int16(v) => ar.write_i16::<LE>(*v)?,
            PropertyValue::Int64(v) => ar.write_i64::<LE>(*v)?,
            PropertyValue::UInt16(v) => ar.write_u16::<LE>(*v)?,
            PropertyValue::UInt32(v) => ar.write_u32::<LE>(*v)?,
            PropertyValue::Float(v) => ar.write_f32::<LE>((*v).into())?,
            PropertyValue::Double(v) => ar.write_f64::<LE>((*v).into())?,
            PropertyValue::Bool(v) => ar.write_u8(u8::from(*v))?,
            PropertyValue::Name(v) => ar.write_string(v)?,
            PropertyValue::Str(v) => ar.write_string(v)?,
            PropertyValue::SoftObject(v) => v.write(ar)?,
            PropertyValue::SoftObjectPath(v) => v.write(ar)?,
            PropertyValue::Object(v) => ar.write_object_ref(v)?,
            PropertyValue::Byte(v) => match v {
                Byte::Byte(b) => ar.write_u8(*b)?,
                Byte::Label(l) => ar.write_string(l)?,
            },
            PropertyValue::Enum(v) => ar.write_string(v)?,
            PropertyValue::Struct(v) => v.write(ar)?,
        };
        Ok(())
    }
}
impl StructValue {
    #[instrument(name = "StructValue_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A, t: &StructType) -> Result<StructValue> {
        Ok(match t {
            StructType::Guid => StructValue::Guid(FGuid::read(ar)?),
            StructType::DateTime => StructValue::DateTime(ar.read_u64::<LE>()?),
            StructType::Timespan => StructValue::Timespan(ar.read_i64::<LE>()?),
            StructType::Vector2D => StructValue::Vector2D(Vector2D::read(ar)?),
            StructType::Vector => StructValue::Vector(Vector::read(ar)?),
            StructType::IntVector => StructValue::IntVector(IntVector::read(ar)?),
            StructType::Box => StructValue::Box(Box::read(ar)?),
            StructType::IntPoint => StructValue::IntPoint(IntPoint::read(ar)?),
            StructType::Quat => StructValue::Quat(Quat::read(ar)?),
            StructType::LinearColor => StructValue::LinearColor(LinearColor::read(ar)?),
            StructType::Color => StructValue::Color(Color::read(ar)?),
            StructType::Rotator => StructValue::Rotator(Rotator::read(ar)?),
            StructType::SoftObjectPath => StructValue::SoftObjectPath(SoftObjectPath::read(ar)?),
            StructType::GameplayTagContainer => {
                StructValue::GameplayTagContainer(GameplayTagContainer::read(ar)?)
            }
            StructType::UniqueNetIdRepl => StructValue::UniqueNetIdRepl(UniqueNetIdRepl::read(ar)?),
            StructType::Raw(_) => unreachable!("should be handled at property level"),
            StructType::Struct(_) => StructValue::Struct(read_properties_until_none(ar)?),
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        match self {
            StructValue::Guid(v) => v.write(ar)?,
            StructValue::DateTime(v) => ar.write_u64::<LE>(*v)?,
            StructValue::Timespan(v) => ar.write_i64::<LE>(*v)?,
            StructValue::Vector2D(v) => v.write(ar)?,
            StructValue::Vector(v) => v.write(ar)?,
            StructValue::IntVector(v) => v.write(ar)?,
            StructValue::Box(v) => v.write(ar)?,
            StructValue::IntPoint(v) => v.write(ar)?,
            StructValue::Quat(v) => v.write(ar)?,
            StructValue::LinearColor(v) => v.write(ar)?,
            StructValue::Color(v) => v.write(ar)?,
            StructValue::Rotator(v) => v.write(ar)?,
            StructValue::SoftObjectPath(v) => v.write(ar)?,
            StructValue::GameplayTagContainer(v) => v.write(ar)?,
            StructValue::UniqueNetIdRepl(v) => v.write(ar)?,
            StructValue::Raw(v) => ar.write_all(v)?,
            StructValue::Struct(v) => write_properties_none_terminated(ar, v)?,
        }
        Ok(())
    }
}
impl ValueVec {
    #[instrument(name = "ValueVec_read", skip_all)]
    fn read<A: ArchiveReader>(
        ar: &mut A,
        t: &PropertyType,
        size: u32,
        count: u32,
    ) -> Result<ValueVec> {
        Ok(match t {
            PropertyType::IntProperty => {
                ValueVec::Int(read_array(count, ar, |r| Ok(r.read_i32::<LE>()?))?)
            }
            PropertyType::Int16Property => {
                ValueVec::Int16(read_array(count, ar, |r| Ok(r.read_i16::<LE>()?))?)
            }
            PropertyType::Int64Property => {
                ValueVec::Int64(read_array(count, ar, |r| Ok(r.read_i64::<LE>()?))?)
            }
            PropertyType::UInt16Property => {
                ValueVec::UInt16(read_array(count, ar, |r| Ok(r.read_u16::<LE>()?))?)
            }
            PropertyType::UInt32Property => {
                ValueVec::UInt32(read_array(count, ar, |r| Ok(r.read_u32::<LE>()?))?)
            }
            PropertyType::FloatProperty => {
                ValueVec::Float(read_array(count, ar, |r| Ok(r.read_f32::<LE>()?.into()))?)
            }
            PropertyType::DoubleProperty => {
                ValueVec::Double(read_array(count, ar, |r| Ok(r.read_f64::<LE>()?.into()))?)
            }
            PropertyType::BoolProperty => {
                ValueVec::Bool(read_array(count, ar, |r| Ok(r.read_u8()? > 0))?)
            }
            PropertyType::ByteProperty => {
                if size == count {
                    ValueVec::Byte(ByteArray::Byte(read_array(
                        count,
                        ar,
                        |r| Ok(r.read_u8()?),
                    )?))
                } else {
                    ValueVec::Byte(ByteArray::Label(read_array(count, ar, |r| {
                        r.read_string()
                    })?))
                }
            }
            PropertyType::EnumProperty => {
                ValueVec::Enum(read_array(count, ar, |r| r.read_string())?)
            }
            PropertyType::StrProperty => ValueVec::Str(read_array(count, ar, |r| r.read_string())?),
            PropertyType::TextProperty => ValueVec::Text(read_array(count, ar, Text::read)?),
            PropertyType::SoftObjectProperty => ValueVec::SoftObject(read_array(count, ar, |r| {
                Ok((r.read_string()?, r.read_string()?))
            })?),
            PropertyType::NameProperty => {
                ValueVec::Name(read_array(count, ar, |r| r.read_string())?)
            }
            PropertyType::ObjectProperty => {
                ValueVec::Object(read_array(count, ar, |r| r.read_object_ref())?)
            }
            _ => return Err(Error::UnknownVecType(format!("{t:?}"))),
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        match &self {
            ValueVec::Int8(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    ar.write_i8(*i)?;
                }
            }
            ValueVec::Int16(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    ar.write_i16::<LE>(*i)?;
                }
            }
            ValueVec::Int(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    ar.write_i32::<LE>(*i)?;
                }
            }
            ValueVec::Int64(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    ar.write_i64::<LE>(*i)?;
                }
            }
            ValueVec::UInt8(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    ar.write_u8(*i)?;
                }
            }
            ValueVec::UInt16(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    ar.write_u16::<LE>(*i)?;
                }
            }
            ValueVec::UInt32(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    ar.write_u32::<LE>(*i)?;
                }
            }
            ValueVec::UInt64(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    ar.write_u64::<LE>(*i)?;
                }
            }
            ValueVec::Float(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    ar.write_f32::<LE>((*i).into())?;
                }
            }
            ValueVec::Double(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    ar.write_f64::<LE>((*i).into())?;
                }
            }
            ValueVec::Bool(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for b in v {
                    ar.write_u8(*b as u8)?;
                }
            }
            ValueVec::Byte(v) => match v {
                ByteArray::Byte(b) => {
                    ar.write_u32::<LE>(b.len() as u32)?;
                    for b in b {
                        ar.write_u8(*b)?;
                    }
                }
                ByteArray::Label(l) => {
                    ar.write_u32::<LE>(l.len() as u32)?;
                    for l in l {
                        ar.write_string(l)?;
                    }
                }
            },
            ValueVec::Enum(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    ar.write_string(i)?;
                }
            }
            ValueVec::Str(v) | ValueVec::Name(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    ar.write_string(i)?;
                }
            }
            ValueVec::Object(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    ar.write_object_ref(i)?;
                }
            }
            ValueVec::Text(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    i.write(ar)?;
                }
            }
            ValueVec::SoftObject(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for (a, b) in v {
                    ar.write_string(a)?;
                    ar.write_string(b)?;
                }
            }
            ValueVec::Box(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    i.write(ar)?;
                }
            }
        }
        Ok(())
    }
}
impl ValueArray {
    #[instrument(name = "ValueArray_read", skip_all)]
    fn read<A: ArchiveReader>(
        ar: &mut A,
        tag: PropertyTagDataFull,
        size: u32,
    ) -> Result<ValueArray> {
        let count = ar.read_u32::<LE>()?;
        Ok(match tag {
            PropertyTagDataFull::Struct { struct_type, id } => {
                let (struct_type, id) = if !ar.version().property_tag() {
                    if ar.version().array_inner_tag() {
                        let tag = PropertyTagFull::read(ar)?.unwrap();
                        match tag.data {
                            PropertyTagDataFull::Struct { struct_type, id } => (struct_type, id),
                            _ => {
                                return Err(Error::Other(format!(
                                    "expected StructProperty tag, found {tag:?}"
                                )))
                            }
                        }
                    } else {
                        // TODO prior to 4.12 struct type is unknown so should be able to
                        // manually specify like Sets/Maps
                        (StructType::Struct(None), Default::default())
                    }
                } else {
                    (struct_type, id)
                };

                let mut value = vec![];
                for _ in 0..count {
                    value.push(StructValue::read(ar, &struct_type)?);
                }
                ValueArray::Struct {
                    type_: PropertyType::StructProperty,
                    struct_type,
                    id: Some(id),
                    value,
                }
            }
            _ => ValueArray::Base(ValueVec::read(ar, &tag.basic_type(), size, count)?),
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A, tag: &PropertyTagFull) -> Result<()> {
        match &self {
            ValueArray::Struct {
                type_,
                struct_type,
                id,
                value,
            } => {
                ar.write_u32::<LE>(value.len() as u32)?;

                if !ar.version().property_tag() && ar.version().array_inner_tag() {
                    ar.write_string(&tag.name)?;
                    type_.write(ar)?;

                    // Write placeholder size
                    let size_pos = ar.stream_position()?;
                    ar.write_u32::<LE>(0)?;
                    ar.write_u32::<LE>(0)?;
                    struct_type.write(ar)?;
                    if let Some(id) = id {
                        id.write(ar)?;
                    }
                    ar.write_u8(0)?;

                    // Write data and measure size
                    let data_start = ar.stream_position()?;
                    for v in value {
                        v.write(ar)?;
                    }
                    let data_end = ar.stream_position()?;
                    let size = (data_end - data_start) as u32;

                    // Seek back and write actual size
                    ar.seek(std::io::SeekFrom::Start(size_pos))?;
                    ar.write_u32::<LE>(size)?;
                    ar.seek(std::io::SeekFrom::Start(data_end))?;
                } else {
                    for v in value {
                        v.write(ar)?;
                    }
                }
            }
            ValueArray::Base(vec) => {
                vec.write(ar)?;
            }
        }
        Ok(())
    }
}
impl ValueSet {
    #[instrument(name = "ValueSet_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A, t: &PropertyTagDataFull, size: u32) -> Result<ValueSet> {
        let count = ar.read_u32::<LE>()?;
        Ok(match t {
            PropertyTagDataFull::Struct { struct_type, .. } => {
                ValueSet::Struct(read_array(count, ar, |r| {
                    StructValue::read(r, struct_type)
                })?)
            }
            _ => ValueSet::Base(ValueVec::read(ar, &t.basic_type(), size, count)?),
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        match &self {
            ValueSet::Struct(value) => {
                ar.write_u32::<LE>(value.len() as u32)?;
                for v in value {
                    v.write(ar)?;
                }
            }
            ValueSet::Base(vec) => {
                vec.write(ar)?;
            }
        }
        Ok(())
    }
}

/// Properties consist of an ID and a value and are present in [`Root`] and [`StructValue::Struct`]
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Property {
    pub tag: PropertyTagPartial,
    #[serde(flatten)]
    pub inner: PropertyInner,
}

/// Properties consist of an ID and a value and are present in [`Root`] and [`StructValue::Struct`]
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum PropertyInner {
    Int8(Int8),
    Int16(Int16),
    Int(Int),
    Int64(Int64),
    UInt8(UInt8),
    UInt16(UInt16),
    UInt32(UInt32),
    UInt64(UInt64),
    Float(Float),
    Double(Double),
    Bool(Bool),
    Byte(Byte),
    Enum(Enum),
    Str(String),
    FieldPath(FieldPath),
    SoftObject(SoftObjectPath),
    Name(String),
    Object(String),
    Text(Text),
    Delegate(Delegate),
    MulticastDelegate(MulticastDelegate),
    MulticastInlineDelegate(MulticastInlineDelegate),
    MulticastSparseDelegate(MulticastSparseDelegate),
    Set(ValueSet),
    Map(Vec<MapEntry>),
    Struct(StructValue),
    Array(ValueArray),
    /// Raw property data when parsing fails
    Raw(Vec<u8>),
}

impl Property {
    #[instrument(name = "Property_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A, tag: PropertyTagFull) -> Result<Property> {
        if tag.data.has_raw_struct() {
            let mut raw = vec![0; tag.size as usize];
            ar.read_exact(&mut raw)?;
            return Ok(Property {
                tag: tag.into_partial(),
                inner: PropertyInner::Raw(raw),
            });
        }
        // Save the current position before attempting to parse
        let start_position = ar.stream_position()?;

        // Try to parse the property directly from the stream
        let inner = match Self::try_read_inner(ar, &tag) {
            Ok(inner) => inner,
            Err(e) => {
                // Parsing failed, seek back to start and read raw data
                if ar.log() {
                    eprintln!("Warning: Failed to parse property '{}': {}", tag.name, e);
                }
                ar.seek(std::io::SeekFrom::Start(start_position))?;
                let mut property_data = vec![0u8; tag.size as usize];
                ar.read_exact(&mut property_data)?;
                PropertyInner::Raw(property_data)
            }
        };

        Ok(Property {
            tag: tag.into_partial(),
            inner,
        })
    }

    fn try_read_inner<A: ArchiveReader>(
        ar: &mut A,
        tag: &PropertyTagFull,
    ) -> Result<PropertyInner> {
        Ok(match &tag.data {
            PropertyTagDataFull::Bool(value) => PropertyInner::Bool(*value),
            PropertyTagDataFull::Byte(ref enum_type) => {
                let value = if enum_type.is_none() {
                    Byte::Byte(ar.read_u8()?)
                } else {
                    Byte::Label(ar.read_string()?)
                };
                PropertyInner::Byte(value)
            }
            PropertyTagDataFull::Enum { .. } => PropertyInner::Enum(ar.read_string()?),
            PropertyTagDataFull::Set { key_type } => {
                ar.read_u32::<LE>()?;
                PropertyInner::Set(ValueSet::read(ar, key_type, tag.size - 8)?)
            }
            PropertyTagDataFull::Map {
                key_type,
                value_type,
            } => {
                ar.read_u32::<LE>()?;
                let count = ar.read_u32::<LE>()?;
                let mut value = vec![];

                for _ in 0..count {
                    value.push(MapEntry::read(ar, key_type, value_type)?)
                }

                PropertyInner::Map(value)
            }
            PropertyTagDataFull::Struct { struct_type, .. } => {
                PropertyInner::Struct(StructValue::read(ar, struct_type)?)
            }
            PropertyTagDataFull::Array(data) => {
                PropertyInner::Array(ValueArray::read(ar, *data.clone(), tag.size - 4)?)
            }
            PropertyTagDataFull::Other(t) => match t {
                PropertyType::BoolProperty
                | PropertyType::ByteProperty
                | PropertyType::EnumProperty
                | PropertyType::SetProperty
                | PropertyType::MapProperty
                | PropertyType::StructProperty
                | PropertyType::ArrayProperty => unreachable!(),
                PropertyType::Int8Property => PropertyInner::Int8(ar.read_i8()?),
                PropertyType::Int16Property => PropertyInner::Int16(ar.read_i16::<LE>()?),
                PropertyType::IntProperty => PropertyInner::Int(ar.read_i32::<LE>()?),
                PropertyType::Int64Property => PropertyInner::Int64(ar.read_i64::<LE>()?),
                PropertyType::UInt8Property => PropertyInner::UInt8(ar.read_u8()?),
                PropertyType::UInt16Property => PropertyInner::UInt16(ar.read_u16::<LE>()?),
                PropertyType::UInt32Property => PropertyInner::UInt32(ar.read_u32::<LE>()?),
                PropertyType::UInt64Property => PropertyInner::UInt64(ar.read_u64::<LE>()?),
                PropertyType::FloatProperty => PropertyInner::Float(ar.read_f32::<LE>()?.into()),
                PropertyType::DoubleProperty => PropertyInner::Double(ar.read_f64::<LE>()?.into()),
                PropertyType::NameProperty => PropertyInner::Name(ar.read_string()?),
                PropertyType::StrProperty => PropertyInner::Str(ar.read_string()?),
                PropertyType::FieldPathProperty => PropertyInner::FieldPath(FieldPath::read(ar)?),
                PropertyType::SoftObjectProperty => {
                    PropertyInner::SoftObject(SoftObjectPath::read(ar)?)
                }
                PropertyType::ObjectProperty => PropertyInner::Object(ar.read_object_ref()?),
                PropertyType::TextProperty => PropertyInner::Text(Text::read(ar)?),
                PropertyType::DelegateProperty => PropertyInner::Delegate(Delegate::read(ar)?),
                PropertyType::MulticastDelegateProperty => {
                    PropertyInner::MulticastDelegate(MulticastDelegate::read(ar)?)
                }
                PropertyType::MulticastInlineDelegateProperty => {
                    PropertyInner::MulticastInlineDelegate(MulticastInlineDelegate::read(ar)?)
                }
                PropertyType::MulticastSparseDelegateProperty => {
                    PropertyInner::MulticastSparseDelegate(MulticastSparseDelegate::read(ar)?)
                }
            },
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A, tag: &PropertyTagFull) -> Result<()> {
        match &self.inner {
            PropertyInner::Int8(value) => {
                ar.write_i8(*value)?;
            }
            PropertyInner::Int16(value) => {
                ar.write_i16::<LE>(*value)?;
            }
            PropertyInner::Int(value) => {
                ar.write_i32::<LE>(*value)?;
            }
            PropertyInner::Int64(value) => {
                ar.write_i64::<LE>(*value)?;
            }
            PropertyInner::UInt8(value) => {
                ar.write_u8(*value)?;
            }
            PropertyInner::UInt16(value) => {
                ar.write_u16::<LE>(*value)?;
            }
            PropertyInner::UInt32(value) => {
                ar.write_u32::<LE>(*value)?;
            }
            PropertyInner::UInt64(value) => {
                ar.write_u64::<LE>(*value)?;
            }
            PropertyInner::Float(value) => {
                ar.write_f32::<LE>((*value).into())?;
            }
            PropertyInner::Double(value) => {
                ar.write_f64::<LE>((*value).into())?;
            }
            PropertyInner::Bool(_) => {}
            PropertyInner::Byte(value) => match value {
                Byte::Byte(b) => {
                    ar.write_u8(*b)?;
                }
                Byte::Label(l) => {
                    ar.write_string(l)?;
                }
            },
            PropertyInner::Enum(value) => {
                ar.write_string(value)?;
            }
            PropertyInner::Name(value) => {
                ar.write_string(value)?;
            }
            PropertyInner::Str(value) => {
                ar.write_string(value)?;
            }
            PropertyInner::FieldPath(value) => {
                value.write(ar)?;
            }
            PropertyInner::SoftObject(value) => {
                value.write(ar)?;
            }
            PropertyInner::Object(value) => {
                ar.write_object_ref(value)?;
            }
            PropertyInner::Text(value) => {
                value.write(ar)?;
            }
            PropertyInner::Delegate(value) => {
                value.write(ar)?;
            }
            PropertyInner::MulticastDelegate(value) => {
                value.write(ar)?;
            }
            PropertyInner::MulticastInlineDelegate(value) => {
                value.write(ar)?;
            }
            PropertyInner::MulticastSparseDelegate(value) => {
                value.write(ar)?;
            }
            PropertyInner::Set(value) => {
                ar.write_u32::<LE>(0)?;
                value.write(ar)?;
            }
            PropertyInner::Map(value) => {
                ar.write_u32::<LE>(0)?;
                ar.write_u32::<LE>(value.len() as u32)?;
                for v in value {
                    v.write(ar)?;
                }
            }
            PropertyInner::Struct(value) => {
                value.write(ar)?;
            }
            PropertyInner::Array(value) => {
                value.write(ar, tag)?;
            }
            PropertyInner::Raw(value) => {
                ar.write_all(value)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustomFormatData {
    pub id: FGuid,
    pub value: i32,
}
impl CustomFormatData {
    #[instrument(name = "CustomFormatData_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(CustomFormatData {
            id: FGuid::read(ar)?,
            value: ar.read_i32::<LE>()?,
        })
    }
}
impl CustomFormatData {
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.id.write(ar)?;
        ar.write_i32::<LE>(self.value)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageVersion {
    ue4: u32,
    ue5: Option<u32>,
}

pub trait VersionInfo {
    fn engine_version_major(&self) -> u16;
    fn engine_version_minor(&self) -> u16;
    fn engine_version_patch(&self) -> u16;
    fn package_file_version_ue4(&self) -> u32;
    fn package_file_version_ue5(&self) -> u32;

    /// Whether the engine uses large world coordinates (FVector with doubles)
    fn large_world_coordinates(&self) -> bool {
        self.engine_version_major() >= 5
    }

    /// Whether property tags include complete type names
    fn property_tag(&self) -> bool {
        // PROPERTY_TAG_COMPLETE_TYPE_NAME
        (self.engine_version_major(), self.engine_version_minor()) >= (5, 4)
    }

    /// Whether property tags include GUIDs
    fn property_guid(&self) -> bool {
        // VER_UE4_PROPERTY_GUID_IN_PROPERTY_TAG
        (self.engine_version_major(), self.engine_version_minor()) >= (4, 12)
    }

    /// Whether array properties have inner type tags
    fn array_inner_tag(&self) -> bool {
        // VAR_UE4_ARRAY_PROPERTY_INNER_TAGS
        (self.engine_version_major(), self.engine_version_minor()) >= (4, 12)
    }

    /// Whether asset paths should not use FNames
    fn remove_asset_path_fnames(&self) -> bool {
        // FSOFTOBJECTPATH_REMOVE_ASSET_PATH_FNAMES
        self.package_file_version_ue5() >= 1007
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
    pub magic: u32,
    pub save_game_version: u32,
    pub package_version: PackageVersion,
    pub engine_version_major: u16,
    pub engine_version_minor: u16,
    pub engine_version_patch: u16,
    pub engine_version_build: u32,
    pub engine_version: String,
    pub custom_version: Option<(u32, Vec<CustomFormatData>)>,
}
impl VersionInfo for Header {
    fn engine_version_major(&self) -> u16 {
        self.engine_version_major
    }
    fn engine_version_minor(&self) -> u16 {
        self.engine_version_minor
    }
    fn engine_version_patch(&self) -> u16 {
        self.engine_version_patch
    }
    fn package_file_version_ue4(&self) -> u32 {
        self.package_version.ue4
    }
    fn package_file_version_ue5(&self) -> u32 {
        self.package_version.ue5.unwrap_or(0)
    }
}
impl Header {
    #[instrument(name = "Header_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let magic = ar.read_u32::<LE>()?;
        if ar.log() && magic != u32::from_le_bytes(*b"GVAS") {
            eprintln!(
                "Found non-standard magic: {:02x?} ({}) expected: GVAS, continuing to parse...",
                &magic.to_le_bytes(),
                String::from_utf8_lossy(&magic.to_le_bytes())
            );
        }
        let save_game_version = ar.read_u32::<LE>()?;
        let package_version = PackageVersion {
            ue4: ar.read_u32::<LE>()?,
            ue5: (save_game_version >= 3 && save_game_version != 34) // TODO 34 is probably a game specific version
                .then(|| ar.read_u32::<LE>())
                .transpose()?,
        };
        let engine_version_major = ar.read_u16::<LE>()?;
        let engine_version_minor = ar.read_u16::<LE>()?;
        let engine_version_patch = ar.read_u16::<LE>()?;
        let engine_version_build = ar.read_u32::<LE>()?;
        let engine_version = ar.read_string()?;
        let custom_version = if (engine_version_major, engine_version_minor) >= (4, 12) {
            Some((
                ar.read_u32::<LE>()?,
                read_array(ar.read_u32::<LE>()?, ar, CustomFormatData::read)?,
            ))
        } else {
            None
        };
        Ok(Header {
            magic,
            save_game_version,
            package_version,
            engine_version_major,
            engine_version_minor,
            engine_version_patch,
            engine_version_build,
            engine_version,
            custom_version,
        })
    }
}
impl Header {
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.magic)?;
        ar.write_u32::<LE>(self.save_game_version)?;
        ar.write_u32::<LE>(self.package_version.ue4)?;
        if let Some(ue5) = self.package_version.ue5 {
            ar.write_u32::<LE>(ue5)?;
        }
        ar.write_u16::<LE>(self.engine_version_major)?;
        ar.write_u16::<LE>(self.engine_version_minor)?;
        ar.write_u16::<LE>(self.engine_version_patch)?;
        ar.write_u32::<LE>(self.engine_version_build)?;
        ar.write_string(&self.engine_version)?;
        if let Some((custom_format_version, custom_format)) = &self.custom_version {
            ar.write_u32::<LE>(*custom_format_version)?;
            ar.write_u32::<LE>(custom_format.len() as u32)?;
            for cf in custom_format {
                cf.write(ar)?;
            }
        }
        Ok(())
    }
}

/// Root struct inside a save file which holds both the Unreal Engine class name and list of properties
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Root {
    pub save_game_type: String,
    pub properties: Properties,
}
impl Root {
    #[instrument(name = "Root_read", skip_all)]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let save_game_type = ar.read_string()?;
        if ar.version().property_tag() {
            ar.read_u8()?;
        }
        let properties = read_properties_until_none(ar)?;
        Ok(Self {
            save_game_type,
            properties,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_string(&self.save_game_type)?;
        if ar.version().property_tag() {
            ar.write_u8(0)?;
        }
        write_properties_none_terminated(ar, &self.properties)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Save {
    pub header: Header,
    pub root: Root,
    pub extra: Vec<u8>,
}
impl Save {
    /// Reads save from the given reader
    #[instrument(name = "Root_read", skip_all)]
    pub fn read<R: Read>(reader: &mut R) -> Result<Self, ParseError> {
        Self::read_with_types(reader, Types::new())
    }
    /// Reads save from the given reader using the provided [`Types`]
    #[instrument(name = "Save_read_with_types", skip_all)]
    pub fn read_with_types<R: Read>(reader: &mut R, types: Types) -> Result<Self, ParseError> {
        SaveReader::new().types(types).read(reader)
    }
    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut buffer = vec![];
        SaveGameArchive::run(&mut Cursor::new(&mut buffer), |writer| -> Result<()> {
            writer.set_version(self.header.clone());
            self.header.write(writer)?;
            self.root.write(writer)?;
            writer.write_all(&self.extra)?;
            Ok(())
        })?;
        writer.write_all(&buffer)?;
        Ok(())
    }
}

pub struct SaveReader {
    log: bool,
    types: Option<Rc<Types>>,
}
impl Default for SaveReader {
    fn default() -> Self {
        Self::new()
    }
}
impl SaveReader {
    pub fn new() -> Self {
        Self {
            log: false,
            types: None,
        }
    }
    pub fn log(mut self, log: bool) -> Self {
        self.log = log;
        self
    }
    pub fn types(mut self, types: Types) -> Self {
        self.types = Some(Rc::new(types));
        self
    }
    pub fn read<S: Read>(self, stream: S) -> Result<Save, ParseError> {
        let types = self.types.unwrap_or_else(|| Rc::new(Types::new()));

        let stream = SeekReader::new(stream);
        let mut reader = SaveGameArchive {
            stream,
            version: None,
            types,
            scope: Scope::root(),
            log: self.log,
        };

        || -> Result<Save> {
            let header = Header::read(&mut reader)?;
            reader.set_version(header.clone());

            let root = Root::read(&mut reader)?;
            let extra = {
                let mut buf = vec![];
                reader.read_to_end(&mut buf)?;
                if reader.log() && buf != [0; 4] {
                    eprintln!(
                        "{} extra bytes. Save may not have been parsed completely.",
                        buf.len()
                    );
                }
                buf
            };

            Ok(Save {
                header,
                root,
                extra,
            })
        }()
        .map_err(|e| error::ParseError {
            offset: reader.stream_position().unwrap() as usize, // our own implemenation which cannot fail
            error: e,
        })
    }
}
