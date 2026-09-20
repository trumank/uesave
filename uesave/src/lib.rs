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

use uesave::{Property, Save};

let save = Save::read(&mut File::open("drg-save-test.sav")?)?;
match save.root.properties["NumberOfGamesPlayed"] {
    Property::Int(value) => {
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
mod game;
pub mod games;
mod serialization;
pub mod compat;

#[cfg(test)]
mod tests;

pub use archive::{ArchiveReader, ArchiveType, ArchiveWriter, SaveGameArchiveType};
pub use context::{PropertySchemas, SaveGameArchive, Scope, Types};
pub use error::{Error, ParseError};
pub use game::{Game, GameStruct, NoGame};

use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use std::{
    borrow::Cow,
    cell::RefCell,
    io::{Cursor, Read, Seek, Write},
    rc::Rc,
};

use serde::{de::Visitor, Deserialize, Deserializer, Serialize, Serializer};

#[cfg(feature = "tracing")]
use tracing::instrument;

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

#[cfg_attr(feature = "tracing", instrument(skip_all))]
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

#[cfg_attr(feature = "tracing", instrument(skip_all, ret))]
pub fn read_string<A: ArchiveReader>(ar: &mut A) -> Result<String> {
    let len = ar.read_i32::<LE>()?;
    if len < 0 {
        let chars = read_array((-len) as u32, ar, |r| Ok(r.read_u16::<LE>()?))?;
        let length = chars.iter().position(|&c| c == 0).unwrap_or(chars.len());
        Ok(match String::from_utf16(&chars[..length]) {
            Ok(s) => s,
            Err(_) => {
                if ar.log() {
                    eprintln!(
                        "Warning: UTF-16 decoding error at '{}', using lossy conversion. Data loss may occur.",
                        ar.path()
                    );
                }
                String::from_utf16_lossy(&chars[..length])
            }
        })
    } else {
        let mut chars = vec![0; len as usize];
        ar.read_exact(&mut chars)?;
        let length = chars.iter().position(|&c| c == 0).unwrap_or(chars.len());
        Ok(String::from_utf8_lossy(&chars[..length]).into_owned())
    }
}
#[cfg_attr(feature = "tracing", instrument(skip(ar)))]
pub fn write_string<A: ArchiveWriter>(ar: &mut A, string: &str) -> Result<()> {
    if string.is_empty() {
        ar.write_u32::<LE>(0)?;
    } else {
        write_string_trailing(ar, string, None)?;
    }
    Ok(())
}

#[cfg_attr(feature = "tracing", instrument(skip_all))]
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
        let string = match String::from_utf16(&chars) {
            Ok(s) => s,
            Err(_) => {
                if ar.log() {
                    eprintln!(
                        "Warning: UTF-16 decoding error in trailing string at '{}', using lossy conversion. Data loss may occur.",
                        ar.path()
                    );
                }
                String::from_utf16_lossy(&chars)
            }
        };
        Ok((string, rest))
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
        let string = match String::from_utf8(chars) {
            Ok(s) => s,
            Err(e) => {
                if ar.log() {
                    eprintln!(
                        "Warning: UTF-8 decoding error in trailing string at '{}', using lossy conversion. Data loss may occur.",
                        ar.path()
                    );
                }
                String::from_utf8_lossy(&e.into_bytes()).into_owned()
            }
        };
        Ok((string, rest))
    }
}
#[cfg_attr(feature = "tracing", instrument(skip_all))]
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

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(bound(serialize = "T::ObjectRef: Serialize"))]
pub struct Properties<T: ArchiveType = SaveGameArchiveType>(
    pub indexmap::IndexMap<PropertyKey, Property<T>>,
);
impl<T: ArchiveType> Properties<T> {
    pub fn insert(&mut self, k: impl Into<PropertyKey>, v: Property<T>) -> Option<Property<T>> {
        self.0.insert(k.into(), v)
    }
}
impl<K, T: ArchiveType> std::ops::Index<K> for Properties<T>
where
    K: Into<PropertyKey>,
{
    type Output = Property<T>;
    fn index(&self, index: K) -> &Self::Output {
        self.0.index(&index.into())
    }
}
impl<K, T: ArchiveType> std::ops::IndexMut<K> for Properties<T>
where
    K: Into<PropertyKey>,
{
    fn index_mut(&mut self, index: K) -> &mut Property<T> {
        self.0.index_mut(&index.into())
    }
}
impl<'a, T: ArchiveType> IntoIterator for &'a Properties<T> {
    type Item = <&'a indexmap::IndexMap<PropertyKey, Property<T>> as IntoIterator>::Item;
    type IntoIter = <&'a indexmap::IndexMap<PropertyKey, Property<T>> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[cfg_attr(feature = "tracing", instrument(skip_all))]
pub fn read_properties_until_none<T: ArchiveType, A: ArchiveReader<ArchiveType = T>>(
    ar: &mut A,
) -> Result<Properties<T>> {
    let mut properties = Properties::default();
    while let Some((name, prop)) = read_property(ar)? {
        properties.insert(name, prop);
    }
    Ok(properties)
}
#[cfg_attr(feature = "tracing", instrument(skip_all))]
pub fn write_properties_none_terminated<T: ArchiveType, A: ArchiveWriter<ArchiveType = T>>(
    ar: &mut A,
    properties: &Properties<T>,
) -> Result<()> {
    for p in properties {
        write_property(p, ar)?;
    }
    ar.write_string("None")?;
    Ok(())
}

#[cfg_attr(feature = "tracing", instrument(skip_all))]
fn read_property<T: ArchiveType, A: ArchiveReader<ArchiveType = T>>(
    ar: &mut A,
) -> Result<Option<(PropertyKey, Property<T>)>> {
    if let Some(mut tag) = PropertyTagFull::read(ar)? {
        let tag_name = tag.name.to_string();
        ar.scope().push(&tag_name);
        let result = Property::read(ar, tag.clone());
        ar.scope().pop();
        let (value, updated_tag_data) = result?;

        // If type information was refined during reading (e.g., array of structs in older UE versions),
        // update the tag data and record the complete schema
        if let Some(new_data) = updated_tag_data {
            tag.data = new_data;
        }

        let key = PropertyKey(tag.index, tag_name.clone());

        // Post-process (may convert game-specific embedded data and refine the
        // tag further) and record the final, complete schema
        ar.scope().push(&tag_name);
        let result = (|ar: &mut A| -> Result<Property<T>> {
            let mut partial = tag.into_partial();
            let value = ar.post_process_property(&mut partial, value)?;
            ar.record_schema(ar.path().to_string(), partial);
            Ok(value)
        })(ar);
        ar.scope().pop();

        Ok(Some((key, result?)))
    } else {
        Ok(None)
    }
}
#[cfg_attr(feature = "tracing", instrument(skip_all))]
fn write_property<T: ArchiveType, A: ArchiveWriter<ArchiveType = T>>(
    prop: (&PropertyKey, &Property<T>),
    ar: &mut A,
) -> Result<()> {
    ar.scope().push(&prop.0 .1);
    let result = (|| {
        let tag_partial = ar
            .get_schema(&ar.path())
            .ok_or_else(|| Error::MissingPropertySchema(ar.path()))?;

        // Pre-process (may convert game-specific typed values back into their
        // embedded representation, replacing both tag and value)
        let (tag_partial, converted) = match ar.pre_write_property(prop.0, &tag_partial, prop.1)? {
            Some((tag_partial, converted)) => (tag_partial, Some(converted)),
            None => (tag_partial, None),
        };
        let value = converted.as_ref().unwrap_or(prop.1);

        let mut tag = tag_partial.into_full(&prop.0 .1, 0, prop.0 .0, value);

        // Write tag with placeholder size
        tag.size = 0;
        let tag_start = ar.stream_position()?;
        tag.write(ar)?;
        let data_start = ar.stream_position()?;

        // Write the actual property data
        value.write(ar, &tag)?;
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
    })();
    ar.scope().pop();
    result
}

/// Upper bound on capacity pre-reserved from a length read out of the stream.
/// A corrupt or speculatively-misread count must not reach `Vec::with_capacity`
/// unchecked: on 32-bit targets (wasm32) a capacity past `isize::MAX` (~2 GB) is
/// an uncatchable `capacity overflow` panic rather than a recoverable error. The
/// vec still grows as elements are read, so a garbage length now fails with a
/// read error instead of aborting the whole parse.
pub(crate) const UNTRUSTED_PREALLOC_CAP: usize = 4096;

pub(crate) fn bounded_prealloc(count: usize) -> usize {
    count.min(UNTRUSTED_PREALLOC_CAP)
}

#[cfg(test)]
mod bounded_prealloc_tests {
    use super::{bounded_prealloc, UNTRUSTED_PREALLOC_CAP};

    #[test]
    fn caps_untrusted_length_but_keeps_small_ones() {
        assert_eq!(bounded_prealloc(0), 0);
        assert_eq!(bounded_prealloc(10), 10);
        assert_eq!(
            bounded_prealloc(UNTRUSTED_PREALLOC_CAP),
            UNTRUSTED_PREALLOC_CAP
        );
        // A garbage count never reaches with_capacity unchecked.
        assert_eq!(bounded_prealloc(u32::MAX as usize), UNTRUSTED_PREALLOC_CAP);
    }
}

#[cfg_attr(feature = "tracing", instrument(skip_all))]
fn read_array<T, F, A: ArchiveReader>(length: u32, ar: &mut A, f: F) -> Result<Vec<T>>
where
    F: Fn(&mut A) -> Result<T>,
{
    let mut out = Vec::with_capacity(bounded_prealloc(length as usize));
    for _ in 0..length {
        out.push(f(ar)?);
    }
    Ok(out)
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
                FGuid::parse_str(value).map_err(|e| E::custom(format!("Invalid UUID: {e}")))
            }
        }

        deserializer.deserialize_str(FGuidVisitor)
    }
}

impl FGuid {
    #[cfg_attr(feature = "tracing", instrument(name = "FGuid_read", skip_all))]
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
    #[cfg_attr(feature = "tracing", instrument(name = "FGuid_write", skip_all))]
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
    fn into_full<T: ArchiveType>(self, prop: &Property<T>) -> PropertyTagDataFull {
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
            Self::Other(PropertyType::BoolProperty) => PropertyTagDataFull::Bool(match prop {
                Property::Bool(value) => *value,
                _ => false,
            }),
            Self::Other(t) => PropertyTagDataFull::Other(t),
        }
    }
    pub(crate) fn has_raw_struct(&self) -> bool {
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
            Self::Other(_) => false,
        }
    }
}

impl PropertyTagDataFull {
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
    fn into_full<'a, T: ArchiveType>(
        self,
        name: &'a str,
        size: u32,
        index: u32,
        prop: &Property<T>,
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
    #[cfg_attr(feature = "tracing", instrument(name = "PropertyTag_read", skip_all))]
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
            ar.scope().push(&name.clone());
            let result = (|| {
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
                    | PropertyType::InterfaceProperty
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
                                ar.scope().push("Key");
                                let result = ar.get_type_or(&StructType::Guid);
                                ar.scope().pop();
                                Some(result?)
                            }
                            _ => None,
                        };
                        let value_type = PropertyType::read(ar)?;
                        let value_struct_type = match value_type {
                            PropertyType::StructProperty => {
                                ar.scope().push("Value");
                                let result = ar.get_type_or(&StructType::Struct(None));
                                ar.scope().pop();
                                Some(result?)
                            }
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
            })();
            ar.scope().pop();
            result
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
                // `full_type` must be a `Package.Name` path here. `StructType::Game(name)`
                // holds only a bare name (no dot) because game structs are embedded as
                // byte arrays and never reach this struct-header write path directly (the
                // write hook swaps the property tag to Array/Byte first) — guard against
                // that invariant breaking instead of panicking.
                let (a, b) = full_type.split_once('.').ok_or_else(|| {
                    crate::Error::Other(format!(
                        "write_full_type: expected a `Package.Name` path, got bare name {full_type:?} \
                         (a StructType::Game value reached the struct-header write path unexpectedly)"
                    ))
                })?;
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

            if self.index != 0 {
                flags |= EPropertyTagFlags::HasArrayIndex;
            }
            if self.id.is_some() {
                flags |= EPropertyTagFlags::HasPropertyGuid;
            }

            ar.write_u8(flags.bits())?;

            if self.index != 0 {
                ar.write_u32::<LE>(self.index)?;
            }
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

macro_rules! define_property_types {
    ($($variant:ident),* $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        pub enum PropertyType {
            $($variant,)*
        }

        impl PropertyType {
            fn get_name(&self) -> &str {
                match self {
                    $(PropertyType::$variant => stringify!($variant),)*
                }
            }

            #[cfg_attr(feature = "tracing", instrument(name = "PropertyType_read", skip_all))]
            fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
                Self::try_from(&ar.read_string()?)
            }

            fn try_from(name: &str) -> Result<Self> {
                match name {
                    $(stringify!($variant) => Ok(PropertyType::$variant),)*
                    _ => Err(Error::UnknownPropertyType(format!("{name:?}"))),
                }
            }

            fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
                ar.write_string(self.get_name())?;
                Ok(())
            }
        }
    };
}

define_property_types! {
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
    InterfaceProperty,
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

macro_rules! define_struct_types {
    ($(($package:literal, $variant:ident)),* $(,)?) => {
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub enum StructType {
            $($variant,)*
            /// A game-specific struct type (see [`Game`]). Holds the bare
            /// `Xxx` type name; resolved against the active game's
            /// [`Game::is_game_struct_type`] / [`Game::read_struct`].
            Game(String),
            Raw(String),
            Struct(Option<String>),
        }

        // `StructType::Game(name)` serializes as the bare type-name string, so a
        // former unit variant (`StructType::PalCharacterData` → `"PalCharacterData"`)
        // and its `Game("PalCharacterData")` replacement produce identical JSON.
        // `Raw`/`Struct` keep their externally-tagged map form; they are therefore
        // never encoded as a bare string, so any bare string that is not a known
        // core variant name deserializes back to `Game`.
        impl Serialize for StructType {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                match self {
                    $(StructType::$variant => serializer.serialize_str(stringify!($variant)),)*
                    StructType::Game(name) => serializer.serialize_str(name),
                    StructType::Raw(t) => {
                        serializer.serialize_newtype_variant("StructType", 0, "Raw", t)
                    }
                    StructType::Struct(t) => {
                        serializer.serialize_newtype_variant("StructType", 1, "Struct", t)
                    }
                }
            }
        }

        impl<'de> Deserialize<'de> for StructType {
            fn deserialize<D: Deserializer<'de>>(
                deserializer: D,
            ) -> std::result::Result<Self, D::Error> {
                struct StructTypeVisitor;
                impl<'de> serde::de::Visitor<'de> for StructTypeVisitor {
                    type Value = StructType;
                    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                        f.write_str("a struct type name string or a tagged Raw/Struct map")
                    }
                    fn visit_str<E: serde::de::Error>(
                        self,
                        v: &str,
                    ) -> std::result::Result<StructType, E> {
                        Ok(match v {
                            $(stringify!($variant) => StructType::$variant,)*
                            "Struct" => StructType::Struct(None),
                            _ => StructType::Game(v.to_owned()),
                        })
                    }
                    fn visit_map<A: serde::de::MapAccess<'de>>(
                        self,
                        mut map: A,
                    ) -> std::result::Result<StructType, A::Error> {
                        let key: String = map
                            .next_key()?
                            .ok_or_else(|| serde::de::Error::custom("empty StructType map"))?;
                        match key.as_str() {
                            "Raw" => Ok(StructType::Raw(map.next_value()?)),
                            "Struct" => Ok(StructType::Struct(map.next_value()?)),
                            other => Err(serde::de::Error::custom(format!(
                                "unknown StructType variant {other:?}"
                            ))),
                        }
                    }
                }
                deserializer.deserialize_any(StructTypeVisitor)
            }
        }

        impl From<&str> for StructType {
            fn from(t: &str) -> Self {
                match t {
                    $(stringify!($variant) => StructType::$variant,)*
                    "Struct" => StructType::Struct(None),
                    _ => StructType::Struct(Some(t.to_owned())),
                }
            }
        }

        impl From<String> for StructType {
            fn from(t: String) -> Self {
                match t.as_str() {
                    $(stringify!($variant) => StructType::$variant,)*
                    "Struct" => StructType::Struct(None),
                    _ => StructType::Struct(Some(t)),
                }
            }
        }

        impl StructType {
            pub fn from_full(t: &str, raw: bool) -> Self {
                match t {
                    $(concat!($package, ".", stringify!($variant)) => StructType::$variant,)*
                    "/Script/CoreUObject.Struct" => StructType::Struct(None),
                    _ if raw => StructType::Raw(t.to_owned()),
                    _ => StructType::Struct(Some(t.to_owned())),
                }
            }

            pub fn full_str(&self) -> &str {
                match self {
                    $(StructType::$variant => concat!($package, ".", stringify!($variant)),)*
                    // Game structs are embedded as byte arrays and never written
                    // as struct headers, so this path holds only the bare name.
                    StructType::Game(t) => t,
                    StructType::Raw(t) => t,
                    StructType::Struct(Some(t)) => t,
                    _ => unreachable!(),
                }
            }

            pub fn as_str(&self) -> &str {
                match self {
                    $(StructType::$variant => stringify!($variant),)*
                    StructType::Game(t) => t,
                    StructType::Raw(t) => t,
                    StructType::Struct(Some(t)) => t,
                    _ => unreachable!(),
                }
            }

            #[cfg_attr(feature = "tracing", instrument(name = "StructType_read", skip_all))]
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
    };
}

define_struct_types! {
    ("/Script/CoreUObject", Guid),
    ("/Script/CoreUObject", DateTime),
    ("/Script/CoreUObject", Timespan),
    ("/Script/CoreUObject", Vector2D),
    ("/Script/CoreUObject", Vector),
    ("/Script/CoreUObject", Vector4),
    ("/Script/CoreUObject", IntVector),
    ("/Script/CoreUObject", Box),
    ("/Script/CoreUObject", Box2D),
    ("/Script/CoreUObject", IntPoint),
    ("/Script/CoreUObject", Quat),
    ("/Script/CoreUObject", Rotator),
    ("/Script/CoreUObject", LinearColor),
    ("/Script/CoreUObject", Color),
    ("/Script/CoreUObject", SoftObjectPath),
    ("/Script/CoreUObject", SoftClassPath),
    ("/Script/GameplayTags", GameplayTagContainer),
    ("/Script/Engine", UniqueNetIdRepl),
    ("/Script/Engine", KeyHandleMap),
    ("/Script/Engine", RichCurveKey),
    ("/Script/Engine", SkeletalMeshSamplingLODBuiltData),
    ("/Script/Engine", PerPlatformFloat),
    ("/Script/CoreUObject", FrameNumber),
    ("/Script/Engine", ExpressionInput),
    ("/Script/Engine", MaterialAttributesInput),
    ("/Script/Engine", ColorMaterialInput),
    ("/Script/Engine", ScalarMaterialInput),
    ("/Script/Engine", ShadingModelMaterialInput),
    ("/Script/Engine", VectorMaterialInput),
    ("/Script/Engine", Vector2MaterialInput),
    ("/Script/MovieScene", MovieSceneFrameRange),
    ("/Script/MovieScene", MovieSceneFloatChannel),
    ("/Script/MovieScene", MovieSceneSequenceID),
    ("/Script/MovieScene", MovieSceneTrackIdentifier),
    ("/Script/MovieScene", MovieSceneEvaluationKey),
    ("/Script/MovieScene", MovieSceneEvaluationFieldEntityTree),
    ("/Script/SlateCore", FontData),
    ("/Script/ClothingSystemRuntimeCommon", ClothLODDataCommon),
    ("/Script/Niagara", NiagaraVariable),
    ("/Script/Niagara", NiagaraVariableBase),
    ("/Script/Niagara", NiagaraVariableWithOffset),
    ("/Script/NiagaraShader", NiagaraDataInterfaceGeneratedFunction),
    ("/Script/NiagaraShader", NiagaraDataInterfaceGPUParamInfo),
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

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MapEntry<T: ArchiveType = SaveGameArchiveType> {
    pub key: Property<T>,
    pub value: Property<T>,
}
impl<T: ArchiveType> MapEntry<T> {
    #[cfg_attr(feature = "tracing", instrument(name = "MapEntry_read", skip_all))]
    fn read<A: ArchiveReader<ArchiveType = T>>(
        ar: &mut A,
        key_type: &PropertyTagDataFull,
        value_type: &PropertyTagDataFull,
    ) -> Result<MapEntry<T>> {
        let key = Property::read_value(ar, key_type)?;
        let value = Property::read_value(ar, value_type)?;
        Ok(Self { key, value })
    }
    fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        self.key.write_value(ar)?;
        self.value.write_value(ar)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T::ObjectRef: Serialize",
    deserialize = "T::ObjectRef: Deserialize<'de>"
))]
pub struct FieldPath<T: ArchiveType = SaveGameArchiveType> {
    pub path: Vec<String>,
    pub owner: T::ObjectRef,
}
impl<T: ArchiveType> FieldPath<T> {
    #[cfg_attr(feature = "tracing", instrument(name = "FieldPath_read", skip_all))]
    fn read<A: ArchiveReader<ArchiveType = T>>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            path: read_array(ar.read_u32::<LE>()?, ar, |ar| ar.read_string())?,
            owner: ar.read_object_ref()?,
        })
    }
    fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.path.len() as u32)?;
        for p in &self.path {
            ar.write_string(p)?;
        }
        ar.write_object_ref(&self.owner)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T::ObjectRef: Serialize",
    deserialize = "T::ObjectRef: Deserialize<'de>"
))]
pub struct Delegate<T: ArchiveType = SaveGameArchiveType> {
    pub object: T::ObjectRef,
    pub delegate: String,
}
impl<T: ArchiveType> Delegate<T> {
    #[cfg_attr(feature = "tracing", instrument(name = "Delegate_read", skip_all))]
    fn read<A: ArchiveReader<ArchiveType = T>>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            object: ar.read_object_ref()?,
            delegate: ar.read_string()?,
        })
    }
    fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        ar.write_object_ref(&self.object)?;
        ar.write_string(&self.delegate)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T::ObjectRef: Serialize",
    deserialize = "T::ObjectRef: Deserialize<'de>"
))]
pub struct MulticastDelegate<T: ArchiveType = SaveGameArchiveType>(pub Vec<Delegate<T>>);
impl<T: ArchiveType> MulticastDelegate<T> {
    #[cfg_attr(
        feature = "tracing",
        instrument(name = "MulticastDelegate_read", skip_all)
    )]
    fn read<A: ArchiveReader<ArchiveType = T>>(ar: &mut A) -> Result<Self> {
        Ok(Self(read_array(ar.read_u32::<LE>()?, ar, Delegate::read)?))
    }
    fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.0.len() as u32)?;
        for entry in &self.0 {
            entry.write(ar)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T::ObjectRef: Serialize",
    deserialize = "T::ObjectRef: Deserialize<'de>"
))]
pub struct MulticastInlineDelegate<T: ArchiveType = SaveGameArchiveType>(pub Vec<Delegate<T>>);
impl<T: ArchiveType> MulticastInlineDelegate<T> {
    #[cfg_attr(
        feature = "tracing",
        instrument(name = "MulticastInlineDelegate_read", skip_all)
    )]
    fn read<A: ArchiveReader<ArchiveType = T>>(ar: &mut A) -> Result<Self> {
        Ok(Self(read_array(ar.read_u32::<LE>()?, ar, Delegate::read)?))
    }
    fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.0.len() as u32)?;
        for entry in &self.0 {
            entry.write(ar)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T::ObjectRef: Serialize",
    deserialize = "T::ObjectRef: Deserialize<'de>"
))]
pub struct MulticastSparseDelegate<T: ArchiveType = SaveGameArchiveType>(pub Vec<Delegate<T>>);
impl<T: ArchiveType> MulticastSparseDelegate<T> {
    #[cfg_attr(
        feature = "tracing",
        instrument(name = "MulticastSparseDelegate_read", skip_all)
    )]
    fn read<A: ArchiveReader<ArchiveType = T>>(ar: &mut A) -> Result<Self> {
        Ok(Self(read_array(ar.read_u32::<LE>()?, ar, Delegate::read)?))
    }
    fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.0.len() as u32)?;
        for entry in &self.0 {
            entry.write(ar)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LinearColor {
    pub r: Float,
    pub g: Float,
    pub b: Float,
    pub a: Float,
}
impl LinearColor {
    #[cfg_attr(feature = "tracing", instrument(name = "LinearColor_read", skip_all))]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Quat {
    pub x: Double,
    pub y: Double,
    pub z: Double,
    pub w: Double,
}
impl Quat {
    #[cfg_attr(feature = "tracing", instrument(name = "Quat_read", skip_all))]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Rotator {
    pub x: Double,
    pub y: Double,
    pub z: Double,
}
impl Rotator {
    #[cfg_attr(feature = "tracing", instrument(name = "Rotator_read", skip_all))]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Color {
    pub r: u8,
    pub g: u8,
    pub b: u8,
    pub a: u8,
}
impl Color {
    #[cfg_attr(feature = "tracing", instrument(name = "Color_read", skip_all))]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Vector {
    pub x: Double,
    pub y: Double,
    pub z: Double,
}
impl Vector {
    #[cfg_attr(feature = "tracing", instrument(name = "Vector_read", skip_all))]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Vector2D {
    pub x: Double,
    pub y: Double,
}
impl Vector2D {
    #[cfg_attr(feature = "tracing", instrument(name = "Vector2D_read", skip_all))]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Box2D {
    pub min: Vector2D,
    pub max: Vector2D,
    pub is_valid: bool,
}
impl Box2D {
    #[cfg_attr(feature = "tracing", instrument(name = "Box2D_read", skip_all))]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            min: Vector2D::read(ar)?,
            max: Vector2D::read(ar)?,
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Vector4 {
    pub x: Double,
    pub y: Double,
    pub z: Double,
    pub w: Double,
}
impl Vector4 {
    #[cfg_attr(feature = "tracing", instrument(name = "Vector4_read", skip_all))]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntVector {
    pub x: i32,
    pub y: i32,
    pub z: i32,
}
impl IntVector {
    #[cfg_attr(feature = "tracing", instrument(name = "IntVector_read", skip_all))]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Box {
    pub min: Vector,
    pub max: Vector,
    pub is_valid: bool,
}
impl Box {
    #[cfg_attr(feature = "tracing", instrument(name = "Box_read", skip_all))]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntPoint {
    pub x: i32,
    pub y: i32,
}
impl IntPoint {
    #[cfg_attr(feature = "tracing", instrument(name = "IntPoint_read", skip_all))]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FKeyHandleMap {}
impl FKeyHandleMap {
    #[cfg_attr(feature = "tracing", instrument(name = "FKeyHandleMap_read", skip_all))]
    fn read<A: ArchiveReader>(_ar: &mut A) -> Result<Self> {
        Ok(Self {})
    }
    fn write<A: ArchiveWriter>(&self, _ar: &mut A) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FRichCurveKey {
    /// Interpolation mode between this key and the next
    pub interp_mode: u8,
    /// Mode for tangents at this key
    pub tangent_mode: u8,
    /// If either tangent at this key is 'weighted'
    pub tangent_weight_mode: u8,
    /// Time at this key
    pub time: Float,
    /// Value at this key
    pub value: Float,
    /// If RCIM_Cubic, the arriving tangent at this key
    pub arrive_tangent: Float,
    /// If RCTWM_WeightedArrive or RCTWM_WeightedBoth, the weight of the left tangent
    pub arrive_tangent_weight: Float,
    /// If RCIM_Cubic, the leaving tangent at this key
    pub leave_tangent: Float,
    /// If RCTWM_WeightedLeave or RCTWM_WeightedBoth, the weight of the right tangent
    pub leave_tangent_weight: Float,
}
impl FRichCurveKey {
    #[cfg_attr(feature = "tracing", instrument(name = "FRichCurveKey_read", skip_all))]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            interp_mode: ar.read_u8()?,
            tangent_mode: ar.read_u8()?,
            tangent_weight_mode: ar.read_u8()?,
            time: ar.read_f32::<LE>()?.into(),
            value: ar.read_f32::<LE>()?.into(),
            arrive_tangent: ar.read_f32::<LE>()?.into(),
            arrive_tangent_weight: ar.read_f32::<LE>()?.into(),
            leave_tangent: ar.read_f32::<LE>()?.into(),
            leave_tangent_weight: ar.read_f32::<LE>()?.into(),
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u8(self.interp_mode)?;
        ar.write_u8(self.tangent_mode)?;
        ar.write_u8(self.tangent_weight_mode)?;
        ar.write_f32::<LE>(self.time.into())?;
        ar.write_f32::<LE>(self.value.into())?;
        ar.write_f32::<LE>(self.arrive_tangent.into())?;
        ar.write_f32::<LE>(self.arrive_tangent_weight.into())?;
        ar.write_f32::<LE>(self.leave_tangent.into())?;
        ar.write_f32::<LE>(self.leave_tangent_weight.into())?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FFrameNumberRangeBound {
    pub bound_type: u8,
    pub value: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FMovieSceneFrameRange {
    pub lower_bound: FFrameNumberRangeBound,
    pub upper_bound: FFrameNumberRangeBound,
}

impl FMovieSceneFrameRange {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            lower_bound: FFrameNumberRangeBound {
                bound_type: ar.read_u8()?,
                value: ar.read_i32::<LE>()?,
            },
            upper_bound: FFrameNumberRangeBound {
                bound_type: ar.read_u8()?,
                value: ar.read_i32::<LE>()?,
            },
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u8(self.lower_bound.bound_type)?;
        ar.write_i32::<LE>(self.lower_bound.value)?;
        ar.write_u8(self.upper_bound.bound_type)?;
        ar.write_i32::<LE>(self.upper_bound.value)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FMovieSceneTangentData {
    pub arrive_tangent: Float,
    pub leave_tangent: Float,
    pub arrive_tangent_weight: Float,
    pub leave_tangent_weight: Float,
    pub tangent_weight_mode: u8,
    /// 3 bytes of compiler-inserted alignment padding (struct is 4-aligned)
    pub _padding: [u8; 3],
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FMovieSceneFloatValue {
    pub value: Float,
    pub tangent: FMovieSceneTangentData,
    pub interp_mode: u8,
    pub tangent_mode: u8,
    pub padding_byte: u8,
    /// 1 byte trailing alignment padding so the struct totals 28 bytes
    pub _padding: u8,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FMovieSceneFloatChannel {
    pub pre_infinity_extrap: u8,
    pub post_infinity_extrap: u8,
    pub times: Vec<i32>,
    pub values: Vec<FMovieSceneFloatValue>,
    pub default_value: Float,
    pub has_default_value: bool,
    pub tick_resolution_numerator: i32,
    pub tick_resolution_denominator: i32,
}

impl FMovieSceneFloatChannel {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let pre_infinity_extrap = ar.read_u8()?;
        let post_infinity_extrap = ar.read_u8()?;

        let times_elem_size = ar.read_i32::<LE>()?;
        if times_elem_size != 4 {
            return Err(Error::Other(format!(
                "FMovieSceneFloatChannel: unexpected Times element size {times_elem_size} (expected 4)"
            )));
        }
        let times_count = ar.read_i32::<LE>()? as usize;
        let mut times = Vec::with_capacity(bounded_prealloc(times_count));
        for _ in 0..times_count {
            times.push(ar.read_i32::<LE>()?);
        }

        let values_elem_size = ar.read_i32::<LE>()?;
        if values_elem_size != 28 {
            return Err(Error::Other(format!(
                "FMovieSceneFloatChannel: unexpected Values element size {values_elem_size} (expected 28)"
            )));
        }
        let values_count = ar.read_i32::<LE>()? as usize;
        let mut values = Vec::with_capacity(bounded_prealloc(values_count));
        for _ in 0..values_count {
            let value = ar.read_f32::<LE>()?.into();
            let arrive_tangent = ar.read_f32::<LE>()?.into();
            let leave_tangent = ar.read_f32::<LE>()?.into();
            let arrive_tangent_weight = ar.read_f32::<LE>()?.into();
            let leave_tangent_weight = ar.read_f32::<LE>()?.into();
            let tangent_weight_mode = ar.read_u8()?;
            let mut tangent_padding = [0u8; 3];
            ar.read_exact(&mut tangent_padding)?;
            let interp_mode = ar.read_u8()?;
            let tangent_mode = ar.read_u8()?;
            let padding_byte = ar.read_u8()?;
            let trailing_padding = ar.read_u8()?;
            values.push(FMovieSceneFloatValue {
                value,
                tangent: FMovieSceneTangentData {
                    arrive_tangent,
                    leave_tangent,
                    arrive_tangent_weight,
                    leave_tangent_weight,
                    tangent_weight_mode,
                    _padding: tangent_padding,
                },
                interp_mode,
                tangent_mode,
                padding_byte,
                _padding: trailing_padding,
            });
        }

        let default_value = ar.read_f32::<LE>()?.into();
        let has_default_value = ar.read_u32::<LE>()? != 0;
        let tick_resolution_numerator = ar.read_i32::<LE>()?;
        let tick_resolution_denominator = ar.read_i32::<LE>()?;

        Ok(Self {
            pre_infinity_extrap,
            post_infinity_extrap,
            times,
            values,
            default_value,
            has_default_value,
            tick_resolution_numerator,
            tick_resolution_denominator,
        })
    }

    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u8(self.pre_infinity_extrap)?;
        ar.write_u8(self.post_infinity_extrap)?;

        ar.write_i32::<LE>(4)?;
        ar.write_i32::<LE>(self.times.len() as i32)?;
        for t in &self.times {
            ar.write_i32::<LE>(*t)?;
        }

        ar.write_i32::<LE>(28)?;
        ar.write_i32::<LE>(self.values.len() as i32)?;
        for v in &self.values {
            ar.write_f32::<LE>(v.value.into())?;
            ar.write_f32::<LE>(v.tangent.arrive_tangent.into())?;
            ar.write_f32::<LE>(v.tangent.leave_tangent.into())?;
            ar.write_f32::<LE>(v.tangent.arrive_tangent_weight.into())?;
            ar.write_f32::<LE>(v.tangent.leave_tangent_weight.into())?;
            ar.write_u8(v.tangent.tangent_weight_mode)?;
            ar.write_all(&v.tangent._padding)?;
            ar.write_u8(v.interp_mode)?;
            ar.write_u8(v.tangent_mode)?;
            ar.write_u8(v.padding_byte)?;
            ar.write_u8(v._padding)?;
        }

        ar.write_f32::<LE>(self.default_value.into())?;
        ar.write_u32::<LE>(if self.has_default_value { 1 } else { 0 })?;
        ar.write_i32::<LE>(self.tick_resolution_numerator)?;
        ar.write_i32::<LE>(self.tick_resolution_denominator)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FExpressionInput {
    pub output_index: i32,
    pub input_name: String,
    pub mask: i32,
    pub mask_r: i32,
    pub mask_g: i32,
    pub mask_b: i32,
    pub mask_a: i32,
    pub expression_name: String,
}

impl FExpressionInput {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            output_index: ar.read_i32::<LE>()?,
            input_name: ar.read_string()?,
            mask: ar.read_i32::<LE>()?,
            mask_r: ar.read_i32::<LE>()?,
            mask_g: ar.read_i32::<LE>()?,
            mask_b: ar.read_i32::<LE>()?,
            mask_a: ar.read_i32::<LE>()?,
            expression_name: ar.read_string()?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_i32::<LE>(self.output_index)?;
        ar.write_string(&self.input_name)?;
        ar.write_i32::<LE>(self.mask)?;
        ar.write_i32::<LE>(self.mask_r)?;
        ar.write_i32::<LE>(self.mask_g)?;
        ar.write_i32::<LE>(self.mask_b)?;
        ar.write_i32::<LE>(self.mask_a)?;
        ar.write_string(&self.expression_name)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FColorMaterialInput {
    pub base: FExpressionInput,
    pub use_constant: bool,
    pub constant: Color,
}

impl FColorMaterialInput {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            base: FExpressionInput::read(ar)?,
            use_constant: ar.read_u32::<LE>()? != 0,
            constant: Color::read(ar)?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.base.write(ar)?;
        ar.write_u32::<LE>(if self.use_constant { 1 } else { 0 })?;
        self.constant.write(ar)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FScalarMaterialInput {
    pub base: FExpressionInput,
    pub use_constant: bool,
    pub constant: Float,
}

impl FScalarMaterialInput {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            base: FExpressionInput::read(ar)?,
            use_constant: ar.read_u32::<LE>()? != 0,
            constant: ar.read_f32::<LE>()?.into(),
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.base.write(ar)?;
        ar.write_u32::<LE>(if self.use_constant { 1 } else { 0 })?;
        ar.write_f32::<LE>(self.constant.into())?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FShadingModelMaterialInput {
    pub base: FExpressionInput,
    pub use_constant: bool,
    pub constant: u32,
}

impl FShadingModelMaterialInput {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            base: FExpressionInput::read(ar)?,
            use_constant: ar.read_u32::<LE>()? != 0,
            constant: ar.read_u32::<LE>()?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.base.write(ar)?;
        ar.write_u32::<LE>(if self.use_constant { 1 } else { 0 })?;
        ar.write_u32::<LE>(self.constant)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FVectorMaterialInput {
    pub base: FExpressionInput,
    pub use_constant: bool,
    pub constant: Vector,
}

impl FVectorMaterialInput {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            base: FExpressionInput::read(ar)?,
            use_constant: ar.read_u32::<LE>()? != 0,
            constant: Vector::read(ar)?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.base.write(ar)?;
        ar.write_u32::<LE>(if self.use_constant { 1 } else { 0 })?;
        self.constant.write(ar)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FVector2MaterialInput {
    pub base: FExpressionInput,
    pub use_constant: bool,
    pub constant: Vector2D,
}

impl FVector2MaterialInput {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            base: FExpressionInput::read(ar)?,
            use_constant: ar.read_u32::<LE>()? != 0,
            constant: Vector2D::read(ar)?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.base.write(ar)?;
        ar.write_u32::<LE>(if self.use_constant { 1 } else { 0 })?;
        self.constant.write(ar)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FFrameNumber {
    pub value: i32,
}

impl FFrameNumber {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            value: ar.read_i32::<LE>()?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_i32::<LE>(self.value)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FMovieSceneSequenceID {
    pub value: u32,
}

impl FMovieSceneSequenceID {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            value: ar.read_u32::<LE>()?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.value)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FMovieSceneTrackIdentifier {
    pub value: u32,
}

impl FMovieSceneTrackIdentifier {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            value: ar.read_u32::<LE>()?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.value)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FEvaluationTreeEntryHandle {
    pub entry_index: i32,
}

impl FEvaluationTreeEntryHandle {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            entry_index: ar.read_i32::<LE>()?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_i32::<LE>(self.entry_index)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FMovieSceneEvaluationTreeNodeHandle {
    pub children_handle: FEvaluationTreeEntryHandle,
    pub index: i32,
}

impl FMovieSceneEvaluationTreeNodeHandle {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            children_handle: FEvaluationTreeEntryHandle::read(ar)?,
            index: ar.read_i32::<LE>()?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.children_handle.write(ar)?;
        ar.write_i32::<LE>(self.index)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FFrameNumberRange {
    pub lower_bound_type: u8,
    pub lower_bound_value: i32,
    pub upper_bound_type: u8,
    pub upper_bound_value: i32,
}

impl FFrameNumberRange {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            lower_bound_type: ar.read_u8()?,
            lower_bound_value: ar.read_i32::<LE>()?,
            upper_bound_type: ar.read_u8()?,
            upper_bound_value: ar.read_i32::<LE>()?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u8(self.lower_bound_type)?;
        ar.write_i32::<LE>(self.lower_bound_value)?;
        ar.write_u8(self.upper_bound_type)?;
        ar.write_i32::<LE>(self.upper_bound_value)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FMovieSceneEvaluationTreeNode {
    pub range: FFrameNumberRange,
    pub parent: FMovieSceneEvaluationTreeNodeHandle,
    pub children_id: FEvaluationTreeEntryHandle,
    pub data_id: FEvaluationTreeEntryHandle,
}

impl FMovieSceneEvaluationTreeNode {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            range: FFrameNumberRange::read(ar)?,
            parent: FMovieSceneEvaluationTreeNodeHandle::read(ar)?,
            children_id: FEvaluationTreeEntryHandle::read(ar)?,
            data_id: FEvaluationTreeEntryHandle::read(ar)?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.range.write(ar)?;
        self.parent.write(ar)?;
        self.children_id.write(ar)?;
        self.data_id.write(ar)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FEvaluationTreeEntry {
    pub start_index: i32,
    pub size: i32,
    pub capacity: i32,
}

impl FEvaluationTreeEntry {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            start_index: ar.read_i32::<LE>()?,
            size: ar.read_i32::<LE>()?,
            capacity: ar.read_i32::<LE>()?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_i32::<LE>(self.start_index)?;
        ar.write_i32::<LE>(self.size)?;
        ar.write_i32::<LE>(self.capacity)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FEntityAndMetaDataIndex {
    pub entity_index: i32,
    pub meta_data_index: i32,
}

impl FEntityAndMetaDataIndex {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            entity_index: ar.read_i32::<LE>()?,
            meta_data_index: ar.read_i32::<LE>()?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_i32::<LE>(self.entity_index)?;
        ar.write_i32::<LE>(self.meta_data_index)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FMovieSceneEvaluationFieldEntityTree {
    pub root_node: FMovieSceneEvaluationTreeNode,
    pub child_nodes_entries: Vec<FEvaluationTreeEntry>,
    pub child_nodes_items: Vec<FMovieSceneEvaluationTreeNode>,
    pub data_entries: Vec<FEvaluationTreeEntry>,
    pub data_items: Vec<FEntityAndMetaDataIndex>,
}

impl FMovieSceneEvaluationFieldEntityTree {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let root_node = FMovieSceneEvaluationTreeNode::read(ar)?;
        let child_nodes_entries_count = ar.read_u32::<LE>()? as usize;
        let mut child_nodes_entries = Vec::with_capacity(child_nodes_entries_count);
        for _ in 0..child_nodes_entries_count {
            child_nodes_entries.push(FEvaluationTreeEntry::read(ar)?);
        }
        let child_nodes_items_count = ar.read_u32::<LE>()? as usize;
        let mut child_nodes_items = Vec::with_capacity(child_nodes_items_count);
        for _ in 0..child_nodes_items_count {
            child_nodes_items.push(FMovieSceneEvaluationTreeNode::read(ar)?);
        }
        let data_entries_count = ar.read_u32::<LE>()? as usize;
        let mut data_entries = Vec::with_capacity(data_entries_count);
        for _ in 0..data_entries_count {
            data_entries.push(FEvaluationTreeEntry::read(ar)?);
        }
        let data_items_count = ar.read_u32::<LE>()? as usize;
        let mut data_items = Vec::with_capacity(data_items_count);
        for _ in 0..data_items_count {
            data_items.push(FEntityAndMetaDataIndex::read(ar)?);
        }
        Ok(Self {
            root_node,
            child_nodes_entries,
            child_nodes_items,
            data_entries,
            data_items,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.root_node.write(ar)?;
        ar.write_u32::<LE>(self.child_nodes_entries.len() as u32)?;
        for e in &self.child_nodes_entries {
            e.write(ar)?;
        }
        ar.write_u32::<LE>(self.child_nodes_items.len() as u32)?;
        for n in &self.child_nodes_items {
            n.write(ar)?;
        }
        ar.write_u32::<LE>(self.data_entries.len() as u32)?;
        for e in &self.data_entries {
            e.write(ar)?;
        }
        ar.write_u32::<LE>(self.data_items.len() as u32)?;
        for d in &self.data_items {
            d.write(ar)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FMovieSceneEvaluationKey {
    pub sequence_id: FMovieSceneSequenceID,
    pub track_identifier: FMovieSceneTrackIdentifier,
    pub section_index: u32,
}

impl FMovieSceneEvaluationKey {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            sequence_id: FMovieSceneSequenceID::read(ar)?,
            track_identifier: FMovieSceneTrackIdentifier::read(ar)?,
            section_index: ar.read_u32::<LE>()?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.sequence_id.write(ar)?;
        self.track_identifier.write(ar)?;
        ar.write_u32::<LE>(self.section_index)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T::ObjectRef: Serialize",
    deserialize = "T::ObjectRef: Deserialize<'de>"
))]
pub struct FFontData<T: ArchiveType = SaveGameArchiveType> {
    pub is_cooked: bool,
    pub font_face_asset: T::ObjectRef,
    /// Only present in cooked data when font_face_asset is null
    pub font_filename: Option<String>,
    pub hinting: Option<u8>,
    pub loading_policy: Option<u8>,
    pub sub_face_index: i32,
}

impl<T: ArchiveType> FFontData<T> {
    fn read<A: ArchiveReader<ArchiveType = T>>(ar: &mut A) -> Result<Self> {
        let is_cooked = ar.read_u32::<LE>()? != 0;
        let font_face_asset = ar.read_object_ref()?;
        let has_extra = T::is_null_object_ref(&font_face_asset);
        let (font_filename, hinting, loading_policy) = if has_extra {
            (
                Some(read_string(ar)?),
                Some(ar.read_u8()?),
                Some(ar.read_u8()?),
            )
        } else {
            (None, None, None)
        };
        let sub_face_index = ar.read_i32::<LE>()?;
        Ok(Self {
            is_cooked,
            font_face_asset,
            font_filename,
            hinting,
            loading_policy,
            sub_face_index,
        })
    }
    fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(if self.is_cooked { 1 } else { 0 })?;
        ar.write_object_ref(&self.font_face_asset)?;
        if T::is_null_object_ref(&self.font_face_asset) {
            write_string(ar, self.font_filename.as_deref().unwrap_or(""))?;
            ar.write_u8(self.hinting.unwrap_or(0))?;
            ar.write_u8(self.loading_policy.unwrap_or(0))?;
        }
        ar.write_i32::<LE>(self.sub_face_index)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FNiagaraDataInterfaceGeneratedFunction {
    pub definition_name: String,
    pub instance_name: String,
    pub specifiers: Vec<(String, String)>,
}

impl FNiagaraDataInterfaceGeneratedFunction {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let definition_name = ar.read_string()?;
        let instance_name = read_string(ar)?;
        let count = ar.read_i32::<LE>()? as usize;
        let mut specifiers = Vec::with_capacity(count);
        for _ in 0..count {
            specifiers.push((ar.read_string()?, ar.read_string()?));
        }
        Ok(Self {
            definition_name,
            instance_name,
            specifiers,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_string(&self.definition_name)?;
        write_string(ar, &self.instance_name)?;
        ar.write_i32::<LE>(self.specifiers.len() as i32)?;
        for (a, b) in &self.specifiers {
            ar.write_string(a)?;
            ar.write_string(b)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FNiagaraDataInterfaceGPUParamInfo {
    pub data_interface_hlsl_symbol: String,
    pub di_class_name: String,
    pub generated_functions: Vec<FNiagaraDataInterfaceGeneratedFunction>,
}

impl FNiagaraDataInterfaceGPUParamInfo {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let data_interface_hlsl_symbol = read_string(ar)?;
        let di_class_name = read_string(ar)?;
        let count = ar.read_i32::<LE>()? as usize;
        let mut generated_functions = Vec::with_capacity(count);
        for _ in 0..count {
            generated_functions.push(FNiagaraDataInterfaceGeneratedFunction::read(ar)?);
        }
        Ok(Self {
            data_interface_hlsl_symbol,
            di_class_name,
            generated_functions,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        write_string(ar, &self.data_interface_hlsl_symbol)?;
        write_string(ar, &self.di_class_name)?;
        ar.write_i32::<LE>(self.generated_functions.len() as i32)?;
        for f in &self.generated_functions {
            f.write(ar)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FMeshToMeshVertData {
    pub position_bary_coords_and_dist: [Float; 4],
    pub normal_bary_coords_and_dist: [Float; 4],
    pub tangent_bary_coords_and_dist: [Float; 4],
    pub source_mesh_vert_indices: [u16; 4],
    pub weight: Float,
    pub padding: u32,
}

impl FMeshToMeshVertData {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let read_v4 = |ar: &mut A| -> Result<[Float; 4]> {
            Ok([
                ar.read_f32::<LE>()?.into(),
                ar.read_f32::<LE>()?.into(),
                ar.read_f32::<LE>()?.into(),
                ar.read_f32::<LE>()?.into(),
            ])
        };
        Ok(Self {
            position_bary_coords_and_dist: read_v4(ar)?,
            normal_bary_coords_and_dist: read_v4(ar)?,
            tangent_bary_coords_and_dist: read_v4(ar)?,
            source_mesh_vert_indices: [
                ar.read_u16::<LE>()?,
                ar.read_u16::<LE>()?,
                ar.read_u16::<LE>()?,
                ar.read_u16::<LE>()?,
            ],
            weight: ar.read_f32::<LE>()?.into(),
            padding: ar.read_u32::<LE>()?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        for v4 in [
            &self.position_bary_coords_and_dist,
            &self.normal_bary_coords_and_dist,
            &self.tangent_bary_coords_and_dist,
        ] {
            for f in v4 {
                ar.write_f32::<LE>((*f).into())?;
            }
        }
        for i in &self.source_mesh_vert_indices {
            ar.write_u16::<LE>(*i)?;
        }
        ar.write_f32::<LE>(self.weight.into())?;
        ar.write_u32::<LE>(self.padding)?;
        Ok(())
    }
}

/// `FClothLODDataCommon::Serialize` writes the struct's tagged-properties and then appends
/// two TArray<FMeshToMeshVertData> (non-USTRUCT) binary blobs for transition skinning data.
/// Reading stops at the None terminator by default, leaving those bytes unconsumed and
/// misaligning the next `LodData` element. Specialize the struct so the trailing binary is
/// read explicitly.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(bound(serialize = "T::ObjectRef: Serialize"))]
pub struct FClothLODDataCommon<T: ArchiveType = SaveGameArchiveType> {
    pub properties: Properties<T>,
    pub transition_up_skin_data: Vec<FMeshToMeshVertData>,
    pub transition_down_skin_data: Vec<FMeshToMeshVertData>,
}

impl<T: ArchiveType> FClothLODDataCommon<T> {
    fn read<A: ArchiveReader<ArchiveType = T>>(ar: &mut A) -> Result<Self> {
        let properties = read_properties_until_none(ar)?;
        let up_count = ar.read_i32::<LE>()? as usize;
        let mut transition_up_skin_data = Vec::with_capacity(up_count);
        for _ in 0..up_count {
            transition_up_skin_data.push(FMeshToMeshVertData::read(ar)?);
        }
        let down_count = ar.read_i32::<LE>()? as usize;
        let mut transition_down_skin_data = Vec::with_capacity(down_count);
        for _ in 0..down_count {
            transition_down_skin_data.push(FMeshToMeshVertData::read(ar)?);
        }
        Ok(Self {
            properties,
            transition_up_skin_data,
            transition_down_skin_data,
        })
    }
    fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        write_properties_none_terminated(ar, &self.properties)?;
        ar.write_i32::<LE>(self.transition_up_skin_data.len() as i32)?;
        for v in &self.transition_up_skin_data {
            v.write(ar)?;
        }
        ar.write_i32::<LE>(self.transition_down_skin_data.len() as i32)?;
        for v in &self.transition_down_skin_data {
            v.write(ar)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(bound(serialize = "T::ObjectRef: Serialize"))]
pub struct FNiagaraVariable<T: ArchiveType = SaveGameArchiveType> {
    pub name: String,
    pub type_def: Properties<T>,
    pub var_data: Vec<u8>,
}

impl<T: ArchiveType> FNiagaraVariable<T> {
    fn read<A: ArchiveReader<ArchiveType = T>>(ar: &mut A) -> Result<Self> {
        let name = ar.read_string()?;
        ar.scope().push("type_def");
        let type_def = read_properties_until_none(ar)?;
        ar.scope().pop();
        let count = ar.read_u32::<LE>()? as usize;
        let mut var_data = vec![0u8; count];
        ar.read_exact(&mut var_data)?;
        Ok(Self {
            name,
            type_def,
            var_data,
        })
    }
    fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        ar.write_string(&self.name)?;
        ar.scope().push("type_def");
        write_properties_none_terminated(ar, &self.type_def)?;
        ar.scope().pop();
        ar.write_u32::<LE>(self.var_data.len() as u32)?;
        ar.write_all(&self.var_data)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(bound(serialize = "T::ObjectRef: Serialize"))]
pub struct FNiagaraVariableBase<T: ArchiveType = SaveGameArchiveType> {
    pub name: String,
    pub type_def: Properties<T>,
}

impl<T: ArchiveType> FNiagaraVariableBase<T> {
    fn read<A: ArchiveReader<ArchiveType = T>>(ar: &mut A) -> Result<Self> {
        let name = ar.read_string()?;
        ar.scope().push("type_def");
        let type_def = read_properties_until_none(ar)?;
        ar.scope().pop();
        Ok(Self { name, type_def })
    }
    fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        ar.write_string(&self.name)?;
        ar.scope().push("type_def");
        write_properties_none_terminated(ar, &self.type_def)?;
        ar.scope().pop();
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(bound(serialize = "T::ObjectRef: Serialize"))]
pub struct FNiagaraVariableWithOffset<T: ArchiveType = SaveGameArchiveType> {
    pub name: String,
    pub type_def: Properties<T>,
    pub offset: i32,
}

impl<T: ArchiveType> FNiagaraVariableWithOffset<T> {
    fn read<A: ArchiveReader<ArchiveType = T>>(ar: &mut A) -> Result<Self> {
        let name = ar.read_string()?;
        ar.scope().push("type_def");
        let type_def = read_properties_until_none(ar)?;
        ar.scope().pop();
        let offset = ar.read_i32::<LE>()?;
        Ok(Self {
            name,
            type_def,
            offset,
        })
    }
    fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        ar.write_string(&self.name)?;
        ar.scope().push("type_def");
        write_properties_none_terminated(ar, &self.type_def)?;
        ar.scope().pop();
        ar.write_i32::<LE>(self.offset)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FWeightedRandomSampler {
    pub prob: Vec<Float>,
    pub alias: Vec<i32>,
    pub total_weight: Float,
}
impl FWeightedRandomSampler {
    #[cfg_attr(
        feature = "tracing",
        instrument(name = "FWeightedRandomSampler_read", skip_all)
    )]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            prob: read_array(ar.read_u32::<LE>()?, ar, |r| Ok(r.read_f32::<LE>()?.into()))?,
            alias: read_array(ar.read_u32::<LE>()?, ar, |r| Ok(r.read_i32::<LE>()?))?,
            total_weight: ar.read_f32::<LE>()?.into(),
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.prob.len() as u32)?;
        for p in &self.prob {
            ar.write_f32::<LE>((*p).into())?;
        }
        ar.write_u32::<LE>(self.alias.len() as u32)?;
        for a in &self.alias {
            ar.write_i32::<LE>(*a)?;
        }
        ar.write_f32::<LE>(self.total_weight.into())?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FSkeletalMeshSamplingLODBuiltData {
    pub weighted_random_sampler: FWeightedRandomSampler,
}
impl FSkeletalMeshSamplingLODBuiltData {
    #[cfg_attr(
        feature = "tracing",
        instrument(name = "SkeletalMeshSamplingLODBuiltData_read", skip_all)
    )]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            weighted_random_sampler: FWeightedRandomSampler::read(ar)?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.weighted_random_sampler.write(ar)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FPerPlatformFloat {
    pub is_cooked: bool,
    pub value: Float,
}
impl FPerPlatformFloat {
    #[cfg_attr(
        feature = "tracing",
        instrument(name = "FPerPlatformFloat_read", skip_all)
    )]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let is_cooked = ar.read_u32::<LE>()? != 0;
        assert!(
            is_cooked,
            "TODO implement !is_cooked (read map of platform => value)"
        );
        Ok(Self {
            is_cooked,
            value: ar.read_f32::<LE>()?.into(),
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.is_cooked as u32)?;
        ar.write_f32::<LE>(self.value.into())?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    #[cfg_attr(
        feature = "tracing",
        instrument(name = "SoftObjectPath_read", skip_all)
    )]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SoftClassPath(pub SoftObjectPath);
impl SoftClassPath {
    #[cfg_attr(feature = "tracing", instrument(name = "SoftClassPath_read", skip_all))]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self(SoftObjectPath::read(ar)?))
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.0.write(ar)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GameplayTag {
    pub name: String,
}
impl GameplayTag {
    #[cfg_attr(feature = "tracing", instrument(name = "GameplayTag_read", skip_all))]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GameplayTagContainer {
    pub gameplay_tags: Vec<GameplayTag>,
}
impl GameplayTagContainer {
    #[cfg_attr(
        feature = "tracing",
        instrument(name = "GameplayTagContainer_read", skip_all)
    )]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UniqueNetIdRepl {
    pub inner: Option<UniqueNetIdReplInner>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UniqueNetIdReplInner {
    pub size: std::num::NonZeroU32,
    pub type_: String,
    pub contents: String,
}
impl UniqueNetIdRepl {
    #[cfg_attr(
        feature = "tracing",
        instrument(name = "UniqueNetIdRepl_read", skip_all)
    )]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FFormatArgumentData {
    name: String,
    value: FFormatArgumentDataValue,
}
impl FFormatArgumentData {
    #[cfg_attr(
        feature = "tracing",
        instrument(name = "FFormatArgumentData_read", skip_all)
    )]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            name: read_string(ar)?,
            value: FFormatArgumentDataValue::read(ar)?,
        })
    }
}
impl FFormatArgumentData {
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        write_string(ar, &self.name)?;
        self.value.write(ar)?;
        Ok(())
    }
}
// very similar to FFormatArgumentValue but serializes ints as 32 bits (TODO changes to 64 bit
// again at some later UE version)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FFormatArgumentDataValue {
    Int(i32),
    UInt(u32),
    Float(Float),
    Double(Double),
    Text(std::boxed::Box<Text>),
    Gender(u64),
}
impl FFormatArgumentDataValue {
    #[cfg_attr(
        feature = "tracing",
        instrument(name = "FFormatArgumentDataValue_read", skip_all)
    )]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FFormatArgumentValue {
    Int(i64),
    UInt(u64),
    Float(Float),
    Double(Double),
    Text(std::boxed::Box<Text>),
    Gender(u64),
}

impl FFormatArgumentValue {
    #[cfg_attr(
        feature = "tracing",
        instrument(name = "FFormatArgumentValue_read", skip_all)
    )]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    #[cfg_attr(
        feature = "tracing",
        instrument(name = "FNumberFormattingOptions_read", skip_all)
    )]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Text {
    flags: u32,
    variant: TextVariant,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i8)]
pub enum EDateTimeStyle {
    Default = 0,
    Short = 1,
    Medium = 2,
    Long = 3,
    Full = 4,
    Custom = 5,
}
impl EDateTimeStyle {
    fn from_i8(value: i8) -> Result<Self> {
        Ok(match value {
            0 => Self::Default,
            1 => Self::Short,
            2 => Self::Medium,
            3 => Self::Long,
            4 => Self::Full,
            5 => Self::Custom,
            _ => {
                return Err(Error::Other(format!("unknown EDateTimeStyle 0x{value:x}")));
            }
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum ETextTransformType {
    ToLower = 0,
    ToUpper = 1,
}
impl ETextTransformType {
    fn from_u8(value: u8) -> Result<Self> {
        Ok(match value {
            0 => Self::ToLower,
            1 => Self::ToUpper,
            _ => {
                return Err(Error::Other(format!(
                    "unknown ETextTransformType 0x{value:x}"
                )));
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FFormatNamedArgument {
    name: String,
    value: FFormatArgumentValue,
}
impl FFormatNamedArgument {
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(Self {
            name: ar.read_string()?,
            value: FFormatArgumentValue::read(ar)?,
        })
    }
    fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_string(&self.name)?;
        self.value.write(ar)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    // 0x1
    NamedFormat {
        format_text: std::boxed::Box<Text>,
        arguments: Vec<FFormatNamedArgument>,
    },
    // 0x2
    OrderedFormat {
        format_text: std::boxed::Box<Text>,
        arguments: Vec<FFormatArgumentValue>,
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
    // 0x5
    AsPercent {
        source_value: FFormatArgumentValue,
        format_options: Option<FNumberFormattingOptions>,
        culture_name: String,
    },
    // 0x6
    AsCurrency {
        currency_code: String,
        source_value: FFormatArgumentValue,
        format_options: Option<FNumberFormattingOptions>,
        culture_name: String,
    },
    // 0x7
    AsDate {
        source_date_time: DateTime,
        date_style: EDateTimeStyle,
        time_zone: String,
        culture_name: String,
    },
    // 0x8
    AsTime {
        source_date_time: DateTime,
        time_style: EDateTimeStyle,
        time_zone: String,
        culture_name: String,
    },
    // 0x9
    AsDateTime {
        source_date_time: DateTime,
        date_style: EDateTimeStyle,
        time_style: EDateTimeStyle,
        /// Only present when `date_style` is `EDateTimeStyle::Custom`.
        custom_pattern: Option<String>,
        time_zone: String,
        culture_name: String,
    },
    // 0xa
    Transform {
        source_text: std::boxed::Box<Text>,
        transform_type: ETextTransformType,
    },
    // 0xb
    StringTableEntry {
        table: String,
        key: String,
    },
    // 0xc
    TextGenerator {
        generator_type_id: String,
        generator_contents: Vec<u8>,
    },
}

impl Text {
    #[cfg_attr(feature = "tracing", instrument(name = "Text_read", skip_all))]
    fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let flags = ar.read_u32::<LE>()?;
        let text_history_type = ar.read_i8()?;
        let variant = match text_history_type {
            -0x1 => Ok(TextVariant::None {
                culture_invariant: (ar.read_u32::<LE>()? != 0) // bHasCultureInvariantString
                    .then(|| read_string(ar))
                    .transpose()?,
            }),
            0x0 => Ok(TextVariant::Base {
                namespace: read_string_trailing(ar)?,
                key: read_string(ar)?,
                source_string: read_string(ar)?,
            }),
            0x1 => Ok(TextVariant::NamedFormat {
                format_text: std::boxed::Box::new(Text::read(ar)?),
                arguments: read_array(ar.read_u32::<LE>()?, ar, FFormatNamedArgument::read)?,
            }),
            0x2 => Ok(TextVariant::OrderedFormat {
                format_text: std::boxed::Box::new(Text::read(ar)?),
                arguments: read_array(ar.read_u32::<LE>()?, ar, FFormatArgumentValue::read)?,
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
            0x5 => Ok(TextVariant::AsPercent {
                source_value: FFormatArgumentValue::read(ar)?,
                format_options: (ar.read_u32::<LE>()? != 0) // bHasFormatOptions
                    .then(|| FNumberFormattingOptions::read(ar))
                    .transpose()?,
                culture_name: ar.read_string()?,
            }),
            0x6 => Ok(TextVariant::AsCurrency {
                currency_code: ar.read_string()?,
                source_value: FFormatArgumentValue::read(ar)?,
                format_options: (ar.read_u32::<LE>()? != 0) // bHasFormatOptions
                    .then(|| FNumberFormattingOptions::read(ar))
                    .transpose()?,
                culture_name: ar.read_string()?,
            }),
            0x7 => Ok(TextVariant::AsDate {
                source_date_time: ar.read_u64::<LE>()?,
                date_style: EDateTimeStyle::from_i8(ar.read_i8()?)?,
                time_zone: ar.read_string()?,
                culture_name: ar.read_string()?,
            }),
            0x8 => Ok(TextVariant::AsTime {
                source_date_time: ar.read_u64::<LE>()?,
                time_style: EDateTimeStyle::from_i8(ar.read_i8()?)?,
                time_zone: ar.read_string()?,
                culture_name: ar.read_string()?,
            }),
            0x9 => {
                let source_date_time = ar.read_u64::<LE>()?;
                let date_style = EDateTimeStyle::from_i8(ar.read_i8()?)?;
                let time_style = EDateTimeStyle::from_i8(ar.read_i8()?)?;
                let custom_pattern = if date_style == EDateTimeStyle::Custom {
                    Some(ar.read_string()?)
                } else {
                    None
                };
                Ok(TextVariant::AsDateTime {
                    source_date_time,
                    date_style,
                    time_style,
                    custom_pattern,
                    time_zone: ar.read_string()?,
                    culture_name: ar.read_string()?,
                })
            }
            0xa => Ok(TextVariant::Transform {
                source_text: std::boxed::Box::new(Text::read(ar)?),
                transform_type: ETextTransformType::from_u8(ar.read_u8()?)?,
            }),
            0xb => Ok(TextVariant::StringTableEntry {
                table: ar.read_string()?,
                key: read_string(ar)?,
            }),
            0xc => Ok(TextVariant::TextGenerator {
                generator_type_id: ar.read_string()?,
                generator_contents: {
                    let len = ar.read_u32::<LE>()? as usize;
                    let mut bytes = vec![0; len];
                    ar.read_exact(&mut bytes)?;
                    bytes
                },
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
                    write_string(ar, culture_invariant)?;
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
                write_string_trailing(ar, &namespace.0, Some(&namespace.1))?;
                write_string(ar, key)?;
                write_string(ar, source_string)?;
            }
            TextVariant::NamedFormat {
                format_text,
                arguments,
            } => {
                ar.write_i8(0x1)?;
                format_text.write(ar)?;
                ar.write_u32::<LE>(arguments.len() as u32)?;
                for a in arguments {
                    a.write(ar)?;
                }
            }
            TextVariant::OrderedFormat {
                format_text,
                arguments,
            } => {
                ar.write_i8(0x2)?;
                format_text.write(ar)?;
                ar.write_u32::<LE>(arguments.len() as u32)?;
                for a in arguments {
                    a.write(ar)?;
                }
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
            TextVariant::AsPercent {
                source_value,
                format_options,
                culture_name,
            } => {
                ar.write_i8(0x5)?;
                source_value.write(ar)?;
                ar.write_u32::<LE>(format_options.is_some() as u32)?;
                if let Some(format_options) = format_options {
                    format_options.write(ar)?;
                }
                ar.write_string(culture_name)?;
            }
            TextVariant::AsCurrency {
                currency_code,
                source_value,
                format_options,
                culture_name,
            } => {
                ar.write_i8(0x6)?;
                ar.write_string(currency_code)?;
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
                ar.write_i8(*date_style as i8)?;
                ar.write_string(time_zone)?;
                ar.write_string(culture_name)?;
            }
            TextVariant::AsTime {
                source_date_time,
                time_style,
                time_zone,
                culture_name,
            } => {
                ar.write_i8(0x8)?;
                ar.write_u64::<LE>(*source_date_time)?;
                ar.write_i8(*time_style as i8)?;
                ar.write_string(time_zone)?;
                ar.write_string(culture_name)?;
            }
            TextVariant::AsDateTime {
                source_date_time,
                date_style,
                time_style,
                custom_pattern,
                time_zone,
                culture_name,
            } => {
                ar.write_i8(0x9)?;
                ar.write_u64::<LE>(*source_date_time)?;
                ar.write_i8(*date_style as i8)?;
                ar.write_i8(*time_style as i8)?;
                if *date_style == EDateTimeStyle::Custom {
                    let pattern = custom_pattern.as_deref().ok_or_else(|| {
                        Error::Other(
                            "AsDateTime with Custom date_style requires custom_pattern".to_string(),
                        )
                    })?;
                    ar.write_string(pattern)?;
                }
                ar.write_string(time_zone)?;
                ar.write_string(culture_name)?;
            }
            TextVariant::Transform {
                source_text,
                transform_type,
            } => {
                ar.write_i8(0xa)?;
                source_text.write(ar)?;
                ar.write_u8(*transform_type as u8)?;
            }
            TextVariant::StringTableEntry { table, key } => {
                ar.write_i8(0xb)?;
                ar.write_string(table)?;
                write_string(ar, key)?;
            }
            TextVariant::TextGenerator {
                generator_type_id,
                generator_contents,
            } => {
                ar.write_i8(0xc)?;
                ar.write_string(generator_type_id)?;
                ar.write_u32::<LE>(generator_contents.len() as u32)?;
                ar.write_all(generator_contents)?;
            }
        }
        Ok(())
    }
}

/// Just a plain byte, or an enum in which case the variant will be a String
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Byte {
    Byte(u8),
    Label(String),
}
/// Vectorized [`Byte`]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ByteArray {
    Byte(Vec<u8>),
    Label(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
#[serde(bound(serialize = "T::ObjectRef: Serialize, T::SoftObjectPath: Serialize"))]
pub enum StructValue<T: ArchiveType = SaveGameArchiveType> {
    Guid(FGuid),
    DateTime(DateTime),
    Timespan(Timespan),
    Vector2D(Vector2D),
    Vector(Vector),
    Vector4(Vector4),
    IntVector(IntVector),
    Box(Box),
    Box2D(Box2D),
    IntPoint(IntPoint),
    Quat(Quat),
    LinearColor(LinearColor),
    Color(Color),
    Rotator(Rotator),
    SoftObjectPath(T::SoftObjectPath),
    SoftClassPath(T::SoftObjectPath),
    GameplayTagContainer(GameplayTagContainer),
    UniqueNetIdRepl(UniqueNetIdRepl),
    KeyHandleMap(FKeyHandleMap),
    RichCurveKey(FRichCurveKey),
    SkeletalMeshSamplingLODBuiltData(FSkeletalMeshSamplingLODBuiltData),
    PerPlatformFloat(FPerPlatformFloat),
    MovieSceneFrameRange(FMovieSceneFrameRange),
    MovieSceneFloatChannel(FMovieSceneFloatChannel),
    FrameNumber(FFrameNumber),
    ExpressionInput(FExpressionInput),
    MaterialAttributesInput(FExpressionInput),
    ColorMaterialInput(FColorMaterialInput),
    ScalarMaterialInput(FScalarMaterialInput),
    ShadingModelMaterialInput(FShadingModelMaterialInput),
    VectorMaterialInput(FVectorMaterialInput),
    Vector2MaterialInput(FVector2MaterialInput),
    MovieSceneSequenceID(FMovieSceneSequenceID),
    MovieSceneTrackIdentifier(FMovieSceneTrackIdentifier),
    MovieSceneEvaluationKey(FMovieSceneEvaluationKey),
    MovieSceneEvaluationFieldEntityTree(FMovieSceneEvaluationFieldEntityTree),
    FontData(FFontData<T>),
    ClothLODDataCommon(FClothLODDataCommon<T>),
    NiagaraVariable(FNiagaraVariable<T>),
    NiagaraVariableBase(FNiagaraVariableBase<T>),
    NiagaraVariableWithOffset(FNiagaraVariableWithOffset<T>),
    NiagaraDataInterfaceGeneratedFunction(FNiagaraDataInterfaceGeneratedFunction),
    NiagaraDataInterfaceGPUParamInfo(FNiagaraDataInterfaceGPUParamInfo),
    /// A game-specific struct value, owned by the active [`Game`] (see
    /// [`GameStruct`]). Serializes untagged, i.e. as just its payload.
    Game(<T::Game as Game>::Struct<T>),
    /// Raw struct data for other unknown structs serialized with HasBinaryOrNativeSerialize
    Raw(Vec<u8>),
    /// User defined struct which is simply a list of properties
    Struct(Properties<T>),
}

/// Vectorized properties to avoid storing the variant with each value
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
#[serde(bound(serialize = "T::ObjectRef: Serialize, T::SoftObjectPath: Serialize"))]
pub enum ValueVec<T: ArchiveType = SaveGameArchiveType> {
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
    SoftObject(Vec<T::SoftObjectPath>),
    Name(Vec<String>),
    Object(Vec<T::ObjectRef>),
    Box(Vec<Box>),
    Box2D(Vec<Box2D>),
    Struct(Vec<StructValue<T>>),
}

impl<T: ArchiveType> StructValue<T> {
    #[cfg_attr(feature = "tracing", instrument(name = "StructValue_read", skip_all))]
    fn read<A: ArchiveReader<ArchiveType = T>>(
        ar: &mut A,
        t: &StructType,
    ) -> Result<StructValue<T>> {
        Ok(match t {
            StructType::Guid => StructValue::Guid(FGuid::read(ar)?),
            StructType::DateTime => StructValue::DateTime(ar.read_u64::<LE>()?),
            StructType::Timespan => StructValue::Timespan(ar.read_i64::<LE>()?),
            StructType::Vector2D => StructValue::Vector2D(Vector2D::read(ar)?),
            StructType::Vector => StructValue::Vector(Vector::read(ar)?),
            StructType::Vector4 => StructValue::Vector4(Vector4::read(ar)?),
            StructType::IntVector => StructValue::IntVector(IntVector::read(ar)?),
            StructType::Box => StructValue::Box(Box::read(ar)?),
            StructType::Box2D => StructValue::Box2D(Box2D::read(ar)?),
            StructType::IntPoint => StructValue::IntPoint(IntPoint::read(ar)?),
            StructType::Quat => StructValue::Quat(Quat::read(ar)?),
            StructType::LinearColor => StructValue::LinearColor(LinearColor::read(ar)?),
            StructType::Color => StructValue::Color(Color::read(ar)?),
            StructType::Rotator => StructValue::Rotator(Rotator::read(ar)?),
            StructType::SoftObjectPath => StructValue::SoftObjectPath(ar.read_soft_object_path()?),
            StructType::SoftClassPath => StructValue::SoftClassPath(ar.read_soft_object_path()?),
            StructType::GameplayTagContainer => {
                StructValue::GameplayTagContainer(GameplayTagContainer::read(ar)?)
            }
            StructType::UniqueNetIdRepl => StructValue::UniqueNetIdRepl(UniqueNetIdRepl::read(ar)?),
            StructType::KeyHandleMap => StructValue::KeyHandleMap(FKeyHandleMap::read(ar)?),
            StructType::RichCurveKey => StructValue::RichCurveKey(FRichCurveKey::read(ar)?),
            StructType::SkeletalMeshSamplingLODBuiltData => {
                StructValue::SkeletalMeshSamplingLODBuiltData(
                    FSkeletalMeshSamplingLODBuiltData::read(ar)?,
                )
            }
            StructType::PerPlatformFloat => {
                StructValue::PerPlatformFloat(FPerPlatformFloat::read(ar)?)
            }
            StructType::MovieSceneFrameRange => {
                StructValue::MovieSceneFrameRange(FMovieSceneFrameRange::read(ar)?)
            }
            StructType::MovieSceneFloatChannel => {
                StructValue::MovieSceneFloatChannel(FMovieSceneFloatChannel::read(ar)?)
            }
            StructType::FrameNumber => StructValue::FrameNumber(FFrameNumber::read(ar)?),
            StructType::ExpressionInput => {
                StructValue::ExpressionInput(FExpressionInput::read(ar)?)
            }
            StructType::MaterialAttributesInput => {
                StructValue::MaterialAttributesInput(FExpressionInput::read(ar)?)
            }
            StructType::ColorMaterialInput => {
                StructValue::ColorMaterialInput(FColorMaterialInput::read(ar)?)
            }
            StructType::ScalarMaterialInput => {
                StructValue::ScalarMaterialInput(FScalarMaterialInput::read(ar)?)
            }
            StructType::ShadingModelMaterialInput => {
                StructValue::ShadingModelMaterialInput(FShadingModelMaterialInput::read(ar)?)
            }
            StructType::VectorMaterialInput => {
                StructValue::VectorMaterialInput(FVectorMaterialInput::read(ar)?)
            }
            StructType::Vector2MaterialInput => {
                StructValue::Vector2MaterialInput(FVector2MaterialInput::read(ar)?)
            }
            StructType::MovieSceneSequenceID => {
                StructValue::MovieSceneSequenceID(FMovieSceneSequenceID::read(ar)?)
            }
            StructType::MovieSceneTrackIdentifier => {
                StructValue::MovieSceneTrackIdentifier(FMovieSceneTrackIdentifier::read(ar)?)
            }
            StructType::MovieSceneEvaluationKey => {
                StructValue::MovieSceneEvaluationKey(FMovieSceneEvaluationKey::read(ar)?)
            }
            StructType::MovieSceneEvaluationFieldEntityTree => {
                StructValue::MovieSceneEvaluationFieldEntityTree(
                    FMovieSceneEvaluationFieldEntityTree::read(ar)?,
                )
            }
            StructType::FontData => StructValue::FontData(FFontData::read(ar)?),
            StructType::ClothLODDataCommon => {
                StructValue::ClothLODDataCommon(FClothLODDataCommon::read(ar)?)
            }
            StructType::NiagaraVariable => {
                StructValue::NiagaraVariable(FNiagaraVariable::read(ar)?)
            }
            StructType::NiagaraVariableBase => {
                StructValue::NiagaraVariableBase(FNiagaraVariableBase::read(ar)?)
            }
            StructType::NiagaraVariableWithOffset => {
                StructValue::NiagaraVariableWithOffset(FNiagaraVariableWithOffset::read(ar)?)
            }
            StructType::NiagaraDataInterfaceGeneratedFunction => {
                StructValue::NiagaraDataInterfaceGeneratedFunction(
                    FNiagaraDataInterfaceGeneratedFunction::read(ar)?,
                )
            }
            StructType::NiagaraDataInterfaceGPUParamInfo => {
                StructValue::NiagaraDataInterfaceGPUParamInfo(
                    FNiagaraDataInterfaceGPUParamInfo::read(ar)?,
                )
            }
            StructType::Game(name) => StructValue::Game(<T::Game as Game>::read_struct(ar, name)?),
            StructType::Raw(_) => unreachable!("should be handled at property level"),
            StructType::Struct(_) => StructValue::Struct(read_properties_until_none(ar)?),
        })
    }
    fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        match self {
            StructValue::Guid(v) => v.write(ar)?,
            StructValue::DateTime(v) => ar.write_u64::<LE>(*v)?,
            StructValue::Timespan(v) => ar.write_i64::<LE>(*v)?,
            StructValue::Vector2D(v) => v.write(ar)?,
            StructValue::Vector(v) => v.write(ar)?,
            StructValue::Vector4(v) => v.write(ar)?,
            StructValue::IntVector(v) => v.write(ar)?,
            StructValue::Box(v) => v.write(ar)?,
            StructValue::Box2D(v) => v.write(ar)?,
            StructValue::IntPoint(v) => v.write(ar)?,
            StructValue::Quat(v) => v.write(ar)?,
            StructValue::LinearColor(v) => v.write(ar)?,
            StructValue::Color(v) => v.write(ar)?,
            StructValue::Rotator(v) => v.write(ar)?,
            StructValue::SoftObjectPath(v) => ar.write_soft_object_path(v)?,
            StructValue::SoftClassPath(v) => ar.write_soft_object_path(v)?,
            StructValue::GameplayTagContainer(v) => v.write(ar)?,
            StructValue::UniqueNetIdRepl(v) => v.write(ar)?,
            StructValue::KeyHandleMap(v) => v.write(ar)?,
            StructValue::RichCurveKey(v) => v.write(ar)?,
            StructValue::SkeletalMeshSamplingLODBuiltData(v) => v.write(ar)?,
            StructValue::PerPlatformFloat(v) => v.write(ar)?,
            StructValue::MovieSceneFrameRange(v) => v.write(ar)?,
            StructValue::MovieSceneFloatChannel(v) => v.write(ar)?,
            StructValue::FrameNumber(v) => v.write(ar)?,
            StructValue::ExpressionInput(v) => v.write(ar)?,
            StructValue::MaterialAttributesInput(v) => v.write(ar)?,
            StructValue::ColorMaterialInput(v) => v.write(ar)?,
            StructValue::ScalarMaterialInput(v) => v.write(ar)?,
            StructValue::ShadingModelMaterialInput(v) => v.write(ar)?,
            StructValue::VectorMaterialInput(v) => v.write(ar)?,
            StructValue::Vector2MaterialInput(v) => v.write(ar)?,
            StructValue::MovieSceneSequenceID(v) => v.write(ar)?,
            StructValue::MovieSceneTrackIdentifier(v) => v.write(ar)?,
            StructValue::MovieSceneEvaluationKey(v) => v.write(ar)?,
            StructValue::MovieSceneEvaluationFieldEntityTree(v) => v.write(ar)?,
            StructValue::FontData(v) => v.write(ar)?,
            StructValue::ClothLODDataCommon(v) => v.write(ar)?,
            StructValue::NiagaraVariable(v) => v.write(ar)?,
            StructValue::NiagaraVariableBase(v) => v.write(ar)?,
            StructValue::NiagaraVariableWithOffset(v) => v.write(ar)?,
            StructValue::NiagaraDataInterfaceGeneratedFunction(v) => v.write(ar)?,
            StructValue::NiagaraDataInterfaceGPUParamInfo(v) => v.write(ar)?,
            StructValue::Game(v) => v.write(ar)?,
            StructValue::Raw(v) => ar.write_all(v)?,
            StructValue::Struct(v) => write_properties_none_terminated(ar, v)?,
        }
        Ok(())
    }
}
impl<T: ArchiveType> ValueVec<T> {
    #[cfg_attr(feature = "tracing", instrument(name = "ValueVec_read", skip_all))]
    fn read<A: ArchiveReader<ArchiveType = T>>(
        ar: &mut A,
        t: &PropertyType,
        size: u32,
        count: u32,
    ) -> Result<ValueVec<T>> {
        Ok(match t {
            PropertyType::Int8Property => {
                ValueVec::Int8(read_array(count, ar, |r| Ok(r.read_i8()?))?)
            }
            PropertyType::Int16Property => {
                ValueVec::Int16(read_array(count, ar, |r| Ok(r.read_i16::<LE>()?))?)
            }
            PropertyType::IntProperty => {
                ValueVec::Int(read_array(count, ar, |r| Ok(r.read_i32::<LE>()?))?)
            }
            PropertyType::Int64Property => {
                ValueVec::Int64(read_array(count, ar, |r| Ok(r.read_i64::<LE>()?))?)
            }
            PropertyType::UInt8Property => {
                ValueVec::UInt8(read_array(count, ar, |r| Ok(r.read_u8()?))?)
            }
            PropertyType::UInt16Property => {
                ValueVec::UInt16(read_array(count, ar, |r| Ok(r.read_u16::<LE>()?))?)
            }
            PropertyType::UInt32Property => {
                ValueVec::UInt32(read_array(count, ar, |r| Ok(r.read_u32::<LE>()?))?)
            }
            PropertyType::UInt64Property => {
                ValueVec::UInt64(read_array(count, ar, |r| Ok(r.read_u64::<LE>()?))?)
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
            PropertyType::StrProperty => ValueVec::Str(read_array(count, ar, |r| read_string(r))?),
            PropertyType::TextProperty => ValueVec::Text(read_array(count, ar, Text::read)?),
            PropertyType::SoftObjectProperty => {
                ValueVec::SoftObject(read_array(count, ar, |r| r.read_soft_object_path())?)
            }
            PropertyType::NameProperty => {
                ValueVec::Name(read_array(count, ar, |r| r.read_string())?)
            }
            PropertyType::ObjectProperty | PropertyType::InterfaceProperty => {
                ValueVec::Object(read_array(count, ar, |r| r.read_object_ref())?)
            }
            _ => return Err(Error::UnknownVecType(format!("{t:?}"))),
        })
    }
    fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
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
            ValueVec::Str(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    write_string(ar, i)?;
                }
            }
            ValueVec::Name(v) => {
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
                for i in v {
                    ar.write_soft_object_path(i)?;
                }
            }
            ValueVec::Box(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    i.write(ar)?;
                }
            }
            ValueVec::Box2D(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    i.write(ar)?;
                }
            }
            ValueVec::Struct(v) => {
                ar.write_u32::<LE>(v.len() as u32)?;
                for i in v {
                    i.write(ar)?;
                }
            }
        }
        Ok(())
    }
}
impl<T: ArchiveType> ValueVec<T> {
    #[cfg_attr(
        feature = "tracing",
        instrument(name = "ValueVec_read_array", skip_all)
    )]
    fn read_array<A: ArchiveReader<ArchiveType = T>>(
        ar: &mut A,
        tag: PropertyTagDataFull,
        size: u32,
    ) -> Result<(ValueVec<T>, Option<PropertyTagDataFull>)> {
        let count = ar.read_u32::<LE>()?;
        Ok(match tag {
            PropertyTagDataFull::Struct { struct_type, id: _ } => {
                let (struct_type, updated) = if !ar.version().property_tag() {
                    // outer tag shows Struct but struct_type is unknown
                    if ar.version().array_inner_tag() {
                        // this is where the actual inner struct type is determined
                        let inner_tag = PropertyTagFull::read(ar)?.unwrap();
                        match inner_tag.data {
                            PropertyTagDataFull::Struct { struct_type, id } => {
                                // Return the discovered type information to update the outer tag
                                (
                                    struct_type.clone(),
                                    Some(PropertyTagDataFull::Struct { struct_type, id }),
                                )
                            }
                            _ => {
                                return Err(Error::Other(format!(
                                    "expected StructProperty tag, found {inner_tag:?}"
                                )))
                            }
                        }
                    } else {
                        // TODO prior to 4.12 struct type is unknown so should be able to
                        // manually specify like Sets/Maps
                        (StructType::Struct(None), None)
                    }
                } else {
                    (struct_type, None)
                };

                let mut value = vec![];
                for _ in 0..count {
                    value.push(StructValue::read(ar, &struct_type)?);
                }
                (ValueVec::Struct(value), updated)
            }
            _ => (ValueVec::read(ar, &tag.basic_type(), size, count)?, None),
        })
    }
    fn write_array<A: ArchiveWriter<ArchiveType = T>>(
        &self,
        ar: &mut A,
        tag: &PropertyTagFull,
    ) -> Result<()> {
        match &self {
            ValueVec::Struct(value) => {
                ar.write_u32::<LE>(value.len() as u32)?;

                if !ar.version().property_tag() && ar.version().array_inner_tag() {
                    // Extract struct type info from tag for older UE versions
                    let (struct_type, id) = match &tag.data {
                        PropertyTagDataFull::Array(inner) => match &**inner {
                            PropertyTagDataFull::Struct { struct_type, id } => (struct_type, id),
                            _ => {
                                return Err(Error::Other(
                                    "Array tag must contain Struct type".into(),
                                ))
                            }
                        },
                        _ => return Err(Error::Other("Expected Array tag".into())),
                    };

                    // Write inner property tag for older format
                    ar.write_string(&tag.name)?;
                    PropertyType::StructProperty.write(ar)?;

                    // Write placeholder size
                    let size_pos = ar.stream_position()?;
                    ar.write_u32::<LE>(0)?;
                    ar.write_u32::<LE>(0)?;
                    struct_type.write(ar)?;
                    id.write(ar)?;
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
            _ => {
                self.write(ar)?;
            }
        }
        Ok(())
    }
    #[cfg_attr(feature = "tracing", instrument(name = "ValueVec_read_set", skip_all))]
    fn read_set<A: ArchiveReader<ArchiveType = T>>(
        ar: &mut A,
        t: &PropertyTagDataFull,
        size: u32,
    ) -> Result<ValueVec<T>> {
        let count = ar.read_u32::<LE>()?;
        Ok(match t {
            PropertyTagDataFull::Struct { struct_type, .. } => {
                ValueVec::Struct(read_array(count, ar, |r| {
                    StructValue::read(r, struct_type)
                })?)
            }
            _ => ValueVec::read(ar, &t.basic_type(), size, count)?,
        })
    }
}

/// Properties consist of a value and are present in [`Root`] and [`StructValue::Struct`]
/// Property schemas (tags) are stored separately in [`PropertySchemas`]
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
#[serde(bound(serialize = "T::ObjectRef: Serialize, T::SoftObjectPath: Serialize"))]
pub enum Property<T: ArchiveType = SaveGameArchiveType> {
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
    FieldPath(FieldPath<T>),
    SoftObject(T::SoftObjectPath),
    Name(String),
    Object(T::ObjectRef),
    Text(Text),
    Delegate(Delegate<T>),
    MulticastDelegate(MulticastDelegate<T>),
    MulticastInlineDelegate(MulticastInlineDelegate<T>),
    MulticastSparseDelegate(MulticastSparseDelegate<T>),
    Set(ValueVec<T>),
    Map(Vec<MapEntry<T>>),
    Struct(StructValue<T>),
    Array(ValueVec<T>),
    /// Raw property data when parsing fails
    Raw(Vec<u8>),
}

impl<T: ArchiveType> Property<T> {
    #[cfg_attr(feature = "tracing", instrument(name = "Property_read", skip_all))]
    fn read<A: ArchiveReader<ArchiveType = T>>(
        ar: &mut A,
        tag: PropertyTagFull,
    ) -> Result<(Property<T>, Option<PropertyTagDataFull>)> {
        if tag.data.has_raw_struct() {
            if ar.log() {
                eprintln!(
                    "Warning: Storing property '{}' as raw bytes; tag contains native-serialized struct with no parser: {:?}",
                    ar.path(),
                    tag.data,
                );
            }
            let mut raw = vec![0; tag.size as usize];
            ar.read_exact(&mut raw)?;
            return Ok((Property::Raw(raw), None));
        }
        // Save the current position before attempting to parse
        let start_position = ar.stream_position()?;

        // Try to parse the property directly from the stream
        let (inner, updated_tag_data) = match Self::try_read_inner(ar, &tag) {
            Ok(result) => result,
            Err(e) => {
                // Check if we should convert errors to raw properties
                if ar.error_to_raw() {
                    // Parsing failed, seek back to start and read raw data
                    if ar.log() {
                        eprintln!("Warning: Failed to parse property '{}': {}", ar.path(), e);
                    }
                    ar.seek(std::io::SeekFrom::Start(start_position))?;
                    let mut property_data = vec![0u8; tag.size as usize];
                    ar.read_exact(&mut property_data)?;
                    (Property::Raw(property_data), None)
                } else {
                    // Error mode: return the error immediately
                    return Err(e);
                }
            }
        };

        Ok((inner, updated_tag_data))
    }

    fn try_read_inner<A: ArchiveReader<ArchiveType = T>>(
        ar: &mut A,
        tag: &PropertyTagFull,
    ) -> Result<(Property<T>, Option<PropertyTagDataFull>)> {
        let (inner, updated_tag_data) = match &tag.data {
            // Collection types need special handling
            PropertyTagDataFull::Set { key_type } => {
                ar.read_u32::<LE>()?;
                (
                    Property::Set(ValueVec::read_set(ar, key_type, tag.size - 8)?),
                    None,
                )
            }
            PropertyTagDataFull::Map {
                key_type,
                value_type,
            } => {
                // used to serialize negative difference against template object/CDO
                let _keys_to_remove = read_array(ar.read_u32::<LE>()?, ar, |ar| {
                    Property::read_value(ar, key_type)
                })?;
                let count = ar.read_u32::<LE>()?;
                let mut value = vec![];

                for _ in 0..count {
                    value.push(MapEntry::read(ar, key_type, value_type)?)
                }

                (Property::Map(value), None)
            }
            PropertyTagDataFull::Array(data) => {
                let (array, updated_data) = ValueVec::read_array(ar, *data.clone(), tag.size - 4)?;
                (Property::Array(array), updated_data)
            }
            // Bool is special - value stored in tag for top-level properties
            PropertyTagDataFull::Bool(value) => (Property::Bool(*value), None),
            PropertyTagDataFull::Other(PropertyType::FieldPathProperty) => {
                (Property::FieldPath(FieldPath::read(ar)?), None)
            }
            PropertyTagDataFull::Other(PropertyType::DelegateProperty) => {
                (Property::Delegate(Delegate::read(ar)?), None)
            }
            PropertyTagDataFull::Other(PropertyType::MulticastDelegateProperty) => (
                Property::MulticastDelegate(MulticastDelegate::read(ar)?),
                None,
            ),
            PropertyTagDataFull::Other(PropertyType::MulticastInlineDelegateProperty) => (
                Property::MulticastInlineDelegate(MulticastInlineDelegate::read(ar)?),
                None,
            ),
            PropertyTagDataFull::Other(PropertyType::MulticastSparseDelegateProperty) => (
                Property::MulticastSparseDelegate(MulticastSparseDelegate::read(ar)?),
                None,
            ),
            // Everything else can use the shared read_value logic
            _ => (Property::read_value(ar, &tag.data)?, None),
        };

        // If we got updated tag data (e.g., from array of structs), wrap it in an Array tag
        let updated_tag =
            updated_tag_data.map(|data| PropertyTagDataFull::Array(std::boxed::Box::new(data)));

        Ok((inner, updated_tag))
    }
    fn write<A: ArchiveWriter<ArchiveType = T>>(
        &self,
        ar: &mut A,
        tag: &PropertyTagFull,
    ) -> Result<()> {
        match &self {
            Property::Set(value) => {
                ar.write_u32::<LE>(0)?;
                value.write(ar)?;
            }
            Property::Map(value) => {
                ar.write_u32::<LE>(0)?;
                ar.write_u32::<LE>(value.len() as u32)?;
                for v in value {
                    v.write(ar)?;
                }
            }
            Property::Array(value) => {
                value.write_array(ar, tag)?;
            }
            Property::Raw(value) => {
                ar.write_all(value)?;
            }
            Property::FieldPath(value) => {
                value.write(ar)?;
            }
            Property::Delegate(value) => {
                value.write(ar)?;
            }
            Property::MulticastDelegate(value) => {
                value.write(ar)?;
            }
            Property::MulticastInlineDelegate(value) => {
                value.write(ar)?;
            }
            Property::MulticastSparseDelegate(value) => {
                value.write(ar)?;
            }
            // Bool is special - it's stored in the tag, not in the data
            Property::Bool(_) => {}
            // Everything else uses shared write_value logic
            _ => self.write_value(ar)?,
        }
        Ok(())
    }

    #[cfg_attr(
        feature = "tracing",
        instrument(name = "Property_read_value", skip_all)
    )]
    fn read_value<A: ArchiveReader<ArchiveType = T>>(
        ar: &mut A,
        t: &PropertyTagDataFull,
    ) -> Result<Property<T>> {
        Ok(match t {
            PropertyTagDataFull::Array(_) => {
                unreachable!("Arrays should be handled at property level")
            }
            PropertyTagDataFull::Set { .. } => {
                unreachable!("Sets should be handled at property level")
            }
            PropertyTagDataFull::Map { .. } => {
                unreachable!("Maps should be handled at property level")
            }
            PropertyTagDataFull::Struct { struct_type, .. } => {
                Property::Struct(StructValue::read(ar, struct_type)?)
            }
            PropertyTagDataFull::Byte(ref enum_type) => {
                let value = if enum_type.is_none() {
                    Byte::Byte(ar.read_u8()?)
                } else {
                    Byte::Label(ar.read_string()?)
                };
                Property::Byte(value)
            }
            PropertyTagDataFull::Enum(_, _) => Property::Enum(ar.read_string()?),
            PropertyTagDataFull::Bool(_) => Property::Bool(ar.read_u8()? > 0),
            PropertyTagDataFull::Other(property_type) => match property_type {
                PropertyType::BoolProperty
                | PropertyType::ByteProperty
                | PropertyType::EnumProperty
                | PropertyType::SetProperty
                | PropertyType::MapProperty
                | PropertyType::StructProperty
                | PropertyType::ArrayProperty
                | PropertyType::FieldPathProperty
                | PropertyType::DelegateProperty
                | PropertyType::MulticastDelegateProperty
                | PropertyType::MulticastInlineDelegateProperty
                | PropertyType::MulticastSparseDelegateProperty => {
                    unreachable!("Should be handled by dedicated tag variants")
                }
                PropertyType::Int8Property => Property::Int8(ar.read_i8()?),
                PropertyType::Int16Property => Property::Int16(ar.read_i16::<LE>()?),
                PropertyType::IntProperty => Property::Int(ar.read_i32::<LE>()?),
                PropertyType::Int64Property => Property::Int64(ar.read_i64::<LE>()?),
                PropertyType::UInt8Property => Property::UInt8(ar.read_u8()?),
                PropertyType::UInt16Property => Property::UInt16(ar.read_u16::<LE>()?),
                PropertyType::UInt32Property => Property::UInt32(ar.read_u32::<LE>()?),
                PropertyType::UInt64Property => Property::UInt64(ar.read_u64::<LE>()?),
                PropertyType::FloatProperty => Property::Float(ar.read_f32::<LE>()?.into()),
                PropertyType::DoubleProperty => Property::Double(ar.read_f64::<LE>()?.into()),
                PropertyType::NameProperty => Property::Name(ar.read_string()?),
                PropertyType::StrProperty => Property::Str(read_string(ar)?),
                PropertyType::SoftObjectProperty => {
                    Property::SoftObject(ar.read_soft_object_path()?)
                }
                PropertyType::ObjectProperty | PropertyType::InterfaceProperty => {
                    Property::Object(ar.read_object_ref()?)
                }
                PropertyType::TextProperty => Property::Text(Text::read(ar)?),
            },
        })
    }

    fn write_value<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        match &self {
            Property::Int8(v) => ar.write_i8(*v)?,
            Property::Int16(v) => ar.write_i16::<LE>(*v)?,
            Property::Int(v) => ar.write_i32::<LE>(*v)?,
            Property::Int64(v) => ar.write_i64::<LE>(*v)?,
            Property::UInt8(v) => ar.write_u8(*v)?,
            Property::UInt16(v) => ar.write_u16::<LE>(*v)?,
            Property::UInt32(v) => ar.write_u32::<LE>(*v)?,
            Property::UInt64(v) => ar.write_u64::<LE>(*v)?,
            Property::Float(v) => ar.write_f32::<LE>((*v).into())?,
            Property::Double(v) => ar.write_f64::<LE>((*v).into())?,
            Property::Bool(v) => ar.write_u8(u8::from(*v))?,
            Property::Byte(v) => match v {
                Byte::Byte(b) => ar.write_u8(*b)?,
                Byte::Label(l) => ar.write_string(l)?,
            },
            Property::Enum(v) => ar.write_string(v)?,
            Property::Name(v) => ar.write_string(v)?,
            Property::Str(v) => write_string(ar, v)?,
            Property::SoftObject(v) => ar.write_soft_object_path(v)?,
            Property::Object(v) => ar.write_object_ref(v)?,
            Property::Text(v) => v.write(ar)?,
            Property::Struct(v) => v.write(ar)?,
            Property::Set(_)
            | Property::Map(_)
            | Property::Array(_)
            | Property::FieldPath(_)
            | Property::Delegate(_)
            | Property::MulticastDelegate(_)
            | Property::MulticastInlineDelegate(_)
            | Property::MulticastSparseDelegate(_)
            | Property::Raw(_) => {
                return Err(Error::Other(format!(
                "Property variant {self:?} cannot be written in value context (Maps/Sets/Arrays)"
            )))
            }
        };
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustomFormatData {
    pub id: FGuid,
    pub value: i32,
}
impl CustomFormatData {
    #[cfg_attr(
        feature = "tracing",
        instrument(name = "CustomFormatData_read", skip_all)
    )]
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
    pub ue4: u32,
    pub ue5: Option<u32>,
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
    #[cfg_attr(feature = "tracing", instrument(name = "Header_read", skip_all))]
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
#[derive(Debug, PartialEq, Serialize)]
#[serde(bound(serialize = "T::ObjectRef: Serialize"))]
pub struct Root<T: ArchiveType = SaveGameArchiveType> {
    pub save_game_type: String,
    pub properties: Properties<T>,
}
impl<T: ArchiveType> Root<T> {
    #[cfg_attr(feature = "tracing", instrument(name = "Root_read", skip_all))]
    fn read<A: ArchiveReader<ArchiveType = T>>(ar: &mut A) -> Result<Self> {
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
    fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        ar.write_string(&self.save_game_type)?;
        if ar.version().property_tag() {
            ar.write_u8(0)?;
        }
        write_properties_none_terminated(ar, &self.properties)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize)]
pub struct Save<G: Game = NoGame> {
    pub header: Header,
    /// Property schemas (tags) separated from property data
    pub schemas: PropertySchemas,
    pub root: Root<SaveGameArchiveType<G>>,
    pub extra: Vec<u8>,
}
impl Save<NoGame> {
    /// Reads save from the given reader
    #[cfg_attr(feature = "tracing", instrument(name = "Root_read", skip_all))]
    pub fn read<R: Read>(reader: &mut R) -> Result<Self, ParseError> {
        Self::read_with_types(reader, Types::new())
    }
    /// Reads save from the given reader using the provided [`Types`]
    #[cfg_attr(
        feature = "tracing",
        instrument(name = "Save_read_with_types", skip_all)
    )]
    pub fn read_with_types<R: Read>(reader: &mut R, types: Types) -> Result<Self, ParseError> {
        SaveReader::new().types(types).read(reader)
    }
}
impl<G: Game> Save<G> {
    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut buffer = vec![];
        let schemas = Rc::new(RefCell::new(self.schemas.clone()));

        let mut archive_writer = SaveGameArchive {
            stream: Cursor::new(&mut buffer),
            version: Some(self.header.clone()),
            types: Rc::new(Types::new()),
            scope: Scope::root(),
            log: false,
            error_to_raw: true,
            schemas,
            _game: std::marker::PhantomData::<G>,
        };

        self.header.write(&mut archive_writer)?;
        self.root.write(&mut archive_writer)?;
        archive_writer.write_all(&self.extra)?;

        writer.write_all(&buffer)?;
        Ok(())
    }
    /// Writes the save through the game's canonical container format (see
    /// [`Game::compress_save`]; e.g. Palworld's zlib-compressed PLZ format).
    /// Games that support multiple container formats may expose additional
    /// explicit helpers (e.g. `Save::<Palworld>::write_plm` for Oodle).
    pub fn write_compressed<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut buffer = Vec::new();
        self.write(&mut buffer)?;

        let output = G::compress_save(&buffer)?;

        writer.write_all(&output)?;
        Ok(())
    }
}

pub struct SaveReader<G: Game = NoGame> {
    log: bool,
    error_to_raw: bool,
    types: Option<Rc<Types>>,
    _game: std::marker::PhantomData<G>,
}
impl Default for SaveReader<NoGame> {
    fn default() -> Self {
        Self::new()
    }
}
impl SaveReader<NoGame> {
    pub fn new() -> Self {
        Self {
            log: false,
            error_to_raw: false,
            types: None,
            _game: std::marker::PhantomData,
        }
    }
}
impl<G: Game> SaveReader<G> {
    pub fn log(mut self, log: bool) -> Self {
        self.log = log;
        self
    }
    /// Configure whether parsing errors should produce Raw properties (true) or fail immediately (false).
    ///
    /// When set to `true` (default), if a property cannot be parsed, it will be stored as a
    /// `Property::Raw` containing the raw bytes. This allows partial parsing of save files
    /// with unknown or corrupted properties.
    ///
    /// When set to `false`, parsing errors will immediately return an error, allowing you to
    /// detect and handle parsing issues explicitly.
    pub fn error_to_raw(mut self, error_to_raw: bool) -> Self {
        self.error_to_raw = error_to_raw;
        self
    }
    pub fn types(mut self, types: Types) -> Self {
        self.types = Some(Rc::new(types));
        self
    }
    /// Rebuild this reader for a different game type, carrying over the
    /// configured options.
    pub fn game<G2: Game>(self) -> SaveReader<G2> {
        SaveReader {
            log: self.log,
            error_to_raw: self.error_to_raw,
            types: self.types,
            _game: std::marker::PhantomData,
        }
    }
    pub fn read<S: Read>(self, mut stream: S) -> Result<Save<G>, ParseError> {
        let types = self.types.unwrap_or_else(|| Rc::new(Types::new()));
        let schemas = Rc::new(RefCell::new(PropertySchemas::new()));

        // Transparently decompress compressed save formats (e.g. Palworld PLM/PLZ).
        // Plain GVAS data is passed through unchanged (the default `Game::decompress_save`).
        let data = G::decompress_save(&mut stream)
            .map_err(|error| error::ParseError { offset: 0, error })?;
        let stream = SeekReader::new(Cursor::new(data));
        let mut reader: SaveGameArchive<_, G> = SaveGameArchive {
            stream,
            version: None,
            types,
            scope: Scope::root(),
            log: self.log,
            error_to_raw: self.error_to_raw,
            schemas: schemas.clone(),
            _game: std::marker::PhantomData,
        };

        let result = || -> Result<_> {
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

            Ok((header, root, extra))
        }();

        let offset = reader.stream_position().unwrap() as usize;

        drop(reader);

        let schemas = Rc::try_unwrap(schemas)
            .expect("Failed to extract schemas")
            .into_inner();

        result
            .map(|(header, root, extra)| Save {
                header,
                schemas,
                root,
                extra,
            })
            .map_err(|e| error::ParseError { offset, error: e })
    }
}
