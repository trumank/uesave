use std::{
    cell::RefCell,
    collections::BTreeMap,
    io::{Read, Seek, Write},
    rc::Rc,
};

use serde::{Deserialize, Serialize};

use crate::{Header, PropertyTagPartial, Result, StructType};

/// Used to disambiguate types within a [`Property::Set`] or [`Property::Map`] during parsing.
#[derive(Debug, Default, Clone)]
pub struct Types {
    types: std::collections::HashMap<String, StructType>,
}
impl Types {
    /// Create an empty [`Types`] specification
    pub fn new() -> Self {
        Self::default()
    }
    /// Add a new type at the given path
    pub fn add(&mut self, path: String, t: StructType) {
        // TODO: Handle escaping of '.' in property names
        // probably should store keys as Vec<String>
        self.types.insert(path, t);
    }
}

/// Storage for property schemas (tags) separated from property data.
/// Maps property paths to their type metadata.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertySchemas {
    schemas: BTreeMap<String, PropertyTagPartial>,
}

impl PropertySchemas {
    /// Create an empty PropertySchemas
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a schema at the given path
    pub fn record(&mut self, path: String, tag: PropertyTagPartial) {
        self.schemas.insert(path, tag);
    }

    /// Get a schema at the given path
    pub fn get(&self, path: &str) -> Option<&PropertyTagPartial> {
        self.schemas.get(path)
    }

    /// Get all schemas
    pub fn schemas(&self) -> &BTreeMap<String, PropertyTagPartial> {
        &self.schemas
    }
}

/// Represents the current position in the property hierarchy as a stack of names.
/// Used for looking up type hints in the Types map.
#[derive(Debug, Clone, Default)]
pub struct Scope {
    components: Vec<String>,
}

impl Scope {
    pub fn root() -> Self {
        Self::default()
    }

    pub fn path(&self) -> String {
        self.components.join(".")
    }

    pub fn push(&mut self, name: &str) {
        self.components.push(name.to_string());
    }

    pub fn pop(&mut self) {
        self.components.pop();
    }
}

#[derive(Debug)]
pub struct SaveGameArchive<S> {
    pub(crate) stream: S,
    pub(crate) version: Option<Header>,
    pub(crate) types: Rc<Types>,
    pub(crate) scope: Scope,
    pub(crate) log: bool,
    pub(crate) error_to_raw: bool,
    pub(crate) schemas: Rc<RefCell<PropertySchemas>>,
    pub(crate) read_limits: Vec<u64>,
}
impl<R: Read + Seek> Read for SaveGameArchive<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if let Some(limit) = self.read_limits.last().copied() {
            let position = self.stream.stream_position()?;
            let remaining = limit.saturating_sub(position);
            if remaining == 0 || buf.is_empty() {
                return Ok(0);
            }
            let max_read = remaining.min(buf.len() as u64) as usize;
            self.stream.read(&mut buf[..max_read])
        } else {
            self.stream.read(buf)
        }
    }
}
impl<S: Seek> Seek for SaveGameArchive<S> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.stream.seek(pos)
    }
}
impl<W: Write + Seek> Write for SaveGameArchive<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.stream.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }
}

impl<S> SaveGameArchive<S> {
    /// Construct a new archive around `stream`. The version (Header) must be set
    /// via [`set_version`](Self::set_version) before reading any property data,
    /// since property serialization depends on engine/package versions.
    pub fn new(stream: S) -> Self {
        SaveGameArchive {
            stream,
            version: None,
            types: Rc::new(Types::new()),
            scope: Scope::root(),
            log: false,
            error_to_raw: false,
            schemas: Rc::new(RefCell::new(PropertySchemas::new())),
            read_limits: vec![],
        }
    }
    #[cfg(test)]
    pub(crate) fn run<F, T>(stream: S, f: F) -> T
    where
        F: FnOnce(&mut SaveGameArchive<S>) -> T,
    {
        f(&mut SaveGameArchive::new(stream))
    }
    fn path(&self) -> String {
        self.scope.path()
    }
    fn get_type(&self) -> Option<&StructType> {
        self.types.types.get(&self.path())
    }
    pub fn set_version(&mut self, version: Header) {
        self.version = Some(version);
    }
    pub(crate) fn version(&self) -> &Header {
        self.version.as_ref().expect("version info not set")
    }
    pub(crate) fn log(&self) -> bool {
        self.log
    }
    pub(crate) fn error_to_raw(&self) -> bool {
        self.error_to_raw
    }
    pub(crate) fn push_read_limit(&mut self, end: u64) {
        self.read_limits.push(end);
    }
    pub(crate) fn pop_read_limit(&mut self) {
        self.read_limits.pop();
    }
}
impl<R: Read + Seek> SaveGameArchive<R> {
    pub(crate) fn get_type_or(&mut self, t: &StructType) -> Result<StructType> {
        let offset = self.stream.stream_position()?;
        Ok(self.get_type().cloned().unwrap_or_else(|| {
            if self.log() {
                eprintln!(
                    "offset {}: StructType for \"{}\" unspecified, assuming {:?}",
                    offset,
                    self.path(),
                    t
                );
            }
            t.clone()
        }))
    }
}
