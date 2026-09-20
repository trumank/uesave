use std::{
    cell::RefCell,
    collections::BTreeMap,
    io::{Read, Seek, Write},
    marker::PhantomData,
    rc::Rc,
};

use serde::{Deserialize, Serialize};

use crate::game::{Game, NoGame};
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
    /// Look up the registered type for an exact path
    pub fn get(&self, path: &str) -> Option<&StructType> {
        self.types.get(path)
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
pub struct SaveGameArchive<S, G: Game = NoGame> {
    pub(crate) stream: S,
    pub(crate) version: Option<Header>,
    pub(crate) types: Rc<Types>,
    pub(crate) scope: Scope,
    pub(crate) log: bool,
    pub(crate) error_to_raw: bool,
    pub(crate) schemas: Rc<RefCell<PropertySchemas>>,
    pub(crate) _game: PhantomData<G>,
}
impl<R: Read, G: Game> Read for SaveGameArchive<R, G> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buf)
    }
}
impl<S: Seek, G: Game> Seek for SaveGameArchive<S, G> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.stream.seek(pos)
    }
}
impl<W: Write + Seek, G: Game> Write for SaveGameArchive<W, G> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.stream.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }
}

impl<S, G: Game> SaveGameArchive<S, G> {
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
            _game: PhantomData,
        }
    }
    pub(crate) fn run<F, T>(stream: S, f: F) -> T
    where
        F: FnOnce(&mut SaveGameArchive<S, G>) -> T,
    {
        f(&mut SaveGameArchive::new(stream))
    }
    fn path(&self) -> String {
        self.scope.path()
    }
    pub(crate) fn get_type(&self) -> Option<&StructType> {
        self.types.types.get(&self.path())
    }
    /// Run `f` with a nested archive over `stream` that shares this archive's
    /// version, types, scope and schemas. Used to parse/serialize game-specific
    /// data embedded as byte arrays (e.g. Palworld RawData properties).
    pub(crate) fn with_nested<S2, T>(
        &mut self,
        stream: S2,
        f: impl FnOnce(&mut SaveGameArchive<S2, G>) -> Result<T>,
    ) -> Result<T> {
        let mut nested = SaveGameArchive {
            stream,
            version: self.version.clone(),
            types: Rc::clone(&self.types),
            scope: std::mem::take(&mut self.scope),
            log: self.log,
            error_to_raw: self.error_to_raw,
            schemas: Rc::clone(&self.schemas),
            _game: PhantomData,
        };
        let result = f(&mut nested);
        self.scope = nested.scope;
        result
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
}
impl<R: Read + Seek, G: Game> SaveGameArchive<R, G> {
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
