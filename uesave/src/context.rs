use std::{
    io::{Read, Seek, Write},
    rc::Rc,
};

use crate::{Header, Result, StructType};

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

/// Represents the current position in the property hierarchy as a stack of names.
/// Used for looking up type hints in the Types map.
#[derive(Debug, Clone, Default)]
pub(crate) struct Scope {
    components: Vec<String>,
}

impl Scope {
    pub(crate) fn root() -> Self {
        Self::default()
    }

    fn path(&self) -> String {
        self.components.join(".")
    }

    fn push(&mut self, name: &str) {
        self.components.push(name.to_string());
    }

    fn pop(&mut self) {
        self.components.pop();
    }
}

#[derive(Debug)]
pub(crate) struct Context<S> {
    pub(crate) stream: S,
    pub(crate) state: ContextState,
}
#[derive(Debug)]
pub(crate) struct ContextState {
    pub(crate) version: Option<Header>,
    pub(crate) types: Rc<Types>,
    pub(crate) scope: Scope,
    pub(crate) log: bool,
}
impl<R: Read> Read for Context<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buf)
    }
}
impl<S: Seek> Seek for Context<S> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.stream.seek(pos)
    }
}
impl<W: Write + Seek> Write for Context<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.stream.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }
}

impl<S> Context<S> {
    pub(crate) fn run<F, T>(stream: S, f: F) -> T
    where
        F: FnOnce(&mut Context<S>) -> T,
    {
        f(&mut Context {
            stream,
            state: ContextState {
                version: None,
                types: Rc::new(Types::new()),
                scope: Scope::root(),
                log: false,
            },
        })
    }
    pub(crate) fn with_scope<F, T>(&mut self, name: &str, f: F) -> T
    where
        F: FnOnce(&mut Context<S>) -> T,
    {
        self.state.scope.push(name);
        let result = f(self);
        self.state.scope.pop();
        result
    }
    fn path(&self) -> String {
        self.state.scope.path()
    }
    fn get_type(&self) -> Option<&StructType> {
        self.state.types.types.get(&self.path())
    }
    pub(crate) fn set_version(&mut self, version: Header) {
        self.state.version = Some(version);
    }
    pub(crate) fn version(&self) -> &Header {
        self.state.version.as_ref().expect("version info not set")
    }
    pub(crate) fn log(&self) -> bool {
        self.state.log
    }
}
impl<R: Read + Seek> Context<R> {
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
