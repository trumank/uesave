use std::io::{Read, Seek, Write};

use crate::Context;

pub trait ArchiveReader: Read + Seek {}
pub trait ArchiveWriter: Write + Seek {}

impl<R> ArchiveReader for Context<R> where R: Read + Seek {}
impl<W> ArchiveWriter for Context<W> where W: Write + Seek {}
