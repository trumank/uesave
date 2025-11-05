use std::io::{Read, Seek, Write};

use crate::{Context, VersionInfo};

pub trait ArchiveReader: Read + Seek {
    fn version(&self) -> &dyn VersionInfo;
}
pub trait ArchiveWriter: Write + Seek {
    fn version(&self) -> &dyn VersionInfo;
}

impl<R> ArchiveReader for Context<R>
where
    R: Read + Seek,
{
    fn version(&self) -> &dyn VersionInfo {
        Context::version(self)
    }
}
impl<W> ArchiveWriter for Context<W>
where
    W: Write + Seek,
{
    fn version(&self) -> &dyn VersionInfo {
        Context::version(self)
    }
}
