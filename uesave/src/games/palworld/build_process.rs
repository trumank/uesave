use crate::{ArchiveReader, ArchiveWriter, FGuid, Result};
use byteorder::{ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalBuildProcess {
    pub state: u8,
    pub id: FGuid,
    pub trailing_bytes: [u8; 4],
}

impl PalBuildProcess {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let state = ar.read_u8()?;
        let id = FGuid::read(ar)?;

        let mut trailing_bytes = [0u8; 4];
        ar.read_exact(&mut trailing_bytes)?;

        let mut remaining = Vec::new();
        if ar.read_to_end(&mut remaining)? > 0 {
            return Err(crate::Error::Other("Warning: EOF not reached".to_string()));
        }

        Ok(PalBuildProcess {
            state,
            id,
            trailing_bytes,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u8(self.state)?;
        self.id.write(ar)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}
