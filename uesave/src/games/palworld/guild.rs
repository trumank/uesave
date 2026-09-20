use crate::{ArchiveReader, ArchiveWriter, FGuid, Result};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalLabResearchInfo {
    pub research_id: String,
    pub work_amount: f32,
}

impl PalLabResearchInfo {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        Ok(PalLabResearchInfo {
            research_id: ar.read_string()?,
            work_amount: ar.read_f32::<LE>()?,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_string(&self.research_id)?;
        ar.write_f32::<LE>(self.work_amount)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalGuildLab {
    pub research_info: Vec<PalLabResearchInfo>,
    pub current_research_id: String,
    pub trailing_bytes: Vec<u8>,
}

impl PalGuildLab {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let research_count = ar.read_u32::<LE>()?;
        let mut research_info =
            Vec::with_capacity(crate::bounded_prealloc(research_count as usize));
        for _ in 0..research_count {
            research_info.push(PalLabResearchInfo::read(ar)?);
        }

        let current_research_id = ar.read_string()?;

        let mut trailing_bytes = Vec::new();
        ar.read_to_end(&mut trailing_bytes)?;

        Ok(PalGuildLab {
            research_info,
            current_research_id,
            trailing_bytes,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        ar.write_u32::<LE>(self.research_info.len() as u32)?;
        for info in &self.research_info {
            info.write(ar)?;
        }

        ar.write_string(&self.current_research_id)?;

        ar.write_all(&self.trailing_bytes)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalGuildItemStorage {
    pub container_id: FGuid,
    pub trailing_bytes: Vec<u8>,
}

impl PalGuildItemStorage {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let container_id = FGuid::read(ar)?;

        let mut trailing_bytes = Vec::new();
        ar.read_to_end(&mut trailing_bytes)?;

        Ok(PalGuildItemStorage {
            container_id,
            trailing_bytes,
        })
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.container_id.write(ar)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}
