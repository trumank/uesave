use crate::{
    ArchiveReader, ArchiveType, ArchiveWriter, FGuid, Properties, Result, SaveGameArchiveType,
};
use byteorder::ReadBytesExt;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T::ObjectRef: Serialize, T::SoftObjectPath: Serialize",
    deserialize = ""
))]
pub struct PalCharacterData<T: ArchiveType = SaveGameArchiveType> {
    pub object: Properties<T>,
    pub unknown_bytes: [u8; 4],
    pub group_id: FGuid,
    pub trailing_bytes: [u8; 4],
}

impl<T: ArchiveType> PalCharacterData<T> {
    pub fn read<A: ArchiveReader<ArchiveType = T>>(ar: &mut A) -> Result<Self> {
        Ok(PalCharacterData {
            object: crate::read_properties_until_none(ar)?,
            unknown_bytes: {
                let mut bytes = [0; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
            group_id: FGuid::read(ar)?,
            trailing_bytes: {
                let mut bytes = [0; 4];
                ar.read_exact(&mut bytes)?;
                bytes
            },
        })
    }
    pub fn write<A: ArchiveWriter<ArchiveType = T>>(&self, ar: &mut A) -> Result<()> {
        crate::write_properties_none_terminated(ar, &self.object)?;
        ar.write_all(&self.unknown_bytes)?;
        self.group_id.write(ar)?;
        ar.write_all(&self.trailing_bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PalCharacterContainer {
    pub player_uid: FGuid,
    pub instance_id: FGuid,
    pub permission_tribe_id: u8,
    pub trailing_bytes: Option<Vec<u8>>,
}

impl PalCharacterContainer {
    pub fn read<A: ArchiveReader>(ar: &mut A) -> Result<Self> {
        let player_uid = FGuid::read(ar)?;
        let instance_id = FGuid::read(ar)?;
        let permission_tribe_id = ar.read_u8()?;
        let mut trailing_bytes = Vec::new();
        if ar.read_to_end(&mut trailing_bytes)? > 0 {
            Ok(PalCharacterContainer {
                player_uid,
                instance_id,
                permission_tribe_id,
                trailing_bytes: Some(trailing_bytes),
            })
        } else {
            Ok(PalCharacterContainer {
                player_uid,
                instance_id,
                permission_tribe_id,
                trailing_bytes: None,
            })
        }
    }
    pub fn write<A: ArchiveWriter>(&self, ar: &mut A) -> Result<()> {
        self.player_uid.write(ar)?;
        self.instance_id.write(ar)?;
        ar.write_all(&[self.permission_tribe_id])?;
        if let Some(trailing) = &self.trailing_bytes {
            ar.write_all(trailing)?;
        }
        Ok(())
    }
}
