use crate::*;
use pretty_assertions::assert_eq;

use std::io::Cursor;

const SAVE: &[u8] = include_bytes!("../drg-save-test.sav");

fn mock_header() -> Header {
    Header {
        magic: u32::from_le_bytes(*b"GVAS"),
        save_game_version: 2,
        package_version: PackageVersion {
            ue4: 522,
            ue5: None,
        },
        engine_version_major: 4,
        engine_version_minor: 27,
        engine_version_patch: 0,
        engine_version_build: 0,
        engine_version: String::from("4.27.0"),
        custom_version: Some((3, vec![])),
    }
}

fn run<S, R, F>(s: S, f: F) -> Result<R>
where
    F: FnOnce(&mut SaveGameArchive<S>) -> Result<R>,
{
    SaveGameArchive::run(s, |s| {
        s.set_version(mock_header());
        f(s)
    })
}

/// Same as [`run`], but pins the archive's game to Palworld instead of the
/// default `NoGame`. Needed to drive dispatch that resolves through
/// `<G as Game>::...` (e.g. `StructValue::read`'s `StructType::Game` arm).
fn run_palworld<S, R, F>(s: S, f: F) -> Result<R>
where
    F: FnOnce(&mut SaveGameArchive<S, games::palworld::Palworld>) -> Result<R>,
{
    SaveGameArchive::run(s, |s| {
        s.set_version(mock_header());
        f(s)
    })
}

fn palworld_header() -> Header {
    Header {
        magic: u32::from_le_bytes(*b"GVAS"),
        save_game_version: 2,
        package_version: PackageVersion {
            ue4: 522,
            ue5: Some(1008),
        },
        engine_version_major: 5,
        engine_version_minor: 1,
        engine_version_patch: 0,
        engine_version_build: 0,
        engine_version: String::from("5.1.0"),
        custom_version: Some((3, vec![])),
    }
}

/// Same as [`run_palworld`], but with a UE 5.1 header, so `Vector`/`Quat`
/// read and write as the f64 layout real Palworld saves use.
fn run_palworld_ue5<S, R, F>(s: S, f: F) -> Result<R>
where
    F: FnOnce(&mut SaveGameArchive<S, games::palworld::Palworld>) -> Result<R>,
{
    SaveGameArchive::run(s, |s| {
        s.set_version(palworld_header());
        f(s)
    })
}

#[test]
fn test_header() -> Result<()> {
    let original = [
        0x47, 0x56, 0x41, 0x53, 0x02, 0x00, 0x00, 0x00, 0x06, 0x02, 0x00, 0x00, 0x04, 0x00, 0x19,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00, 0x2B, 0x2B, 0x55, 0x45,
        0x34, 0x2B, 0x52, 0x65, 0x6C, 0x65, 0x61, 0x73, 0x65, 0x2D, 0x34, 0x2E, 0x32, 0x35, 0x50,
        0x6C, 0x75, 0x73, 0x00, 0x03, 0x00, 0x00, 0x00, 0x31, 0x00, 0x00, 0x00, 0xFA, 0x7A, 0xF5,
        0xFC, 0x83, 0x42, 0x76, 0x50, 0x58, 0xE6, 0xA9, 0xB9, 0x32, 0x2D, 0xA0, 0xFF, 0x3D, 0x00,
        0x00, 0x00, 0xF3, 0x7A, 0xBB, 0x24, 0x83, 0x4F, 0x46, 0x56, 0xC2, 0x2D, 0x2F, 0x1F, 0xFF,
        0x96, 0xAD, 0x49, 0x05, 0x00, 0x00, 0x00, 0x29, 0x23, 0xA5, 0x76, 0xB5, 0x45, 0x23, 0x09,
        0x41, 0xD8, 0xAE, 0x98, 0xD8, 0x6A, 0x2F, 0xCF, 0x02, 0x00, 0x00, 0x00, 0x07, 0x69, 0xBC,
        0x5F, 0xAE, 0x40, 0xC8, 0x55, 0x84, 0xF1, 0x67, 0x8E, 0x3F, 0xF1, 0xFF, 0x5E, 0x01, 0x00,
        0x00, 0x00, 0x12, 0xE4, 0x26, 0xFB, 0x4D, 0x4B, 0x15, 0x1F, 0x0A, 0x55, 0x72, 0x93, 0x70,
        0x2F, 0x1D, 0x96, 0x03, 0x00, 0x00, 0x00, 0x22, 0xD5, 0x54, 0x9C, 0xBE, 0x4F, 0x26, 0xA8,
        0x46, 0x07, 0x21, 0x94, 0xD0, 0x82, 0xB4, 0x61, 0x1E, 0x00, 0x00, 0x00, 0xE4, 0x32, 0xD8,
        0xB0, 0x0D, 0x4F, 0x89, 0x1F, 0xB7, 0x7E, 0xCF, 0xAC, 0xA2, 0x4A, 0xFD, 0x36, 0x0A, 0x00,
        0x00, 0x00, 0x28, 0x43, 0xC6, 0xE1, 0x53, 0x4D, 0x2C, 0xA2, 0x86, 0x8E, 0x6C, 0xA3, 0x8C,
        0xBD, 0x17, 0x64, 0x00, 0x00, 0x00, 0x00, 0x3C, 0xC1, 0x5E, 0x37, 0xFB, 0x48, 0xE4, 0x06,
        0xF0, 0x84, 0x00, 0xB5, 0x7E, 0x71, 0x2A, 0x26, 0x04, 0x00, 0x00, 0x00, 0xED, 0x68, 0xB0,
        0xE4, 0xE9, 0x42, 0x94, 0xF4, 0x0B, 0xDA, 0x31, 0xA2, 0x41, 0xBB, 0x46, 0x2E, 0x26, 0x00,
        0x00, 0x00, 0x3F, 0x74, 0xFC, 0xCF, 0x80, 0x44, 0xB0, 0x43, 0xDF, 0x14, 0x91, 0x93, 0x73,
        0x20, 0x1D, 0x17, 0x25, 0x00, 0x00, 0x00, 0xB5, 0x49, 0x2B, 0xB0, 0xE9, 0x44, 0x20, 0xBB,
        0xB7, 0x32, 0x04, 0xA3, 0x60, 0x03, 0xE4, 0x52, 0x02, 0x00, 0x00, 0x00, 0x5C, 0x10, 0xE4,
        0xA4, 0xB5, 0x49, 0xA1, 0x59, 0xC4, 0x40, 0xC5, 0xA7, 0xEE, 0xDF, 0x7E, 0x54, 0x00, 0x00,
        0x00, 0x00, 0xC9, 0x31, 0xC8, 0x39, 0xDC, 0x47, 0xE6, 0x5A, 0x17, 0x9C, 0x44, 0x9A, 0x7C,
        0x8E, 0x1C, 0x3E, 0x00, 0x00, 0x00, 0x00, 0x33, 0x1B, 0xF0, 0x78, 0x98, 0x4F, 0xEA, 0xEB,
        0xEA, 0x84, 0xB4, 0xB9, 0xA2, 0x5A, 0xB9, 0xCC, 0x04, 0x00, 0x00, 0x00, 0x0F, 0x38, 0x31,
        0x66, 0xE0, 0x43, 0x4D, 0x2D, 0x27, 0xCF, 0x09, 0x80, 0x5A, 0xA9, 0x56, 0x69, 0x00, 0x00,
        0x00, 0x00, 0x9F, 0x8B, 0xF8, 0x12, 0xFC, 0x4A, 0x75, 0x88, 0x0C, 0xD9, 0x7C, 0xA6, 0x29,
        0xBD, 0x3A, 0x38, 0x2B, 0x00, 0x00, 0x00, 0x4C, 0xE7, 0x5A, 0x7B, 0x10, 0x4C, 0x70, 0xD2,
        0x98, 0x57, 0x58, 0xA9, 0x5A, 0x2A, 0x21, 0x0B, 0x0C, 0x00, 0x00, 0x00, 0x18, 0x69, 0x29,
        0xD7, 0xDD, 0x4B, 0xD6, 0x1D, 0xA8, 0x64, 0xE2, 0x9D, 0x84, 0x38, 0xC1, 0x3C, 0x03, 0x00,
        0x00, 0x00, 0x78, 0x52, 0xA1, 0xC2, 0xFE, 0x4A, 0xE7, 0xBF, 0xFF, 0x90, 0x17, 0x6C, 0x55,
        0xF7, 0x1D, 0x53, 0x01, 0x00, 0x00, 0x00, 0xD4, 0xA3, 0xAC, 0x6E, 0xC1, 0x4C, 0xEC, 0x40,
        0xED, 0x8B, 0x86, 0xB7, 0xC5, 0x8F, 0x42, 0x09, 0x03, 0x00, 0x00, 0x00, 0xDD, 0x75, 0xE5,
        0x29, 0x27, 0x46, 0xA3, 0xE0, 0x76, 0xD2, 0x10, 0x9D, 0xEA, 0xDC, 0x2C, 0x23, 0x11, 0x00,
        0x00, 0x00, 0x5D, 0xA6, 0x43, 0xAF, 0x47, 0x49, 0xD3, 0x7F, 0x8E, 0x3E, 0x73, 0x98, 0x05,
        0xBB, 0xC1, 0xD9, 0x07, 0x00, 0x00, 0x00, 0xEC, 0x6C, 0x26, 0x6B, 0x8F, 0x4B, 0xC7, 0x1E,
        0xD9, 0xE4, 0x0B, 0xA3, 0x07, 0xFC, 0x42, 0x09, 0x01, 0x00, 0x00, 0x00, 0x61, 0x3D, 0xF7,
        0x0D, 0xEA, 0x47, 0x3F, 0xA2, 0xE9, 0x89, 0x27, 0xB7, 0x9A, 0x49, 0x41, 0x0C, 0x01, 0x00,
        0x00, 0x00, 0x86, 0x18, 0x1D, 0x60, 0x84, 0x4F, 0x64, 0xAC, 0xDE, 0xD3, 0x16, 0xAA, 0xD6,
        0xC7, 0xEA, 0x0D, 0x1F, 0x00, 0x00, 0x00, 0xD6, 0xBC, 0xFF, 0x9D, 0x58, 0x01, 0x4F, 0x49,
        0x82, 0x12, 0x21, 0xE2, 0x88, 0xA8, 0x92, 0x3C, 0x0A, 0x00, 0x00, 0x00, 0xAC, 0xD0, 0xAE,
        0xF2, 0x6F, 0x41, 0xFE, 0x9A, 0x7F, 0xAA, 0x64, 0x86, 0xFC, 0xD6, 0x26, 0xFA, 0x01, 0x00,
        0x00, 0x00, 0x0B, 0x1F, 0x4F, 0x17, 0xA5, 0x45, 0xC6, 0xB4, 0xE8, 0x2E, 0x3F, 0xB1, 0x7D,
        0x91, 0xFB, 0xD0, 0x0A, 0x00, 0x00, 0x00, 0x83, 0x4A, 0xF9, 0x35, 0x6C, 0x40, 0x58, 0xE2,
        0xF5, 0x09, 0x18, 0xA3, 0x7C, 0x24, 0x10, 0x96, 0x25, 0x00, 0x00, 0x00, 0x6E, 0xC1, 0x8F,
        0xB6, 0xE2, 0x42, 0x1B, 0x8B, 0x5C, 0x21, 0x53, 0xB4, 0xFE, 0x44, 0x88, 0x05, 0x01, 0x00,
        0x00, 0x00, 0x06, 0x85, 0xE1, 0xB2, 0xC2, 0xCF, 0x73, 0x42, 0xBB, 0xF4, 0x4E, 0xA5, 0x07,
        0xBA, 0x8B, 0x75, 0x01, 0x00, 0x00, 0x00, 0x50, 0x32, 0x68, 0x54, 0xAF, 0x48, 0x99, 0x80,
        0x96, 0x98, 0xC8, 0x8B, 0xB7, 0xF9, 0xAD, 0xFB, 0x00, 0x00, 0x00, 0x00, 0x19, 0x4D, 0x0C,
        0x43, 0x70, 0x49, 0x54, 0x71, 0x69, 0x9B, 0x69, 0x87, 0xE5, 0xB0, 0x90, 0xDF, 0x0E, 0x00,
        0x00, 0x00, 0xBD, 0x32, 0xFE, 0xAA, 0x14, 0x4C, 0x95, 0x53, 0x25, 0x5E, 0x6A, 0xB6, 0xDD,
        0xD1, 0x32, 0x10, 0x01, 0x00, 0x00, 0x00, 0x8E, 0xE1, 0xAF, 0x23, 0x58, 0x4E, 0xE1, 0x4C,
        0x52, 0xC2, 0x61, 0x8D, 0xB7, 0xBE, 0x53, 0xB9, 0x0B, 0x00, 0x00, 0x00, 0xEA, 0xB7, 0x62,
        0xA4, 0x3A, 0x4E, 0x99, 0xF4, 0x1F, 0xEC, 0xC1, 0x99, 0xB2, 0xE1, 0x24, 0x82, 0x02, 0x00,
        0x00, 0x00, 0xBD, 0xFD, 0xB5, 0x2E, 0x10, 0x4D, 0xAC, 0x01, 0x8F, 0xF3, 0x36, 0x81, 0xDA,
        0xA5, 0x93, 0x33, 0x05, 0x00, 0x00, 0x00, 0x4F, 0x35, 0x9D, 0x50, 0x2F, 0x49, 0xE6, 0xF6,
        0xB2, 0x85, 0x49, 0xA7, 0x1C, 0x63, 0x3C, 0x07, 0x00, 0x00, 0x00, 0x00, 0xE7, 0x9E, 0x7F,
        0x71, 0x3A, 0x49, 0xB0, 0xE9, 0x32, 0x91, 0xB3, 0x88, 0x07, 0x81, 0x38, 0x1B, 0x06, 0x00,
        0x00, 0x00, 0x40, 0xEB, 0x56, 0x4A, 0xDC, 0x11, 0xF5, 0x10, 0x7E, 0x34, 0xD3, 0x92, 0xE7,
        0x6A, 0xC9, 0xB2, 0x02, 0x00, 0x00, 0x00, 0x00, 0x4A, 0x8A, 0xD7, 0x97, 0x46, 0x58, 0xE8,
        0xB5, 0x19, 0xA8, 0xBA, 0xB4, 0x46, 0x7D, 0x48, 0x11, 0x00, 0x00, 0x00, 0x86, 0xF8, 0x79,
        0x55, 0x1F, 0x4C, 0x3A, 0x93, 0x7B, 0x08, 0xBA, 0x83, 0x2F, 0xB9, 0x61, 0x63, 0x01, 0x00,
        0x00, 0x00, 0x52, 0xBE, 0x2F, 0x61, 0x0B, 0x40, 0x53, 0xDA, 0x91, 0x4F, 0x0D, 0x91, 0x7C,
        0x85, 0xB1, 0x9F, 0x01, 0x00, 0x00, 0x00, 0x36, 0x7A, 0x23, 0xA4, 0xC9, 0x41, 0xEA, 0xCA,
        0xF8, 0x18, 0xA2, 0x8F, 0xF3, 0x1B, 0x68, 0x58, 0x04, 0x00, 0x00, 0x00, 0x75, 0x3F, 0x4E,
        0x80, 0x49, 0x4B, 0x88, 0x70, 0x06, 0x8C, 0xD6, 0xA4, 0xDC, 0xB6, 0x7E, 0x3C, 0x05, 0x00,
        0x00, 0x00, 0xF2, 0x0A, 0x68, 0xFB, 0xA3, 0x4B, 0xEF, 0x59, 0xB5, 0x19, 0xA8, 0xBA, 0x3D,
        0x44, 0xC8, 0x73, 0x02, 0x00, 0x00, 0x00, 0x0E, 0xB7, 0x50, 0x99, 0x17, 0x4E, 0x1A, 0xB4,
        0x0D, 0xFA, 0xCC, 0xBB, 0xD6, 0x7F, 0x81, 0x57, 0x01, 0x00, 0x00, 0x00, 0x96, 0x51, 0x96,
        0xAB, 0xFC, 0x08, 0xD8, 0x45, 0x8D, 0x22, 0xD7, 0xB7, 0x9E, 0x56, 0xAD, 0x78, 0x01, 0x00,
        0x00, 0x00,
    ];
    let mut reader = Cursor::new(&original);
    let header = run(&mut reader, Header::read)?;
    let mut reconstructed = vec![];
    run(&mut Cursor::new(&mut reconstructed), |writer| {
        header.write(writer)
    })?;
    assert_eq!(original, &reconstructed[..]);
    Ok(())
}

#[test]
fn test_uuid() -> Result<()> {
    let id = FGuid::parse_str("2eb5fdbd4d1001ac8ff33681daa59333")?;
    let mut writer = vec![];
    run(&mut Cursor::new(&mut writer), |writer| id.write(writer))?;
    let mut reader = Cursor::new(&writer);
    let rid = run(&mut reader, FGuid::read)?;
    assert_eq!(id, rid);
    Ok(())
}

#[test]
fn test_uuid2() -> Result<()> {
    let id = FGuid::parse_str("85b20ca1-49fb-7138-a154-c89a2c20e2cd")?;
    let mut writer = vec![];
    run(&mut Cursor::new(&mut writer), |writer| id.write(writer))?;
    assert_eq!(
        writer,
        [
            0xa1, 0x0c, 0xb2, 0x85, 0x38, 0x71, 0xfb, 0x49, 0x9a, 0xc8, 0x54, 0xa1, 0xcd, 0xe2,
            0x20, 0x2c
        ]
    );
    Ok(())
}

#[test]
fn test_uuid3() -> Result<()> {
    let s = "85b20ca1-49fb-7138-a154-c89a2c20e2cd";
    let id = FGuid::parse_str(s)?;
    assert_eq!(s, id.to_string());
    Ok(())
}

#[test]
fn test_rw_save1() -> Result<()> {
    let mut reader = Cursor::new(&SAVE);
    let obj = Save::read(&mut reader).unwrap();
    let mut reconstructed: Vec<u8> = vec![];
    obj.write(&mut Cursor::new(&mut reconstructed))?;
    assert_eq!(SAVE, reconstructed);
    Ok(())
}

#[test]
fn test_read_int_property() -> Result<()> {
    let bytes = [
        0x0E, 0x00, 0x00, 0x00, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x4E, 0x75, 0x6D, 0x62,
        0x65, 0x72, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x49, 0x6E, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65,
        0x72, 0x74, 0x79, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
        0x00, 0x00,
    ];
    let mut reader = Cursor::new(bytes);
    assert_eq!(
        run(&mut reader, read_property)?,
        Some(("VersionNumber".into(), Property::Int(2),))
    );
    Ok(())
}

#[test]
fn test_read_struct_property() -> Result<()> {
    let bytes = [
        0x12, 0x00, 0x00, 0x00, 0x56, 0x61, 0x6E, 0x69, 0x74, 0x79, 0x4D, 0x61, 0x73, 0x74, 0x65,
        0x72, 0x79, 0x53, 0x61, 0x76, 0x65, 0x00, 0x0F, 0x00, 0x00, 0x00, 0x53, 0x74, 0x72, 0x75,
        0x63, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x8D, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x12, 0x00, 0x00, 0x00, 0x56, 0x61, 0x6E, 0x69, 0x74, 0x79, 0x4D,
        0x61, 0x73, 0x74, 0x65, 0x72, 0x79, 0x53, 0x61, 0x76, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x00,
        0x00, 0x00, 0x4C, 0x65, 0x76, 0x65, 0x6C, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x49, 0x6E, 0x74,
        0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x8C, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x58, 0x50, 0x00, 0x0C,
        0x00, 0x00, 0x00, 0x49, 0x6E, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00,
        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3A, 0x23, 0x00, 0x00, 0x1A, 0x00,
        0x00, 0x00, 0x48, 0x61, 0x73, 0x41, 0x77, 0x61, 0x72, 0x64, 0x65, 0x64, 0x46, 0x6F, 0x72,
        0x4F, 0x6C, 0x64, 0x50, 0x75, 0x72, 0x63, 0x68, 0x61, 0x73, 0x65, 0x73, 0x00, 0x0D, 0x00,
        0x00, 0x00, 0x42, 0x6F, 0x6F, 0x6C, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x05, 0x00, 0x00, 0x00, 0x4E,
        0x6F, 0x6E, 0x65, 0x00,
    ];
    let mut reader = Cursor::new(bytes);
    assert_eq!(
        run(&mut reader, read_property)?,
        Some((
            "VanityMasterySave".into(),
            Property::Struct(StructValue::Struct(Properties(indexmap::IndexMap::from([
                ("Level".into(), Property::Int(140)),
                ("XP".into(), Property::Int(9018)),
                ("HasAwardedForOldPurchases".into(), Property::Bool(true)),
            ]))),)
        ))
    );
    Ok(())
}

#[test]
fn test_read_array_property() -> Result<()> {
    let bytes = [
        0x0C, 0x00, 0x00, 0x00, 0x53, 0x74, 0x61, 0x74, 0x49, 0x6E, 0x64, 0x69, 0x63, 0x65, 0x73,
        0x00, 0x0E, 0x00, 0x00, 0x00, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72, 0x6F, 0x70, 0x65,
        0x72, 0x74, 0x79, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0C, 0x00, 0x00,
        0x00, 0x49, 0x6E, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    let mut reader = Cursor::new(bytes);
    assert_eq!(
        run(&mut reader, read_property)?,
        Some((
            "StatIndices".into(),
            Property::Array(ValueVec::Int(vec![0])),
        ))
    );
    Ok(())
}

fn rw_property(original: &[u8]) -> Result<()> {
    let mut reader = Cursor::new(&original);
    let (property, schemas) = run(&mut reader, |reader| {
        let property = read_property(reader)?.unwrap();
        println!("{property:#?}");
        // Extract schemas from the reader archive
        let schemas = reader.schemas.borrow().clone();
        Ok((property, schemas))
    })?;

    let mut reconstructed: Vec<u8> = vec![];
    run(&mut Cursor::new(&mut reconstructed), |writer| {
        // Inject schemas into the writer archive
        *writer.schemas.borrow_mut() = schemas;
        write_property((&property.0, &property.1), writer)
    })?;

    assert_eq!(original, &reconstructed[..]);
    Ok(())
}

#[test]
fn test_rw_property_int() -> Result<()> {
    let original = [
        0x0E, 0x00, 0x00, 0x00, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x4E, 0x75, 0x6D, 0x62,
        0x65, 0x72, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x49, 0x6E, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65,
        0x72, 0x74, 0x79, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
        0x00, 0x00,
    ];
    rw_property(&original)
}

#[test]
fn test_rw_property_bool() -> Result<()> {
    let original = [
        0x13, 0x00, 0x00, 0x00, 0x48, 0x61, 0x76, 0x65, 0x53, 0x6B, 0x69, 0x6E, 0x73, 0x42, 0x65,
        0x65, 0x6E, 0x52, 0x65, 0x73, 0x65, 0x74, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x42, 0x6F, 0x6F,
        0x6C, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x01, 0x00,
    ];
    rw_property(&original)
}

#[test]
fn test_rw_property_struct() -> Result<()> {
    let original = [
        0x08, 0x00, 0x00, 0x00, 0x46, 0x6F, 0x72, 0x67, 0x69, 0x6E, 0x67, 0x00, 0x0F, 0x00, 0x00,
        0x00, 0x53, 0x74, 0x72, 0x75, 0x63, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79,
        0x00, 0x2D, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x46, 0x6F,
        0x72, 0x67, 0x69, 0x6E, 0x67, 0x53, 0x61, 0x76, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
        0x00, 0x58, 0x50, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x49, 0x6E, 0x74, 0x50, 0x72, 0x6F, 0x70,
        0x65, 0x72, 0x74, 0x79, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xB3,
        0x01, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x4E, 0x6F, 0x6E, 0x65, 0x00,
    ];
    rw_property(&original)
}

#[test]
fn test_rw_property_struct2() -> Result<()> {
    let original = [
        0x07, 0x00, 0x00, 0x00, 0x44, 0x72, 0x69, 0x6E, 0x6B, 0x73, 0x00, 0x0F, 0x00, 0x00, 0x00,
        0x53, 0x74, 0x72, 0x75, 0x63, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00,
        0x52, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x00, 0x00, 0x44, 0x72, 0x69,
        0x6E, 0x6B, 0x53, 0x61, 0x76, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F, 0x00, 0x00, 0x00, 0x55, 0x6E,
        0x6C, 0x6F, 0x63, 0x6B, 0x65, 0x64, 0x44, 0x72, 0x69, 0x6E, 0x6B, 0x73, 0x00, 0x0C, 0x00,
        0x00, 0x00, 0x53, 0x65, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0xD8,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F, 0x00, 0x00, 0x00, 0x53, 0x74, 0x72, 0x75,
        0x63, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x0D, 0x00, 0x00, 0x00, 0x80, 0xC0, 0x60, 0x4B, 0x01, 0xA7, 0x22, 0x4F, 0x9D, 0xDD,
        0x58, 0x9C, 0xAC, 0x18, 0xF5, 0x23, 0x0A, 0x33, 0xAD, 0x36, 0xBB, 0xA5, 0x6E, 0x41, 0xB6,
        0x2C, 0xC8, 0x8E, 0xBB, 0xE4, 0x14, 0x13, 0xC0, 0x31, 0x45, 0x66, 0xAA, 0xDC, 0x7A, 0x43,
        0x83, 0x22, 0x26, 0xB0, 0x5B, 0xE0, 0x54, 0xCB, 0x85, 0x94, 0xDA, 0x82, 0xA8, 0xCC, 0x79,
        0x41, 0xB9, 0x96, 0xE7, 0x04, 0x71, 0xB1, 0x4D, 0xBA, 0x9E, 0x8C, 0x88, 0xA4, 0x30, 0xD8,
        0x3F, 0x45, 0xB3, 0x3B, 0xD1, 0x52, 0x57, 0x4A, 0xCB, 0xCA, 0x19, 0xB5, 0x58, 0x12, 0xE5,
        0xB4, 0x60, 0x42, 0xBE, 0xCD, 0xF0, 0x67, 0xB4, 0x33, 0xAF, 0xA3, 0x4E, 0x48, 0x34, 0x2B,
        0x70, 0x48, 0xC1, 0x4D, 0x92, 0xBD, 0x67, 0x1C, 0x1B, 0x8A, 0x76, 0x68, 0x3F, 0x77, 0x73,
        0x5B, 0x24, 0x29, 0x35, 0x4A, 0x88, 0x62, 0x18, 0xDE, 0xBA, 0x8C, 0x79, 0x7D, 0x74, 0x51,
        0xAD, 0x20, 0x57, 0x18, 0xFA, 0x45, 0xB0, 0x09, 0xF8, 0xF8, 0xB1, 0x47, 0x37, 0x64, 0x88,
        0x43, 0xC8, 0x85, 0x21, 0x73, 0x2D, 0x4B, 0x9D, 0x09, 0x09, 0x26, 0xA0, 0x91, 0xA7, 0x9C,
        0xA6, 0xAF, 0x43, 0xD6, 0xEE, 0x8B, 0x0D, 0x45, 0xB3, 0xC4, 0xE3, 0x37, 0x46, 0x7B, 0xED,
        0x5E, 0x5A, 0xA9, 0x4A, 0x29, 0xA9, 0x43, 0xBD, 0x4C, 0xB8, 0xEE, 0x0D, 0x03, 0x42, 0x44,
        0x29, 0x13, 0x8D, 0x21, 0xB5, 0x1B, 0x4D, 0xC3, 0xA3, 0x4A, 0x8A, 0xA8, 0x19, 0x92, 0xBA,
        0xB6, 0x94, 0x98, 0x13, 0x00, 0x00, 0x00, 0x48, 0x61, 0x73, 0x55, 0x6E, 0x6C, 0x6F, 0x63,
        0x6B, 0x65, 0x64, 0x53, 0x70, 0x65, 0x63, 0x69, 0x61, 0x6C, 0x00, 0x0D, 0x00, 0x00, 0x00,
        0x42, 0x6F, 0x6F, 0x6C, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x05, 0x00, 0x00, 0x00, 0x4E, 0x6F, 0x6E,
        0x65, 0x00,
    ];
    rw_property(&original)
}

#[test]
fn test_rw_property_array() -> Result<()> {
    let original = [
        0x0E, 0x00, 0x00, 0x00, 0x55, 0x6E, 0x6C, 0x6F, 0x63, 0x6B, 0x65, 0x64, 0x49, 0x74, 0x65,
        0x6D, 0x73, 0x00, 0x0E, 0x00, 0x00, 0x00, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72, 0x6F,
        0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0xCB, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F,
        0x00, 0x00, 0x00, 0x53, 0x74, 0x72, 0x75, 0x63, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72,
        0x74, 0x79, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x0E, 0x00, 0x00, 0x00, 0x55, 0x6E, 0x6C,
        0x6F, 0x63, 0x6B, 0x65, 0x64, 0x49, 0x74, 0x65, 0x6D, 0x73, 0x00, 0x0F, 0x00, 0x00, 0x00,
        0x53, 0x74, 0x72, 0x75, 0x63, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00,
        0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x47, 0x75, 0x69,
        0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x76, 0x3A, 0x96, 0xF0, 0x20, 0x19, 0x26, 0x46, 0xA6, 0x1D, 0x41,
        0xF4, 0x98, 0xBE, 0x42, 0x85, 0xB9, 0x5E, 0xDE, 0xC1, 0xBD, 0xDB, 0x1C, 0x47, 0x8E, 0x5D,
        0xC5, 0x73, 0x22, 0x10, 0x51, 0x34, 0xDD, 0xB5, 0xDA, 0x90, 0x86, 0xCD, 0x44, 0x42, 0xAF,
        0x60, 0x06, 0xDE, 0x2A, 0x43, 0x90, 0xA9, 0x28, 0xD2, 0x24, 0x72, 0xC4, 0x09, 0x22, 0x4E,
        0xAF, 0xFA, 0xCA, 0xC8, 0xB1, 0x37, 0x72, 0x87, 0x82, 0x08, 0x2A, 0xAB, 0x25, 0x31, 0x9F,
        0x43, 0xB4, 0x3A, 0x84, 0x58, 0xF5, 0x22, 0x8E, 0x8C, 0x4E, 0x96, 0xF4, 0x32, 0xD0, 0x18,
        0x23, 0x43, 0xA6, 0x2E, 0x81, 0xD8, 0x21, 0xA1, 0x48, 0x0F, 0x79, 0xE9, 0x1B, 0xF1, 0x5A,
        0xC1, 0x69, 0x4B, 0xA7, 0x98, 0x7C, 0x69, 0x69, 0x59, 0xC0, 0xD1, 0x97, 0x36, 0xEE, 0x11,
        0x0D, 0x4C, 0x87, 0x49, 0xA2, 0x24, 0xAF, 0x6F, 0x2D, 0xBA, 0x01, 0x60,
    ];
    rw_property(&original)
}

#[test]
fn test_rw_property_str() -> Result<()> {
    let original = [
        0x11, 0x00, 0x00, 0x00, 0x57, 0x61, 0x74, 0x63, 0x68, 0x65, 0x64, 0x54, 0x75, 0x74, 0x6F,
        0x72, 0x69, 0x61, 0x6C, 0x73, 0x00, 0x0E, 0x00, 0x00, 0x00, 0x41, 0x72, 0x72, 0x61, 0x79,
        0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x01, 0x07, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x0F, 0x00, 0x00, 0x00, 0x53, 0x74, 0x72, 0x75, 0x63, 0x74, 0x50, 0x72, 0x6F,
        0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x00, 0x0E, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00,
        0x57, 0x61, 0x74, 0x63, 0x68, 0x65, 0x64, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69, 0x61, 0x6C,
        0x73, 0x00, 0x0F, 0x00, 0x00, 0x00, 0x53, 0x74, 0x72, 0x75, 0x63, 0x74, 0x50, 0x72, 0x6F,
        0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0xA8, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10,
        0x00, 0x00, 0x00, 0x57, 0x61, 0x74, 0x63, 0x68, 0x65, 0x64, 0x54, 0x75, 0x74, 0x6F, 0x72,
        0x69, 0x61, 0x6C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72,
        0x69, 0x61, 0x6C, 0x4E, 0x61, 0x6D, 0x65, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x53, 0x74, 0x72,
        0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x1B, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69, 0x61, 0x6C,
        0x5F, 0x48, 0x69, 0x6E, 0x74, 0x5F, 0x44, 0x65, 0x70, 0x6F, 0x73, 0x69, 0x74, 0x65, 0x00,
        0x06, 0x00, 0x00, 0x00, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x49,
        0x6E, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x04, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x4E, 0x6F,
        0x6E, 0x65, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69, 0x61, 0x6C,
        0x4E, 0x61, 0x6D, 0x65, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x53, 0x74, 0x72, 0x50, 0x72, 0x6F,
        0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x21, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x1D, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69, 0x61, 0x6C, 0x5F, 0x48, 0x69,
        0x6E, 0x74, 0x5F, 0x45, 0x71, 0x75, 0x69, 0x70, 0x43, 0x6C, 0x61, 0x73, 0x73, 0x54, 0x6F,
        0x6F, 0x6C, 0x00, 0x06, 0x00, 0x00, 0x00, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x00, 0x0C, 0x00,
        0x00, 0x00, 0x49, 0x6E, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x04,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00,
        0x00, 0x4E, 0x6F, 0x6E, 0x65, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72,
        0x69, 0x61, 0x6C, 0x4E, 0x61, 0x6D, 0x65, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x53, 0x74, 0x72,
        0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x1F, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x1B, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69, 0x61, 0x6C,
        0x5F, 0x48, 0x69, 0x6E, 0x74, 0x5F, 0x45, 0x71, 0x75, 0x69, 0x70, 0x47, 0x72, 0x65, 0x6E,
        0x61, 0x64, 0x65, 0x00, 0x06, 0x00, 0x00, 0x00, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x00, 0x0C,
        0x00, 0x00, 0x00, 0x49, 0x6E, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00,
        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x00, 0x00, 0x05, 0x00,
        0x00, 0x00, 0x4E, 0x6F, 0x6E, 0x65, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F,
        0x72, 0x69, 0x61, 0x6C, 0x4E, 0x61, 0x6D, 0x65, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x53, 0x74,
        0x72, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x1F, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x1B, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69, 0x61,
        0x6C, 0x5F, 0x48, 0x69, 0x6E, 0x74, 0x5F, 0x4C, 0x61, 0x73, 0x65, 0x72, 0x50, 0x6F, 0x69,
        0x6E, 0x74, 0x65, 0x72, 0x00, 0x06, 0x00, 0x00, 0x00, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x00,
        0x0C, 0x00, 0x00, 0x00, 0x49, 0x6E, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x05,
        0x00, 0x00, 0x00, 0x4E, 0x6F, 0x6E, 0x65, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74,
        0x6F, 0x72, 0x69, 0x61, 0x6C, 0x4E, 0x61, 0x6D, 0x65, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x53,
        0x74, 0x72, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x27, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x23, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69,
        0x61, 0x6C, 0x5F, 0x48, 0x69, 0x6E, 0x74, 0x5F, 0x45, 0x71, 0x75, 0x69, 0x70, 0x53, 0x65,
        0x63, 0x6F, 0x6E, 0x64, 0x61, 0x72, 0x79, 0x57, 0x65, 0x61, 0x70, 0x6F, 0x6E, 0x00, 0x06,
        0x00, 0x00, 0x00, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x49, 0x6E,
        0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x4E, 0x6F, 0x6E,
        0x65, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69, 0x61, 0x6C, 0x4E,
        0x61, 0x6D, 0x65, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x53, 0x74, 0x72, 0x50, 0x72, 0x6F, 0x70,
        0x65, 0x72, 0x74, 0x79, 0x00, 0x25, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21,
        0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69, 0x61, 0x6C, 0x5F, 0x48, 0x69, 0x6E,
        0x74, 0x5F, 0x45, 0x71, 0x75, 0x69, 0x70, 0x54, 0x72, 0x61, 0x76, 0x65, 0x72, 0x73, 0x61,
        0x6C, 0x54, 0x6F, 0x6F, 0x6C, 0x00, 0x06, 0x00, 0x00, 0x00, 0x63, 0x6F, 0x75, 0x6E, 0x74,
        0x00, 0x0C, 0x00, 0x00, 0x00, 0x49, 0x6E, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74,
        0x79, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x00, 0x00,
        0x05, 0x00, 0x00, 0x00, 0x4E, 0x6F, 0x6E, 0x65, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x54, 0x75,
        0x74, 0x6F, 0x72, 0x69, 0x61, 0x6C, 0x4E, 0x61, 0x6D, 0x65, 0x00, 0x0C, 0x00, 0x00, 0x00,
        0x53, 0x74, 0x72, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x2B, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x27, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72,
        0x69, 0x61, 0x6C, 0x5F, 0x48, 0x69, 0x6E, 0x74, 0x5F, 0x45, 0x67, 0x67, 0x4D, 0x69, 0x73,
        0x73, 0x69, 0x6F, 0x6E, 0x54, 0x65, 0x72, 0x72, 0x61, 0x69, 0x6E, 0x53, 0x63, 0x61, 0x6E,
        0x6E, 0x65, 0x72, 0x00, 0x06, 0x00, 0x00, 0x00, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x00, 0x0C,
        0x00, 0x00, 0x00, 0x49, 0x6E, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00,
        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x05, 0x00,
        0x00, 0x00, 0x4E, 0x6F, 0x6E, 0x65, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F,
        0x72, 0x69, 0x61, 0x6C, 0x4E, 0x61, 0x6D, 0x65, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x53, 0x74,
        0x72, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x19, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x15, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69, 0x61,
        0x6C, 0x5F, 0x48, 0x69, 0x6E, 0x74, 0x5F, 0x46, 0x6C, 0x61, 0x72, 0x65, 0x73, 0x00, 0x06,
        0x00, 0x00, 0x00, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x49, 0x6E,
        0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x4E, 0x6F, 0x6E,
        0x65, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69, 0x61, 0x6C, 0x4E,
        0x61, 0x6D, 0x65, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x53, 0x74, 0x72, 0x50, 0x72, 0x6F, 0x70,
        0x65, 0x72, 0x74, 0x79, 0x00, 0x1C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18,
        0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69, 0x61, 0x6C, 0x5F, 0x48, 0x69, 0x6E,
        0x74, 0x5F, 0x4C, 0x6F, 0x77, 0x4F, 0x6E, 0x41, 0x6D, 0x6D, 0x6F, 0x00, 0x06, 0x00, 0x00,
        0x00, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x49, 0x6E, 0x74, 0x50,
        0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x4E, 0x6F, 0x6E, 0x65, 0x00,
        0x0D, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69, 0x61, 0x6C, 0x4E, 0x61, 0x6D,
        0x65, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x53, 0x74, 0x72, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72,
        0x74, 0x79, 0x00, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00,
        0x00, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69, 0x61, 0x6C, 0x5F, 0x48, 0x69, 0x6E, 0x74, 0x5F,
        0x4E, 0x69, 0x74, 0x72, 0x61, 0x00, 0x06, 0x00, 0x00, 0x00, 0x63, 0x6F, 0x75, 0x6E, 0x74,
        0x00, 0x0C, 0x00, 0x00, 0x00, 0x49, 0x6E, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74,
        0x79, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x05, 0x00, 0x00, 0x00, 0x4E, 0x6F, 0x6E, 0x65, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x54, 0x75,
        0x74, 0x6F, 0x72, 0x69, 0x61, 0x6C, 0x4E, 0x61, 0x6D, 0x65, 0x00, 0x0C, 0x00, 0x00, 0x00,
        0x53, 0x74, 0x72, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x1C, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72,
        0x69, 0x61, 0x6C, 0x5F, 0x54, 0x68, 0x72, 0x6F, 0x77, 0x43, 0x61, 0x72, 0x72, 0x69, 0x61,
        0x62, 0x6C, 0x65, 0x00, 0x06, 0x00, 0x00, 0x00, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x00, 0x0C,
        0x00, 0x00, 0x00, 0x49, 0x6E, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00,
        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x05, 0x00,
        0x00, 0x00, 0x4E, 0x6F, 0x6E, 0x65, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F,
        0x72, 0x69, 0x61, 0x6C, 0x4E, 0x61, 0x6D, 0x65, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x53, 0x74,
        0x72, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x28, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x24, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69, 0x61,
        0x6C, 0x5F, 0x48, 0x69, 0x6E, 0x74, 0x5F, 0x42, 0x6F, 0x73, 0x63, 0x6F, 0x46, 0x69, 0x72,
        0x73, 0x74, 0x53, 0x6F, 0x6C, 0x6F, 0x4D, 0x69, 0x73, 0x73, 0x69, 0x6F, 0x6E, 0x00, 0x06,
        0x00, 0x00, 0x00, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x49, 0x6E,
        0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x4E, 0x6F, 0x6E,
        0x65, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69, 0x61, 0x6C, 0x4E,
        0x61, 0x6D, 0x65, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x53, 0x74, 0x72, 0x50, 0x72, 0x6F, 0x70,
        0x65, 0x72, 0x74, 0x79, 0x00, 0x1F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1B,
        0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69, 0x61, 0x6C, 0x5F, 0x48, 0x69, 0x6E,
        0x74, 0x5F, 0x42, 0x6F, 0x73, 0x63, 0x6F, 0x41, 0x62, 0x69, 0x6C, 0x69, 0x74, 0x79, 0x00,
        0x06, 0x00, 0x00, 0x00, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x49,
        0x6E, 0x74, 0x50, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x04, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x4E, 0x6F,
        0x6E, 0x65, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69, 0x61, 0x6C,
        0x4E, 0x61, 0x6D, 0x65, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x53, 0x74, 0x72, 0x50, 0x72, 0x6F,
        0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x1B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x17, 0x00, 0x00, 0x00, 0x54, 0x75, 0x74, 0x6F, 0x72, 0x69, 0x61, 0x6C, 0x5F, 0x48, 0x69,
        0x6E, 0x74, 0x5F, 0x52, 0x65, 0x73, 0x75, 0x70, 0x70, 0x6C, 0x79, 0x00, 0x06, 0x00, 0x00,
        0x00, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x49, 0x6E, 0x74, 0x50,
        0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x4E, 0x6F, 0x6E, 0x65, 0x00,
    ];
    rw_property(&original)
}

#[test]
fn test_rw_header() -> Result<()> {
    let original = [
        0x47, 0x56, 0x41, 0x53, 0x02, 0x00, 0x00, 0x00, 0x0A, 0x02, 0x00, 0x00, 0x04, 0x00, 0x1B,
        0x00, 0x02, 0x00, 0xFA, 0x14, 0x01, 0x80, 0x05, 0x00, 0x00, 0x00, 0x6D, 0x61, 0x69, 0x6E,
        0x00, 0x03, 0x00, 0x00, 0x00, 0x32, 0x00, 0x00, 0x00, 0x4E, 0x7C, 0xE7, 0x82, 0xA5, 0x43,
        0x23, 0x33, 0xC5, 0x13, 0x6B, 0xB4, 0xF3, 0x0D, 0x31, 0x97, 0x00, 0x00, 0x00, 0x00, 0xFA,
        0x7A, 0xF5, 0xFC, 0x83, 0x42, 0x76, 0x50, 0x58, 0xE6, 0xA9, 0xB9, 0x32, 0x2D, 0xA0, 0xFF,
        0x44, 0x00, 0x00, 0x00, 0xF3, 0x7A, 0xBB, 0x24, 0x83, 0x4F, 0x46, 0x56, 0xC2, 0x2D, 0x2F,
        0x1F, 0xFF, 0x96, 0xAD, 0x49, 0x05, 0x00, 0x00, 0x00, 0x12, 0xE4, 0x26, 0xFB, 0x4D, 0x4B,
        0x15, 0x1F, 0x0A, 0x55, 0x72, 0x93, 0x70, 0x2F, 0x1D, 0x96, 0x03, 0x00, 0x00, 0x00, 0x22,
        0xD5, 0x54, 0x9C, 0xBE, 0x4F, 0x26, 0xA8, 0x46, 0x07, 0x21, 0x94, 0xD0, 0x82, 0xB4, 0x61,
        0x2B, 0x00, 0x00, 0x00, 0xE4, 0x32, 0xD8, 0xB0, 0x0D, 0x4F, 0x89, 0x1F, 0xB7, 0x7E, 0xCF,
        0xAC, 0xA2, 0x4A, 0xFD, 0x36, 0x0A, 0x00, 0x00, 0x00, 0x28, 0x43, 0xC6, 0xE1, 0x53, 0x4D,
        0x2C, 0xA2, 0x86, 0x8E, 0x6C, 0xA3, 0x8C, 0xBD, 0x17, 0x64, 0x00, 0x00, 0x00, 0x00, 0x3C,
        0xC1, 0x5E, 0x37, 0xFB, 0x48, 0xE4, 0x06, 0xF0, 0x84, 0x00, 0xB5, 0x7E, 0x71, 0x2A, 0x26,
        0x04, 0x00, 0x00, 0x00, 0xED, 0x68, 0xB0, 0xE4, 0xE9, 0x42, 0x94, 0xF4, 0x0B, 0xDA, 0x31,
        0xA2, 0x41, 0xBB, 0x46, 0x2E, 0x28, 0x00, 0x00, 0x00, 0x3F, 0x74, 0xFC, 0xCF, 0x80, 0x44,
        0xB0, 0x43, 0xDF, 0x14, 0x91, 0x93, 0x73, 0x20, 0x1D, 0x17, 0x25, 0x00, 0x00, 0x00, 0xB5,
        0x49, 0x2B, 0xB0, 0xE9, 0x44, 0x20, 0xBB, 0xB7, 0x32, 0x04, 0xA3, 0x60, 0x03, 0xE4, 0x52,
        0x03, 0x00, 0x00, 0x00, 0x5C, 0x10, 0xE4, 0xA4, 0xB5, 0x49, 0xA1, 0x59, 0xC4, 0x40, 0xC5,
        0xA7, 0xEE, 0xDF, 0x7E, 0x54, 0x00, 0x00, 0x00, 0x00, 0xC9, 0x31, 0xC8, 0x39, 0xDC, 0x47,
        0xE6, 0x5A, 0x17, 0x9C, 0x44, 0x9A, 0x7C, 0x8E, 0x1C, 0x3E, 0x00, 0x00, 0x00, 0x00, 0x33,
        0x1B, 0xF0, 0x78, 0x98, 0x4F, 0xEA, 0xEB, 0xEA, 0x84, 0xB4, 0xB9, 0xA2, 0x5A, 0xB9, 0xCC,
        0x0E, 0x00, 0x00, 0x00, 0x0F, 0x38, 0x31, 0x66, 0xE0, 0x43, 0x4D, 0x2D, 0x27, 0xCF, 0x09,
        0x80, 0x5A, 0xA9, 0x56, 0x69, 0x00, 0x00, 0x00, 0x00, 0x9F, 0x8B, 0xF8, 0x12, 0xFC, 0x4A,
        0x75, 0x88, 0x0C, 0xD9, 0x7C, 0xA6, 0x29, 0xBD, 0x3A, 0x38, 0x2D, 0x00, 0x00, 0x00, 0x4C,
        0xE7, 0x5A, 0x7B, 0x10, 0x4C, 0x70, 0xD2, 0x98, 0x57, 0x58, 0xA9, 0x5A, 0x2A, 0x21, 0x0B,
        0x0D, 0x00, 0x00, 0x00, 0x18, 0x69, 0x29, 0xD7, 0xDD, 0x4B, 0xD6, 0x1D, 0xA8, 0x64, 0xE2,
        0x9D, 0x84, 0x38, 0xC1, 0x3C, 0x03, 0x00, 0x00, 0x00, 0x78, 0x52, 0xA1, 0xC2, 0xFE, 0x4A,
        0xE7, 0xBF, 0xFF, 0x90, 0x17, 0x6C, 0x55, 0xF7, 0x1D, 0x53, 0x01, 0x00, 0x00, 0x00, 0xD4,
        0xA3, 0xAC, 0x6E, 0xC1, 0x4C, 0xEC, 0x40, 0xED, 0x8B, 0x86, 0xB7, 0xC5, 0x8F, 0x42, 0x09,
        0x03, 0x00, 0x00, 0x00, 0xDD, 0x75, 0xE5, 0x29, 0x27, 0x46, 0xA3, 0xE0, 0x76, 0xD2, 0x10,
        0x9D, 0xEA, 0xDC, 0x2C, 0x23, 0x11, 0x00, 0x00, 0x00, 0x5D, 0xA6, 0x43, 0xAF, 0x47, 0x49,
        0xD3, 0x7F, 0x8E, 0x3E, 0x73, 0x98, 0x05, 0xBB, 0xC1, 0xD9, 0x0F, 0x00, 0x00, 0x00, 0xEC,
        0x6C, 0x26, 0x6B, 0x8F, 0x4B, 0xC7, 0x1E, 0xD9, 0xE4, 0x0B, 0xA3, 0x07, 0xFC, 0x42, 0x09,
        0x01, 0x00, 0x00, 0x00, 0x61, 0x3D, 0xF7, 0x0D, 0xEA, 0x47, 0x3F, 0xA2, 0xE9, 0x89, 0x27,
        0xB7, 0x9A, 0x49, 0x41, 0x0C, 0x01, 0x00, 0x00, 0x00, 0x86, 0x18, 0x1D, 0x60, 0x84, 0x4F,
        0x64, 0xAC, 0xDE, 0xD3, 0x16, 0xAA, 0xD6, 0xC7, 0xEA, 0x0D, 0x2F, 0x00, 0x00, 0x00, 0x68,
        0x63, 0x08, 0xE7, 0x58, 0x4C, 0x23, 0x6B, 0x70, 0x1B, 0x39, 0x84, 0x91, 0x5E, 0x26, 0x16,
        0x01, 0x00, 0x00, 0x00, 0xD6, 0xBC, 0xFF, 0x9D, 0x58, 0x01, 0x4F, 0x49, 0x82, 0x12, 0x21,
        0xE2, 0x88, 0xA8, 0x92, 0x3C, 0x0A, 0x00, 0x00, 0x00, 0xAC, 0xD0, 0xAE, 0xF2, 0x6F, 0x41,
        0xFE, 0x9A, 0x7F, 0xAA, 0x64, 0x86, 0xFC, 0xD6, 0x26, 0xFA, 0x01, 0x00, 0x00, 0x00, 0x0B,
        0x1F, 0x4F, 0x17, 0xA5, 0x45, 0xC6, 0xB4, 0xE8, 0x2E, 0x3F, 0xB1, 0x7D, 0x91, 0xFB, 0xD0,
        0x0A, 0x00, 0x00, 0x00, 0x83, 0x4A, 0xF9, 0x35, 0x6C, 0x40, 0x58, 0xE2, 0xF5, 0x09, 0x18,
        0xA3, 0x7C, 0x24, 0x10, 0x96, 0x29, 0x00, 0x00, 0x00, 0x6E, 0xC1, 0x8F, 0xB6, 0xE2, 0x42,
        0x1B, 0x8B, 0x5C, 0x21, 0x53, 0xB4, 0xFE, 0x44, 0x88, 0x05, 0x01, 0x00, 0x00, 0x00, 0x06,
        0x85, 0xE1, 0xB2, 0xC2, 0xCF, 0x73, 0x42, 0xBB, 0xF4, 0x4E, 0xA5, 0x07, 0xBA, 0x8B, 0x75,
        0x01, 0x00, 0x00, 0x00, 0x36, 0x89, 0xF5, 0x64, 0xBA, 0x42, 0x1B, 0xFD, 0x89, 0x72, 0x96,
        0xBA, 0x4E, 0xFA, 0xD0, 0xD5, 0x01, 0x00, 0x00, 0x00, 0x27, 0xD8, 0x0E, 0x6F, 0x95, 0x48,
        0x09, 0xA6, 0x8D, 0x99, 0x91, 0x9C, 0xA4, 0x0E, 0x18, 0x90, 0x02, 0x00, 0x00, 0x00, 0xE7,
        0x9E, 0x7F, 0x71, 0x3A, 0x49, 0xB0, 0xE9, 0x32, 0x91, 0xB3, 0x88, 0x07, 0x81, 0x38, 0x1B,
        0x08, 0x00, 0x00, 0x00, 0x50, 0x32, 0x68, 0x54, 0xAF, 0x48, 0x99, 0x80, 0x96, 0x98, 0xC8,
        0x8B, 0xB7, 0xF9, 0xAD, 0xFB, 0x00, 0x00, 0x00, 0x00, 0x19, 0x4D, 0x0C, 0x43, 0x70, 0x49,
        0x54, 0x71, 0x69, 0x9B, 0x69, 0x87, 0xE5, 0xB0, 0x90, 0xDF, 0x0F, 0x00, 0x00, 0x00, 0xBD,
        0x32, 0xFE, 0xAA, 0x14, 0x4C, 0x95, 0x53, 0x25, 0x5E, 0x6A, 0xB6, 0xDD, 0xD1, 0x32, 0x10,
        0x01, 0x00, 0x00, 0x00, 0x8E, 0xE1, 0xAF, 0x23, 0x58, 0x4E, 0xE1, 0x4C, 0x52, 0xC2, 0x61,
        0x8D, 0xB7, 0xBE, 0x53, 0xB9, 0x0B, 0x00, 0x00, 0x00, 0xEA, 0xB7, 0x62, 0xA4, 0x3A, 0x4E,
        0x99, 0xF4, 0x1F, 0xEC, 0xC1, 0x99, 0xB2, 0xE1, 0x24, 0x82, 0x04, 0x00, 0x00, 0x00, 0xBD,
        0xFD, 0xB5, 0x2E, 0x10, 0x4D, 0xAC, 0x01, 0x8F, 0xF3, 0x36, 0x81, 0xDA, 0xA5, 0x93, 0x33,
        0x05, 0x00, 0x00, 0x00, 0x4F, 0x35, 0x9D, 0x50, 0x2F, 0x49, 0xE6, 0xF6, 0xB2, 0x85, 0x49,
        0xA7, 0x1C, 0x63, 0x3C, 0x07, 0x00, 0x00, 0x00, 0x00, 0x40, 0xEB, 0x56, 0x4A, 0xDC, 0x11,
        0xF5, 0x10, 0x7E, 0x34, 0xD3, 0x92, 0xE7, 0x6A, 0xC9, 0xB2, 0x02, 0x00, 0x00, 0x00, 0x00,
        0x4A, 0x8A, 0xD7, 0x97, 0x46, 0x58, 0xE8, 0xB5, 0x19, 0xA8, 0xBA, 0xB4, 0x46, 0x7D, 0x48,
        0x12, 0x00, 0x00, 0x00, 0x86, 0xF8, 0x79, 0x55, 0x1F, 0x4C, 0x3A, 0x93, 0x7B, 0x08, 0xBA,
        0x83, 0x2F, 0xB9, 0x61, 0x63, 0x02, 0x00, 0x00, 0x00, 0x52, 0xBE, 0x2F, 0x61, 0x0B, 0x40,
        0x53, 0xDA, 0x91, 0x4F, 0x0D, 0x91, 0x7C, 0x85, 0xB1, 0x9F, 0x01, 0x00, 0x00, 0x00, 0x36,
        0x7A, 0x23, 0xA4, 0xC9, 0x41, 0xEA, 0xCA, 0xF8, 0x18, 0xA2, 0x8F, 0xF3, 0x1B, 0x68, 0x58,
        0x04, 0x00, 0x00, 0x00, 0x75, 0x3F, 0x4E, 0x80, 0x49, 0x4B, 0x88, 0x70, 0x06, 0x8C, 0xD6,
        0xA4, 0xDC, 0xB6, 0x7E, 0x3C, 0x05, 0x00, 0x00, 0x00, 0xF2, 0x0A, 0x68, 0xFB, 0xA3, 0x4B,
        0xEF, 0x59, 0xB5, 0x19, 0xA8, 0xBA, 0x3D, 0x44, 0xC8, 0x73, 0x02, 0x00, 0x00, 0x00, 0x0E,
        0xB7, 0x50, 0x99, 0x17, 0x4E, 0x1A, 0xB4, 0x0D, 0xFA, 0xCC, 0xBB, 0xD6, 0x7F, 0x81, 0x57,
        0x01, 0x00, 0x00, 0x00,
    ];
    let mut reader = Cursor::new(&original);
    run(&mut reader, |reader| {
        let obj = Header::read(reader)?;
        let mut reconstructed: Vec<u8> = vec![];
        run(&mut Cursor::new(&mut reconstructed), |writer| {
            obj.write(writer)
        })?;
        assert_eq!(original, &reconstructed[..]);
        Ok(())
    })
}

#[test]
fn test_json_roundtrip() {
    use crate::SaveReader;

    let save = SaveReader::new().read(Cursor::new(SAVE)).unwrap();
    let json = serde_json::to_string(&save).unwrap();
    let roundtripped: Save = serde_json::from_str(&json).unwrap();
    let mut original_bytes = vec![];
    save.write(&mut Cursor::new(&mut original_bytes)).unwrap();

    let mut roundtripped_bytes = vec![];
    roundtripped
        .write(&mut Cursor::new(&mut roundtripped_bytes))
        .unwrap();

    assert!(original_bytes == roundtripped_bytes);
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

/// Parses an `Expedition` ConcreteModel payload, asserting full consumption
/// and a byte-exact rewrite, and returns the mission model.
fn parse_team_mission(
    payload: &[u8],
) -> Result<games::palworld::PalMapObjectCharacterTeamMissionModel> {
    let len = payload.len() as u64;
    let parsed = run(Cursor::new(payload.to_vec()), |ar| {
        let parsed = games::palworld::PalMapConcreteModel::read_with_object_id(ar, "Expedition")?;
        assert_eq!(
            len,
            ar.stream_position()?,
            "parser must consume the entire payload"
        );
        Ok(parsed)
    })?;

    let mut reconstructed = vec![];
    run(Cursor::new(&mut reconstructed), |ar| parsed.write(ar))?;
    assert_eq!(payload, reconstructed);

    match parsed.model_data {
        games::palworld::PalMapConcreteModelVariant::CharacterTeamMission(mission) => Ok(mission),
        other => panic!("expected CharacterTeamMission, got {other:?}"),
    }
}

/// Real `Expedition` map object ConcreteModel payloads from a Palworld world
/// save. The current layout has the mission id preceded by an unknown 4-byte
/// field; it must be extracted, not hidden inside opaque tail bytes.
#[test]
fn test_palworld_character_team_mission_current_layout() -> Result<()> {
    let cases = [
        ("ecfe5f9758d3c4428804842978fdbbd2d55ed9ffe1e4de458207c8f0ca273ca100000000050000004e6f6e6500000000000110d47bcfad0e000000000000", "None", 1),
        ("fe5056754750e940b0a6005ae54ac5f3d81b3eac213e5c40968659f5abec6a1c000000001400000044554e47454f4e5f564f4c43414e4f48415244000000000001d0a20c702b12000000000000", "DUNGEON_VOLCANOHARD", 1),
    ];

    for (payload, expected_mission_id, expected_state) in cases {
        let mission = parse_team_mission(&unhex(payload))?;
        assert_eq!(expected_mission_id, mission.mission_id);
        assert_eq!(expected_state, mission.state);
    }
    Ok(())
}

/// Parses a ConcreteModel payload for `object_id`, asserting full consumption
/// and a byte-exact rewrite, and returns the model variant.
fn parse_concrete_model(
    payload: &[u8],
    object_id: &str,
) -> Result<games::palworld::PalMapConcreteModelVariant> {
    let len = payload.len() as u64;
    let parsed = run(Cursor::new(payload.to_vec()), |ar| {
        let parsed = games::palworld::PalMapConcreteModel::read_with_object_id(ar, object_id)?;
        assert_eq!(
            len,
            ar.stream_position()?,
            "parser must consume the entire payload"
        );
        Ok(parsed)
    })?;

    let mut reconstructed = vec![];
    run(Cursor::new(&mut reconstructed), |ar| parsed.write(ar))?;
    assert_eq!(payload, reconstructed, "rewrite must be byte-exact");

    Ok(parsed.model_data)
}

/// The 2026-07 update records the pals sent on an expedition. What older
/// parsers read as 4 opaque bytes after the mission id is the count of that
/// array; a mission with pals assigned overruns a parser that ignores it.
///
/// Payload is a verbatim `Expedition` ConcreteModel from a live post-update
/// save: mission `DUNGEON_SNOW` with 99 assigned individuals.
#[test]
fn test_palworld_character_team_mission_assigned_individuals() -> Result<()> {
    let payload = unhex(include_str!("tests_fixtures/expedition_dungeon_snow.hex").trim());
    let mission = parse_team_mission(&payload)?;

    assert_eq!("DUNGEON_SNOW", mission.mission_id);
    assert_eq!(99, mission.assigned_individuals.len());
    assert_eq!(2, mission.state);
    Ok(())
}

/// A pre-update expedition has an empty assigned-individuals array, so the
/// four bytes stay zero and the blob is unchanged.
#[test]
fn test_palworld_character_team_mission_without_assigned_individuals() -> Result<()> {
    let payload = unhex("ecfe5f9758d3c4428804842978fdbbd2d55ed9ffe1e4de458207c8f0ca273ca100000000050000004e6f6e6500000000000110d47bcfad0e000000000000");
    let mission = parse_team_mission(&payload)?;

    assert_eq!("None", mission.mission_id);
    assert!(mission.assigned_individuals.is_empty());
    Ok(())
}

/// The 2026-07 update appended two arrays to the breed farm: the parent pals
/// occupying it and the target breed item ids. Pre-update blobs stop after
/// `trailing_bytes`, so both are optional.
#[test]
fn test_palworld_breed_farm_post_update_fields() -> Result<()> {
    // Verbatim BreedFarm ConcreteModel from a live post-update save: 2 parents.
    let payload = unhex("2b859e9ea5b64e42ba32fcc588b0680cc0bf79dd16ced94d93dacbb5a709c07c00000000000000009ff437420200000000000000000000000000000000000000a240e9663ae6f547bec44e588681839500000000000000000000000000000000c4f32d8ce30fa4438f9b05384ee8874800000000");

    let farm = match parse_concrete_model(&payload, "BreedFarm")? {
        games::palworld::PalMapConcreteModelVariant::BreedFarm(farm) => farm,
        other => panic!("expected BreedFarm, got {other:?}"),
    };

    assert!(farm.spawned_egg_instance_ids.is_empty());
    let breeding = farm.breeding.as_ref().expect("post-update breed farm");
    assert_eq!(2, breeding.last_proceed_worker_individual_ids.len());
    assert!(breeding.target_breed_item_ids.is_empty());
    Ok(())
}

/// A post-update breed farm with nothing in it still carries both arrays, empty
/// -- eight bytes a pre-update parser would leave unconsumed.
#[test]
fn test_palworld_breed_farm_post_update_empty_arrays() -> Result<()> {
    let payload = unhex("39cdb16fa66c444088b55b143651aa92a37fb2866d164a46a2e0a2ece31d6c410000000000000000000000000000000000000000");

    let farm = match parse_concrete_model(&payload, "BreedFarm")? {
        games::palworld::PalMapConcreteModelVariant::BreedFarm(farm) => farm,
        other => panic!("expected BreedFarm, got {other:?}"),
    };

    let breeding = farm.breeding.as_ref().expect("post-update breed farm");
    assert!(breeding.last_proceed_worker_individual_ids.is_empty());
    assert!(breeding.target_breed_item_ids.is_empty());
    Ok(())
}

/// Pre-update breed farms end at `trailing_bytes`. The appended arrays are
/// absent, not empty: writing them back as two empty arrays would append eight
/// bytes that were never in the blob.
#[test]
fn test_palworld_breed_farm_pre_update_blob_roundtrips() -> Result<()> {
    let payload = unhex(
        "d4db056ca6fbd641bb535e4fb5316ac728d95e3c66de414a80157342156ee8f4000000000000000000000000",
    );

    // parse_concrete_model asserts full consumption and a byte-exact rewrite.
    let farm = match parse_concrete_model(&payload, "BreedFarm")? {
        games::palworld::PalMapConcreteModelVariant::BreedFarm(farm) => farm,
        other => panic!("expected BreedFarm, got {other:?}"),
    };

    assert!(farm.breeding.is_none());
    Ok(())
}

/// The 2026-07 lamp light-color update appended `is_manually_turned_off` plus
/// four bytes to `PalMapObjectLampModel`, which had no fields of its own.
#[test]
fn test_palworld_lamp_is_manually_turned_off() -> Result<()> {
    let cases = [
        (
            "e3ad64f7c6a02b4c89109b0135bb9c04828547a8abf294459438d7edd1c9dc61000000000100000000000000",
            1,
        ),
        (
            "8a40f64b6d901249806024d64c42f47d9a6a133fcce95c4a85c202518d24a0fb000000000000000000000000",
            0,
        ),
    ];

    for (payload, expected_turned_off) in cases {
        let lamp = match parse_concrete_model(&unhex(payload), "Light_FloorLamp01")? {
            games::palworld::PalMapConcreteModelVariant::Lamp(lamp) => lamp,
            other => panic!("expected Lamp, got {other:?}"),
        };
        let light = lamp.light.as_ref().expect("post-update lamp");
        assert_eq!(expected_turned_off, light.is_manually_turned_off);
    }
    Ok(())
}

/// Level pickups now serialize the full `PalMapObjectPickableItemModelBase`:
/// `auto_picked_up` followed by the pickup guard's player uid and timer handle.
#[test]
fn test_palworld_pickup_item_on_level_post_update_pickup_guard() -> Result<()> {
    // Verbatim PickupItem_Log ConcreteModel from a live post-update save.
    let payload = unhex("f799612b0992dd46abbe8ee595702fa1031fa159a355854dab1c40747256009300000000000000000000000000000000000000000000000000000000");

    let pickup = match parse_concrete_model(&payload, "PickupItem_Log")? {
        games::palworld::PalMapConcreteModelVariant::PickupItemOnLevel(pickup) => pickup,
        other => panic!("expected PickupItemOnLevel, got {other:?}"),
    };

    assert_eq!(0, pickup.auto_picked_up);
    let guard = pickup.pickup_guard.as_ref().expect("post-update pickup");
    assert_eq!(crate::FGuid::default(), guard.pickupable_player_uid);
    assert_eq!(0, guard.remove_pickup_guard_timer_handle);
    Ok(())
}

/// Pre-update level pickups end at `auto_picked_up`; the guard is absent and
/// must not be written back.
#[test]
fn test_palworld_pickup_item_on_level_pre_update_blob_roundtrips() -> Result<()> {
    let payload = unhex("ad698c406b298d49a13dea49625ea1ab03e797c2733de14cb046694b0a52962b00000000");

    let pickup = match parse_concrete_model(&payload, "PickupItem_Stone")? {
        games::palworld::PalMapConcreteModelVariant::PickupItemOnLevel(pickup) => pickup,
        other => panic!("expected PickupItemOnLevel, got {other:?}"),
    };

    assert!(pickup.pickup_guard.is_none());
    Ok(())
}

/// Parses a ConcreteModel module payload, asserting full consumption and a
/// byte-exact rewrite, and returns the module data.
fn parse_module(
    payload: &[u8],
    module_type: &str,
) -> Result<games::palworld::PalMapConcreteModelModuleData> {
    let len = payload.len() as u64;
    let parsed = run(Cursor::new(payload.to_vec()), |ar| {
        let parsed = games::palworld::PalMapConcreteModelModule::read_with_module_type(
            ar,
            module_type,
            payload.to_vec(),
            vec![],
        )?;
        assert_eq!(
            len,
            ar.stream_position()?,
            "parser must consume the entire payload"
        );
        Ok(parsed)
    })?;

    let mut reconstructed = vec![];
    run(Cursor::new(&mut reconstructed), |ar| parsed.write(ar))?;
    assert_eq!(payload, reconstructed, "rewrite must be byte-exact");

    Ok(parsed.data)
}

/// `GuildSecurity` (added 2026-07) holds the guild roles allowed to use an
/// object. Left undecoded it round-trips as opaque bytes, but the roles are not
/// reachable.
#[test]
fn test_palworld_guild_security_module() -> Result<()> {
    // Verbatim bytes from every GuildSecurity module in a live post-update save:
    // a two-element role array then the usual 4-byte module trailer.
    let payload = unhex("02000000020300000000");

    match parse_module(
        &payload,
        "EPalMapObjectConcreteModelModuleType::GuildSecurity",
    )? {
        games::palworld::PalMapConcreteModelModuleData::GuildSecurity { allowed_roles, .. } => {
            // EPalGuildRole: SubMaster = 2, Member = 3.
            assert_eq!(vec![2, 3], allowed_roles);
            Ok(())
        }
        other => panic!("expected GuildSecurity, got {other:?}"),
    }
}

/// `ColorSetting` (added 2026-07) stores the light color a player picked for a
/// lamp, as FName -> FLinearColor entries.
#[test]
fn test_palworld_color_setting_module() -> Result<()> {
    let payload = unhex("010000001b00000050616c4d61704f626a6563744c616d704c69676874436f6c6f7200d43ce53d56ad543fd43ce53d0000803f00000000");

    match parse_module(
        &payload,
        "EPalMapObjectConcreteModelModuleType::ColorSetting",
    )? {
        games::palworld::PalMapConcreteModelModuleData::ColorSetting { color_entries, .. } => {
            assert_eq!(1, color_entries.len());
            assert_eq!("PalMapObjectLampLightColor", color_entries[0].key);
            assert_eq!(1.0, color_entries[0].color.a);
            Ok(())
        }
        other => panic!("expected ColorSetting, got {other:?}"),
    }
}

/// `OperationalLoad` (added 2026-07) holds a single float.
#[test]
fn test_palworld_operational_load_module() -> Result<()> {
    let mut payload = 0.5f32.to_le_bytes().to_vec();
    payload.extend_from_slice(&[0u8; 8]);

    match parse_module(
        &payload,
        "EPalMapObjectConcreteModelModuleType::OperationalLoad",
    )? {
        games::palworld::PalMapConcreteModelModuleData::OperationalLoad {
            current_load, ..
        } => {
            assert_eq!(0.5, current_load);
            Ok(())
        }
        other => panic!("expected OperationalLoad, got {other:?}"),
    }
}

/// Parses a `GroupSaveDataMap` RawData payload for `group_type`, asserting full
/// consumption and a byte-exact rewrite.
fn parse_group(payload: &[u8], group_type: &str) -> Result<games::palworld::PalGroupData> {
    let len = payload.len() as u64;
    let parsed = run(Cursor::new(payload.to_vec()), |ar| {
        let parsed = games::palworld::PalGroupData::read_with_group_type(ar, group_type)?;
        assert_eq!(
            len,
            ar.stream_position()?,
            "parser must consume the entire payload"
        );
        Ok(parsed)
    })?;

    let mut reconstructed = vec![];
    run(Cursor::new(&mut reconstructed), |ar| parsed.write(ar))?;
    assert_eq!(payload, reconstructed, "rewrite must be byte-exact");

    Ok(parsed)
}

const GUILD: &str = "EPalGroupType::Guild";
const ORGANIZATION: &str = "EPalGroupType::Organization";

/// The 2026-07 update reshaped the guild tail: chest roles, a per-member role
/// byte and role permissions. What older parsers read as four opaque bytes was
/// the count of the (until then always empty) guild marker array.
#[test]
fn test_palworld_post_update_guild_decodes_roles_and_permissions() -> Result<()> {
    // Fresh post-update world, guild "Unnamed Guild", no map markers.
    let payload = unhex("c29ea93edd3d454b97c4437e963311a42100000030303030303030303030303030303030303030303030303030303030303030310001000000000000000000000000000000010000008ca4a0e244f33244b5b6f77f3f0860fc0000000000000000000000000001000000000000000e000000556e6e616d6564204775696c640000000000000000000000000000000000000000000200000002030000000000000000000000000000000001000000010000000000000000000000000000000100000070b9567f00000000020000004f0001030000000205000000000304050703020000000407040000000000000000");

    let guild = match parse_group(&payload, GUILD)?.data {
        games::palworld::PalGroupVariant::Guild(guild) => guild,
        other => panic!("expected Guild, got {other:?}"),
    };

    assert_eq!("Unnamed Guild", guild.guild_name);
    assert!(guild.guild_markers.is_empty());

    let tail = match &guild.tail {
        games::palworld::PalGuildTail::PostUpdate(tail) => tail,
        other => panic!("expected a post-update guild tail, got {other:?}"),
    };

    // EPalGuildRole: GuildMaster = 1, SubMaster = 2, Member = 3, Guest = 4.
    assert_eq!(vec![2, 3], tail.guild_chest_allowed_roles);
    assert_eq!(1, tail.players.len());
    assert_eq!("O", tail.players[0].player_info.player_name);
    assert_eq!(1, tail.players[0].role);

    let permissions: std::collections::HashMap<u8, Vec<u8>> = tail
        .role_permissions
        .iter()
        .map(|rp| (rp.role, rp.permissions.clone()))
        .collect();
    assert_eq!(vec![0, 3, 4, 5, 7], permissions[&2]);
    assert_eq!(vec![4, 7], permissions[&3]);
    assert_eq!(Vec::<u8>::new(), permissions[&4]);
    Ok(())
}

/// Guild map markers, added by the same update, are the array whose count used
/// to be read as four opaque bytes.
#[test]
fn test_palworld_guild_markers() -> Result<()> {
    let payload = unhex(include_str!("tests_fixtures/guild_with_markers.hex").trim());

    let guild = match parse_group(&payload, GUILD)?.data {
        games::palworld::PalGroupVariant::Guild(guild) => guild,
        other => panic!("expected Guild, got {other:?}"),
    };

    assert_eq!("DeBugging", guild.guild_name);
    assert_eq!(2, guild.guild_markers.len());
    assert_eq!(
        vec![7, 3],
        guild
            .guild_markers
            .iter()
            .map(|m| m.icon_type)
            .collect::<Vec<_>>()
    );
    assert_eq!(0.0, guild.guild_markers[0].icon_location.z.0);
    Ok(())
}

/// Guilds saved before the update have no markers and the older, shorter tail.
/// They must still round-trip: the new fields are absent, not empty.
#[test]
fn test_palworld_pre_update_guild_still_roundtrips() -> Result<()> {
    let payload = unhex("4ba7019b3387a842a71ce44f34337d6d00000000010000001b5e38c0000000000000000000000000e0cd652ee089f548a86422caf4d7654b0000000000000000000000000001000000000000000e000000556e6e616d6564204775696c640000000000000000000000000000000000000000001b5e38c0000000000000000000000000010000001b5e38c00000000000000000000000001020ee3afb19000002000000450000000000");

    let guild = match parse_group(&payload, GUILD)?.data {
        games::palworld::PalGroupVariant::Guild(guild) => guild,
        other => panic!("expected Guild, got {other:?}"),
    };

    assert!(guild.guild_markers.is_empty());
    match &guild.tail {
        games::palworld::PalGuildTail::PreUpdate(tail) => assert_eq!(1, tail.players.len()),
        other => panic!("expected a pre-update guild tail, got {other:?}"),
    }
    Ok(())
}

/// `EPalGroupType::Organization` was not touched by the update; both old and new
/// blobs must round-trip unchanged.
#[test]
fn test_palworld_organization_group_unchanged() -> Result<()> {
    let payloads = [
        "6b18d8988c44c2468a602aa18e35d5c2000000000000000000000000020000000000000000",
        "671203f05e2fc9419cebd80e14ce9d13000000000000000000000000020000000000000000",
    ];

    for payload in payloads {
        // parse_group asserts full consumption and a byte-exact rewrite.
        let group = parse_group(&unhex(payload), ORGANIZATION)?;
        assert!(matches!(
            group.data,
            games::palworld::PalGroupVariant::Organization(_)
        ));
    }
    Ok(())
}

/// Post-update weapons carry an FName (default "None") between the passive
/// skill list and the trailing bytes. A parser that does not read it overruns,
/// fails its end-of-payload check and silently degrades the whole item to an
/// opaque blob -- it still round-trips, but durability and passive skills are
/// no longer reachable.
#[test]
fn test_palworld_post_update_weapon_decodes_as_weapon() -> Result<()> {
    // Verbatim DynamicItemSaveData RawData for a SniperRifle_Default.
    let payload = unhex("00000000000000000000000000000000ef8aa41f4542f82f314924941e5520a214000000536e697065725269666c655f44656661756c7400000000000400000000007a4400000000050000004e6f6e650000000000");

    let item: games::palworld::PalDynamicItem = run(Cursor::new(payload.clone()), |ar| {
        games::palworld::PalDynamicItem::read(ar)
    })?;

    let mut reconstructed = vec![];
    run(Cursor::new(&mut reconstructed), |ar| item.write(ar))?;
    assert_eq!(payload, reconstructed, "rewrite must be byte-exact");

    match item.item_type {
        games::palworld::PalDynamicItemType::Weapon {
            unknown_str,
            passive_skill_list,
            ..
        } => {
            assert_eq!(Some("None".to_string()), unknown_str);
            assert!(passive_skill_list.is_empty());
            Ok(())
        }
        other => panic!("expected Weapon, got {other:?}"),
    }
}

/// These flags are u32 in the blob, not bool. Coercing them to bool and writing
/// back `1` silently rewrites any other value -- here a `2` would come back as
/// a `1`, corrupting the save on a round-trip that changed nothing.
#[test]
fn test_palworld_non_boolean_u32_flag_is_preserved() -> Result<()> {
    // PickupItemOnLevel: two GUIDs (32 bytes) then a single u32, set here to 2.
    let mut payload = vec![0u8; 32];
    payload.extend_from_slice(&2u32.to_le_bytes());

    let pickup = match parse_concrete_model(&payload, "PickupItem_Flint")? {
        games::palworld::PalMapConcreteModelVariant::PickupItemOnLevel(pickup) => pickup,
        other => panic!("expected PickupItemOnLevel, got {other:?}"),
    };

    assert_eq!(2, pickup.auto_picked_up);
    Ok(())
}

/// Parses a `WorkSaveData` RawData payload for `work_type`, asserting full
/// consumption and a byte-exact rewrite.
fn parse_work(payload: &[u8], work_type: &str) -> Result<games::palworld::PalWork> {
    let len = payload.len() as u64;
    let parsed = run(Cursor::new(payload.to_vec()), |ar| {
        let parsed = games::palworld::PalWork::read_with_work_type(ar, work_type)?;
        assert_eq!(
            len,
            ar.stream_position()?,
            "parser must consume the entire payload"
        );
        Ok(parsed)
    })?;

    let mut reconstructed = vec![];
    run(Cursor::new(&mut reconstructed), |ar| parsed.write(ar))?;
    assert_eq!(payload, reconstructed, "rewrite must be byte-exact");

    Ok(parsed)
}

/// Parses a `WorkAssignMap` RawData payload for the owning work's `work_type`,
/// asserting full consumption and a byte-exact rewrite.
fn parse_work_assign(payload: &[u8], work_type: &str) -> Result<games::palworld::PalWorkAssign> {
    let len = payload.len() as u64;
    let parsed = run(Cursor::new(payload.to_vec()), |ar| {
        let parsed = games::palworld::PalWorkAssign::read_with_work_type(ar, work_type)?;
        assert_eq!(
            len,
            ar.stream_position()?,
            "parser must consume the entire payload"
        );
        Ok(parsed)
    })?;

    let mut reconstructed = vec![];
    run(Cursor::new(&mut reconstructed), |ar| parsed.write(ar))?;
    assert_eq!(payload, reconstructed, "rewrite must be byte-exact");

    Ok(parsed)
}

const WORK_PROGRESS: &str = "EPalWorkableType::Progress";
const WORK_PROGRESS_MULTI_TYPE: &str = "EPalWorkableType::Progress_MultiType";

/// The 2026-07 update added `Progress_MultiType` work: a progress work that
/// tracks progress per work suitability, so its payload carries two extra
/// arrays after the shared progress fields.
#[test]
fn test_palworld_progress_multi_type_work_decodes_per_suitability_progress() -> Result<()> {
    let payload = unhex(include_str!("tests_fixtures/work_progress_multitype.hex").trim());

    // An AncientBlastFurnace, which can be worked by kindling and by watering.
    let work = parse_work(&payload, WORK_PROGRESS_MULTI_TYPE)?;
    let base_data = work.base_data.expect("multi-type work serializes a base");
    assert_eq!("AncientBlastFurnace_0", base_data.assign_define_data_id);

    let progress_entries = match &work.work_specific_data {
        games::palworld::PalWorkTypeSpecificData::ProgressMultiType {
            progress_entries, ..
        } => progress_entries,
        other => panic!("expected ProgressMultiType, got {other:?}"),
    };
    assert_eq!(2, progress_entries.len());
    assert_eq!(1, progress_entries[0].work_suitability);
    assert_eq!(31818.182, progress_entries[0].max_progress);
    assert_eq!(11, progress_entries[1].work_suitability);
    assert_eq!(30070.281, progress_entries[1].current_progress);
    Ok(())
}

/// Workers assigned to a `Progress_MultiType` work record which suitability they
/// were assigned to, appending seven bytes to the plain work assign payload.
#[test]
fn test_palworld_progress_multi_type_work_assign_records_suitability() -> Result<()> {
    let payload = unhex(include_str!("tests_fixtures/work_assign_multitype.hex").trim());

    let assign = parse_work_assign(&payload, WORK_PROGRESS_MULTI_TYPE)?;
    let multi_type = assign
        .multi_type
        .expect("multi-type work assign records the assigned suitability");
    assert_eq!(11, multi_type.assigned_work_suitability);
    assert_eq!(28, multi_type.assigned_work_type);
    assert_eq!(42, multi_type.assigned_work_action_type);
    Ok(())
}

/// A work assign for any other work type stops at the trailing bytes.
#[test]
fn test_palworld_plain_work_assign_has_no_suitability() -> Result<()> {
    let payload = unhex(include_str!("tests_fixtures/work_assign.hex").trim());

    let assign = parse_work_assign(&payload, WORK_PROGRESS)?;
    assert!(assign.multi_type.is_none());
    Ok(())
}

/// Work types that predate the update must keep parsing and rewriting exactly.
#[test]
fn test_palworld_work_types_round_trip() -> Result<()> {
    let cases = [
        (
            include_str!("tests_fixtures/work_progress.hex"),
            WORK_PROGRESS,
        ),
        (
            include_str!("tests_fixtures/work_monster_farm.hex"),
            "EPalWorkableType::MonsterFarm",
        ),
        (
            include_str!("tests_fixtures/work_only_join.hex"),
            "EPalWorkableType::OnlyJoin",
        ),
        (
            include_str!("tests_fixtures/work_repair.hex"),
            "EPalWorkableType::Repair",
        ),
    ];

    for (payload, work_type) in cases {
        let work = parse_work(&unhex(payload.trim()), work_type)?;
        assert!(work.base_data.is_some(), "{work_type} serializes a base");
    }
    Ok(())
}

/// An unhandled work type keeps its bytes rather than losing them.
#[test]
fn test_palworld_unknown_work_type_falls_back_to_raw_bytes() -> Result<()> {
    let payload = vec![1u8, 2, 3, 4, 5];

    let work = parse_work(&payload, "EPalWorkableType::CollectItem")?;
    match &work.work_specific_data {
        games::palworld::PalWorkTypeSpecificData::Unknown { data } => {
            assert_eq!(&payload, data);
        }
        other => panic!("expected Unknown, got {other:?}"),
    }
    Ok(())
}

/// End-to-end proof that a Palworld save carrying typed embedded structs
/// round-trips through JSON *deserialization*, the regression this task closes.
///
/// Two `RawData` byte-array properties are written to a GVAS save, then read
/// back with the Palworld game + types so they parse into typed
/// `StructValue::Game(PalStruct::...)` values (a rich `PalDynamicItem` weapon
/// and a `PalCharacterData`, both schema-threading variants). The typed save is
/// serialized to JSON and deserialized back as `Save<Palworld>`; equality (and
/// byte-exact rewrites) prove the game-aware seed pipeline reconstructs the
/// typed structs, including `PalCharacterData`'s nested `Properties`.
#[test]
fn test_palworld_embedded_struct_json_roundtrip() -> Result<()> {
    use games::palworld::{PalStruct, Palworld};

    // A real DynamicItemSaveData RawData for a SniperRifle_Default (parses to a
    // Weapon), and a minimal PalCharacterData blob (empty `object`, i.e. just a
    // "None" terminator, then the unknown/group_id/trailing fields).
    let dyn_blob = unhex("00000000000000000000000000000000ef8aa41f4542f82f314924941e5520a214000000536e697065725269666c655f44656661756c7400000000000400000000007a4400000000050000004e6f6e650000000000");
    let char_blob = unhex("050000004e6f6e650001020304000102030405060708090a0b0c0d0e0f0a0b0c0d");

    // Build a Save<Palworld> whose root holds the two blobs as byte arrays.
    let byte_tag = PropertyTagPartial {
        id: None,
        data: PropertyTagDataPartial::Array(std::boxed::Box::new(PropertyTagDataPartial::Byte(
            None,
        ))),
    };
    let mut schemas = PropertySchemas::new();
    schemas.record("DynItem".to_string(), byte_tag.clone());
    schemas.record("Char".to_string(), byte_tag);

    let mut props = indexmap::IndexMap::new();
    props.insert(
        PropertyKey::from("DynItem"),
        Property::Array(ValueVec::Byte(ByteArray::Byte(dyn_blob))),
    );
    props.insert(
        PropertyKey::from("Char"),
        Property::Array(ValueVec::Byte(ByteArray::Byte(char_blob))),
    );

    let s0: Save<Palworld> = Save {
        header: mock_header(),
        schemas,
        root: Root {
            save_game_type: "TestSave".to_string(),
            properties: Properties(props),
        },
        extra: vec![],
    };

    let mut bytes = vec![];
    s0.write(&mut bytes)?;

    // Read the bytes back with the Palworld game and types registered so the
    // byte arrays parse into typed game structs (error_to_raw off => a parse
    // failure fails the test rather than degrading to raw bytes).
    let mut types = Types::new();
    types.add(
        "DynItem".to_string(),
        StructType::Game("PalDynamicItem".to_string()),
    );
    types.add(
        "Char".to_string(),
        StructType::Game("PalCharacterData".to_string()),
    );

    let s1: Save<Palworld> = SaveReader::new()
        .game::<Palworld>()
        .error_to_raw(false)
        .types(types)
        .read(Cursor::new(&bytes))
        .expect("Palworld save should read back with typed structs");

    // The blobs must have become typed game structs, not stayed raw bytes.
    assert!(
        matches!(
            s1.root.properties.0.get(&PropertyKey::from("DynItem")),
            Some(Property::Struct(StructValue::Game(PalStruct::DynamicItem(
                _
            ))))
        ),
        "DynItem should parse into a typed PalDynamicItem"
    );
    assert!(
        matches!(
            s1.root.properties.0.get(&PropertyKey::from("Char")),
            Some(Property::Struct(StructValue::Game(
                PalStruct::CharacterData(_)
            )))
        ),
        "Char should parse into a typed PalCharacterData"
    );

    // The crux: serialize the typed save to JSON and deserialize it back.
    let json = serde_json::to_string(&s1).unwrap();
    let s2: Save<Palworld> = serde_json::from_str(&json).unwrap();
    assert_eq!(s1, s2, "Palworld save must survive a JSON round trip");

    // And both must rewrite to the exact original bytes.
    let mut b2 = vec![];
    s2.write(&mut b2)?;
    assert_eq!(
        bytes, b2,
        "JSON-roundtripped save must rewrite byte-for-byte"
    );

    Ok(())
}

/// [`test_palworld_embedded_struct_json_roundtrip`] only exercises an *empty*
/// `PalCharacterData.object`, so it never proves the schema-threading path
/// (thread-local ctx -> `deserialize_properties` -> `PropertiesSeed` schema
/// lookup at `{path}.{prop}`) actually reconstructs a populated nested
/// `Properties`. This test closes that gap: `object` carries a real property,
/// so a successful round trip proves the nested `Properties` were rebuilt
/// from schema-tagged JSON, not just an empty map that happens to compare
/// equal either way.
///
/// (This does not additionally exercise `GameStructOrBytesSeed::visit_seq`,
/// the "entry stayed raw bytes" fallback branch -- see the note at the end of
/// the test for why that turns out not to be reachable through the JSON
/// pipeline as currently implemented.)
#[test]
fn test_palworld_embedded_struct_json_roundtrip_populated_properties() -> Result<()> {
    use games::palworld::{PalCharacterData, PalStruct, Palworld};

    // Build a real (non-empty) PalCharacterData payload: `object` carries one
    // Int property, so its nested Properties are provably reconstructed, not
    // just an empty map. This is generated through the crate's own
    // read/write code (not hand-typed hex): only a scratch schema table is
    // needed so `write_property` can find a tag for "Level" while writing
    // this standalone blob.
    let mut char_write_schemas = PropertySchemas::new();
    char_write_schemas.record(
        "Level".to_string(),
        PropertyTagPartial {
            id: None,
            data: PropertyTagDataPartial::Other(PropertyType::IntProperty),
        },
    );
    let char_data = PalCharacterData::<SaveGameArchiveType<NoGame>> {
        object: Properties(indexmap::IndexMap::from([(
            PropertyKey::from("Level"),
            Property::Int(42),
        )])),
        unknown_bytes: [1, 2, 3, 4],
        group_id: FGuid::parse_str("2eb5fdbd4d1001ac8ff33681daa59333")?,
        trailing_bytes: [5, 6, 7, 8],
    };
    let mut char_blob = vec![];
    run(&mut Cursor::new(&mut char_blob), |writer| {
        *writer.schemas.borrow_mut() = char_write_schemas.clone();
        char_data.write(writer)
    })?;

    // Build a Save<Palworld> whose root holds the blob as a byte array, same
    // shape as test_palworld_embedded_struct_json_roundtrip.
    let byte_tag = PropertyTagPartial {
        id: None,
        data: PropertyTagDataPartial::Array(std::boxed::Box::new(PropertyTagDataPartial::Byte(
            None,
        ))),
    };
    let mut schemas = PropertySchemas::new();
    schemas.record("Char".to_string(), byte_tag);

    let mut props = indexmap::IndexMap::new();
    props.insert(
        PropertyKey::from("Char"),
        Property::Array(ValueVec::Byte(ByteArray::Byte(char_blob))),
    );

    let s0: Save<Palworld> = Save {
        header: mock_header(),
        schemas,
        root: Root {
            save_game_type: "TestSave".to_string(),
            properties: Properties(props),
        },
        extra: vec![],
    };

    let mut bytes = vec![];
    s0.write(&mut bytes)?;

    // Read the bytes back with the Palworld game and types registered so the
    // byte array parses into a typed PalCharacterData.
    let mut types = Types::new();
    types.add(
        "Char".to_string(),
        StructType::Game("PalCharacterData".to_string()),
    );

    let s1: Save<Palworld> = SaveReader::new()
        .game::<Palworld>()
        .error_to_raw(false)
        .types(types)
        .read(Cursor::new(&bytes))
        .expect("Palworld save should read back with a typed struct");

    // The struct must be typed, and its nested `object` Properties non-empty.
    match s1.root.properties.0.get(&PropertyKey::from("Char")) {
        Some(Property::Struct(StructValue::Game(PalStruct::CharacterData(data)))) => {
            assert_eq!(
                Some(&Property::Int(42)),
                data.object.0.get(&PropertyKey::from("Level")),
                "nested object Properties must have been reconstructed, not left empty"
            );
        }
        other => panic!("Char should parse into a typed PalCharacterData, got {other:?}"),
    }

    // The crux: serialize the typed save to JSON and deserialize it back.
    // Reconstructing `s1` from JSON exercises `PalCharacterData`'s
    // `Deserialize` impl, which reads `object` through the thread-local
    // (schemas, path) context installed by `Game::deserialize_struct` and
    // looks its properties up at "Char.Level" -- proving the schema-threading
    // path, not just an empty-map equality check.
    let json = serde_json::to_string(&s1).unwrap();
    let s2: Save<Palworld> = serde_json::from_str(&json).unwrap();
    assert_eq!(s1, s2, "Palworld save must survive a JSON round trip");

    // And both must rewrite to the exact original bytes.
    let mut b2 = vec![];
    s2.write(&mut b2)?;
    assert_eq!(
        bytes, b2,
        "JSON-roundtripped save must rewrite byte-for-byte"
    );

    Ok(())
}

/// A game-struct-typed property (`StructType::Game` schema) whose value stayed
/// an empty/unparsed byte array must round-trip through JSON as a byte array.
/// Real saves share one schema path across many map entries (e.g. every
/// `worldSaveData.MapObjectSaveData.Model.Connector.RawData` shares one
/// `PalConnector` schema), so an entry that stayed raw serializes as
/// `{"Byte": []}` -- a JSON *map* -- under a `Game` schema. It must be read back
/// as a byte array, not (mis)parsed as the typed struct.
#[test]
fn test_palworld_empty_game_struct_bytes_json_roundtrip() -> Result<()> {
    use games::palworld::Palworld;

    let game_tag = PropertyTagPartial {
        id: None,
        data: PropertyTagDataPartial::Struct {
            struct_type: StructType::Game("PalConnector".to_string()),
            id: FGuid::default(),
        },
    };
    let mut schemas = PropertySchemas::new();
    schemas.record("Conn".to_string(), game_tag);

    let mut props = indexmap::IndexMap::new();
    props.insert(
        PropertyKey::from("Conn"),
        Property::Array(ValueVec::Byte(ByteArray::Byte(vec![]))),
    );

    let s0: Save<Palworld> = Save {
        header: mock_header(),
        schemas,
        root: Root {
            save_game_type: "TestSave".to_string(),
            properties: Properties(props),
        },
        extra: vec![],
    };

    let json = serde_json::to_string(&s0).unwrap();
    let s2: Save<Palworld> = serde_json::from_str(&json)
        .expect("empty game-struct byte array must deserialize as a byte array");
    assert_eq!(
        s0, s2,
        "empty game-struct payload must survive a JSON round trip as bytes"
    );

    Ok(())
}

/// Covers the `StructType::Game(name)` arm of `StructValue::read`/`write`,
/// which is distinct from the RawData hook path the embedded-struct tests
/// exercise. It is reached when a `Set`/`Map` struct element has no embedded
/// type path (the older save format omits it) and is resolved from a caller
/// supplied `Types` hint, which `StructType`'s `Deserialize` maps to
/// `StructType::Game` for any non-core name.
#[test]
fn test_palworld_struct_value_game_dispatch() -> Result<()> {
    use games::palworld::{PalStruct, Palworld};

    // A minimal PalConnector blob: supported_level=7, connect index=3, zero
    // `any_place` entries, and two trailing "unknown" bytes (PalConnector's
    // `read` consumes the rest of the stream as `unknown_bytes`).
    let mut payload = vec![];
    payload.extend_from_slice(&7i32.to_le_bytes());
    payload.push(3u8);
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(&[0xAA, 0xBB]);

    let value: StructValue<SaveGameArchiveType<Palworld>> = run_palworld(
        Cursor::new(payload.clone()),
        |ar: &mut SaveGameArchive<Cursor<Vec<u8>>, Palworld>| {
            StructValue::read(ar, &StructType::Game("PalConnector".to_string()))
        },
    )?;

    match &value {
        StructValue::Game(PalStruct::Connector(connector)) => {
            assert_eq!(connector.supported_level, 7);
            assert_eq!(connector.connect.index, 3);
            assert!(connector.connect.any_place.is_empty());
            assert_eq!(connector.unknown_bytes, vec![0xAA, 0xBB]);
        }
        other => panic!("expected StructValue::Game(PalStruct::Connector(_)), got {other:?}"),
    }

    // And the mirrored `StructValue::write` Game arm must reproduce the exact
    // original bytes.
    let mut rewritten = vec![];
    run_palworld(
        Cursor::new(&mut rewritten),
        |ar: &mut SaveGameArchive<Cursor<&mut Vec<u8>>, Palworld>| value.write(ar),
    )?;
    assert_eq!(payload, rewritten, "rewrite must be byte-exact");

    Ok(())
}

#[cfg(feature = "cli")]
#[test]
fn test_registry_contains_none_game() {
    let reg = games::registry::registry();
    assert!(
        reg.iter().any(|h| h.name() == "none"),
        "registry must contain the default game"
    );
    assert!(games::registry::get("none").is_some());
    assert!(games::registry::get("definitely-not-a-game").is_none());
}

#[cfg(feature = "cli")]
#[test]
fn test_nogame_facade_json_roundtrip() -> Result<()> {
    // A plain GVAS save round-trips bytes -> JSON -> bytes through the facade.
    let mut schemas = PropertySchemas::new();
    schemas.record(
        "Level".to_string(),
        PropertyTagPartial {
            id: None,
            data: PropertyTagDataPartial::Other(PropertyType::IntProperty),
        },
    );
    let s0: Save<NoGame> = Save {
        header: mock_header(),
        schemas,
        root: Root {
            save_game_type: "TestSave".to_string(),
            properties: Properties(indexmap::IndexMap::from([(
                PropertyKey::from("Level"),
                Property::Int(7),
            )])),
        },
        extra: vec![],
    };
    let mut bytes = vec![];
    s0.write(&mut bytes)?;

    let handler = games::registry::get("none").expect("none game");
    assert!(
        handler.formats().is_empty(),
        "plain GVAS has no container formats"
    );

    let mut json = vec![];
    handler.to_json(&mut Cursor::new(&bytes), &mut json, Types::new(), false)?;

    let mut out = vec![];
    handler.from_json(&mut Cursor::new(&json), &mut out, None)?;
    assert_eq!(
        bytes, out,
        "facade JSON round trip must rewrite byte-for-byte"
    );
    Ok(())
}

#[cfg(feature = "cli")]
#[test]
fn test_registry_contains_palworld_with_formats() {
    let names: Vec<&str> = games::registry::registry()
        .iter()
        .map(|h| h.name())
        .collect();
    assert!(
        names.contains(&"palworld"),
        "registry must contain palworld, got {names:?}"
    );

    let handler = games::registry::get("palworld").expect("palworld game");
    assert!(
        handler.formats().contains(&"zlib"),
        "palworld must expose the zlib (PLZ) format"
    );
    #[cfg(feature = "oodle")]
    assert!(
        handler.formats().contains(&"oodle"),
        "palworld must expose the oodle (PLM) format"
    );

    let types = handler.default_types();
    assert!(
        types
            .get("worldSaveData.CharacterSaveParameterMap.Key")
            .is_some(),
        "palworld default_types must include the Palworld type hints"
    );
}

#[cfg(feature = "cli")]
#[test]
fn test_palworld_facade_json_roundtrip() -> Result<()> {
    use games::palworld::{PalStruct, Palworld};

    // The RawData must sit at a path palworld_types() registers, so that the
    // facade's default_types() drives real struct parsing; at an unregistered
    // path the blob stays an opaque byte array and the round trip would pass
    // even with the Palworld codecs gone.
    //
    // A real SniperRifle_Default DynamicItemSaveData blob, which parses to a
    // Weapon.
    let dyn_blob = unhex("00000000000000000000000000000000ef8aa41f4542f82f314924941e5520a214000000536e697065725269666c655f44656661756c7400000000000400000000007a4400000000050000004e6f6e650000000000");

    let mut schemas = PropertySchemas::new();
    schemas.record(
        "worldSaveData".to_string(),
        PropertyTagPartial {
            id: None,
            data: PropertyTagDataPartial::Struct {
                struct_type: StructType::Struct(Some("TestPkg.worldSaveData".to_string())),
                id: FGuid::default(),
            },
        },
    );
    schemas.record(
        "worldSaveData.DynamicItemSaveData".to_string(),
        PropertyTagPartial {
            id: None,
            data: PropertyTagDataPartial::Struct {
                struct_type: StructType::Struct(Some("TestPkg.DynamicItemSaveData".to_string())),
                id: FGuid::default(),
            },
        },
    );
    schemas.record(
        "worldSaveData.DynamicItemSaveData.RawData".to_string(),
        PropertyTagPartial {
            id: None,
            data: PropertyTagDataPartial::Array(std::boxed::Box::new(
                PropertyTagDataPartial::Byte(None),
            )),
        },
    );

    let dynamic_item_save_data = Properties(indexmap::IndexMap::from([(
        PropertyKey::from("RawData"),
        Property::Array(ValueVec::Byte(ByteArray::Byte(dyn_blob))),
    )]));
    let world_save_data = Properties(indexmap::IndexMap::from([(
        PropertyKey::from("DynamicItemSaveData"),
        Property::Struct(StructValue::Struct(dynamic_item_save_data)),
    )]));

    let s0: Save<Palworld> = Save {
        header: mock_header(),
        schemas,
        root: Root {
            save_game_type: "TestSave".to_string(),
            properties: Properties(indexmap::IndexMap::from([(
                PropertyKey::from("worldSaveData"),
                Property::Struct(StructValue::Struct(world_save_data)),
            )])),
        },
        extra: vec![],
    };
    let mut bytes = vec![];
    s0.write(&mut bytes)?;

    let handler = games::registry::get("palworld").expect("palworld game");
    let types = handler.default_types();

    // Prove the registered path actually drives typed parsing before trusting
    // the facade round trip below: read directly and check the struct is
    // Property::Struct(StructValue::Game(_)), not a byte array.
    let checked: Save<Palworld> = SaveReader::new()
        .game::<Palworld>()
        .error_to_raw(false)
        .types(types.clone())
        .read(Cursor::new(&bytes))
        .expect("Palworld save should read back with a typed struct");
    match checked
        .root
        .properties
        .0
        .get(&PropertyKey::from("worldSaveData"))
    {
        Some(Property::Struct(StructValue::Struct(world_props))) => {
            match world_props.0.get(&PropertyKey::from("DynamicItemSaveData")) {
                Some(Property::Struct(StructValue::Struct(dyn_props))) => {
                    assert!(
                        matches!(
                            dyn_props.0.get(&PropertyKey::from("RawData")),
                            Some(Property::Struct(StructValue::Game(PalStruct::DynamicItem(_))))
                        ),
                        "RawData should parse into a typed PalDynamicItem, not stay a raw byte array"
                    );
                }
                other => panic!("expected nested DynamicItemSaveData struct, got {other:?}"),
            }
        }
        other => panic!("expected nested worldSaveData struct, got {other:?}"),
    }

    let mut json = vec![];
    handler.to_json(&mut Cursor::new(&bytes), &mut json, types, false)?;

    let mut out = vec![];
    handler.from_json(&mut Cursor::new(&json), &mut out, None)?;
    assert_eq!(
        bytes, out,
        "palworld facade round trip must rewrite byte-for-byte"
    );
    Ok(())
}

/// The base camp's worker director: a fixed 118-byte payload naming the base's
/// worker character container.
#[test]
fn test_palworld_worker_director_round_trips() -> Result<()> {
    use games::palworld::PalWorkerDirector;

    let mut payload = Vec::new();
    payload.extend_from_slice(&unhex("ed321d99a74720d45205bca743a4b131")); // id
    payload.extend_from_slice(&[0u8; 80]); // spawn_transform: 10 f64s
    payload.push(3); // current_order_type
    payload.push(1); // current_battle_type
    payload.extend_from_slice(&unhex("ea696a6c4954bc615adc1ea15d76b4b0")); // container_id
    payload.extend_from_slice(&[0, 0, 0, 0]); // trailing_bytes
    assert_eq!(payload.len(), 118, "the layout is fixed at 118 bytes");

    let director = run_palworld_ue5(Cursor::new(payload.clone()), |ar| {
        PalWorkerDirector::read(ar)
    })?;
    assert_eq!(director.current_order_type, 3);
    assert_eq!(director.current_battle_type, 1);

    let mut out = Cursor::new(Vec::new());
    run_palworld_ue5(&mut out, |ar| director.write(ar))?;
    assert_eq!(out.into_inner(), payload, "write is the inverse of read");
    Ok(())
}

/// The base camp's work collection: the list of work ids the base owns.
#[test]
fn test_palworld_work_collection_round_trips() -> Result<()> {
    use games::palworld::PalWorkCollection;

    let mut payload = Vec::new();
    payload.extend_from_slice(&unhex("723a35bf4a5ac811f04e79b613ac7529")); // own_id
    payload.extend_from_slice(&2i32.to_le_bytes()); // work_ids count
    payload.extend_from_slice(&unhex("bb232bbd4b0c8ccf3a948b8fd52396e3"));
    payload.extend_from_slice(&unhex("eaad63d6491d6abf8b96f0873ef2c939"));
    payload.extend_from_slice(&0u32.to_le_bytes()); // trailing_bytes

    let collection =
        run_palworld_ue5(Cursor::new(payload.clone()), |ar| PalWorkCollection::read(ar))?;
    assert_eq!(collection.work_ids.len(), 2);

    let mut out = Cursor::new(Vec::new());
    run_palworld_ue5(&mut out, |ar| collection.write(ar))?;
    assert_eq!(out.into_inner(), payload, "write is the inverse of read");
    Ok(())
}

/// An empty collection is a count of zero, not an absent array.
#[test]
fn test_palworld_work_collection_handles_no_works() -> Result<()> {
    use games::palworld::PalWorkCollection;

    let mut payload = Vec::new();
    payload.extend_from_slice(&unhex("723a35bf4a5ac811f04e79b613ac7529"));
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes());

    let collection = run_palworld_ue5(Cursor::new(payload), |ar| PalWorkCollection::read(ar))?;
    assert!(collection.work_ids.is_empty());
    Ok(())
}

/// A transport-item director carries the pals assigned to hauling.
#[test]
fn test_palworld_base_camp_module_transport_item_director_round_trips() -> Result<()> {
    use games::palworld::{PalBaseCampModule, PalBaseCampModuleData};

    let mut payload = Vec::new();
    payload.extend_from_slice(&1i32.to_le_bytes()); // one character info
    payload.extend_from_slice(&0i32.to_le_bytes()); // its item_infos: empty
    payload.extend_from_slice(&1.0f64.to_le_bytes()); // character_location.x
    payload.extend_from_slice(&2.0f64.to_le_bytes()); // y
    payload.extend_from_slice(&3.0f64.to_le_bytes()); // z
    payload.extend_from_slice(&[0, 0, 0, 0]); // trailing_bytes

    let module = run_palworld_ue5(Cursor::new(payload.clone()), |ar| {
        PalBaseCampModule::read_with_module_type(
            ar,
            "EPalBaseCampModuleType::TransportItemDirector",
        )
    })?;
    match &module.data {
        PalBaseCampModuleData::TransportItemDirector { character_infos, .. } => {
            assert_eq!(character_infos.len(), 1);
            assert_eq!(character_infos[0].character_location.z.0, 3.0);
        }
        other => panic!("expected TransportItemDirector, got {other:?}"),
    }

    let mut out = Cursor::new(Vec::new());
    run_palworld_ue5(&mut out, |ar| module.write(ar))?;
    assert_eq!(out.into_inner(), payload, "write is the inverse of read");
    Ok(())
}

/// A module type carrying no payload stays empty rather than consuming bytes.
#[test]
fn test_palworld_base_camp_module_no_op_type_reads_empty() -> Result<()> {
    use games::palworld::{PalBaseCampModule, PalBaseCampModuleData};

    let module = run_palworld_ue5(Cursor::new(Vec::new()), |ar| {
        PalBaseCampModule::read_with_module_type(ar, "EPalBaseCampModuleType::Energy")
    })?;
    assert_eq!(module.data, PalBaseCampModuleData::NoOp);
    Ok(())
}

/// An unhandled module type keeps its bytes rather than losing them.
#[test]
fn test_palworld_base_camp_module_unknown_type_falls_back_to_raw_bytes() -> Result<()> {
    use games::palworld::{PalBaseCampModule, PalBaseCampModuleData};

    let payload = vec![9u8, 8, 7];
    let module = run_palworld_ue5(Cursor::new(payload.clone()), |ar| {
        PalBaseCampModule::read_with_module_type(ar, "EPalBaseCampModuleType::SomethingNew")
    })?;
    assert_eq!(module.data, PalBaseCampModuleData::Unknown { data: payload });
    Ok(())
}

/// The standalone codec (no property tree) round-trips the same bytes as
/// `read_with_module_type`/`write`, and picks the f64 `Vector` layout when
/// `engine_version_major` is 5.
#[test]
fn test_palworld_base_camp_module_standalone_bytes_round_trip() -> Result<()> {
    use games::palworld::PalBaseCampModule;

    let mut payload = Vec::new();
    payload.extend_from_slice(&1i32.to_le_bytes());
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload.extend_from_slice(&1.0f64.to_le_bytes());
    payload.extend_from_slice(&2.0f64.to_le_bytes());
    payload.extend_from_slice(&3.0f64.to_le_bytes());
    payload.extend_from_slice(&[0, 0, 0, 0]);

    let module = PalBaseCampModule::from_bytes(
        &payload,
        "EPalBaseCampModuleType::TransportItemDirector",
        5,
    )?;
    assert_eq!(module.to_bytes(5)?, payload, "to_bytes is the inverse of from_bytes");
    Ok(())
}

#[cfg(feature = "gvas-json")]
#[test]
fn test_gvas_json_error_carries_the_property_path() {
    let err = Error::GvasJson {
        path: "worldSaveData.MapObjectSaveData".to_string(),
        message: "unknown property type: WidgetProperty".to_string(),
    };
    assert_eq!(
        err.to_string(),
        "gvas json at worldSaveData.MapObjectSaveData: unknown property type: WidgetProperty"
    );
}

#[cfg(feature = "gvas-json")]
mod gvas_json_tests {
    use crate::compat::gvas_json::property_from_json;
    use crate::games::palworld::Palworld;
    use crate::*;
    use pretty_assertions::assert_eq;

    fn prop(json: &str) -> Property<SaveGameArchiveType<Palworld>> {
        let value: serde_json::Value = serde_json::from_str(json).expect("valid json");
        property_from_json::<Palworld>(&value, "test").expect("converts")
    }

    fn prop_at(path: &str, json: &str) -> Property<SaveGameArchiveType<Palworld>> {
        let value: serde_json::Value = serde_json::from_str(json).expect("valid json");
        property_from_json::<Palworld>(&value, path).expect("converts")
    }

    #[test]
    fn reads_scalar_properties() {
        assert_eq!(prop(r#"{"type":"IntProperty","value":42,"id":null}"#), Property::Int(42));
        assert_eq!(prop(r#"{"type":"BoolProperty","value":true,"id":null}"#), Property::Bool(true));
        assert_eq!(
            prop(r#"{"type":"NameProperty","value":"ItemChest_04","id":null}"#),
            Property::Name("ItemChest_04".to_string())
        );
        assert_eq!(
            prop(r#"{"type":"StrProperty","value":"Home","id":null}"#),
            Property::Str("Home".to_string())
        );
        assert_eq!(
            prop(r#"{"type":"Int64Property","value":8589934592,"id":null}"#),
            Property::Int64(8589934592)
        );
        assert_eq!(
            prop(r#"{"type":"FloatProperty","value":1.5,"id":null}"#),
            Property::Float(Float(1.5))
        );
        assert_eq!(
            prop(r#"{"type":"DoubleProperty","value":2.5,"id":null}"#),
            Property::Double(Double(2.5))
        );
        assert_eq!(prop(r#"{"type":"Int8Property","value":-12,"id":null}"#), Property::Int8(-12));
        assert_eq!(
            prop(r#"{"type":"Int16Property","value":-1200,"id":null}"#),
            Property::Int16(-1200)
        );
        assert_eq!(prop(r#"{"type":"UInt8Property","value":200,"id":null}"#), Property::UInt8(200));
        // `RankUpExp` on `PalIndividualCharacterSaveParameter` is the real field a
        // corpus sample surfaced this arm from.
        assert_eq!(
            prop(r#"{"type":"UInt16Property","value":999,"id":null}"#),
            Property::UInt16(999)
        );
        assert_eq!(
            prop(r#"{"type":"UInt32Property","value":4000000000,"id":null}"#),
            Property::UInt32(4000000000)
        );
        assert_eq!(
            prop(r#"{"type":"UInt64Property","value":18000000000000000000,"id":null}"#),
            Property::UInt64(18000000000000000000)
        );
    }

    #[test]
    fn reads_a_plain_numeric_byte_property() {
        assert_eq!(
            prop(r#"{"type":"ByteProperty","id":null,"value":{"type":"None","value":7}}"#),
            Property::Byte(Byte::Byte(7))
        );
    }

    #[test]
    fn reads_an_enum_backed_byte_property() {
        assert_eq!(
            prop(
                r#"{"type":"ByteProperty","id":null,
                    "value":{"type":"EPalGenderType","value":"EPalGenderType::Male"}}"#
            ),
            Property::Byte(Byte::Label("EPalGenderType::Male".to_string()))
        );
    }

    /// The dialect nests the enum's own type alongside its value.
    #[test]
    fn reads_an_enum_property_from_its_nested_value() {
        assert_eq!(
            prop(r#"{"type":"EnumProperty","id":null,
                     "value":{"type":"EPalWorkableType","value":"EPalWorkableType::MonsterFarm"}}"#),
            Property::Enum("EPalWorkableType::MonsterFarm".to_string())
        );
    }

    #[test]
    fn an_unknown_property_type_names_its_path() {
        let value: serde_json::Value =
            serde_json::from_str(r#"{"type":"WidgetProperty","value":1}"#).unwrap();
        let err = property_from_json::<Palworld>(&value, "worldSaveData.Thing").unwrap_err();
        assert_eq!(
            err.to_string(),
            "gvas json at worldSaveData.Thing: unknown property type: WidgetProperty"
        );
    }

    #[test]
    fn reads_a_guid_struct() {
        assert_eq!(
            prop(r#"{"type":"StructProperty","struct_type":"Guid",
                     "struct_id":"00000000-0000-0000-0000-000000000000","id":null,
                     "value":"fac62390-ccce-4cc4-9ce6-3b2eb894b17c"}"#),
            Property::Struct(StructValue::Guid(
                FGuid::parse_str("fac62390-ccce-4cc4-9ce6-3b2eb894b17c").unwrap()
            ))
        );
    }

    #[test]
    fn reads_a_named_struct_as_nested_properties() {
        let value = prop(
            r#"{"type":"StructProperty","struct_type":"PalItemContainerBelongInfo",
                "struct_id":"00000000-0000-0000-0000-000000000000","id":null,
                "value":{"bControllableOthers":{"type":"BoolProperty","value":false,"id":null}}}"#,
        );
        let Property::Struct(StructValue::Struct(props)) = value else {
            panic!("expected a nested struct");
        };
        assert_eq!(props.0.get(&PropertyKey::from("bControllableOthers")), Some(&Property::Bool(false)));
    }

    /// Engine math structs write their fields as plain numbers, not nested
    /// property objects like a user-defined struct does.
    #[test]
    fn reads_a_vector_struct_from_plain_numbers() {
        assert_eq!(
            prop(
                r#"{"type":"StructProperty","struct_type":"Vector",
                    "struct_id":"00000000-0000-0000-0000-000000000000","id":null,
                    "value":{"x":1.0,"y":2.0,"z":3.0}}"#
            ),
            Property::Struct(StructValue::Vector(Vector { x: Double(1.0), y: Double(2.0), z: Double(3.0) }))
        );
    }

    #[test]
    fn reads_a_vector2d_struct_from_plain_numbers() {
        assert_eq!(
            prop(
                r#"{"type":"StructProperty","struct_type":"Vector2D",
                    "struct_id":"00000000-0000-0000-0000-000000000000","id":null,
                    "value":{"x":4.0,"y":5.0}}"#
            ),
            Property::Struct(StructValue::Vector2D(Vector2D { x: Double(4.0), y: Double(5.0) }))
        );
    }

    #[test]
    fn reads_a_rotator_struct_from_plain_numbers() {
        assert_eq!(
            prop(
                r#"{"type":"StructProperty","struct_type":"Rotator",
                    "struct_id":"00000000-0000-0000-0000-000000000000","id":null,
                    "value":{"x":10.0,"y":20.0,"z":30.0}}"#
            ),
            Property::Struct(StructValue::Rotator(Rotator { x: Double(10.0), y: Double(20.0), z: Double(30.0) }))
        );
    }

    #[test]
    fn reads_a_quat_struct_from_plain_numbers() {
        assert_eq!(
            prop(
                r#"{"type":"StructProperty","struct_type":"Quat",
                    "struct_id":"00000000-0000-0000-0000-000000000000","id":null,
                    "value":{"x":0.0,"y":0.0,"z":0.0,"w":1.0}}"#
            ),
            Property::Struct(StructValue::Quat(Quat {
                x: Double(0.0),
                y: Double(0.0),
                z: Double(0.0),
                w: Double(1.0)
            }))
        );
    }

    #[test]
    fn reads_an_opaque_byte_array() {
        assert_eq!(
            prop(r#"{"type":"ArrayProperty","array_type":"ByteProperty","id":null,
                     "value":{"values":[1,0,0,0]}}"#),
            Property::Array(ValueVec::Byte(ByteArray::Byte(vec![1, 0, 0, 0])))
        );
    }

    #[test]
    fn reads_a_struct_array_from_its_values() {
        let value = prop(
            r#"{"type":"ArrayProperty","array_type":"StructProperty","id":null,
                "value":{"prop_name":"Slots","prop_type":"StructProperty",
                         "type_name":"PalCharacterSlotSaveData",
                         "id":"00000000-0000-0000-0000-000000000000",
                         "values":[{"SlotIndex":{"type":"IntProperty","value":3,"id":null}}]}}"#,
        );
        let Property::Array(ValueVec::Struct(items)) = value else { panic!("expected a struct array") };
        assert_eq!(items.len(), 1);
        let StructValue::Struct(props) = &items[0] else { panic!("expected a struct element") };
        assert_eq!(props.0.get(&PropertyKey::from("SlotIndex")), Some(&Property::Int(3)));
    }

    /// A `Guid`-typed struct array (e.g. `OldOwnerPlayerUIds`) carries bare uuid
    /// strings rather than nested property objects.
    #[test]
    fn reads_a_guid_typed_struct_array_from_bare_uuids() {
        let value = prop(
            r#"{"type":"ArrayProperty","array_type":"StructProperty","id":null,
                "value":{"prop_name":"OldOwnerPlayerUIds","prop_type":"StructProperty",
                         "type_name":"Guid","id":"00000000-0000-0000-0000-000000000000",
                         "values":["fac62390-ccce-4cc4-9ce6-3b2eb894b17c"]}}"#,
        );
        let Property::Array(ValueVec::Struct(items)) = value else { panic!("expected a struct array") };
        assert_eq!(
            items,
            vec![StructValue::Guid(FGuid::parse_str("fac62390-ccce-4cc4-9ce6-3b2eb894b17c").unwrap())]
        );
    }

    #[test]
    fn reads_a_name_array_from_its_values() {
        assert_eq!(
            prop(
                r#"{"type":"ArrayProperty","array_type":"NameProperty","id":null,
                    "value":{"values":["Foo","Bar"]}}"#
            ),
            Property::Array(ValueVec::Name(vec!["Foo".to_string(), "Bar".to_string()]))
        );
    }

    #[test]
    fn reads_an_enum_array_from_its_values() {
        assert_eq!(
            prop(
                r#"{"type":"ArrayProperty","array_type":"EnumProperty","id":null,
                    "value":{"values":["EPalWorkableType::MonsterFarm","EPalWorkableType::Handiwork"]}}"#
            ),
            Property::Array(ValueVec::Enum(vec![
                "EPalWorkableType::MonsterFarm".to_string(),
                "EPalWorkableType::Handiwork".to_string()
            ]))
        );
    }

    #[test]
    fn reads_an_enum_keyed_map() {
        let value = prop(
            r#"{"type":"MapProperty","key_type":"EnumProperty","value_type":"StructProperty",
                "id":null,
                "value":[{"key":"EPalMapObjectConcreteModelModuleType::ItemContainer",
                          "value":{"SlotNum":{"type":"IntProperty","value":7,"id":null}}}]}"#,
        );
        let Property::Map(entries) = value else { panic!("expected a map") };
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].key,
            Property::Enum("EPalMapObjectConcreteModelModuleType::ItemContainer".to_string())
        );
        let Property::Struct(StructValue::Struct(props)) = &entries[0].value else {
            panic!("expected a struct value");
        };
        assert_eq!(props.0.get(&PropertyKey::from("SlotNum")), Some(&Property::Int(7)));
    }

    #[test]
    fn reads_an_int_keyed_map() {
        let value = prop(
            r#"{"type":"MapProperty","key_type":"IntProperty","value_type":"StructProperty",
                "id":null,"value":[{"key":0,"value":{}}]}"#,
        );
        let Property::Map(entries) = value else { panic!("expected a map") };
        assert_eq!(entries[0].key, Property::Int(0));
    }

    #[test]
    fn an_empty_map_reads_as_no_entries() {
        let value = prop(
            r#"{"type":"MapProperty","key_type":"IntProperty","value_type":"StructProperty",
                "id":null,"value":[]}"#,
        );
        assert_eq!(value, Property::Map(vec![]));
    }

    use crate::games::palworld::compat::{bool_or_u32, transform};

    /// The dialect writes a JSON bool where uesave keeps the integer.
    #[test]
    fn a_u32_field_accepts_both_a_bool_and_an_integer() {
        assert_eq!(bool_or_u32(&serde_json::json!(true), "p").unwrap(), 1);
        assert_eq!(bool_or_u32(&serde_json::json!(false), "p").unwrap(), 0);
        assert_eq!(bool_or_u32(&serde_json::json!(7), "p").unwrap(), 7);
        assert!(bool_or_u32(&serde_json::json!("nope"), "p").is_err());
    }

    /// The dialect spells the scale component `scale3d`.
    #[test]
    fn a_transform_reads_scale_from_scale3d() {
        let value = serde_json::json!({
            "rotation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
            "translation": {"x": 1.0, "y": 2.0, "z": 3.0},
            "scale3d": {"x": 1.0, "y": 1.0, "z": 1.0}
        });
        let parsed = transform(&value, "p").expect("transform converts");
        assert_eq!(parsed.translation.x.0, 1.0);
        assert_eq!(parsed.scale.z.0, 1.0);
    }

    /// A game that implements no hook leaves decoded payloads to the default.
    #[test]
    fn the_default_game_hook_returns_none() {
        let decoded = serde_json::json!({"state": 1});
        assert!(NoGame::game_struct_from_gvas_json("anything", &decoded).unwrap().is_none());
    }

    /// When neither hook types a decoded payload, the walker must not silently drop
    /// it as opaque bytes: it has no bytes to keep, only fields neither hook claimed.
    #[test]
    fn a_decoded_payload_neither_hook_types_errs() {
        let value: serde_json::Value = serde_json::from_str(
            r#"{"type":"ArrayProperty","array_type":"ByteProperty","id":null,
                "value":{"state":1}}"#,
        )
        .unwrap();
        let error = property_from_json::<NoGame>(&value, "worldSaveData.Thing").unwrap_err();
        let Error::GvasJson { path, message } = error else { panic!("expected a GvasJson error") };
        assert_eq!(path, "worldSaveData.Thing");
        assert!(message.contains("does not type"), "message was: {message}");
    }

    use crate::games::palworld::PalStruct;

    fn game_struct(path: &str, json: &str) -> PalStruct<SaveGameArchiveType<Palworld>> {
        let value: serde_json::Value = serde_json::from_str(json).expect("valid json");
        Palworld::game_struct_from_gvas_json(path, &value)
            .expect("adapter runs")
            .expect("path is typed")
    }

    #[test]
    fn adapts_a_base_camp() {
        let adapted = game_struct(
            "worldSaveData.BaseCampSaveData.RawData",
            r#"{"id":"c4e01fc6-3f51-477c-9338-11a15af52460","name":"Home","state":1,
                "transform":{"rotation":{"x":0.0,"y":0.0,"z":0.0,"w":1.0},
                             "translation":{"x":10.0,"y":20.0,"z":30.0},
                             "scale3d":{"x":1.0,"y":1.0,"z":1.0}},
                "area_range":3500.0,
                "group_id_belong_to":"9d2ae570-428b-4517-4df1-68b7ed01dd4b",
                "fast_travel_local_transform":{"rotation":{"x":0.0,"y":0.0,"z":0.0,"w":1.0},
                             "translation":{"x":0.0,"y":0.0,"z":0.0},
                             "scale3d":{"x":1.0,"y":1.0,"z":1.0}},
                "owner_map_object_instance_id":"00000000-0000-0000-0000-000000000000",
                "trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::BaseCamp(camp) = adapted else { panic!("expected BaseCamp") };
        assert_eq!(camp.name, "Home");
        assert_eq!(camp.area_range, 3500.0);
        assert_eq!(camp.transform.translation.y.0, 20.0);
        assert_eq!(camp.transform.scale.x.0, 1.0, "scale3d must land on scale");
    }

    #[test]
    fn adapts_a_build_process() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.Model.BuildProcess.RawData",
            r#"{"state":1,"id":"00000000-0000-0000-0000-000000000000","trailing_bytes":[0,0,0,0]}"#,
        );
        assert!(matches!(adapted, PalStruct::BuildProcess(_)));
    }

    #[test]
    fn an_untyped_path_returns_none() {
        let value = serde_json::json!({"anything": 1});
        assert!(Palworld::game_struct_from_gvas_json("worldSaveData.Nope.RawData", &value)
            .unwrap()
            .is_none());
    }

    /// The dialect misspells this field; the typo IS the wire format.
    #[test]
    fn adapts_a_map_model_from_the_misspelled_transform_cache() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.Model.RawData",
            r#"{"instance_id":"c57b4ddf-42a1-d22e-c831-ddac2315ea46",
                "concrete_model_instance_id":"9f1f4b78-4df0-da8d-2e69-42b70b266f99",
                "base_camp_id_belong_to":"c4e01fc6-3f51-477c-9338-11a15af52460",
                "group_id_belong_to":"9d2ae570-428b-4517-4df1-68b7ed01dd4b",
                "hp":{"current":15000,"max":15000},
                "initital_transform_cache":{"rotation":{"x":0.0,"y":0.0,"z":0.0,"w":1.0},
                    "translation":{"x":84158.75,"y":-145737.89,"z":-2041.54},
                    "scale3d":{"x":1.0,"y":1.0,"z":1.0}},
                "repair_work_id":"00000000-0000-0000-0000-000000000000",
                "owner_spawner_level_object_instance_id":"00000000-0000-0000-0000-000000000000",
                "owner_instance_id":"00000000-0000-0000-0000-000000000000",
                "build_player_uid":"00000000-0000-0000-0000-000000000000",
                "interact_restrict_type":0,
                "deterioration_damage":0.0,
                "stage_instance_id_belong_to":{"id":"00000000-0000-0000-0000-000000000000","valid":0},
                "unknown_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapModel(model) = adapted else { panic!("expected MapModel") };
        assert_eq!(model.hp.current, 15000);
        assert_eq!(model.initial_transform_cache.translation.z.0, -2041.54);
        assert_eq!(model.initial_transform_cache.scale.y.0, 1.0);
    }

    /// Real exports write the nested `stage_instance_id_belong_to.valid` as a JSON
    /// bool, one level deeper than `map_model`'s own `from_value` call reaches.
    #[test]
    fn adapts_a_map_model_with_a_boolean_stage_instance_valid() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.Model.RawData",
            r#"{"instance_id":"c57b4ddf-42a1-d22e-c831-ddac2315ea46",
                "concrete_model_instance_id":"9f1f4b78-4df0-da8d-2e69-42b70b266f99",
                "base_camp_id_belong_to":"c4e01fc6-3f51-477c-9338-11a15af52460",
                "group_id_belong_to":"9d2ae570-428b-4517-4df1-68b7ed01dd4b",
                "hp":{"current":15000,"max":15000},
                "initital_transform_cache":{"rotation":{"x":0.0,"y":0.0,"z":0.0,"w":1.0},
                    "translation":{"x":84158.75,"y":-145737.89,"z":-2041.54},
                    "scale3d":{"x":1.0,"y":1.0,"z":1.0}},
                "repair_work_id":"00000000-0000-0000-0000-000000000000",
                "owner_spawner_level_object_instance_id":"00000000-0000-0000-0000-000000000000",
                "owner_instance_id":"00000000-0000-0000-0000-000000000000",
                "build_player_uid":"00000000-0000-0000-0000-000000000000",
                "interact_restrict_type":0,
                "deterioration_damage":0.0,
                "stage_instance_id_belong_to":{"id":"00000000-0000-0000-0000-000000000000","valid":true},
                "unknown_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapModel(model) = adapted else { panic!("expected MapModel") };
        assert_eq!(model.stage_instance_id_belong_to.valid, 1);
    }

    /// A committed fixture elsewhere writes `valid` as a large `u32`; both forms occur
    /// in real exports and both must survive.
    #[test]
    fn adapts_a_map_model_with_a_large_u32_stage_instance_valid() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.Model.RawData",
            r#"{"instance_id":"c57b4ddf-42a1-d22e-c831-ddac2315ea46",
                "concrete_model_instance_id":"9f1f4b78-4df0-da8d-2e69-42b70b266f99",
                "base_camp_id_belong_to":"c4e01fc6-3f51-477c-9338-11a15af52460",
                "group_id_belong_to":"9d2ae570-428b-4517-4df1-68b7ed01dd4b",
                "hp":{"current":15000,"max":15000},
                "initital_transform_cache":{"rotation":{"x":0.0,"y":0.0,"z":0.0,"w":1.0},
                    "translation":{"x":84158.75,"y":-145737.89,"z":-2041.54},
                    "scale3d":{"x":1.0,"y":1.0,"z":1.0}},
                "repair_work_id":"00000000-0000-0000-0000-000000000000",
                "owner_spawner_level_object_instance_id":"00000000-0000-0000-0000-000000000000",
                "owner_instance_id":"00000000-0000-0000-0000-000000000000",
                "build_player_uid":"00000000-0000-0000-0000-000000000000",
                "interact_restrict_type":0,
                "deterioration_damage":0.0,
                "stage_instance_id_belong_to":{"id":"00000000-0000-0000-0000-000000000000","valid":3864341120},
                "unknown_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapModel(model) = adapted else { panic!("expected MapModel") };
        assert_eq!(model.stage_instance_id_belong_to.valid, 3864341120);
    }

    #[test]
    fn adapts_a_worker_director() {
        let adapted = game_struct(
            "worldSaveData.BaseCampSaveData.WorkerDirector.RawData",
            r#"{"id":"ed321d99-47a7-20d4-5205-bca743a4b131",
                "spawn_transform":{"rotation":{"x":0.0,"y":0.0,"z":0.0,"w":1.0},
                    "translation":{"x":-102520.48,"y":40252.85,"z":2387.99},
                    "scale3d":{"x":1.0,"y":1.0,"z":1.0}},
                "current_order_type":0,"current_battle_type":0,
                "container_id":"ea696a6c-4954-bc61-5adc-1ea15d76b4b0",
                "trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::WorkerDirector(director) = adapted else { panic!("expected WorkerDirector") };
        assert_eq!(
            director.container_id,
            FGuid::parse_str("ea696a6c-4954-bc61-5adc-1ea15d76b4b0").unwrap()
        );
        assert_eq!(director.spawn_transform.translation.y.0, 40252.85);
    }

    /// The dialect calls it `id`; uesave calls it `own_id`.
    #[test]
    fn adapts_a_work_collection_from_its_id_field() {
        let adapted = game_struct(
            "worldSaveData.BaseCampSaveData.WorkCollection.RawData",
            r#"{"id":"723a35bf-4a5a-c811-f04e-79b613ac7529",
                "work_ids":["bb232bbd-4b0c-8ccf-3a94-8b8fd52396e3"],
                "trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::WorkCollection(collection) = adapted else { panic!("expected WorkCollection") };
        assert_eq!(
            collection.own_id,
            FGuid::parse_str("723a35bf-4a5a-c811-f04e-79b613ac7529").unwrap()
        );
        assert_eq!(collection.work_ids.len(), 1);
    }

    /// `trailing_bytes` is the little-endian `u32` those 4 wire bytes decode to.
    #[test]
    fn a_work_collection_decodes_trailing_bytes_as_little_endian() {
        let adapted = game_struct(
            "worldSaveData.BaseCampSaveData.WorkCollection.RawData",
            r#"{"id":"723a35bf-4a5a-c811-f04e-79b613ac7529",
                "work_ids":[],
                "trailing_bytes":[1,0,0,0]}"#,
        );
        let PalStruct::WorkCollection(collection) = adapted else { panic!("expected WorkCollection") };
        assert_eq!(collection.trailing_bytes, 1);
    }

    use crate::games::palworld::{PalBaseCampModule, PalBaseCampModuleData, PalBaseCampPassiveEffect};

    /// `ModuleMap.RawData` stays opaque bytes: no `PalStruct::BaseCampModule`
    /// variant exists. The bytes hook re-encodes a decoded payload so it still
    /// round-trips through the property tree as a byte array.
    fn module_bytes(json: &str) -> Vec<u8> {
        let value: serde_json::Value = serde_json::from_str(json).expect("valid json");
        let property = property_from_json::<Palworld>(
            &value,
            "worldSaveData.BaseCampSaveData.ModuleMap.RawData",
        )
        .expect("adapter runs");
        let Property::Array(ValueVec::Byte(ByteArray::Byte(bytes))) = property else {
            panic!("expected an opaque byte array")
        };
        bytes
    }

    #[test]
    fn adapts_a_transport_item_director_module_to_bytes() {
        let bytes = module_bytes(
            r#"{"type":"ArrayProperty","array_type":"ByteProperty","id":null,
                "value":{"transport_item_character_infos":[],"trailing_bytes":[0,0,0,0]}}"#,
        );
        let module = PalBaseCampModule::from_bytes(
            &bytes,
            "EPalBaseCampModuleType::TransportItemDirector",
            5,
        )
        .expect("decodes");
        assert!(matches!(
            module.data,
            PalBaseCampModuleData::TransportItemDirector { .. }
        ));
    }

    /// A module the dialect left empty carries no payload.
    #[test]
    fn adapts_an_empty_base_camp_module_as_no_op_bytes() {
        let bytes = module_bytes(
            r#"{"type":"ArrayProperty","array_type":"ByteProperty","id":null,"value":{}}"#,
        );
        assert!(bytes.is_empty());
    }

    /// `effect_type == 2` carries an extra byte plus a 12-byte trailer; the only
    /// nontrivial shape the module bytes adapter encodes.
    #[test]
    fn adapts_passive_effects_module_and_round_trips_the_work_hard_trailer() {
        let bytes = module_bytes(
            r#"{"type":"ArrayProperty","array_type":"ByteProperty","id":null,
                "value":{"passive_effects":[
                    {"effect_type":1,"work_hard_type":null,"unknown_trailer":null},
                    {"effect_type":2,"work_hard_type":3,
                     "unknown_trailer":[1,2,3,4,5,6,7,8,9,10,11,12]}
                ]}}"#,
        );
        let module = PalBaseCampModule::from_bytes(&bytes, "EPalBaseCampModuleType::PassiveEffect", 5)
            .expect("decodes");
        assert_eq!(
            module.data,
            PalBaseCampModuleData::PassiveEffect {
                passive_effects: vec![
                    PalBaseCampPassiveEffect {
                        effect_type: 1,
                        work_hard_type: None,
                        unknown_trailer: None,
                    },
                    PalBaseCampPassiveEffect {
                        effect_type: 2,
                        work_hard_type: Some(3),
                        unknown_trailer: Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
                    },
                ],
            }
        );
    }

    /// An empty dict is a legitimate no-payload module; a non-empty dict whose
    /// fields match no known module shape must error rather than silently
    /// becoming `NoOp` and losing data.
    #[test]
    fn a_nonempty_unrecognized_module_shape_errs_but_empty_stays_no_op() {
        let value: serde_json::Value = serde_json::from_str(
            r#"{"type":"ArrayProperty","array_type":"ByteProperty","id":null,
                "value":{"something_new":1}}"#,
        )
        .expect("valid json");
        let error = property_from_json::<Palworld>(
            &value,
            "worldSaveData.BaseCampSaveData.ModuleMap.RawData",
        )
        .expect_err("unrecognized fields must error");
        let Error::GvasJson { path, message } = error else { panic!("expected a GvasJson error") };
        assert_eq!(path, "worldSaveData.BaseCampSaveData.ModuleMap.RawData");
        assert!(message.contains("something_new"), "message was: {message}");

        let bytes = module_bytes(
            r#"{"type":"ArrayProperty","array_type":"ByteProperty","id":null,"value":{}}"#,
        );
        assert!(bytes.is_empty(), "an empty dict must still adapt as NoOp");
    }

    #[test]
    fn the_default_bytes_hook_returns_none() {
        let decoded = serde_json::json!({"state": 1});
        assert!(NoGame::game_bytes_from_gvas_json("anything", &decoded).unwrap().is_none());
    }

    use crate::games::palworld::PalMapConcreteModelModuleData;

    #[test]
    fn adapts_an_item_container_module() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.ModuleMap.RawData",
            r#"{"target_container_id":"ea696a6c-4954-bc61-5adc-1ea15d76b4b0",
                "slot_attribute_indexes":[],"all_slot_attribute":[],
                "drop_item_at_disposed":true,"usage_type":0,"trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModelModule(module) = adapted else { panic!("expected a module") };
        match module.data {
            PalMapConcreteModelModuleData::ItemContainer { drop_item_at_disposed, .. } => {
                assert_eq!(drop_item_at_disposed, 1, "the dialect writes this bool; uesave keeps a u32");
            }
            other => panic!("expected ItemContainer, got {other:?}"),
        }
    }

    #[test]
    fn adapts_a_character_container_module() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.ModuleMap.RawData",
            r#"{"target_container_id":"ea696a6c-4954-bc61-5adc-1ea15d76b4b0",
                "trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModelModule(module) = adapted else { panic!("expected a module") };
        assert!(matches!(module.data, PalMapConcreteModelModuleData::CharacterContainer { .. }));
    }

    #[test]
    fn adapts_a_workee_module() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.ModuleMap.RawData",
            r#"{"target_work_id":"bb232bbd-4b0c-8ccf-3a94-8b8fd52396e3","trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModelModule(module) = adapted else { panic!("expected a module") };
        assert!(matches!(module.data, PalMapConcreteModelModuleData::Workee { .. }));
    }

    #[test]
    fn adapts_a_password_lock_module() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.ModuleMap.RawData",
            r#"{"lock_state":1,"password":"1234",
                "player_infos":[{"player_uid":"ea696a6c-4954-bc61-5adc-1ea15d76b4b0",
                                  "try_failed_count":2,"try_success_cache":1}],
                "trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModelModule(module) = adapted else { panic!("expected a module") };
        match module.data {
            PalMapConcreteModelModuleData::PasswordLock { lock_state, password, player_infos, .. } => {
                assert_eq!(lock_state, 1);
                assert_eq!(password, "1234");
                assert_eq!(player_infos.len(), 1);
                assert_eq!(player_infos[0].try_failed_count, 2);
            }
            other => panic!("expected PasswordLock, got {other:?}"),
        }
    }

    #[test]
    fn adapts_a_switch_module() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.ModuleMap.RawData",
            r#"{"switch_state":1,"trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModelModule(module) = adapted else { panic!("expected a module") };
        match module.data {
            PalMapConcreteModelModuleData::Switch { switch_state, .. } => {
                assert_eq!(switch_state, 1);
            }
            other => panic!("expected Switch, got {other:?}"),
        }
    }

    #[test]
    fn adapts_an_unknown_module() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.ModuleMap.RawData",
            r#"{"unknown_bytes":[1,2,3,4]}"#,
        );
        let PalStruct::MapConcreteModelModule(module) = adapted else { panic!("expected a module") };
        match module.data {
            PalMapConcreteModelModuleData::Unknown { raw_bytes, .. } => {
                assert_eq!(raw_bytes, vec![1, 2, 3, 4]);
            }
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    /// An unrecognized non-empty shape must error rather than silently falling
    /// through to a default variant and losing data.
    #[test]
    fn a_nonempty_unrecognized_concrete_model_module_shape_errs() {
        let value: serde_json::Value = serde_json::from_str(r#"{"something_new":1}"#).unwrap();
        let error = Palworld::game_struct_from_gvas_json(
            "worldSaveData.MapObjectSaveData.ConcreteModel.ModuleMap.RawData",
            &value,
        )
        .expect_err("unrecognized fields must error");
        let Error::GvasJson { path, message } = error else { panic!("expected a GvasJson error") };
        assert_eq!(path, "worldSaveData.MapObjectSaveData.ConcreteModel.ModuleMap.RawData");
        assert!(message.contains("something_new"), "message was: {message}");
    }

    /// The sibling CustomVersionData is lifted into the struct, then removed from the
    /// property tree. Left in place it would be written twice.
    #[test]
    fn a_module_entry_consumes_its_sibling_custom_version_data() {
        let value = prop_at(
            "worldSaveData.MapObjectSaveData.ConcreteModel.ModuleMap",
            r#"{"type":"MapProperty","key_type":"EnumProperty","value_type":"StructProperty",
                "id":null,
                "value":[{"key":"EPalMapObjectConcreteModelModuleType::CharacterContainer",
                  "value":{
                    "RawData":{"type":"ArrayProperty","array_type":"ByteProperty","id":null,
                      "value":{"target_container_id":"ea696a6c-4954-bc61-5adc-1ea15d76b4b0",
                               "trailing_bytes":[0,0,0,0]}},
                    "CustomVersionData":{"type":"ArrayProperty","array_type":"ByteProperty",
                      "id":null,"value":{"values":[1,0,0,0]}}}}]}"#,
        );
        let Property::Map(entries) = value else { panic!("expected a map") };
        let Property::Struct(StructValue::Struct(props)) = &entries[0].value else {
            panic!("expected a struct value");
        };
        assert!(
            props.0.get(&PropertyKey::from("CustomVersionData")).is_none(),
            "the sibling must be consumed, not duplicated"
        );
        let Some(Property::Struct(StructValue::Game(PalStruct::MapConcreteModelModule(module)))) =
            props.0.get(&PropertyKey::from("RawData"))
        else {
            panic!("expected a typed module");
        };
        assert_eq!(module.custom_version_data, vec![1, 0, 0, 0]);
    }

    /// A base camp module entry has the same shape (a decoded `RawData` beside a
    /// `CustomVersionData` sibling), but `PalBaseCampModule` has nowhere to store
    /// the sibling. Gating the lift on the path (not the shape) must leave it alone.
    #[test]
    fn a_base_camp_module_entry_keeps_its_sibling_custom_version_data() {
        let value = prop_at(
            "worldSaveData.BaseCampSaveData.ModuleMap",
            r#"{"type":"MapProperty","key_type":"EnumProperty","value_type":"StructProperty",
                "id":null,
                "value":[{"key":"EPalBaseCampModuleType::TransportItemDirector",
                  "value":{
                    "RawData":{"type":"ArrayProperty","array_type":"ByteProperty","id":null,
                      "value":{}},
                    "CustomVersionData":{"type":"ArrayProperty","array_type":"ByteProperty",
                      "id":null,"value":{"values":[1,0,0,0]}}}}]}"#,
        );
        let Property::Map(entries) = value else { panic!("expected a map") };
        let Property::Struct(StructValue::Struct(props)) = &entries[0].value else {
            panic!("expected a struct value");
        };
        assert_eq!(
            props.0.get(&PropertyKey::from("CustomVersionData")),
            Some(&Property::Array(ValueVec::Byte(ByteArray::Byte(vec![1, 0, 0, 0])))),
            "a base camp module has nowhere to fold the sibling, so it must survive"
        );
        assert_eq!(
            props.0.get(&PropertyKey::from("RawData")),
            Some(&Property::Array(ValueVec::Byte(ByteArray::Byte(vec![])))),
            "RawData must not have absorbed the sibling bytes"
        );
    }

    /// If `RawData` is still opaque (the dialect kept its bytes rather than decoding
    /// them), the ByteProperty arm takes the `values` branch and never looks at an
    /// injected key. Lifting the sibling in regardless would remove it from the tree
    /// while its bytes go nowhere. The lift must be skipped, leaving the sibling intact.
    #[test]
    fn a_module_entry_with_an_opaque_raw_data_keeps_its_sibling() {
        let value = prop_at(
            "worldSaveData.MapObjectSaveData.ConcreteModel.ModuleMap",
            r#"{"type":"MapProperty","key_type":"EnumProperty","value_type":"StructProperty",
                "id":null,
                "value":[{"key":"EPalMapObjectConcreteModelModuleType::CharacterContainer",
                  "value":{
                    "RawData":{"type":"ArrayProperty","array_type":"ByteProperty","id":null,
                      "value":{"values":[1,2,3,4]}},
                    "CustomVersionData":{"type":"ArrayProperty","array_type":"ByteProperty",
                      "id":null,"value":{"values":[1,0,0,0]}}}}]}"#,
        );
        let Property::Map(entries) = value else { panic!("expected a map") };
        let Property::Struct(StructValue::Struct(props)) = &entries[0].value else {
            panic!("expected a struct value");
        };
        assert_eq!(
            props.0.get(&PropertyKey::from("CustomVersionData")),
            Some(&Property::Array(ValueVec::Byte(ByteArray::Byte(vec![1, 0, 0, 0])))),
            "an opaque RawData has nowhere to receive the lifted bytes, so the sibling must survive"
        );
        assert_eq!(
            props.0.get(&PropertyKey::from("RawData")),
            Some(&Property::Array(ValueVec::Byte(ByteArray::Byte(vec![1, 2, 3, 4])))),
            "RawData's own bytes must be untouched too"
        );
    }

    use crate::games::palworld::PalMapConcreteModelVariant;

    #[test]
    fn adapts_a_base_camp_point_concrete_model() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"9f1f4b78-4df0-da8d-2e69-42b70b266f99",
                "model_instance_id":"c57b4ddf-42a1-d22e-c831-ddac2315ea46",
                "concrete_model_type":"PalMapObjectBaseCampPoint",
                "leading_bytes":[0,0,0,0],
                "base_camp_id":"c4e01fc6-3f51-477c-9338-11a15af52460",
                "trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        assert!(matches!(model.model_data, PalMapConcreteModelVariant::BaseCampPoint(_)));
    }

    #[test]
    fn adapts_an_item_chest_concrete_model() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"9f1f4b78-4df0-da8d-2e69-42b70b266f99",
                "model_instance_id":"c57b4ddf-42a1-d22e-c831-ddac2315ea46",
                "concrete_model_type":"PalMapObjectItemChestModel",
                "leading_bytes":[0,0,0,0],
                "private_lock_player_uid":"00000000-0000-0000-0000-000000000000",
                "trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        assert!(matches!(model.model_data, PalMapConcreteModelVariant::ItemChest(_)));
    }

    #[test]
    fn adapts_an_item_chest_affect_corruption_concrete_model() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"9f1f4b78-4df0-da8d-2e69-42b70b266f99",
                "model_instance_id":"c57b4ddf-42a1-d22e-c831-ddac2315ea46",
                "concrete_model_type":"PalMapObjectItemChest_AffectCorruption",
                "leading_bytes":[0,0,0,0],
                "private_lock_player_uid":"00000000-0000-0000-0000-000000000000",
                "trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        assert!(matches!(model.model_data, PalMapConcreteModelVariant::ItemChestAffectCorruption(_)));
    }

    #[test]
    fn adapts_a_product_item_concrete_model() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"9f1f4b78-4df0-da8d-2e69-42b70b266f99",
                "model_instance_id":"c57b4ddf-42a1-d22e-c831-ddac2315ea46",
                "concrete_model_type":"PalMapObjectProductItemModel",
                "leading_bytes":[0,0,0,0],
                "work_speed_additional_rate":1.5,
                "product_item_id":"Wood",
                "trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        let PalMapConcreteModelVariant::ProductItem(product) = model.model_data else {
            panic!("expected ProductItem");
        };
        assert_eq!(product.product_item_id, "Wood");
        assert_eq!(product.work_speed_additional_rate, 1.5);
    }

    /// uesave reads a pickup guard the dialect never writes; its absence is expected.
    #[test]
    fn a_pickup_item_model_without_a_guard_adapts_to_none() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"9f1f4b78-4df0-da8d-2e69-42b70b266f99",
                "model_instance_id":"c57b4ddf-42a1-d22e-c831-ddac2315ea46",
                "concrete_model_type":"PalMapObjectPickupItemOnLevelModel",
                "auto_picked_up":true}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        let PalMapConcreteModelVariant::PickupItemOnLevel(pickup) = model.model_data else {
            panic!("expected PickupItemOnLevel");
        };
        assert_eq!(pickup.auto_picked_up, 1, "the dialect writes a bool where uesave keeps a u32");
        assert!(pickup.pickup_guard.is_none(), "the dialect writes no pickup guard");
    }

    /// The dialect misspells this field; uesave's `DropItem` struct spells it
    /// correctly, so the dialect's key must be read and reassigned.
    #[test]
    fn adapts_a_drop_item_concrete_model_from_the_misspelled_player_uid() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"9f1f4b78-4df0-da8d-2e69-42b70b266f99",
                "model_instance_id":"c57b4ddf-42a1-d22e-c831-ddac2315ea46",
                "concrete_model_type":"PalMapObjectDropItemModel",
                "auto_picked_up":true,
                "pickupdable_player_uid":"c4e01fc6-3f51-477c-9338-11a15af52460",
                "remove_pickup_guard_timer_handle":0,
                "item_id":{"static_id":"Wood",
                    "dynamic_id":{"created_world_id":"00000000-0000-0000-0000-000000000000",
                        "local_id_in_created_world":"00000000-0000-0000-0000-000000000000"}},
                "trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        let PalMapConcreteModelVariant::DropItem(drop) = model.model_data else {
            panic!("expected DropItem");
        };
        assert_eq!(drop.auto_picked_up, 1, "the dialect writes a bool where uesave keeps a u32");
        assert_eq!(
            drop.pickupable_player_uid,
            FGuid::parse_str("c4e01fc6-3f51-477c-9338-11a15af52460").unwrap()
        );
        assert_eq!(drop.item_id.static_id, "Wood");
    }

    /// The dialect misspells this field the same way uesave's own `PalEgg` struct
    /// does, so here (unlike `DropItem`) no rename is needed: the dialect's key
    /// already matches the struct field's name.
    #[test]
    fn adapts_a_pal_egg_concrete_model_from_the_misspelled_player_uid() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"9f1f4b78-4df0-da8d-2e69-42b70b266f99",
                "model_instance_id":"c57b4ddf-42a1-d22e-c831-ddac2315ea46",
                "concrete_model_type":"PalMapObjectPalEggModel",
                "auto_picked_up":true,
                "pickupdable_player_uid":"c4e01fc6-3f51-477c-9338-11a15af52460",
                "remove_pickup_guard_timer_handle":0}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        let PalMapConcreteModelVariant::PalEgg(egg) = model.model_data else {
            panic!("expected PalEgg");
        };
        assert_eq!(egg.auto_picked_up, 1, "the dialect writes a bool where uesave keeps a u32");
        assert_eq!(
            egg.pickupdable_player_uid,
            FGuid::parse_str("c4e01fc6-3f51-477c-9338-11a15af52460").unwrap()
        );
    }

    #[test]
    fn adapts_a_torch_concrete_model() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"1821dedc-456d-91c3-4151-b4b7bf25640a",
                "model_instance_id":"dcf69e1d-4a21-f37f-530b-12b577c9f3ff",
                "concrete_model_type":"PalMapObjectTorchModel",
                "ignition_minutes":0,
                "extinction_date_time":53274670000000,
                "trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        let PalMapConcreteModelVariant::Torch(torch) = model.model_data else {
            panic!("expected Torch");
        };
        assert_eq!(torch.ignition_minutes, 0);
        assert_eq!(torch.extinction_date_time, 53274670000000);
    }

    #[test]
    fn adapts_a_convert_item_concrete_model() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"acce5d38-4f7b-05ee-5340-7abef21c0074",
                "model_instance_id":"fc0d5e40-45be-2773-eb0c-9ca12409c28a",
                "concrete_model_type":"PalMapObjectConvertItemModel",
                "leading_bytes":[0,0,0,0],
                "current_recipe_id":"None",
                "requested_product_num":0,
                "remain_product_num":0,
                "work_speed_additional_rate":1.5,
                "trailing_bytes":[0,0,0,0,0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        let PalMapConcreteModelVariant::ConvertItem(convert) = model.model_data else {
            panic!("expected ConvertItem");
        };
        assert_eq!(convert.current_recipe_id, "None");
        assert_eq!(convert.work_speed_additional_rate, 1.5);
        assert_eq!(convert.requested_product_num, 0);
        assert_eq!(convert.remain_product_num, 0);
    }

    #[test]
    fn adapts_a_farm_block_v2_concrete_model() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"28bc66ab-4c0e-da49-da31-23ad7a2e6add",
                "model_instance_id":"af9d5b90-4676-8c7b-2880-95943cc3c570",
                "concrete_model_type":"PalMapObjectFarmBlockV2Model",
                "crop_progress_rate":0.0,
                "crop_data_id":"Berries",
                "current_state":4,
                "crop_progress_rate_value":0.4391110837459564,
                "water_stack_rate_value":0.0,
                "state_machine":{"growup_required_time":180.0,"growup_progress_time":0.0},
                "trailing_bytes":[0,0,0,0,0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        let PalMapConcreteModelVariant::FarmBlockV2(farm) = model.model_data else {
            panic!("expected FarmBlockV2");
        };
        assert_eq!(farm.crop_data_id, "Berries");
        assert_eq!(farm.current_state, 4);
        assert_eq!(farm.crop_progress_rate_value, 0.4391110837459564);
        assert_eq!(farm.state_machine.growup_required_time, 180.0);
        assert_eq!(farm.state_machine.growup_progress_time, 0.0);
    }

    /// The dialect spells this field `consume_energy_speed`; uesave's struct spells
    /// the same field `consume_energy_amount`, so the dialect's key must be read and
    /// reassigned.
    #[test]
    fn adapts_a_generate_energy_concrete_model_from_the_renamed_consume_field() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"91419bd8-4b4d-a9ab-9ff3-669f9d80b7e3",
                "model_instance_id":"9b47d440-4faa-0656-9a8a-c597debe30cb",
                "concrete_model_type":"PalMapObjectGenerateEnergyModel",
                "generate_energy_rate_by_worker":0.0,
                "stored_energy_amount":250000.0,
                "consume_energy_speed":0.0}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        let PalMapConcreteModelVariant::GenerateEnergy(generator) = model.model_data else {
            panic!("expected GenerateEnergy");
        };
        assert_eq!(generator.stored_energy_amount, 250000.0);
        assert_eq!(generator.consume_energy_amount, 0.0);
        assert_eq!(generator.generate_energy_rate_by_worker, 0.0);
    }

    /// A model type the dialect did not decode keeps the generic shape.
    #[test]
    fn an_unrecognized_concrete_model_type_adapts_to_unknown() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"9f1f4b78-4df0-da8d-2e69-42b70b266f99",
                "model_instance_id":"c57b4ddf-42a1-d22e-c831-ddac2315ea46",
                "concrete_model_type":"PalMapObjectSomethingNewModel",
                "trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        assert!(matches!(model.model_data, PalMapConcreteModelVariant::Unknown(_)));
    }

    /// `BaseModel` only carries `trailing_bytes`; a field-structured dialect payload
    /// for an unwired type must not be waved through and silently dropped.
    #[test]
    fn an_unrecognized_concrete_model_type_with_extra_fields_errs() {
        let value: serde_json::Value = serde_json::from_str(
            r#"{"instance_id":"9f1f4b78-4df0-da8d-2e69-42b70b266f99",
                "model_instance_id":"c57b4ddf-42a1-d22e-c831-ddac2315ea46",
                "concrete_model_type":"PalMapObjectSomethingNewModel",
                "work_speed_additional_rate":1.5,
                "trailing_bytes":[0,0,0,0]}"#,
        )
        .unwrap();
        let error = Palworld::game_struct_from_gvas_json(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            &value,
        )
        .expect_err("unrecognized fields must error");
        let Error::GvasJson { path, message } = error else { panic!("expected a GvasJson error") };
        assert_eq!(path, "worldSaveData.MapObjectSaveData.ConcreteModel.RawData");
        assert!(message.contains("work_speed_additional_rate"), "message was: {message}");
    }

    /// The dialect sometimes splits a catch-all model's raw tail across two keys where
    /// uesave's binary reader takes one continuous run; the two must concatenate in
    /// wire order rather than erroring as an unrecognized field.
    #[test]
    fn an_unrecognized_concrete_model_type_concatenates_the_split_trailing_bytes() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"a21372f8-17a2-40ce-ba62-fdf53af20423",
                "model_instance_id":"d3ae265c-66f6-43f8-8139-17adce497235",
                "concrete_model_type":"PalMapObjectLampModel",
                "trailing_bytes":[0,0,0,0],
                "trailing_unknown_bytes":[0,0,0,0,0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        let PalMapConcreteModelVariant::Unknown(base) = model.model_data else {
            panic!("expected the generic Unknown shape");
        };
        assert_eq!(base.trailing_bytes, vec![0u8; 12]);
    }

    /// A model type with no dedicated struct on either side carries its raw tail as
    /// `unknown_bytes` instead of `trailing_bytes`.
    #[test]
    fn an_unrecognized_concrete_model_type_reads_unknown_bytes_as_the_trailing_run() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"b617bfde-4d74-040c-73ca-d18d1aebb454",
                "model_instance_id":"75ad2398-4ea0-6027-f600-f9be1af3ed2d",
                "concrete_model_type":"PalMapObjectMultiHatchingEggModel",
                "unknown_bytes":[0,0,0,0,0,0,0,0,0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        let PalMapConcreteModelVariant::Unknown(base) = model.model_data else {
            panic!("expected the generic Unknown shape");
        };
        assert_eq!(base.trailing_bytes, vec![0u8; 12]);
    }

    #[test]
    fn adapts_an_energy_storage_concrete_model() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"50922270-ade7-458c-946a-f3e54812d6aa",
                "model_instance_id":"eb6035d0-7cf0-4eda-8d90-813776b273a6",
                "concrete_model_type":"PalMapObjectEnergyStorageModel",
                "stored_energy_amount":0.0,
                "trailing_bytes":[255,132,207,73,0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        let PalMapConcreteModelVariant::EnergyStorage(storage) = model.model_data else {
            panic!("expected EnergyStorage");
        };
        assert_eq!(storage.trailing_bytes, [255, 132, 207, 73, 0, 0, 0, 0]);
    }

    #[test]
    fn adapts_a_signboard_concrete_model() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"c8dfcfb3-d6d6-4a81-90dc-48c9433eab87",
                "model_instance_id":"9e22e572-4061-4c4b-ae70-be90bbe8ee7c",
                "concrete_model_type":"PalMapObjectSignboardModel",
                "leading_bytes":[0,0,0,0],
                "signboard_text":"<blue>Encubadoras</>\r\n<blue>de Ovos</>",
                "last_modified_player_uid":"00000000-0000-0000-0000-000000000001",
                "trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        let PalMapConcreteModelVariant::Signboard(sign) = model.model_data else {
            panic!("expected Signboard");
        };
        assert_eq!(sign.signboard_text, "<blue>Encubadoras</>\r\n<blue>de Ovos</>");
    }

    #[test]
    fn adapts_a_defense_bullet_launcher_concrete_model() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"7e8b4b51-4cb0-bac3-490e-0fa0ed83a78a",
                "model_instance_id":"3e5838df-46ba-b8cb-5be0-fdbd5c52efeb",
                "concrete_model_type":"PalMapObjectDefenseBulletLauncherModel",
                "leading_bytes":[0,0,0,0],
                "remaining_bullets":0,
                "magazine_size":200,
                "bullet_item_name":"RifleBullet",
                "trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        let PalMapConcreteModelVariant::DefenseBulletLauncher(launcher) = model.model_data else {
            panic!("expected DefenseBulletLauncher");
        };
        assert_eq!(launcher.bullet_item_name, "RifleBullet");
        assert_eq!(launcher.magazine_size, 200);
    }

    /// `Properties<T>`'s own `Deserialize` impl needs a schema-aware context a bare
    /// `serde_json::from_value` never establishes, so this field is built through the
    /// property-tree walker instead (same pattern as `character_data`).
    #[test]
    fn adapts_a_hatching_egg_concrete_model_with_an_empty_save_parameter() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"0b958946-48e3-f56a-bc05-c9a6132ce9fa",
                "model_instance_id":"fd71698d-4da9-4a90-b8d5-dfb6b382885c",
                "concrete_model_type":"PalMapObjectHatchingEggModel",
                "leading_bytes":[0,0,0,0],
                "hatched_character_save_parameter":{},
                "current_pal_egg_temp_diff":0,
                "hatched_character_guid":"00000000-0000-0000-0000-000000000000",
                "trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        let PalMapConcreteModelVariant::HatchingEgg(egg) = model.model_data else {
            panic!("expected HatchingEgg");
        };
        assert_eq!(egg.current_pal_egg_temp_diff, 0);
        assert!(egg.hatched_character_save_parameter.0.is_empty());
    }

    #[test]
    fn adapts_a_breed_farm_concrete_model_with_no_spawned_eggs() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"14880031-4ee5-7009-378d-0dbf03e0bdf0",
                "model_instance_id":"cb90d832-4af1-f9bb-df68-13b16ff246f3",
                "concrete_model_type":"PalMapObjectBreedFarmModel",
                "leading_bytes":[0,0,0,0],
                "spawned_egg_instance_ids":[],
                "trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        let PalMapConcreteModelVariant::BreedFarm(farm) = model.model_data else {
            panic!("expected BreedFarm");
        };
        assert!(farm.spawned_egg_instance_ids.is_empty());
        assert!(farm.breeding.is_none());
    }

    /// The dialect has no field of its own for the optional `breeding` block; when
    /// present its bytes arrive as `trailing_unknown_bytes` and must be decoded with
    /// uesave's own reader.
    #[test]
    fn adapts_a_breed_farm_concrete_model_decoding_breeding_from_trailing_unknown_bytes() {
        let adapted = game_struct(
            "worldSaveData.MapObjectSaveData.ConcreteModel.RawData",
            r#"{"instance_id":"bc670f6e-d399-479a-bef5-23ab1e284748",
                "model_instance_id":"09fcd54d-1d16-490a-b88c-123b34edd10e",
                "concrete_model_type":"PalMapObjectBreedFarmModel",
                "leading_bytes":[0,0,0,0],
                "spawned_egg_instance_ids":[],
                "trailing_bytes":[0,0,0,0],
                "trailing_unknown_bytes":[0,0,0,0,0,0,0,0]}"#,
        );
        let PalStruct::MapConcreteModel(model) = adapted else { panic!("expected a concrete model") };
        let PalMapConcreteModelVariant::BreedFarm(farm) = model.model_data else {
            panic!("expected BreedFarm");
        };
        let breeding = farm.breeding.expect("breeding decoded from trailing_unknown_bytes");
        assert!(breeding.last_proceed_worker_individual_ids.is_empty());
        assert!(breeding.target_breed_item_ids.is_empty());
    }

    use crate::games::palworld::PalDynamicItemType;

    #[test]
    fn adapts_character_data_with_its_nested_object() {
        let adapted = game_struct(
            "worldSaveData.CharacterSaveParameterMap.RawData",
            r#"{"group_id":"9d2ae570-428b-4517-4df1-68b7ed01dd4b",
                "object":{"SaveParameter":{"type":"StructProperty","struct_type":"PalIndividualCharacterSaveParameter",
                    "struct_id":"00000000-0000-0000-0000-000000000000","id":null,
                    "value":{"Level":{"type":"IntProperty","value":12,"id":null}}}},
                "trailing_bytes":[0,0,0,0],"unknown_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::CharacterData(character) = adapted else { panic!("expected CharacterData") };
        let Some(Property::Struct(StructValue::Struct(save_parameter))) =
            character.object.0.get(&PropertyKey::from("SaveParameter"))
        else {
            panic!("expected a nested SaveParameter struct");
        };
        assert_eq!(save_parameter.0.get(&PropertyKey::from("Level")), Some(&Property::Int(12)));
    }

    /// The corpus nests `static_id` inside `id`, matching the dialect; uesave splits
    /// it into a sibling field on `PalDynamicItem`.
    #[test]
    fn adapts_a_dynamic_item() {
        let adapted = game_struct(
            "worldSaveData.DynamicItemSaveData.RawData",
            r#"{"id":{"created_world_id":"00000000-0000-0000-0000-000000000000",
                     "local_id_in_created_world":"723a35bf-4a5a-c811-f04e-79b613ac7529",
                     "static_id":"Handgun"},
                "type":"weapon","durability":100.0,"remaining_bullets":7,
                "passive_skill_list":[],
                "leading_bytes":[0,0,0,0],"trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::DynamicItem(item) = adapted else { panic!("expected DynamicItem") };
        assert_eq!(item.static_id, "Handgun");
        let PalDynamicItemType::Weapon { durability, remaining_bullets, unknown_str, .. } =
            &item.item_type
        else {
            panic!("expected Weapon");
        };
        assert_eq!(*durability, 100.0);
        assert_eq!(*remaining_bullets, 7);
        assert!(unknown_str.is_none());
    }

    #[test]
    fn adapts_an_armor_dynamic_item() {
        let adapted = game_struct(
            "worldSaveData.DynamicItemSaveData.RawData",
            r#"{"id":{"created_world_id":"00000000-0000-0000-0000-000000000000",
                     "local_id_in_created_world":"8ea7c9cf-97b6-4b55-8570-7a6372f2d4a0",
                     "static_id":"LeatherArmor"},
                "type":"armor","durability":80.0,
                "leading_bytes":[0,0,0,0],"trailing_bytes":[0,0,0,0]}"#,
        );
        let PalStruct::DynamicItem(item) = adapted else { panic!("expected DynamicItem") };
        assert_eq!(item.static_id, "LeatherArmor");
        let PalDynamicItemType::Armor { durability, .. } = item.item_type else {
            panic!("expected Armor");
        };
        assert_eq!(durability, 80.0);
    }

    /// Real corpus bytes for a post-update weapon: the dialect's `trailing_bytes` is the
    /// FString length prefix for the FName "None", and the true trailing bytes hide
    /// inside `unknown_bytes`. Recovering them must produce the same typed `Weapon`
    /// uesave's own binary reader would, so an item's representation does not depend
    /// on whether it came from a real save or this JSON dialect.
    #[test]
    fn adapts_a_post_update_weapon_by_recovering_its_fstring() {
        let adapted = game_struct(
            "worldSaveData.DynamicItemSaveData.RawData",
            r#"{"id":{"created_world_id":"00000000-0000-0000-0000-000000000000",
                     "local_id_in_created_world":"8ea7c9cf-97b6-4b55-8570-7a6372f2d4a0",
                     "static_id":"Pickaxe_Steal"},
                "type":"weapon","durability":100.0,"remaining_bullets":0,
                "passive_skill_list":[],
                "leading_bytes":[0,0,0,0],
                "trailing_bytes":[5,0,0,0],
                "unknown_bytes":[78,111,110,101,0,0,0,0,0]}"#,
        );
        let PalStruct::DynamicItem(item) = adapted else { panic!("expected DynamicItem") };
        let PalDynamicItemType::Weapon { unknown_str, trailing_bytes, .. } = item.item_type else {
            panic!("expected Weapon");
        };
        assert_eq!(unknown_str, Some("None".to_string()));
        assert_eq!(trailing_bytes, [0, 0, 0, 0]);
    }

    /// A trailer that does not decode as a plausible FString followed by exactly 4
    /// trailing bytes must fall back to `Unknown` with the bytes reassembled in their
    /// original order, rather than guessing and silently corrupting the item.
    #[test]
    fn a_malformed_weapon_trailer_falls_back_to_unknown() {
        let adapted = game_struct(
            "worldSaveData.DynamicItemSaveData.RawData",
            r#"{"id":{"created_world_id":"00000000-0000-0000-0000-000000000000",
                     "local_id_in_created_world":"00000000-0000-0000-0000-000000000000",
                     "static_id":"Weird"},
                "type":"weapon","durability":50.0,"remaining_bullets":3,
                "passive_skill_list":[],
                "leading_bytes":[1,2,3,4],
                "trailing_bytes":[255,255,255,127],
                "unknown_bytes":[9,9,9,9]}"#,
        );
        let PalStruct::DynamicItem(item) = adapted else { panic!("expected DynamicItem") };
        let PalDynamicItemType::Unknown { trailer } = item.item_type else {
            panic!("expected a fallback to Unknown");
        };
        let mut expected = Vec::new();
        expected.extend_from_slice(&[1, 2, 3, 4]);
        expected.extend_from_slice(&50.0f32.to_le_bytes());
        expected.extend_from_slice(&3i32.to_le_bytes());
        expected.extend_from_slice(&0u32.to_le_bytes());
        expected.extend_from_slice(&[255, 255, 255, 127]);
        expected.extend_from_slice(&[9, 9, 9, 9]);
        assert_eq!(trailer, expected);
    }

    /// The recovered FString's UTF-16 branch: a plausible non-ASCII trailer, negative
    /// length prefix and all, must decode to the same string uesave's own FString
    /// reader would produce, not just the ASCII fast path.
    #[test]
    fn recovers_a_non_ascii_fstring_from_a_weapon_trailer() {
        let adapted = game_struct(
            "worldSaveData.DynamicItemSaveData.RawData",
            r#"{"id":{"created_world_id":"00000000-0000-0000-0000-000000000000",
                     "local_id_in_created_world":"00000000-0000-0000-0000-000000000000",
                     "static_id":"Weird"},
                "type":"weapon","durability":100.0,"remaining_bullets":0,
                "passive_skill_list":[],
                "leading_bytes":[0,0,0,0],
                "trailing_bytes":[254,255,255,255],
                "unknown_bytes":[233,0,0,0,1,2,3,4]}"#,
        );
        let PalStruct::DynamicItem(item) = adapted else { panic!("expected DynamicItem") };
        let PalDynamicItemType::Weapon { unknown_str, trailing_bytes, .. } = item.item_type else {
            panic!("expected Weapon");
        };
        assert_eq!(unknown_str, Some("\u{e9}".to_string()));
        assert_eq!(trailing_bytes, [1, 2, 3, 4]);
    }

    /// The fallback trailer's own FString encoder: a non-ASCII passive skill name
    /// must round-trip through the same negative-length UTF-16 encoding uesave's own
    /// writer would use, not just the ASCII fast path.
    #[test]
    fn a_fallback_trailer_encodes_a_non_ascii_passive_skill() {
        let adapted = game_struct(
            "worldSaveData.DynamicItemSaveData.RawData",
            r#"{"id":{"created_world_id":"00000000-0000-0000-0000-000000000000",
                     "local_id_in_created_world":"00000000-0000-0000-0000-000000000000",
                     "static_id":"Weird"},
                "type":"weapon","durability":50.0,"remaining_bullets":3,
                "passive_skill_list":["é"],
                "leading_bytes":[1,2,3,4],
                "trailing_bytes":[255,255,255,127],
                "unknown_bytes":[9,9,9,9]}"#,
        );
        let PalStruct::DynamicItem(item) = adapted else { panic!("expected DynamicItem") };
        let PalDynamicItemType::Unknown { trailer } = item.item_type else {
            panic!("expected a fallback to Unknown");
        };
        let mut expected = Vec::new();
        expected.extend_from_slice(&[1, 2, 3, 4]);
        expected.extend_from_slice(&50.0f32.to_le_bytes());
        expected.extend_from_slice(&3i32.to_le_bytes());
        expected.extend_from_slice(&1u32.to_le_bytes());
        // "é" encoded as a negative-length, null-terminated UTF-16 FString.
        expected.extend_from_slice(&(-2i32).to_le_bytes());
        expected.extend_from_slice(&0x00e9u16.to_le_bytes());
        expected.extend_from_slice(&[0, 0]);
        expected.extend_from_slice(&[255, 255, 255, 127]);
        expected.extend_from_slice(&[9, 9, 9, 9]);
        assert_eq!(trailer, expected);
    }

    /// An unrecognized `type` (here `egg`, a shape this adapter does not implement)
    /// must error naming its fields rather than silently becoming a default.
    #[test]
    fn an_unrecognized_dynamic_item_type_errs() {
        let value: serde_json::Value = serde_json::from_str(
            r#"{"id":{"created_world_id":"00000000-0000-0000-0000-000000000000",
                     "local_id_in_created_world":"00000000-0000-0000-0000-000000000000",
                     "static_id":"Egg"},
                "type":"egg","character_id":"SomePal","leading_bytes":[0,0,0,0]}"#,
        )
        .unwrap();
        let error =
            Palworld::game_struct_from_gvas_json("worldSaveData.DynamicItemSaveData.RawData", &value)
                .expect_err("unrecognized fields must error");
        let Error::GvasJson { path, message } = error else { panic!("expected a GvasJson error") };
        assert_eq!(path, "worldSaveData.DynamicItemSaveData.RawData");
        assert!(message.contains("character_id"), "message was: {message}");
    }

    use crate::games::palworld::{PalWork, PalWorkTypeSpecificData};

    /// Every work payload below is a real dict from the sample corpus, one per
    /// `WorkableType` the corpus exercises.
    fn work_from(json: &str) -> PalWork {
        let adapted = game_struct("worldSaveData.WorkSaveData.RawData", json);
        let PalStruct::Work(work) = adapted else { panic!("expected Work") };
        *work
    }

    /// The flat dict splits into a base, a transform and a type-specific payload, and
    /// the three bools the dialect writes for `u32` fields come back as `u32`s.
    #[test]
    fn adapts_a_progress_work_splitting_the_flat_dialect_dict() {
        let work = work_from(
            r#"{"id":"b38e2281-f228-44bc-992f-d9268cd0799b",
                "workable_bounds":{"location":{"x":0.0,"y":0.0,"z":250.0},
                    "rotation":{"x":0.0,"y":-0.0,"z":0.0,"w":1.0},
                    "box_sphere_bounds":{"origin":{"x":0.0,"y":0.0,"z":0.0},
                        "box_extent":{"x":400.0,"y":400.0,"z":250.0},
                        "sphere_radius":618.4658438426491}},
                "base_camp_id_belong_to":"16a18bfe-f989-4d61-ae64-061de5eb1b09",
                "owner_map_object_model_id":"f4cb4274-5066-4681-8a72-01c5ae137a45",
                "owner_map_object_concrete_model_id":"8d46db52-67f6-4aba-9c0e-1df40a3a15d0",
                "current_state":1,"assign_locations":[],"behaviour_type":0,
                "assign_define_data_id":"OilPump_0","override_work_type":0,
                "assignable_fixed_type":0,
                "assignable_otomo":true,"can_trigger_worker_event":true,
                "can_steal_assign":true,
                "required_work_amount":0.0,"current_work_amount":2000.0,
                "work_exp":0,"work_exp_calc_type":0,
                "auto_work_self_amount_by_sec":1428.32470703125,
                "progress_time_since_last_tick":30.0,"tick_process_min_interval":0.0,
                "transform":{"type":2,
                    "map_object_instance_id":"f4cb4274-5066-4681-8a72-01c5ae137a45",
                    "trailing_bytes":[0,0,0,0,0,0,0,0]}}"#,
        );
        assert_eq!(work.work_type, "", "the sibling WorkableType is re-attached by the caller");

        let base = work.base_data.as_ref().expect("the flat dict carries base data");
        assert_eq!(base.assign_define_data_id, "OilPump_0");
        assert_eq!(base.workable_bounds.box_sphere_bounds.sphere_radius, 618.4658438426491);
        assert_eq!(base.assignable_otomo, 1, "the dialect writes a bool; uesave keeps a u32");
        assert_eq!(base.can_trigger_worker_event, 1);
        assert_eq!(base.can_steal_assign, 1);

        let transform = work.transform.as_ref().expect("a type-2 transform is carried");
        assert_eq!(transform.transform_type, 2);
        assert_eq!(
            transform.map_object_instance_id,
            Some(FGuid::parse_str("f4cb4274-5066-4681-8a72-01c5ae137a45").unwrap())
        );
        assert_eq!(transform.trailing_bytes, Some([0; 8]));

        let PalWorkTypeSpecificData::Progress {
            current_work_amount,
            auto_work_self_amount_by_sec,
            progress_time_since_last_tick,
            ..
        } = work.work_specific_data
        else {
            panic!("expected Progress");
        };
        assert_eq!(current_work_amount, 2000.0);
        assert_eq!(auto_work_self_amount_by_sec, 1428.324_7);
        assert_eq!(progress_time_since_last_tick, 30.0);
    }

    /// `OnlyJoin` carries nothing past the base but a work amount, which uesave reads
    /// as the `SimpleWork` variant.
    #[test]
    fn adapts_an_only_join_work_as_simple_work() {
        let work = work_from(
            r#"{"id":"3c926ece-a73d-4f9e-ab5d-02145e823c70",
                "workable_bounds":{"location":{"x":0.0,"y":0.0,"z":22.5},
                    "rotation":{"x":0.0,"y":-0.0,"z":0.0,"w":1.0},
                    "box_sphere_bounds":{"origin":{"x":0.0,"y":0.0,"z":0.0},
                        "box_extent":{"x":100.0,"y":100.0,"z":22.5},
                        "sphere_radius":143.20003491619687}},
                "base_camp_id_belong_to":"16a18bfe-f989-4d61-ae64-061de5eb1b09",
                "owner_map_object_model_id":"42b314b6-8f77-407e-b0af-0bbdb3de26ab",
                "owner_map_object_concrete_model_id":"ac2b507e-4415-4303-ba35-df4c3e26c4d1",
                "current_state":1,"assign_locations":[],"behaviour_type":0,
                "assign_define_data_id":"CoolerPalFoodBox_0","override_work_type":0,
                "assignable_fixed_type":0,
                "assignable_otomo":true,"can_trigger_worker_event":true,
                "can_steal_assign":true,"required_work_amount":0.0,
                "transform":{"type":2,
                    "map_object_instance_id":"42b314b6-8f77-407e-b0af-0bbdb3de26ab",
                    "trailing_bytes":[0,0,0,0,0,0,0,0]}}"#,
        );
        let base = work.base_data.as_ref().expect("base data");
        assert_eq!(base.assign_define_data_id, "CoolerPalFoodBox_0");
        assert_eq!(work.transform.map(|t| t.transform_type), Some(2));
        assert_eq!(
            work.work_specific_data,
            PalWorkTypeSpecificData::SimpleWork { required_work_amount: 0.0 }
        );
    }

    /// `Progress_MultiType` is the one work type the dialect stops decoding early: it
    /// keeps the whole run past the progress fields raw in `multi_tail` and emits no
    /// sibling `transform`. Decoding that run must produce the same progress entries,
    /// suitability entries and transform uesave's own binary reader would, so a work's
    /// representation does not depend on which side it was read from.
    #[test]
    fn adapts_a_progress_multi_type_work_from_its_raw_tail() {
        let work = work_from(
            r#"{"id":"c4957227-428b-364b-0ffc-c4aaa60f091c",
                "workable_bounds":{"location":{"x":0.0,"y":0.0,"z":50.0},
                    "rotation":{"x":0.0,"y":-0.0,"z":0.0,"w":1.0},
                    "box_sphere_bounds":{"origin":{"x":0.0,"y":0.0,"z":0.0},
                        "box_extent":{"x":120.0,"y":180.0,"z":47.5},
                        "sphere_radius":221.48645556782924}},
                "base_camp_id_belong_to":"16a18bfe-f989-4d61-ae64-061de5eb1b09",
                "owner_map_object_model_id":"306ae54d-329e-48a5-ba28-a8a50e202237",
                "owner_map_object_concrete_model_id":"6c388cac-cb5a-40cb-b9a0-9928bf85c7c3",
                "current_state":1,
                "assign_locations":[
                    {"location":{"x":85.0,"y":204.23773820483794,"z":7.389644451905042e-13},
                     "facing_direction":{"x":1.0,"y":0.0,"z":0.0}},
                    {"location":{"x":85.0,"y":-195.20660119221301,"z":-1.7825982467911672e-07},
                     "facing_direction":{"x":1.0,"y":0.0,"z":0.0}}],
                "behaviour_type":0,
                "assign_define_data_id":"AncientBlastFurnace_0","override_work_type":0,
                "assignable_fixed_type":0,
                "assignable_otomo":true,"can_trigger_worker_event":true,
                "can_steal_assign":true,
                "required_work_amount":0.0,"current_work_amount":0.0,
                "work_exp":0,"work_exp_calc_type":0,
                "auto_work_self_amount_by_sec":0.0,
                "progress_time_since_last_tick":0.0,"tick_process_min_interval":0.0,
                "multi_tail":[2,0,0,0,1,0,0,0,0,0,0,72,66,0,0,0,0,
                              11,0,0,0,0,0,0,72,66,0,0,0,0,
                              0,0,0,0,
                              2,77,229,106,48,165,72,158,50,165,168,40,186,55,34,32,14,
                              0,0,0,0,0,0,0,0]}"#,
        );
        let base = work.base_data.as_ref().expect("base data");
        assert_eq!(base.assign_define_data_id, "AncientBlastFurnace_0");
        assert_eq!(base.assign_locations.len(), 2);

        let PalWorkTypeSpecificData::ProgressMultiType {
            progress_entries,
            suitability_info_entries,
            ..
        } = &work.work_specific_data
        else {
            panic!("expected ProgressMultiType");
        };
        assert_eq!(
            progress_entries.iter().map(|e| e.work_suitability).collect::<Vec<_>>(),
            vec![1, 11]
        );
        assert!(progress_entries.iter().all(|e| e.max_progress == 50.0));
        assert!(suitability_info_entries.is_empty());

        let transform = work.transform.as_ref().expect("the transform hides in multi_tail");
        assert_eq!(transform.transform_type, 2);
        assert_eq!(
            transform.map_object_instance_id,
            Some(FGuid::parse_str("306ae54d-329e-48a5-ba28-a8a50e202237").unwrap()),
            "the tail's transform points back at the work's own map object"
        );
        assert_eq!(transform.trailing_bytes, Some([0; 8]));
    }

    /// Same field set as `OnlyJoin`, and the sample that shows all three booleanized
    /// `u32`s are not always the same value.
    #[test]
    fn adapts_an_only_join_and_walk_around_work() {
        let work = work_from(
            r#"{"id":"a93df801-879e-4b34-874e-9d5b9cba667a",
                "workable_bounds":{
                    "location":{"x":16.384420795031673,"y":-3.410605131648481e-13,
                                "z":73.15610928924264},
                    "rotation":{"x":0.0,"y":-0.0,"z":0.0,"w":1.0},
                    "box_sphere_bounds":{"origin":{"x":0.0,"y":0.0,"z":0.0},
                        "box_extent":{"x":127.496191,"y":148.672348,"z":69.498043},
                        "sphere_radius":207.81896872078696}},
                "base_camp_id_belong_to":"16a18bfe-f989-4d61-ae64-061de5eb1b09",
                "owner_map_object_model_id":"ded6b8d7-6f39-4e61-a7bd-0cc9366da72d",
                "owner_map_object_concrete_model_id":"5b851235-786c-41da-9ffd-ca7e43bd934a",
                "current_state":1,"assign_locations":[],"behaviour_type":0,
                "assign_define_data_id":"Clinic_0","override_work_type":0,
                "assignable_fixed_type":2,
                "assignable_otomo":false,"can_trigger_worker_event":false,
                "can_steal_assign":true,"required_work_amount":0.0,
                "transform":{"type":2,
                    "map_object_instance_id":"ded6b8d7-6f39-4e61-a7bd-0cc9366da72d",
                    "trailing_bytes":[0,0,0,0,0,0,0,0]}}"#,
        );
        let base = work.base_data.as_ref().expect("base data");
        assert_eq!(base.assign_define_data_id, "Clinic_0");
        assert_eq!(base.assignable_fixed_type, 2);
        assert_eq!(base.assignable_otomo, 0);
        assert_eq!(base.can_trigger_worker_event, 0);
        assert_eq!(base.can_steal_assign, 1);
        assert_eq!(
            work.work_specific_data,
            PalWorkTypeSpecificData::SimpleWork { required_work_amount: 0.0 }
        );
    }

    /// `Defense` is the only corpus shape with its own leading and trailing bytes, and
    /// the only one with no work amount at all.
    #[test]
    fn adapts_a_defense_work() {
        let work = work_from(
            r#"{"id":"bdad384a-4bf8-7917-19b0-e2821cdee4a0",
                "workable_bounds":{"location":{"x":0.0,"y":0.0,"z":50.0},
                    "rotation":{"x":0.0,"y":-0.0,"z":0.0,"w":1.0},
                    "box_sphere_bounds":{"origin":{"x":0.0,"y":0.0,"z":0.0},
                        "box_extent":{"x":220.0,"y":220.0,"z":47.5},
                        "sphere_radius":314.7320288753593}},
                "base_camp_id_belong_to":"ed321d99-47a7-20d4-5205-bca743a4b131",
                "owner_map_object_model_id":"b2f7c483-4834-e727-6f7f-51a489eeb9ab",
                "owner_map_object_concrete_model_id":"89515eea-4878-362a-8641-a3b65d028cb7",
                "current_state":1,"assign_locations":[],"behaviour_type":0,
                "assign_define_data_id":"DefenseMachinegun_0","override_work_type":24,
                "assignable_fixed_type":2,
                "assignable_otomo":true,"can_trigger_worker_event":true,
                "can_steal_assign":true,
                "leading_bytes":[0,0,0,0],"defense_combat_type":2,
                "trailing_bytes":[0,0,0,0],
                "transform":{"type":2,
                    "map_object_instance_id":"b2f7c483-4834-e727-6f7f-51a489eeb9ab",
                    "trailing_bytes":[0,0,0,0,0,0,0,0]}}"#,
        );
        let base = work.base_data.as_ref().expect("base data");
        assert_eq!(base.override_work_type, 24);
        assert_eq!(work.transform.map(|t| t.transform_type), Some(2));
        assert_eq!(
            work.work_specific_data,
            PalWorkTypeSpecificData::Defense {
                leading_bytes: [0; 4],
                defense_combat_type: 2,
                trailing_bytes: [0; 4],
            },
            "the work's own trailing bytes are not the transform's"
        );
    }

    #[test]
    fn adapts_a_monster_farm_work() {
        let work = work_from(
            r#"{"id":"e05d9dfb-f721-4a53-b438-edb25f8f5c95",
                "workable_bounds":{"location":{"x":0.0,"y":0.0,"z":100.0},
                    "rotation":{"x":0.0,"y":0.0,"z":-0.7071067811865477,
                                "w":0.7071067811865475},
                    "box_sphere_bounds":{"origin":{"x":0.0,"y":0.0,"z":0.0},
                        "box_extent":{"x":550.0,"y":550.0,"z":100.0},
                        "sphere_radius":784.2193570679061}},
                "base_camp_id_belong_to":"16a18bfe-f989-4d61-ae64-061de5eb1b09",
                "owner_map_object_model_id":"597e59bd-c6e9-473f-afb8-99feae79811d",
                "owner_map_object_concrete_model_id":"71e76921-f89f-4a57-b417-b3380106af68",
                "current_state":1,"assign_locations":[],"behaviour_type":0,
                "assign_define_data_id":"MonsterFarm_0","override_work_type":0,
                "assignable_fixed_type":0,
                "assignable_otomo":false,"can_trigger_worker_event":false,
                "can_steal_assign":true,"required_work_amount":0.0,
                "transform":{"type":2,
                    "map_object_instance_id":"597e59bd-c6e9-473f-afb8-99feae79811d",
                    "trailing_bytes":[0,0,0,0,0,0,0,0]}}"#,
        );
        let base = work.base_data.as_ref().expect("base data");
        assert_eq!(base.assign_define_data_id, "MonsterFarm_0");
        assert_eq!(base.workable_bounds.rotation.w.0, 0.7071067811865475);
        assert_eq!(
            work.work_specific_data,
            PalWorkTypeSpecificData::SimpleWork { required_work_amount: 0.0 }
        );
    }

    #[test]
    fn adapts_a_booth_work() {
        let work = work_from(
            r#"{"id":"9429e7c2-4313-f584-f885-7588b3366182",
                "workable_bounds":{"location":{"x":-18.0,"y":0.0,"z":86.0},
                    "rotation":{"x":0.0,"y":-0.0,"z":0.7071067811865476,
                                "w":0.7071067811865477},
                    "box_sphere_bounds":{"origin":{"x":0.0,"y":0.0,"z":0.0},
                        "box_extent":{"x":183.718549,"y":211.989501,"z":59.043001},
                        "sphere_radius":286.6672805675255}},
                "base_camp_id_belong_to":"ed321d99-47a7-20d4-5205-bca743a4b131",
                "owner_map_object_model_id":"04c9ece1-46d3-19ca-0403-81a425565971",
                "owner_map_object_concrete_model_id":"8f312c4e-4278-be11-dad0-2da44c15d2bf",
                "current_state":1,
                "assign_locations":[
                    {"location":{"x":-52.0,"y":0.6591444713109914,"z":57.99999973404579},
                     "facing_direction":{"x":-1.0,"y":1.2246467991473532e-16,"z":0.0}}],
                "behaviour_type":0,
                "assign_define_data_id":"ItemBooth_0","override_work_type":0,
                "assignable_fixed_type":2,
                "assignable_otomo":false,"can_trigger_worker_event":true,
                "can_steal_assign":true,"required_work_amount":0.0,
                "transform":{"type":2,
                    "map_object_instance_id":"04c9ece1-46d3-19ca-0403-81a425565971",
                    "trailing_bytes":[0,0,0,0,0,0,0,0]}}"#,
        );
        let base = work.base_data.as_ref().expect("base data");
        assert_eq!(base.assign_define_data_id, "ItemBooth_0");
        assert_eq!(base.assign_locations.len(), 1);
        assert_eq!(base.assign_locations[0].location.x.0, -52.0);
        assert_eq!(base.can_trigger_worker_event, 1);
        assert_eq!(
            work.work_specific_data,
            PalWorkTypeSpecificData::SimpleWork { required_work_amount: 0.0 }
        );
    }

    /// A work base whose type adds nothing of its own decodes to the base's keys and
    /// nothing else, which is a payload rather than an unrecognized shape.
    #[test]
    fn a_work_with_no_type_specific_fields_carries_no_specific_data() {
        let work = work_from(
            r#"{"id":"e05d9dfb-f721-4a53-b438-edb25f8f5c95",
                "workable_bounds":{"location":{"x":0.0,"y":0.0,"z":100.0},
                    "rotation":{"x":0.0,"y":0.0,"z":0.0,"w":1.0},
                    "box_sphere_bounds":{"origin":{"x":0.0,"y":0.0,"z":0.0},
                        "box_extent":{"x":550.0,"y":550.0,"z":100.0},
                        "sphere_radius":784.2193570679061}},
                "base_camp_id_belong_to":"16a18bfe-f989-4d61-ae64-061de5eb1b09",
                "owner_map_object_model_id":"597e59bd-c6e9-473f-afb8-99feae79811d",
                "owner_map_object_concrete_model_id":"71e76921-f89f-4a57-b417-b3380106af68",
                "current_state":1,"assign_locations":[],"behaviour_type":0,
                "assign_define_data_id":"BootUp_0","override_work_type":0,
                "assignable_fixed_type":0,
                "assignable_otomo":false,"can_trigger_worker_event":false,
                "can_steal_assign":true,
                "transform":{"type":2,
                    "map_object_instance_id":"597e59bd-c6e9-473f-afb8-99feae79811d",
                    "trailing_bytes":[0,0,0,0,0,0,0,0]}}"#,
        );
        assert_eq!(work.work_specific_data, PalWorkTypeSpecificData::None);
    }

    /// A key no work shape accounts for must error naming it. `unknown_bytes` is the
    /// dialect's own catch-all for a payload longer than its decoder expected; `PalWork`
    /// has nowhere to keep it, so it cannot be waved through.
    #[test]
    fn an_unrecognized_work_field_errs() {
        let value: serde_json::Value = serde_json::from_str(
            r#"{"id":"b38e2281-f228-44bc-992f-d9268cd0799b",
                "workable_bounds":{"location":{"x":0.0,"y":0.0,"z":250.0},
                    "rotation":{"x":0.0,"y":-0.0,"z":0.0,"w":1.0},
                    "box_sphere_bounds":{"origin":{"x":0.0,"y":0.0,"z":0.0},
                        "box_extent":{"x":400.0,"y":400.0,"z":250.0},
                        "sphere_radius":618.4658438426491}},
                "base_camp_id_belong_to":"16a18bfe-f989-4d61-ae64-061de5eb1b09",
                "owner_map_object_model_id":"f4cb4274-5066-4681-8a72-01c5ae137a45",
                "owner_map_object_concrete_model_id":"8d46db52-67f6-4aba-9c0e-1df40a3a15d0",
                "current_state":1,"assign_locations":[],"behaviour_type":0,
                "assign_define_data_id":"OilPump_0","override_work_type":0,
                "assignable_fixed_type":0,
                "assignable_otomo":true,"can_trigger_worker_event":true,
                "can_steal_assign":true,
                "required_work_amount":0.0,"current_work_amount":2000.0,
                "work_exp":0,"work_exp_calc_type":0,
                "auto_work_self_amount_by_sec":0.0,
                "progress_time_since_last_tick":0.0,"tick_process_min_interval":0.0,
                "unknown_bytes":[1,2,3],
                "transform":{"type":2,
                    "map_object_instance_id":"f4cb4274-5066-4681-8a72-01c5ae137a45",
                    "trailing_bytes":[0,0,0,0,0,0,0,0]}}"#,
        )
        .unwrap();
        let error = Palworld::game_struct_from_gvas_json("worldSaveData.WorkSaveData.RawData", &value)
            .expect_err("an unaccounted key must error");
        let Error::GvasJson { path, message } = error else { panic!("expected a GvasJson error") };
        assert_eq!(path, "worldSaveData.WorkSaveData.RawData");
        assert!(message.contains("unknown_bytes"), "message was: {message}");
    }

    /// A `multi_tail` that is not exactly the run uesave reads after the progress
    /// fields is reported rather than silently truncated.
    #[test]
    fn a_multi_tail_that_does_not_account_for_its_bytes_errs() {
        let value: serde_json::Value = serde_json::from_str(
            r#"{"id":"c4957227-428b-364b-0ffc-c4aaa60f091c",
                "workable_bounds":{"location":{"x":0.0,"y":0.0,"z":50.0},
                    "rotation":{"x":0.0,"y":-0.0,"z":0.0,"w":1.0},
                    "box_sphere_bounds":{"origin":{"x":0.0,"y":0.0,"z":0.0},
                        "box_extent":{"x":120.0,"y":180.0,"z":47.5},
                        "sphere_radius":221.48645556782924}},
                "base_camp_id_belong_to":"16a18bfe-f989-4d61-ae64-061de5eb1b09",
                "owner_map_object_model_id":"306ae54d-329e-48a5-ba28-a8a50e202237",
                "owner_map_object_concrete_model_id":"6c388cac-cb5a-40cb-b9a0-9928bf85c7c3",
                "current_state":1,"assign_locations":[],"behaviour_type":0,
                "assign_define_data_id":"AncientBlastFurnace_0","override_work_type":0,
                "assignable_fixed_type":0,
                "assignable_otomo":true,"can_trigger_worker_event":true,
                "can_steal_assign":true,
                "required_work_amount":0.0,"current_work_amount":0.0,
                "work_exp":0,"work_exp_calc_type":0,
                "auto_work_self_amount_by_sec":0.0,
                "progress_time_since_last_tick":0.0,"tick_process_min_interval":0.0,
                "multi_tail":[0,0,0,0,0,0,0,0,0,7,7,7]}"#,
        )
        .unwrap();
        let error = Palworld::game_struct_from_gvas_json("worldSaveData.WorkSaveData.RawData", &value)
            .expect_err("leftover tail bytes must error");
        let Error::GvasJson { path, message } = error else { panic!("expected a GvasJson error") };
        assert_eq!(path, "worldSaveData.WorkSaveData.RawData");
        assert!(message.contains("multi_tail"), "message was: {message}");
    }

    const WORK_ASSIGN_PATH: &str = "worldSaveData.WorkSaveData.WorkAssignMap.RawData";

    fn work_assign_from(json: &str) -> games::palworld::PalWorkAssign {
        let adapted = game_struct(WORK_ASSIGN_PATH, json);
        let PalStruct::WorkAssign(assign) = adapted else { panic!("expected WorkAssign") };
        assign
    }

    /// The shape most `WorkAssignMap` entries in the corpus take: no `multi_type`
    /// tail, and `fixed` arrives as a JSON bool the way `PalWork`'s own booleanized
    /// fields do.
    #[test]
    fn adapts_a_work_assign_with_no_multi_type_tail() {
        let assign = work_assign_from(
            r#"{"id":"c161ee77-67e8-4d7a-b88c-2638451fc914",
                "location_index":0,
                "assign_type":1,
                "assigned_individual_id":{
                    "player_uid":"00000000-0000-0000-0000-000000000000",
                    "instance_id":"2c3b55ae-8f45-4d7f-9f4d-f9f42fc179fe"},
                "state":2,
                "fixed":false,
                "trailing_bytes":[0,0,0,0]}"#,
        );
        assert_eq!(assign.id, FGuid::parse_str("c161ee77-67e8-4d7a-b88c-2638451fc914").unwrap());
        assert_eq!(assign.location_index, 0);
        assert_eq!(assign.assign_type, 1);
        assert_eq!(assign.assigned_individual_id.guid, FGuid::default());
        assert_eq!(
            assign.assigned_individual_id.instance_id,
            FGuid::parse_str("2c3b55ae-8f45-4d7f-9f4d-f9f42fc179fe").unwrap()
        );
        assert_eq!(assign.state, 2);
        assert_eq!(assign.fixed, 0, "the dialect writes a bool where uesave keeps a u32");
        assert!(assign.multi_type.is_none());
    }

    /// Assignments to a `Progress_MultiType` work carry a `multi_type` tail; the
    /// dialect keeps the bytes uesave would read for it raw in `unknown_bytes` instead.
    #[test]
    fn adapts_a_work_assign_with_a_multi_type_tail_from_unknown_bytes() {
        let assign = work_assign_from(
            r#"{"id":"c161ee77-67e8-4d7a-b88c-2638451fc914",
                "location_index":0,
                "assign_type":1,
                "assigned_individual_id":{
                    "player_uid":"00000000-0000-0000-0000-000000000000",
                    "instance_id":"2c3b55ae-8f45-4d7f-9f4d-f9f42fc179fe"},
                "state":2,
                "fixed":false,
                "trailing_bytes":[0,0,0,0],
                "unknown_bytes":[1,14,35,0,0,0,0]}"#,
        );
        let multi_type = assign.multi_type.expect("multi_type decoded from unknown_bytes");
        assert_eq!(multi_type.assigned_work_suitability, 1);
        assert_eq!(multi_type.assigned_work_type, 14);
        assert_eq!(multi_type.assigned_work_action_type, 35);
        assert_eq!(multi_type.trailing_bytes, [0, 0, 0, 0]);
    }

    /// `unknown_bytes` with a byte left over after the tail is not the shape
    /// `multi_type` decodes to, and is reported rather than truncated.
    #[test]
    fn a_work_assign_multi_type_tail_with_leftover_bytes_errs() {
        let value: serde_json::Value = serde_json::from_str(
            r#"{"id":"c161ee77-67e8-4d7a-b88c-2638451fc914",
                "location_index":0,
                "assign_type":1,
                "assigned_individual_id":{
                    "player_uid":"00000000-0000-0000-0000-000000000000",
                    "instance_id":"2c3b55ae-8f45-4d7f-9f4d-f9f42fc179fe"},
                "state":2,
                "fixed":false,
                "trailing_bytes":[0,0,0,0],
                "unknown_bytes":[1,14,35,0,0,0,0,99]}"#,
        )
        .unwrap();
        let error = Palworld::game_struct_from_gvas_json(WORK_ASSIGN_PATH, &value)
            .expect_err("leftover unknown_bytes must error");
        let Error::GvasJson { path, message } = error else { panic!("expected a GvasJson error") };
        assert_eq!(path, WORK_ASSIGN_PATH);
        assert!(message.contains("unknown_bytes"), "message was: {message}");
    }

    /// A key neither shape accounts for must error naming it.
    #[test]
    fn an_unrecognized_work_assign_field_errs() {
        let value: serde_json::Value = serde_json::from_str(
            r#"{"id":"c161ee77-67e8-4d7a-b88c-2638451fc914",
                "location_index":0,
                "assign_type":1,
                "assigned_individual_id":{
                    "player_uid":"00000000-0000-0000-0000-000000000000",
                    "instance_id":"2c3b55ae-8f45-4d7f-9f4d-f9f42fc179fe"},
                "state":2,
                "fixed":false,
                "trailing_bytes":[0,0,0,0],
                "extra_field":5}"#,
        )
        .unwrap();
        let error = Palworld::game_struct_from_gvas_json(WORK_ASSIGN_PATH, &value)
            .expect_err("an unaccounted key must error");
        let Error::GvasJson { path, message } = error else { panic!("expected a GvasJson error") };
        assert_eq!(path, WORK_ASSIGN_PATH);
        assert!(message.contains("extra_field"), "message was: {message}");
    }
}
