use std::fs::{self, File, OpenOptions};
use std::io::{stdin, stdout, BufRead, BufReader, BufWriter, Cursor, Write};

use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};

use uesave::games::{registry, GameCli};
use uesave::{StructType, Types};

#[derive(Parser, Debug)]
struct ActionToJson {
    #[arg(short, long, default_value = "-")]
    input: String,

    #[arg(short, long, default_value = "-")]
    output: String,

    /// Silence any parse warnings
    #[arg(long)]
    no_warn: bool,

    /// Save files do not contain enough context to parse structs inside MapProperty or SetProperty.
    /// uesave will attempt to guess, but if it is incorrect the save will fail to parse and the
    /// type must be manually specified.
    ///
    /// Examples:
    ///   -t .UnlockedItemSkins.Skins=Guid
    ///   -t .EnemiesKilled.Key=Guid
    ///   -t .EnemiesKilled.Value=Struct
    #[arg(short, long, value_parser = parse_type)]
    r#type: Vec<(String, StructType)>,

    /// Game whose save format to use (an invalid name lists the available games)
    #[arg(long, default_value = "none")]
    game: String,
}

#[derive(Parser, Debug)]
struct ActionFromJson {
    #[arg(short, long, default_value = "-")]
    input: String,

    #[arg(short, long, default_value = "-")]
    output: String,

    /// Game whose save format to use (an invalid name lists the available games)
    #[arg(long, default_value = "none")]
    game: String,

    /// Output container format for this game (default: uncompressed)
    #[arg(long)]
    format: Option<String>,
}

#[derive(Parser, Debug)]
struct ActionEdit {
    #[arg(required = true, index = 1)]
    path: String,

    /// Silence any parse warnings
    #[arg(long)]
    no_warn: bool,

    /// Save files do not contain enough context to parse structs inside MapProperty or SetProperty.
    /// uesave will attempt to guess, but if it is incorrect the save will fail to parse and the
    /// type must be manually specified.
    ///
    /// Examples:
    ///   -t .UnlockedItemSkins.Skins=Guid
    ///   -t .EnemiesKilled.Key=Guid
    ///   -t .EnemiesKilled.Value=Struct
    #[arg(short, long, value_parser = parse_type)]
    r#type: Vec<(String, StructType)>,

    /// Game whose save format to use (an invalid name lists the available games)
    #[arg(long, default_value = "none")]
    game: String,

    /// Output container format for this game (default: uncompressed)
    #[arg(long)]
    format: Option<String>,
}

#[derive(Parser, Debug)]
struct ActionTestResave {
    #[arg(required = true, index = 1)]
    path: String,

    /// If resave fails, write input.sav and output.sav to working directory for debugging
    #[arg(short, long)]
    debug: bool,

    /// Silence any parse warnings
    #[arg(long)]
    no_warn: bool,

    /// Trace and generate trace.json file
    #[cfg(feature = "tracing")]
    #[arg(long)]
    trace: bool,

    /// Save files do not contain enough context to parse structs inside MapProperty or SetProperty.
    /// uesave will attempt to guess, but if it is incorrect the save will fail to parse and the
    /// type must be manually specified.
    ///
    /// Examples:
    ///   -t .UnlockedItemSkins.Skins=Guid
    ///   -t .EnemiesKilled.Key=Guid
    ///   -t .EnemiesKilled.Value=Struct
    #[arg(short, long, value_parser = parse_type)]
    r#type: Vec<(String, StructType)>,

    /// Game whose save format to use (an invalid name lists the available games)
    #[arg(long, default_value = "none")]
    game: String,
}

#[derive(Subcommand, Debug)]
enum Action {
    /// Convert binary save to plain text JSON
    ToJson(ActionToJson),
    /// Convert JSON back to binary save
    FromJson(ActionFromJson),
    /// Launch editor to edit a save file as JSON in place
    Edit(ActionEdit),
    /// Test resave
    TestResave(ActionTestResave),
}

#[derive(Parser, Debug)]
#[command(author, version)]
struct Args {
    #[command(subcommand)]
    action: Action,
}

fn parse_type(t: &str) -> Result<(String, StructType)> {
    if let Some((l, r)) = t.rsplit_once('=') {
        Ok((l.to_owned(), r.into()))
    } else {
        Err(anyhow!("Malformed type"))
    }
}

fn pick<'a>(reg: &'a [Box<dyn GameCli>], name: &str) -> Result<&'a dyn GameCli> {
    reg.iter()
        .map(|h| h.as_ref())
        .find(|h| h.name() == name)
        .ok_or_else(|| {
            let names: Vec<_> = reg.iter().map(|h| h.name()).collect();
            anyhow!("unknown game {name:?}; available: {}", names.join(", "))
        })
}

fn check_format(handler: &dyn GameCli, format: Option<&str>) -> Result<()> {
    let Some(f) = format else { return Ok(()) };
    if handler.formats().contains(&f) {
        return Ok(());
    }
    let available = if handler.formats().is_empty() {
        "(none)".to_string()
    } else {
        handler.formats().join(", ")
    };
    Err(anyhow!(
        "unknown format {f:?} for game {}; available: {available}",
        handler.name()
    ))
}

/// Game defaults plus any user `-t path=Type` overrides.
fn merge_types(mut types: Types, overrides: Vec<(String, StructType)>) -> Types {
    for (path, t) in overrides {
        types.add(path, t);
    }
    types
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    let reg = registry();
    match args.action {
        Action::ToJson(mut action) => {
            let handler = pick(&reg, &action.game)?;
            let types = merge_types(handler.default_types(), std::mem::take(&mut action.r#type));
            let mut buf = vec![];
            handler.to_json(&mut input(&action.input)?, &mut buf, types, !action.no_warn)?;
            output(&action.output)?.write_all(&buf)?;
        }
        Action::FromJson(action) => {
            let handler = pick(&reg, &action.game)?;
            check_format(handler, action.format.as_deref())?;
            let mut buf = vec![];
            handler.from_json(
                &mut input(&action.input)?,
                &mut buf,
                action.format.as_deref(),
            )?;
            output(&action.output)?.write_all(&buf)?;
        }
        Action::Edit(mut action) => {
            let handler = pick(&reg, &action.game)?;
            check_format(handler, action.format.as_deref())?;
            let types = merge_types(handler.default_types(), std::mem::take(&mut action.r#type));

            let bytes = fs::read(&action.path)?;
            let mut json = vec![];
            handler.to_json(&mut Cursor::new(&bytes), &mut json, types, !action.no_warn)?;

            let edited = edit::edit_bytes_with_builder(
                json.clone(),
                tempfile::Builder::new().suffix(".json"),
            )?;

            if edited == json {
                println!("File unchanged, doing nothing.");
            } else {
                println!("File modified, writing new save.");
                let mut buf = vec![];
                handler.from_json(
                    &mut Cursor::new(&edited),
                    &mut buf,
                    action.format.as_deref(),
                )?;
                let mut writer = open_for_write(&action.path)?;
                writer.write_all(&buf)?;
            }
        }
        Action::TestResave(mut action) => {
            let handler = pick(&reg, &action.game)?;
            let mut types =
                merge_types(handler.default_types(), std::mem::take(&mut action.r#type));

            let path = std::path::Path::new(&action.path);
            if let Ok(types_file) = fs::read_to_string(path.with_extension("types")) {
                for t in types_file.lines() {
                    if let Ok((p, ty)) = parse_type(t) {
                        types.add(p, ty);
                    }
                }
            }

            let bytes = fs::read(path)?;
            let want_debug = action.debug;
            if want_debug {
                fs::write("input.sav", &bytes)?;
            }
            let mut debug = |name: &str, data: &[u8]| -> std::result::Result<(), uesave::Error> {
                if want_debug {
                    fs::write(name, data)?;
                }
                Ok(())
            };

            let log = !action.no_warn;
            #[cfg(feature = "tracing")]
            {
                if action.trace {
                    ser_hex::read("trace.json", &mut Cursor::new(&bytes), |r| {
                        handler.test_resave(r, &bytes, types, log, &mut debug)
                    })?;
                } else {
                    handler.test_resave(
                        &mut Cursor::new(&bytes),
                        &bytes,
                        types,
                        log,
                        &mut debug,
                    )?;
                }
            }
            #[cfg(not(feature = "tracing"))]
            handler.test_resave(&mut Cursor::new(&bytes), &bytes, types, log, &mut debug)?;

            println!("Resave successful");
        }
    }
    Ok(())
}

fn open_for_write(path: &str) -> Result<BufWriter<File>> {
    Ok(BufWriter::new(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?,
    ))
}

fn input<'a>(path: &str) -> Result<Box<dyn BufRead + 'a>> {
    Ok(match path {
        "-" => Box::new(BufReader::new(stdin().lock())),
        p => Box::new(BufReader::new(File::open(p)?)),
    })
}

fn output<'a>(path: &str) -> Result<Box<dyn Write + 'a>> {
    Ok(match path {
        "-" => Box::new(BufWriter::new(stdout().lock())),
        p => Box::new(BufWriter::new(
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(p)?,
        )),
    })
}
