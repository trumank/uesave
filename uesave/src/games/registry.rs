//! Runtime registry of games, and a type-erased facade over per-game
//! operations so tools can select a game by name without naming its type.

use std::io::{Read, Write};
use std::marker::PhantomData;

use crate::game::{Game, NoGame};
use crate::{Error, Result, Save, SaveReader, Types};

/// Per-game metadata used by tooling. Implemented alongside [`Game`].
pub trait GameInfo: Game {
    /// Selector name, e.g. `"palworld"`.
    const NAME: &'static str;

    /// Default type hints needed to parse this game's saves.
    fn default_types() -> Types {
        Types::new()
    }

    /// Named container formats this game can write, beyond the default
    /// (uncompressed GVAS).
    fn formats() -> &'static [&'static str] {
        &[]
    }

    /// Write `save` in the named container format; `None` writes plain
    /// uncompressed GVAS. Only called with `None` or a name from [`Self::formats`].
    fn write_format(save: &Save<Self>, name: Option<&str>, w: &mut dyn Write) -> Result<()>
    where
        Self: Sized,
    {
        match name {
            None => {
                let mut w = w;
                save.write(&mut w)
            }
            Some(other) => Err(Error::Other(format!(
                "unknown format {other:?} for game {}",
                Self::NAME
            ))),
        }
    }
}

/// Type-erased per-game operations. Obtained from [`registry`] or [`get`].
pub trait GameCli {
    fn name(&self) -> &'static str;
    fn default_types(&self) -> Types;
    fn formats(&self) -> &'static [&'static str];

    /// Read a save from `input` and write pretty JSON to `out`.
    fn to_json(
        &self,
        input: &mut dyn Read,
        out: &mut dyn Write,
        types: Types,
        log: bool,
    ) -> Result<()>;

    /// Read JSON from `input` and write a save to `out` in `format`
    /// (`None` = uncompressed GVAS).
    #[expect(clippy::wrong_self_convention, reason = "pairs with to_json")]
    fn from_json(
        &self,
        input: &mut dyn Read,
        out: &mut dyn Write,
        format: Option<&str>,
    ) -> Result<()>;

    /// Resave and JSON-round-trip `input`, requiring both to match `original`
    /// byte for byte. `debug` receives named intermediate buffers.
    fn test_resave(
        &self,
        input: &mut dyn Read,
        original: &[u8],
        types: Types,
        log: bool,
        debug: &mut dyn FnMut(&str, &[u8]) -> Result<()>,
    ) -> Result<()>;
}

/// Carries a game type as a [`GameCli`] value.
pub struct Handle<G>(PhantomData<G>);

/// Build a [`GameCli`] for any game, including games defined outside this crate.
pub fn handle<G: GameInfo>() -> Box<dyn GameCli> {
    Box::new(Handle::<G>(PhantomData))
}

impl<G: GameInfo> GameCli for Handle<G> {
    fn name(&self) -> &'static str {
        G::NAME
    }

    fn default_types(&self) -> Types {
        G::default_types()
    }

    fn formats(&self) -> &'static [&'static str] {
        G::formats()
    }

    fn to_json(
        &self,
        input: &mut dyn Read,
        out: &mut dyn Write,
        types: Types,
        log: bool,
    ) -> Result<()> {
        let save: Save<G> = SaveReader::new()
            .game::<G>()
            .log(log)
            .error_to_raw(true)
            .types(types)
            .read(input)
            .map_err(|e| Error::Other(e.to_string()))?;
        serde_json::to_writer_pretty(out, &save).map_err(|e| Error::Other(e.to_string()))?;
        Ok(())
    }

    fn from_json(
        &self,
        input: &mut dyn Read,
        out: &mut dyn Write,
        format: Option<&str>,
    ) -> Result<()> {
        let save: Save<G> =
            serde_json::from_reader(input).map_err(|e| Error::Other(e.to_string()))?;
        G::write_format(&save, format, out)
    }

    fn test_resave(
        &self,
        input: &mut dyn Read,
        original: &[u8],
        types: Types,
        log: bool,
        debug: &mut dyn FnMut(&str, &[u8]) -> Result<()>,
    ) -> Result<()> {
        let save: Save<G> = SaveReader::new()
            .game::<G>()
            .log(log)
            .error_to_raw(true)
            .types(types)
            .read(input)
            .map_err(|e| Error::Other(e.to_string()))?;

        let mut output = vec![];
        save.write(&mut output)?;
        debug("output.sav", &output)?;
        if original != output.as_slice() {
            return Err(Error::Other("Resave did not match".into()));
        }

        let input_json =
            serde_json::to_vec_pretty(&save).map_err(|e| Error::Other(e.to_string()))?;
        debug("input.json", &input_json)?;

        let save_from_json: Save<G> =
            serde_json::from_slice(&input_json).map_err(|e| Error::Other(e.to_string()))?;
        let output_json =
            serde_json::to_vec_pretty(&save_from_json).map_err(|e| Error::Other(e.to_string()))?;
        debug("output.json", &output_json)?;

        let mut output = vec![];
        save_from_json.write(&mut output)?;
        debug("output.sav", &output)?;
        if original != output.as_slice() {
            return Err(Error::Other("JSON round trip did not match".into()));
        }
        Ok(())
    }
}

impl GameInfo for NoGame {
    const NAME: &'static str = "none";
}

/// Games built into this crate. Callers may extend the returned vector with
/// [`handle`] to plug in games defined elsewhere.
pub fn registry() -> Vec<Box<dyn GameCli>> {
    vec![
        handle::<NoGame>(),
        handle::<crate::games::palworld::Palworld>(),
    ]
}

/// Look up a built-in game by [`GameInfo::NAME`].
pub fn get(name: &str) -> Option<Box<dyn GameCli>> {
    registry().into_iter().find(|h| h.name() == name)
}
