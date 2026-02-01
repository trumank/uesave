use std::io::Cursor;

use uesave::{Save, SaveReader};
use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
pub fn sav_to_json(data: &[u8]) -> Result<String, JsValue> {
    let save = SaveReader::new()
        .error_to_raw(true)
        .read(Cursor::new(data))
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;

    serde_json::to_string_pretty(&save).map_err(|e| JsValue::from_str(&format!("{e}")))
}

#[wasm_bindgen]
pub fn json_to_sav(json: &str) -> Result<Vec<u8>, JsValue> {
    let save: Save = serde_json::from_str(json).map_err(|e| JsValue::from_str(&format!("{e}")))?;

    let mut buffer = Vec::new();
    save.write(&mut buffer)
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;

    Ok(buffer)
}
