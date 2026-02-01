import init, { sav_to_json, json_to_sav } from "../wasm/uesave_wasm";

let initialized = false;

async function ensureInit(): Promise<void> {
  if (!initialized) {
    await init();
    initialized = true;
  }
}

export async function savToJson(data: Uint8Array): Promise<string> {
  await ensureInit();
  return sav_to_json(data);
}

export async function jsonToSav(json: string): Promise<Uint8Array> {
  await ensureInit();
  return json_to_sav(json);
}
