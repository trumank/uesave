//! Adapters from the GVAS JSON dialect into Palworld's typed structs.
//!
//! # The dialect is not the wire format
//!
//! The codecs in this directory implement the binary wire format. The adapters in
//! THIS file must accept what the JSON dialect actually carries, and the two have
//! diverged. Every known divergence is listed below; add to it rather than working
//! around a case locally.
//!
//! | Class | Case | Handling |
//! |---|---|---|
//! | Booleanized u32 | `auto_picked_up` on the pickup/drop/egg model variants: the dialect writes a JSON bool where the wire format keeps a `u32` | [`bool_or_u32`] |
//! | Booleanized u32 | `assignable_otomo`, `can_trigger_worker_event`, `can_steal_assign` in `PalWork` | [`bool_or_u32`] |
//! | Booleanized u32 | `fixed` in `PalWorkAssign` | [`bool_or_u32`] |
//! | Typo'd name | `initital_transform_cache` (the dialect) vs `initial_transform_cache` | serde rename |
//! | Typo'd name | `pickupdable_player_uid` (the dialect) on `DropItem`; uesave's struct spells it `pickupable_player_uid` | read the dialect's key, assign the correctly-spelled field |
//! | Typo'd name, no-op | `pickupdable_player_uid` on `PalEgg`; uesave's own struct carries the *same* typo here, so the dialect's key already matches | plain field read, no rename |
//! | Version parity | uesave reads an optional `PalPickupGuard` the dialect never writes | map to `None` |
//! | Sibling absorption | uesave folds a concrete model's `ModuleMap.CustomVersionData` into the struct; the dialect keeps it a separate property. Base camp modules carry the identical shape but their struct has nowhere to put it, so the lift is gated on the property path, not the shape | lift then drop the sibling, only under `ConcreteModel.ModuleMap` |
//! | Nested field | the dialect nests a dynamic item's `static_id` inside its `id` object, alongside the two world GUIDs; uesave's `PalDynamicItem` keeps it a sibling field | read `id.static_id`, assign to the sibling field |
//! | Shifted field boundary | the dialect's `DynamicItemSaveData` weapon shape omits the post-skill-list FString that uesave's own codec reads, so its flat `trailing_bytes` is really that FString's length prefix and the true trailing bytes hide inside `unknown_bytes` | decode an FString from `trailing_bytes ++ unknown_bytes`; on failure fall back to `Unknown` |
//! | Renamed discriminator | the dialect tags a work's `transform` sub-union `type`; uesave's `PalWorkTransform` spells that field `transform_type` | read the tag, then fill the variant's fields |
//! | Renamed field | the dialect spells `PalInstanceId`'s player guid `player_uid`; uesave's struct spells the same field `guid` | read `player_uid`, assign to `guid` |
//! | Shape mismatch | the dialect emits `WorkCollection.RawData.trailing_bytes` as a 4-element byte array; uesave's `PalWorkCollection.trailing_bytes` is the little-endian `u32` those 4 bytes decode to, matching how `read`/`write` treat it on the wire | decode the array as little-endian `u32` |
//! | Truncated decode | uesave reads a `Progress_MultiType` work's `progress_entries` and `suitability_info_entries`, then its transform; the dialect stops right after the progress fields and keeps the whole run past them raw in `multi_tail`, emitting no sibling `transform` for this one work type | decode `multi_tail` with uesave's own readers and lift the transform out of it |
//! | Truncated decode | for a `Progress_MultiType` work's assignment, the dialect never decodes `UPalWorkAssign_WorkProgressMultiType`, so those unconsumed bytes land in `unknown_bytes` instead | decode `unknown_bytes` into `multi_type` with uesave's own readers; error on leftover bytes |
//! | Tolerated trailing data | uesave's `armor`/`weapon` codecs consume every byte and reject a remainder; the dialect instead appends whatever is left as `unknown_bytes` | `armor` has no field to hold it, so its presence always falls back to `Unknown` with the bytes reconstructed in original order |
//! | Renamed field | the dialect spells `PalMapObjectGenerateEnergyModel`'s consumption field `consume_energy_speed`; uesave's struct spells the same field `consume_energy_amount` | read `consume_energy_speed`, assign to `consume_energy_amount` |
//! | Booleanized u32 | `PalMapModel.stage_instance_id_belong_to.valid` (`PalStageInstanceId`, nested one level below the struct `from_value` reaches) | [`bool_or_u32`] |
//! | Split field | a concrete model catch-all's raw tail sometimes arrives as two dialect keys, `trailing_bytes` then `trailing_unknown_bytes`, where uesave's binary reader takes one continuous `read_to_end` | concatenate `trailing_bytes ++ trailing_unknown_bytes` into the catch-all's `trailing_bytes` |
//! | Renamed field | a concrete model type uesave has no dedicated struct for (so its data is generic `BaseModel` bytes) sometimes carries its raw tail as `unknown_bytes` instead of `trailing_bytes` | treat `unknown_bytes` as the catch-all's `trailing_bytes` when `trailing_bytes` is absent |
//! | Truncated decode | `PalMapObjectBreedFarmModel`'s optional trailing `breeding` block has no field of its own in the dialect; when present, its bytes arrive as the catch-all-shaped `trailing_unknown_bytes` | decode `trailing_unknown_bytes` into `breeding` with uesave's own readers; error on leftover bytes |

use super::types::{PalDynamicId, PalInstanceId, PalTransform};
use super::{
    BaseModel, PalBaseCamp, PalBaseCampModule, PalBaseCampModuleData, PalBreedFarmBreeding,
    PalCharacterData, PalDynamicItem, PalDynamicItemType, PalMapConcreteModel,
    PalMapConcreteModelModule, PalMapConcreteModelModuleData, PalMapConcreteModelVariant,
    PalMapModel, PalMapObjectBreedFarmModel, PalMapObjectDropItemModel,
    PalMapObjectGenerateEnergyModel, PalMapObjectHatchingEggModel, PalMapObjectPalEggModel,
    PalMapObjectPickupItemOnLevelModel, PalStruct, PalWork, PalWorkAssign, PalWorkAssignMultiType,
    PalWorkBase, PalWorkCollection, PalWorkProgressEntry, PalWorkSuitabilityInfo, PalWorkTransform,
    PalWorkTypeSpecificData, PalWorkerDirector, Palworld,
};
use crate::compat::gvas_json::{err, join, properties_from_json};
use crate::{read_array, Quat, Result, SaveGameArchive, SaveGameArchiveType, Vector};
use byteorder::{ReadBytesExt, LE};
use serde_json::{Map, Value};
use std::io::{Cursor, Seek};

/// A `u32` field the dialect may have written as a JSON bool.
pub fn bool_or_u32(value: &Value, path: &str) -> Result<u32> {
    match value {
        Value::Bool(true) => Ok(1),
        Value::Bool(false) => Ok(0),
        Value::Number(n) => n
            .as_u64()
            .map(|v| v as u32)
            .ok_or_else(|| err(path, "expected a u32")),
        _ => Err(err(path, "expected a bool or u32")),
    }
}

fn f64_at(value: &Value, key: &str, path: &str) -> Result<f64> {
    value
        .get(key)
        .and_then(Value::as_f64)
        .ok_or_else(|| err(path, format!("expected a number at {key:?}")))
}

fn vector(value: &Value, path: &str) -> Result<Vector> {
    Ok(Vector {
        x: crate::Double(f64_at(value, "x", path)?),
        y: crate::Double(f64_at(value, "y", path)?),
        z: crate::Double(f64_at(value, "z", path)?),
    })
}

fn quat(value: &Value, path: &str) -> Result<Quat> {
    Ok(Quat {
        x: crate::Double(f64_at(value, "x", path)?),
        y: crate::Double(f64_at(value, "y", path)?),
        z: crate::Double(f64_at(value, "z", path)?),
        w: crate::Double(f64_at(value, "w", path)?),
    })
}

/// The dialect spells the scale component `scale3d`; uesave calls it `scale`.
pub fn transform(value: &Value, path: &str) -> Result<PalTransform> {
    let at = |key: &str| value.get(key).ok_or_else(|| err(path, format!("transform has no {key:?}")));
    Ok(PalTransform {
        rotation: quat(at("rotation")?, path)?,
        translation: vector(at("translation")?, path)?,
        scale: vector(at("scale3d")?, path)?,
    })
}

/// Deserializes `value` onto `T`, tagging any failure with the property path.
fn from_value<T: serde::de::DeserializeOwned>(value: &Value, path: &str) -> Result<T> {
    serde_json::from_value(value.clone()).map_err(|e| err(path, e.to_string()))
}

/// The dialect spells this model's consumption field `consume_energy_speed`; uesave's
/// struct spells the same field `consume_energy_amount`.
fn generate_energy(value: &Value, path: &str) -> Result<PalMapObjectGenerateEnergyModel> {
    Ok(PalMapObjectGenerateEnergyModel {
        generate_energy_rate_by_worker: f32_at(value, "generate_energy_rate_by_worker", path)?,
        stored_energy_amount: f32_at(value, "stored_energy_amount", path)?,
        consume_energy_amount: f32_at(value, "consume_energy_speed", path)?,
    })
}

/// A base camp, whose two transforms need the `scale3d` rename.
fn base_camp(value: &Value, path: &str) -> Result<PalBaseCamp> {
    let mut camp: PalBaseCamp = {
        // The transforms are replaced below; feed the deserializer placeholders so
        // its field set matches, then overwrite from the dialect's own spelling.
        let mut stripped = value.clone();
        for key in ["transform", "fast_travel_local_transform"] {
            stripped[key] = serde_json::json!({
                "rotation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
                "translation": {"x": 0.0, "y": 0.0, "z": 0.0},
                "scale": {"x": 1.0, "y": 1.0, "z": 1.0}
            });
        }
        from_value(&stripped, path)?
    };
    camp.transform = transform(
        value.get("transform").ok_or_else(|| err(path, "base camp has no transform"))?,
        path,
    )?;
    camp.fast_travel_local_transform = transform(
        value
            .get("fast_travel_local_transform")
            .ok_or_else(|| err(path, "base camp has no fast_travel_local_transform"))?,
        path,
    )?;
    Ok(camp)
}

/// The dialect spells this `initital_transform_cache` (sic) and `scale3d`. The nested
/// `stage_instance_id_belong_to.valid` is also a booleanized u32; see [`bool_or_u32`].
fn map_model(value: &Value, path: &str) -> Result<PalMapModel> {
    let cache = value
        .get("initital_transform_cache")
        .ok_or_else(|| err(path, "map model has no initital_transform_cache"))?;
    let cache = transform(cache, path)?;

    let stage_valid = bool_or_u32(
        value
            .get("stage_instance_id_belong_to")
            .and_then(|stage| stage.get("valid"))
            .ok_or_else(|| err(path, "map model has no stage_instance_id_belong_to.valid"))?,
        path,
    )?;

    let mut stripped = value.clone();
    stripped.as_object_mut().ok_or_else(|| err(path, "expected an object"))?
        .remove("initital_transform_cache");
    stripped["initial_transform_cache"] = serde_json::json!({
        "rotation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
        "translation": {"x": 0.0, "y": 0.0, "z": 0.0},
        "scale": {"x": 1.0, "y": 1.0, "z": 1.0}
    });
    if let Some(stage) = stripped.get_mut("stage_instance_id_belong_to") {
        stage["valid"] = serde_json::json!(0);
    }

    let mut model: PalMapModel = from_value(&stripped, path)?;
    model.initial_transform_cache = cache;
    model.stage_instance_id_belong_to.valid = stage_valid;
    Ok(model)
}

fn worker_director(value: &Value, path: &str) -> Result<PalWorkerDirector> {
    let spawn = transform(
        value.get("spawn_transform").ok_or_else(|| err(path, "no spawn_transform"))?,
        path,
    )?;
    let mut stripped = value.clone();
    stripped["spawn_transform"] = serde_json::json!({
        "rotation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
        "translation": {"x": 0.0, "y": 0.0, "z": 0.0},
        "scale": {"x": 1.0, "y": 1.0, "z": 1.0}
    });
    let mut director: PalWorkerDirector = from_value(&stripped, path)?;
    director.spawn_transform = spawn;
    Ok(director)
}

/// The dialect calls this field `id`; uesave calls it `own_id`.
fn work_collection(value: &Value, path: &str) -> Result<PalWorkCollection> {
    Ok(PalWorkCollection {
        own_id: guid_at(value, "id", path)?,
        work_ids: from_value(
            value.get("work_ids").ok_or_else(|| err(path, "no work_ids"))?,
            path,
        )?,
        trailing_bytes: u32::from_le_bytes(bytes_at(value, "trailing_bytes", path)?),
    })
}

/// The module's variant is unambiguous from the field set the dialect emitted.
fn base_camp_module_data(value: &Value, path: &str) -> Result<PalBaseCampModuleData> {
    Ok(if let Some(infos) = value.get("transport_item_character_infos") {
        PalBaseCampModuleData::TransportItemDirector {
            character_infos: from_value(infos, path)?,
            trailing_bytes: trailing4(value, path)?,
        }
    } else if let Some(effects) = value.get("passive_effects") {
        PalBaseCampModuleData::PassiveEffect { passive_effects: from_value(effects, path)? }
    } else {
        let object = value.as_object().ok_or_else(|| err(path, "expected an object"))?;
        if object.is_empty() {
            // The dialect emits an empty dict for module types with no payload.
            PalBaseCampModuleData::NoOp
        } else {
            let keys = object.keys().cloned().collect::<Vec<_>>().join(", ");
            return Err(err(path, format!("unrecognized base camp module fields: {keys}")));
        }
    })
}

fn guid_at(value: &Value, key: &str, path: &str) -> Result<crate::FGuid> {
    let raw = value.get(key).and_then(Value::as_str).ok_or_else(|| err(path, format!("no {key:?}")))?;
    crate::FGuid::parse_str(raw).map_err(|e| err(path, format!("invalid guid at {key:?}: {e}")))
}

/// A fixed-width byte field. The dialect's array must be exactly `N` bytes long;
/// a shorter or longer array is malformed input, so it errors rather than
/// silently zero-padding or truncating.
fn bytes_at<const N: usize>(value: &Value, key: &str, path: &str) -> Result<[u8; N]> {
    let bytes = value
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| err(path, format!("no {key:?}")))?;
    if bytes.len() != N {
        return Err(err(path, format!("{key:?} has {} bytes, expected {N}", bytes.len())));
    }
    let mut out = [0u8; N];
    for (slot, byte) in out.iter_mut().zip(bytes) {
        *slot = byte
            .as_u64()
            .filter(|b| *b <= u8::MAX as u64)
            .ok_or_else(|| err(path, format!("{key:?} holds a non-byte")))? as u8;
    }
    Ok(out)
}

fn trailing4(value: &Value, path: &str) -> Result<[u8; 4]> {
    bytes_at(value, "trailing_bytes", path)
}

fn byte_vec_at(value: &Value, key: &str) -> Vec<u8> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(|bytes| bytes.iter().map(|b| b.as_u64().unwrap_or(0) as u8).collect())
        .unwrap_or_default()
}

/// The keys `BaseModel` accounts for: everything the concrete model catch-all itself
/// reads, plus its one field, `trailing_bytes` (see [`base_model_trailing_bytes`] for
/// the two other spellings this can arrive under). A field-structured dialect payload
/// for an unwired type carries more than this, and must not be waved through as a
/// raw-bytes model.
const CONCRETE_MODEL_BASE_KEYS: [&str; 6] = [
    "instance_id",
    "model_instance_id",
    "concrete_model_type",
    "trailing_bytes",
    "trailing_unknown_bytes",
    "unknown_bytes",
];

/// `BaseModel`'s raw tail, reconstructed however the dialect split it. uesave's binary
/// reader takes this in one continuous `read_to_end`, but the dialect sometimes divides
/// that single run across two keys: `trailing_bytes` followed by `trailing_unknown_bytes`
/// for a type that still carries the flat `trailing_bytes` key, or `unknown_bytes` alone
/// for a type with no dedicated struct on our side at all (so there is no `trailing_bytes`
/// key to split in the first place). Both are concatenated in wire order, reproducing the
/// bytes the binary reader would have produced. See the divergence table above.
fn base_model_trailing_bytes(value: &Value, path: &str) -> Result<Vec<u8>> {
    let trailing = value.get("trailing_bytes");
    let trailing_unknown = value.get("trailing_unknown_bytes");
    let unknown = value.get("unknown_bytes");

    match (trailing, trailing_unknown, unknown) {
        (Some(_), None, None) => Ok(byte_vec_at(value, "trailing_bytes")),
        (Some(_), Some(_), None) => {
            let mut bytes = byte_vec_at(value, "trailing_bytes");
            bytes.extend(byte_vec_at(value, "trailing_unknown_bytes"));
            Ok(bytes)
        }
        (None, None, Some(_)) => Ok(byte_vec_at(value, "unknown_bytes")),
        (None, None, None) => Ok(Vec::new()),
        _ => Err(err(path, "unexpected combination of trailing byte fields")),
    }
}

/// Reads a `PalBreedFarmBreeding` tail with uesave's own readers, also reporting how
/// many of `bytes` it consumed.
fn read_breed_farm_breeding(bytes: &[u8]) -> Result<(PalBreedFarmBreeding, u64)> {
    let mut ar: SaveGameArchive<Cursor<&[u8]>, Palworld> = SaveGameArchive::new(Cursor::new(bytes));
    ar.set_version(super::standalone_header(5));
    let breeding = PalBreedFarmBreeding::read(&mut ar)?;
    let consumed = ar.stream_position()?;
    Ok((breeding, consumed))
}

/// Decodes a `PalMapObjectBreedFarmModel`'s optional `breeding` block, which the
/// dialect has no field of its own for: when present, its bytes arrive as the
/// catch-all-shaped `trailing_unknown_bytes`. Bytes left over mean the run is not
/// that shape, and are reported rather than guessed at.
fn breed_farm_breeding(bytes: &[u8], path: &str) -> Result<PalBreedFarmBreeding> {
    let (breeding, consumed) = read_breed_farm_breeding(bytes)
        .map_err(|e| err(path, format!("trailing_unknown_bytes does not decode: {e}")))?;
    let unconsumed = (bytes.len() as u64).saturating_sub(consumed);
    if unconsumed != 0 {
        return Err(err(path, format!("trailing_unknown_bytes has {unconsumed} unconsumed bytes")));
    }
    Ok(breeding)
}

/// A breed farm. The dialect has no field for the optional trailing `breeding` block;
/// when the model was bred at least once, its bytes show up as `trailing_unknown_bytes`
/// instead (see the divergence table above).
fn breed_farm(value: &Value, path: &str) -> Result<PalMapObjectBreedFarmModel> {
    let breeding = match value.get("trailing_unknown_bytes") {
        Some(_) => Some(breed_farm_breeding(&byte_vec_at(value, "trailing_unknown_bytes"), path)?),
        None => None,
    };
    Ok(PalMapObjectBreedFarmModel {
        leading_bytes: bytes_at(value, "leading_bytes", path)?,
        spawned_egg_instance_ids: from_value(
            value.get("spawned_egg_instance_ids").unwrap_or(&Value::Array(vec![])),
            path,
        )?,
        trailing_bytes: trailing4(value, path)?,
        breeding,
    })
}

/// A hatching egg. `Properties<T>`'s own `Deserialize` impl only works inside a
/// schema-aware game-struct context that a bare `serde_json::from_value` call never
/// establishes (see `character_data`), so `hatched_character_save_parameter` is built
/// directly through the same property-tree walker.
fn hatching_egg(
    value: &Value,
    path: &str,
) -> Result<PalMapObjectHatchingEggModel<SaveGameArchiveType<Palworld>>> {
    let save_parameter = value
        .get("hatched_character_save_parameter")
        .ok_or_else(|| err(path, "no hatched_character_save_parameter"))?;
    Ok(PalMapObjectHatchingEggModel {
        leading_bytes: bytes_at(value, "leading_bytes", path)?,
        hatched_character_save_parameter: properties_from_json::<Palworld>(
            save_parameter,
            &join(path, "hatched_character_save_parameter"),
        )?,
        current_pal_egg_temp_diff: i32_at(value, "current_pal_egg_temp_diff", path)?,
        hatched_character_guid: guid_at(value, "hatched_character_guid", path)?,
        trailing_bytes: trailing4(value, path)?,
    })
}

/// A concrete model, dispatched by its flat `concrete_model_type` string. The dialect
/// only specializes a handful of model types and leaves the rest generic; an
/// unrecognized type string is therefore expected and lands on
/// [`PalMapConcreteModelVariant::Unknown`], exactly as uesave would read it from a real
/// save. A *recognized* type whose payload does not match its field set is still an
/// error, tagged with `path` like every other adapter here.
fn concrete_model(
    value: &Value,
    path: &str,
) -> Result<PalMapConcreteModel<SaveGameArchiveType<Palworld>>> {
    let concrete_model_type = value
        .get("concrete_model_type")
        .and_then(Value::as_str)
        .ok_or_else(|| err(path, "no concrete_model_type"))?
        .to_string();

    let auto_picked_up = |value: &Value| {
        bool_or_u32(
            value.get("auto_picked_up").ok_or_else(|| err(path, "no auto_picked_up"))?,
            path,
        )
    };
    let remove_pickup_guard_timer_handle = |value: &Value| {
        value
            .get("remove_pickup_guard_timer_handle")
            .and_then(Value::as_i64)
            .ok_or_else(|| err(path, "no remove_pickup_guard_timer_handle"))
    };

    let model_data = match concrete_model_type.as_str() {
        "PalMapObjectBaseCampPoint" => {
            PalMapConcreteModelVariant::BaseCampPoint(from_value(value, path)?)
        }
        "PalMapObjectItemChestModel" => {
            PalMapConcreteModelVariant::ItemChest(from_value(value, path)?)
        }
        "PalMapObjectItemChest_AffectCorruption" => {
            PalMapConcreteModelVariant::ItemChestAffectCorruption(from_value(value, path)?)
        }
        "PalMapObjectProductItemModel" => {
            PalMapConcreteModelVariant::ProductItem(from_value(value, path)?)
        }
        "PalMapObjectTorchModel" => PalMapConcreteModelVariant::Torch(from_value(value, path)?),
        "PalMapObjectConvertItemModel" => {
            PalMapConcreteModelVariant::ConvertItem(from_value(value, path)?)
        }
        "PalMapObjectFarmBlockV2Model" => {
            PalMapConcreteModelVariant::FarmBlockV2(from_value(value, path)?)
        }
        "PalMapObjectSignboardModel" => {
            PalMapConcreteModelVariant::Signboard(from_value(value, path)?)
        }
        "PalMapObjectDefenseBulletLauncherModel" => {
            PalMapConcreteModelVariant::DefenseBulletLauncher(from_value(value, path)?)
        }
        "PalMapObjectEnergyStorageModel" => {
            PalMapConcreteModelVariant::EnergyStorage(from_value(value, path)?)
        }
        "PalMapObjectHatchingEggModel" => {
            PalMapConcreteModelVariant::HatchingEgg(hatching_egg(value, path)?)
        }
        "PalMapObjectBreedFarmModel" => {
            // The dialect has no field for the optional `breeding` block; see the
            // divergence table above.
            PalMapConcreteModelVariant::BreedFarm(breed_farm(value, path)?)
        }
        "PalMapObjectGenerateEnergyModel" => {
            // The dialect renames `consume_energy_amount`; see the divergence table above.
            PalMapConcreteModelVariant::GenerateEnergy(generate_energy(value, path)?)
        }
        "PalMapObjectPickupItemOnLevelModel" => {
            // Version parity: uesave reads an optional guard the dialect never writes.
            PalMapConcreteModelVariant::PickupItemOnLevel(PalMapObjectPickupItemOnLevelModel {
                auto_picked_up: auto_picked_up(value)?,
                pickup_guard: None,
            })
        }
        "PalMapObjectDropItemModel" => {
            // The dialect's typo'd key; this struct spells the field correctly.
            PalMapConcreteModelVariant::DropItem(PalMapObjectDropItemModel {
                auto_picked_up: auto_picked_up(value)?,
                pickupable_player_uid: guid_at(value, "pickupdable_player_uid", path)?,
                remove_pickup_guard_timer_handle: remove_pickup_guard_timer_handle(value)?,
                item_id: from_value(
                    value.get("item_id").ok_or_else(|| err(path, "no item_id"))?,
                    path,
                )?,
                trailing_bytes: trailing4(value, path)?,
            })
        }
        "PalMapObjectPalEggModel" => {
            // Unlike `DropItem`, this struct's own field carries the same typo as the
            // dialect's key, so no rename is needed here.
            PalMapConcreteModelVariant::PalEgg(PalMapObjectPalEggModel {
                auto_picked_up: auto_picked_up(value)?,
                pickupdable_player_uid: guid_at(value, "pickupdable_player_uid", path)?,
                remove_pickup_guard_timer_handle: remove_pickup_guard_timer_handle(value)?,
            })
        }
        _ => {
            let object = value.as_object().ok_or_else(|| err(path, "expected an object"))?;
            reject_unconsumed(object, &[&CONCRETE_MODEL_BASE_KEYS], "concrete model", path)?;
            PalMapConcreteModelVariant::Unknown(BaseModel {
                trailing_bytes: base_model_trailing_bytes(value, path)?,
            })
        }
    };

    Ok(PalMapConcreteModel {
        instance_id: guid_at(value, "instance_id", path)?,
        model_instance_id: guid_at(value, "model_instance_id", path)?,
        concrete_model_type,
        model_data,
    })
}

/// A concrete model's map object module. uesave's `PalMapConcreteModelModule` folds a
/// sibling `CustomVersionData` property into `custom_version_data`; the walker lifts
/// it into this payload before this function ever sees it (see `gvas_json`).
///
/// The dialect drops the module's own type name at this layer (it lives on the
/// enclosing map's key, not the entry value), so the variant is recovered from the
/// field set the dialect emitted rather than a discriminator.
///
/// Both `module_type` fields below (the outer one and the nested copy on
/// `Unknown`) are left `String::new()` by contract: this adapter has no access to
/// the enclosing map key that names the module, so re-attaching it is left to a
/// later stage. No write path reads either field, so the bytes this adapter
/// produces are unaffected; this is purely so a future reader does not assume the
/// value means anything yet. The nested copy on `Unknown` is doubly inert: even
/// once a caller re-attaches the outer field, nothing reads back through it to
/// reach this one.
fn concrete_model_module(value: &Value, path: &str) -> Result<PalMapConcreteModelModule> {
    let custom_version_data = byte_vec_at(value, "custom_version_data");
    let object = value.as_object().ok_or_else(|| err(path, "expected an object"))?;

    let data = if object.contains_key("target_work_id") {
        PalMapConcreteModelModuleData::Workee {
            target_work_id: guid_at(value, "target_work_id", path)?,
            trailing_bytes: trailing4(value, path)?,
        }
    } else if object.contains_key("usage_type") {
        PalMapConcreteModelModuleData::ItemContainer {
            target_container_id: guid_at(value, "target_container_id", path)?,
            slot_attribute_indexes: from_value(
                value.get("slot_attribute_indexes").unwrap_or(&Value::Array(vec![])),
                path,
            )?,
            all_slot_attribute: from_value(
                value.get("all_slot_attribute").unwrap_or(&Value::Array(vec![])),
                path,
            )?,
            drop_item_at_disposed: bool_or_u32(
                value.get("drop_item_at_disposed").unwrap_or(&Value::Bool(false)),
                path,
            )?,
            usage_type: u8_at(value, "usage_type", path)?,
            trailing_bytes: trailing4(value, path)?,
        }
    } else if object.contains_key("target_container_id") {
        PalMapConcreteModelModuleData::CharacterContainer {
            target_container_id: guid_at(value, "target_container_id", path)?,
            trailing_bytes: trailing4(value, path)?,
        }
    } else if object.contains_key("lock_state")
        || object.contains_key("password")
        || object.contains_key("player_infos")
    {
        PalMapConcreteModelModuleData::PasswordLock {
            lock_state: u8_at(value, "lock_state", path)?,
            password: value.get("password").and_then(Value::as_str).unwrap_or("").to_string(),
            player_infos: from_value(
                value.get("player_infos").unwrap_or(&Value::Array(vec![])),
                path,
            )?,
            trailing_bytes: trailing4(value, path)?,
        }
    } else if object.contains_key("switch_state") {
        PalMapConcreteModelModuleData::Switch {
            switch_state: u8_at(value, "switch_state", path)?,
            trailing_bytes: trailing4(value, path)?,
        }
    } else if object.contains_key("unknown_bytes") {
        PalMapConcreteModelModuleData::Unknown {
            module_type: String::new(),
            raw_bytes: byte_vec_at(value, "unknown_bytes"),
        }
    } else {
        let keys = object.keys().cloned().collect::<Vec<_>>().join(", ");
        return Err(err(path, format!("unrecognized concrete model module fields: {keys}")));
    };

    Ok(PalMapConcreteModelModule { module_type: String::new(), data, custom_version_data })
}

/// Re-encodes a decoded `ModuleMap.RawData` payload back to the bytes its
/// property carries. This path is deliberately left untyped (its format breaks
/// on newer game versions), so unlike the other adapters this produces bytes
/// rather than a [`PalStruct`]; the module type is not recoverable at this
/// layer, so the caller re-attaches it from the sibling map key.
fn base_camp_module_bytes(value: &Value, path: &str) -> Result<Vec<u8>> {
    let module = PalBaseCampModule {
        module_type: String::new(),
        data: base_camp_module_data(value, path)?,
    };
    module.to_bytes(5)
}

/// Character save data, whose `object` is a nested property tree rather than a flat
/// struct, so it is built directly instead of through [`from_value`]: `Properties<T>`'s
/// own `Deserialize` impl only works inside a schema-aware game-struct context that a
/// bare `serde_json::from_value` call never establishes.
fn character_data(value: &Value, path: &str) -> Result<PalCharacterData<SaveGameArchiveType<Palworld>>> {
    let object_value = value.get("object").ok_or_else(|| err(path, "character data has no object"))?;
    Ok(PalCharacterData {
        object: properties_from_json::<Palworld>(object_value, &join(path, "object"))?,
        unknown_bytes: bytes_at(value, "unknown_bytes", path)?,
        group_id: guid_at(value, "group_id", path)?,
        trailing_bytes: trailing4(value, path)?,
    })
}

/// The dialect nests `static_id` inside `id` alongside the two world GUIDs; uesave
/// splits it into a sibling field on `PalDynamicItem`.
fn dynamic_id(value: &Value, path: &str) -> Result<(PalDynamicId, String)> {
    let id = value.get("id").ok_or_else(|| err(path, "dynamic item has no id"))?;
    let static_id = id
        .get("static_id")
        .and_then(Value::as_str)
        .ok_or_else(|| err(path, "id has no static_id"))?
        .to_string();
    Ok((
        PalDynamicId {
            created_world_id: guid_at(id, "created_world_id", path)?,
            local_id_in_created_world: guid_at(id, "local_id_in_created_world", path)?,
        },
        static_id,
    ))
}

/// Encodes a UE `FString` the same way uesave's own writer would (length-prefixed,
/// null-terminated, UTF-16 with a negative length for non-ASCII text).
fn encode_fstring(s: &str) -> Vec<u8> {
    let mut out = Vec::new();
    if s.is_empty() {
        out.extend_from_slice(&0u32.to_le_bytes());
    } else if s.is_ascii() {
        out.extend_from_slice(&((s.len() + 1) as u32).to_le_bytes());
        out.extend_from_slice(s.as_bytes());
        out.push(0);
    } else {
        let chars: Vec<u16> = s.encode_utf16().collect();
        out.extend_from_slice(&(-((chars.len() + 1) as i32)).to_le_bytes());
        for c in chars {
            out.extend_from_slice(&c.to_le_bytes());
        }
        out.extend_from_slice(&[0, 0]);
    }
    out
}

/// Recovers the FString the dialect's weapon shape does not split out of
/// `trailing_bytes ++ unknown_bytes` (see the divergence table above), returning the
/// decoded string and the real 4-byte trailer. `None` means the bytes do not parse as
/// a plausible `FString` followed by exactly 4 trailing bytes.
fn decode_fstring_tail(prefix: [u8; 4], tail: &[u8]) -> Option<(String, [u8; 4])> {
    let len = i32::from_le_bytes(prefix);
    let char_len: usize = if len < 0 {
        (len as i64).unsigned_abs().checked_mul(2)?.try_into().ok()?
    } else {
        len as usize
    };
    if tail.len() != char_len.checked_add(4)? {
        return None;
    }
    let (chars, trailing) = tail.split_at(char_len);
    let decoded = if len < 0 {
        let units: Vec<u16> = chars.chunks_exact(2).map(|c| u16::from_le_bytes([c[0], c[1]])).collect();
        let end = units.iter().position(|&c| c == 0).unwrap_or(units.len());
        String::from_utf16_lossy(&units[..end])
    } else {
        let end = chars.iter().position(|&c| c == 0).unwrap_or(chars.len());
        String::from_utf8_lossy(&chars[..end]).into_owned()
    };
    let mut trailing_bytes = [0u8; 4];
    trailing_bytes.copy_from_slice(trailing);
    Some((decoded, trailing_bytes))
}

/// A weapon-shaped dynamic item. When the dialect also emitted `unknown_bytes`, the
/// real trailing bytes and an FString the dialect does not split out are
/// buried inside it (see the divergence table); a successful reconstruction produces a
/// typed `Weapon` matching what uesave's own binary reader would, so an item's
/// representation does not depend on whether it came from a real save or this JSON
/// dialect. A reconstruction that does not check out falls back to `Unknown` with the
/// bytes reassembled in their original order, rather than guessing.
fn weapon(value: &Value, path: &str) -> Result<PalDynamicItemType<SaveGameArchiveType<Palworld>>> {
    let leading_bytes = bytes_at(value, "leading_bytes", path)?;
    let durability = f64_at(value, "durability", path)? as f32;
    let remaining_bullets = {
        let value = value
            .get("remaining_bullets")
            .and_then(Value::as_i64)
            .ok_or_else(|| err(path, "weapon has no remaining_bullets"))?;
        i32::try_from(value)
            .map_err(|_| err(path, format!("remaining_bullets {value} does not fit in i32")))?
    };
    let passive_skill_list: Vec<String> = from_value(
        value.get("passive_skill_list").ok_or_else(|| err(path, "weapon has no passive_skill_list"))?,
        path,
    )?;
    let trailing_bytes = trailing4(value, path)?;

    if value.get("unknown_bytes").is_some() {
        let unknown_bytes = byte_vec_at(value, "unknown_bytes");
        if let Some((unknown_str, trailing_bytes)) = decode_fstring_tail(trailing_bytes, &unknown_bytes) {
            return Ok(PalDynamicItemType::Weapon {
                leading_bytes,
                durability,
                remaining_bullets,
                passive_skill_list,
                unknown_str: Some(unknown_str),
                trailing_bytes,
            });
        }

        let mut trailer = Vec::new();
        trailer.extend_from_slice(&leading_bytes);
        trailer.extend_from_slice(&durability.to_le_bytes());
        trailer.extend_from_slice(&remaining_bullets.to_le_bytes());
        trailer.extend_from_slice(&(passive_skill_list.len() as u32).to_le_bytes());
        for skill in &passive_skill_list {
            trailer.extend(encode_fstring(skill));
        }
        trailer.extend_from_slice(&trailing_bytes);
        trailer.extend_from_slice(&unknown_bytes);
        return Ok(PalDynamicItemType::Unknown { trailer });
    }

    Ok(PalDynamicItemType::Weapon {
        leading_bytes,
        durability,
        remaining_bullets,
        passive_skill_list,
        unknown_str: None,
        trailing_bytes,
    })
}

/// An armor-shaped dynamic item. `PalArmor` has no field for unrecognized trailing
/// data, so a present `unknown_bytes` (the trailing data the dialect tolerates where
/// uesave's codec rejects it) always falls back to `Unknown`, its bytes reassembled in
/// original order.
fn armor(value: &Value, path: &str) -> Result<PalDynamicItemType<SaveGameArchiveType<Palworld>>> {
    let leading_bytes = bytes_at(value, "leading_bytes", path)?;
    let durability = f64_at(value, "durability", path)? as f32;
    let trailing_bytes = trailing4(value, path)?;

    if value.get("unknown_bytes").is_some() {
        let mut trailer = Vec::new();
        trailer.extend_from_slice(&leading_bytes);
        trailer.extend_from_slice(&durability.to_le_bytes());
        trailer.extend_from_slice(&trailing_bytes);
        trailer.extend_from_slice(&byte_vec_at(value, "unknown_bytes"));
        return Ok(PalDynamicItemType::Unknown { trailer });
    }

    Ok(PalDynamicItemType::Armor { leading_bytes, durability, trailing_bytes })
}

/// A dynamic item, dispatched by the dialect's flat `type` discriminator. The
/// `PalDynamicId`/`static_id`/`item_type` nesting `PalDynamicItem` declares is
/// unwound from the dialect's flat field set here.
fn dynamic_item(value: &Value, path: &str) -> Result<PalDynamicItem<SaveGameArchiveType<Palworld>>> {
    let (id, static_id) = dynamic_id(value, path)?;
    let type_name = value.get("type").and_then(Value::as_str).unwrap_or("");

    let item_type = match type_name {
        "weapon" => weapon(value, path)?,
        "armor" => armor(value, path)?,
        _ => {
            let object = value.as_object().ok_or_else(|| err(path, "expected an object"))?;
            let keys = object.keys().cloned().collect::<Vec<_>>().join(", ");
            return Err(err(path, format!("unrecognized dynamic item fields: {keys}")));
        }
    };

    Ok(PalDynamicItem { id, static_id, item_type })
}

/// The flat dict's keys that `PalWorkBase` and the work's own transform account for.
/// Everything else the dialect emitted selects the `work_specific_data` variant, so an
/// unconsumed key means a work shape this adapter does not know.
const WORK_BASE_KEYS: [&str; 15] = [
    "id",
    "workable_bounds",
    "base_camp_id_belong_to",
    "owner_map_object_model_id",
    "owner_map_object_concrete_model_id",
    "current_state",
    "assign_locations",
    "behaviour_type",
    "assign_define_data_id",
    "override_work_type",
    "assignable_fixed_type",
    "assignable_otomo",
    "can_trigger_worker_event",
    "can_steal_assign",
    "transform",
];

const PROGRESS_KEYS: [&str; 7] = [
    "required_work_amount",
    "current_work_amount",
    "work_exp",
    "work_exp_calc_type",
    "auto_work_self_amount_by_sec",
    "progress_time_since_last_tick",
    "tick_process_min_interval",
];

const DEFENSE_KEYS: [&str; 3] = ["leading_bytes", "defense_combat_type", "trailing_bytes"];

fn u8_at(value: &Value, key: &str, path: &str) -> Result<u8> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|v| u8::try_from(v).ok())
        .ok_or_else(|| err(path, format!("expected a byte at {key:?}")))
}

fn f32_at(value: &Value, key: &str, path: &str) -> Result<f32> {
    Ok(f64_at(value, key, path)? as f32)
}

fn i32_at(value: &Value, key: &str, path: &str) -> Result<i32> {
    let value = value
        .get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| err(path, format!("expected an integer at {key:?}")))?;
    i32::try_from(value).map_err(|_| err(path, format!("{key:?} value {value} does not fit in i32")))
}

/// Errors naming every key the chosen shape did not account for, rather than
/// dropping data the dialect carried. `label` names the shape in the message
/// (e.g. "work", "concrete model").
fn reject_unconsumed(
    object: &Map<String, Value>,
    consumed: &[&[&str]],
    label: &str,
    path: &str,
) -> Result<()> {
    let extra = object
        .keys()
        .map(String::as_str)
        .filter(|key| !consumed.iter().any(|set| set.contains(key)))
        .collect::<Vec<_>>();
    if extra.is_empty() {
        return Ok(());
    }
    Err(err(path, format!("unrecognized {label} fields: {}", extra.join(", "))))
}

/// The work's `UPalWorkBase` half, which the dialect flattens in alongside the
/// type-specific fields. Three of its `u32`s arrive as JSON bools.
fn work_base(value: &Value, path: &str) -> Result<PalWorkBase> {
    Ok(PalWorkBase {
        id: guid_at(value, "id", path)?,
        workable_bounds: from_value(
            value.get("workable_bounds").ok_or_else(|| err(path, "no workable_bounds"))?,
            path,
        )?,
        base_camp_id_belong_to: guid_at(value, "base_camp_id_belong_to", path)?,
        owner_map_object_model_id: guid_at(value, "owner_map_object_model_id", path)?,
        owner_map_object_concrete_model_id: guid_at(
            value,
            "owner_map_object_concrete_model_id",
            path,
        )?,
        current_state: u8_at(value, "current_state", path)?,
        assign_locations: from_value(
            value.get("assign_locations").unwrap_or(&Value::Array(vec![])),
            path,
        )?,
        behaviour_type: u8_at(value, "behaviour_type", path)?,
        assign_define_data_id: value
            .get("assign_define_data_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        override_work_type: u8_at(value, "override_work_type", path)?,
        assignable_fixed_type: u8_at(value, "assignable_fixed_type", path)?,
        assignable_otomo: bool_or_u32(
            value.get("assignable_otomo").unwrap_or(&Value::Bool(false)),
            path,
        )?,
        can_trigger_worker_event: bool_or_u32(
            value.get("can_trigger_worker_event").unwrap_or(&Value::Bool(false)),
            path,
        )?,
        can_steal_assign: bool_or_u32(
            value.get("can_steal_assign").unwrap_or(&Value::Bool(false)),
            path,
        )?,
    })
}

/// The work's transform, a sub-union of its own tagged by a numeric `type`. Only tag 2
/// carries a payload; uesave leaves every other tag's two fields empty, so they are read
/// straight off the tag rather than inventing a shape for keys the dialect never wrote.
fn work_transform(value: &Value, path: &str) -> Result<PalWorkTransform> {
    let transform_type = value
        .get("type")
        .and_then(Value::as_u64)
        .ok_or_else(|| err(path, "transform has no type"))? as u8;
    if transform_type != 2 {
        return Ok(PalWorkTransform {
            transform_type,
            map_object_instance_id: None,
            trailing_bytes: None,
        });
    }
    Ok(PalWorkTransform {
        transform_type,
        map_object_instance_id: Some(guid_at(value, "map_object_instance_id", path)?),
        trailing_bytes: Some(bytes_at(value, "trailing_bytes", path)?),
    })
}

/// What a `Progress_MultiType` work's raw tail resolves to: everything uesave reads
/// after the progress fields.
type MultiTypeTail = (Vec<PalWorkProgressEntry>, Vec<PalWorkSuitabilityInfo>, PalWorkTransform);

/// Reads a multi-type tail, also reporting how many of `bytes` it consumed.
fn read_multi_type_tail(bytes: &[u8]) -> Result<(MultiTypeTail, u64)> {
    let mut ar: SaveGameArchive<Cursor<&[u8]>, Palworld> = SaveGameArchive::new(Cursor::new(bytes));
    ar.set_version(super::standalone_header(5));
    let progress_count = ar.read_u32::<LE>()?;
    let progress_entries = read_array(progress_count, &mut ar, PalWorkProgressEntry::read)?;
    let suitability_count = ar.read_u32::<LE>()?;
    let suitability_info_entries =
        read_array(suitability_count, &mut ar, PalWorkSuitabilityInfo::read)?;
    let transform = PalWorkTransform::read(&mut ar)?;
    let consumed = ar.stream_position()?;
    Ok((
        (progress_entries, suitability_info_entries, transform),
        consumed,
    ))
}

/// Decodes the raw run a `Progress_MultiType` work keeps in `multi_tail`. The dialect
/// stops decoding at the progress fields, so these bytes still hold everything uesave
/// reads afterwards: the per-suitability progress entries, the suitability
/// requirements, and the work's transform. Bytes left over mean the run is not that
/// shape, and are reported rather than guessed at.
fn multi_type_tail(bytes: &[u8], path: &str) -> Result<MultiTypeTail> {
    let (tail, consumed) = read_multi_type_tail(bytes)
        .map_err(|e| err(path, format!("multi_tail does not decode: {e}")))?;
    let unconsumed = (bytes.len() as u64).saturating_sub(consumed);
    if unconsumed != 0 {
        return Err(err(path, format!("multi_tail has {unconsumed} unconsumed bytes")));
    }
    Ok(tail)
}

/// A work. The dialect emits one flat dict where `PalWork` nests a base, a transform
/// and a type-specific payload, and the `WorkableType` that chose the layout lives in a
/// sibling property this adapter cannot see, so the field set is the only discriminator
/// left. `work_type` is left `String::new()` by contract, for a later stage to
/// re-attach from that sibling; no write path reads it, so this adapter's bytes are
/// unaffected. Same contract as the module types whose own discriminator sits on an
/// enclosing map key.
fn work(value: &Value, path: &str) -> Result<PalWork> {
    let object = value.as_object().ok_or_else(|| err(path, "expected an object"))?;
    let mut transform = value
        .get("transform")
        .map(|t| work_transform(t, &join(path, "transform")))
        .transpose()?;

    let work_specific_data = if object.contains_key("defense_combat_type") {
        reject_unconsumed(object, &[&WORK_BASE_KEYS, &DEFENSE_KEYS], "work", path)?;
        PalWorkTypeSpecificData::Defense {
            leading_bytes: bytes_at(value, "leading_bytes", path)?,
            defense_combat_type: u8_at(value, "defense_combat_type", path)?,
            trailing_bytes: trailing4(value, path)?,
        }
    } else if object.contains_key("multi_tail") {
        reject_unconsumed(object, &[&WORK_BASE_KEYS, &PROGRESS_KEYS, &["multi_tail"]], "work", path)?;
        let (progress_entries, suitability_info_entries, tail_transform) =
            multi_type_tail(&byte_vec_at(value, "multi_tail"), path)?;
        // The dialect writes no sibling transform for this work type; the transform is the
        // tail of the run it kept raw. `.or` means an actual sibling transform, were one
        // ever present, would silently win over this decoded one instead of erroring on
        // the conflict; not reachable with the dialect's own output, which never pairs a
        // `multi_tail` work with a sibling transform.
        transform = transform.or(Some(tail_transform));
        PalWorkTypeSpecificData::ProgressMultiType {
            required_work_amount: f32_at(value, "required_work_amount", path)?,
            current_work_amount: f32_at(value, "current_work_amount", path)?,
            work_exp: i32_at(value, "work_exp", path)?,
            work_exp_calc_type: u8_at(value, "work_exp_calc_type", path)?,
            auto_work_self_amount_by_sec: f32_at(value, "auto_work_self_amount_by_sec", path)?,
            progress_time_since_last_tick: f32_at(value, "progress_time_since_last_tick", path)?,
            tick_process_min_interval: f32_at(value, "tick_process_min_interval", path)?,
            progress_entries,
            suitability_info_entries,
        }
    } else if object.contains_key("current_work_amount") {
        reject_unconsumed(object, &[&WORK_BASE_KEYS, &PROGRESS_KEYS], "work", path)?;
        PalWorkTypeSpecificData::Progress {
            required_work_amount: f32_at(value, "required_work_amount", path)?,
            current_work_amount: f32_at(value, "current_work_amount", path)?,
            work_exp: i32_at(value, "work_exp", path)?,
            work_exp_calc_type: u8_at(value, "work_exp_calc_type", path)?,
            auto_work_self_amount_by_sec: f32_at(value, "auto_work_self_amount_by_sec", path)?,
            progress_time_since_last_tick: f32_at(value, "progress_time_since_last_tick", path)?,
            tick_process_min_interval: f32_at(value, "tick_process_min_interval", path)?,
        }
    } else if object.contains_key("required_work_amount") {
        reject_unconsumed(object, &[&WORK_BASE_KEYS, &["required_work_amount"]], "work", path)?;
        PalWorkTypeSpecificData::SimpleWork {
            required_work_amount: f32_at(value, "required_work_amount", path)?,
        }
    } else {
        // A base with no payload of its own: the dialect emits the base's keys and
        // nothing more, as an empty module dict stands for a module with no payload.
        reject_unconsumed(object, &[&WORK_BASE_KEYS], "work", path)?;
        PalWorkTypeSpecificData::None
    };

    Ok(PalWork {
        // The sibling WorkableType property carries this; the caller re-attaches it.
        work_type: String::new(),
        base_data: Some(work_base(value, path)?),
        transform,
        work_specific_data,
    })
}

const WORK_ASSIGN_KEYS: [&str; 8] = [
    "id",
    "location_index",
    "assign_type",
    "assigned_individual_id",
    "state",
    "fixed",
    "trailing_bytes",
    "unknown_bytes",
];

/// Errors naming every key a `WorkAssignMap` entry did not account for, rather
/// than dropping data the dialect carried.
fn reject_unconsumed_work_assign(object: &Map<String, Value>, path: &str) -> Result<()> {
    reject_unconsumed(object, &[&WORK_ASSIGN_KEYS], "work assign", path)
}

/// The dialect spells `PalInstanceId`'s player guid `player_uid`; uesave's struct
/// spells the same field `guid`.
fn pal_instance_id(value: &Value, path: &str) -> Result<PalInstanceId> {
    Ok(PalInstanceId {
        guid: guid_at(value, "player_uid", path)?,
        instance_id: guid_at(value, "instance_id", path)?,
    })
}

/// Reads a `WorkAssign` entry's `multi_type` tail, also reporting how many of
/// `bytes` it consumed.
fn read_work_assign_multi_type(bytes: &[u8]) -> Result<(PalWorkAssignMultiType, u64)> {
    let mut ar: SaveGameArchive<Cursor<&[u8]>, Palworld> = SaveGameArchive::new(Cursor::new(bytes));
    ar.set_version(super::standalone_header(5));
    let multi_type = PalWorkAssignMultiType::read(&mut ar)?;
    let consumed = ar.stream_position()?;
    Ok((multi_type, consumed))
}

/// Decodes a `WorkAssign` entry's `unknown_bytes` into `multi_type`. Only an
/// assignment to a `Progress_MultiType` work carries this: the dialect never decodes
/// `UPalWorkAssign_WorkProgressMultiType`, so the bytes uesave would read there
/// land here instead. Bytes left over mean the run is not that shape, and are
/// reported rather than guessed at.
fn work_assign_multi_type(bytes: &[u8], path: &str) -> Result<PalWorkAssignMultiType> {
    let (multi_type, consumed) = read_work_assign_multi_type(bytes)
        .map_err(|e| err(path, format!("unknown_bytes does not decode: {e}")))?;
    let unconsumed = (bytes.len() as u64).saturating_sub(consumed);
    if unconsumed != 0 {
        return Err(err(path, format!("unknown_bytes has {unconsumed} unconsumed bytes")));
    }
    Ok(multi_type)
}

/// A `WorkAssignMap` entry: one worker's assignment to a work.
fn work_assign(value: &Value, path: &str) -> Result<PalWorkAssign> {
    let object = value.as_object().ok_or_else(|| err(path, "expected an object"))?;
    reject_unconsumed_work_assign(object, path)?;

    let assigned_individual_id = value
        .get("assigned_individual_id")
        .ok_or_else(|| err(path, "no assigned_individual_id"))?;
    let multi_type = if object.contains_key("unknown_bytes") {
        Some(work_assign_multi_type(&byte_vec_at(value, "unknown_bytes"), path)?)
    } else {
        None
    };

    Ok(PalWorkAssign {
        id: guid_at(value, "id", path)?,
        location_index: i32_at(value, "location_index", path)?,
        assign_type: u8_at(value, "assign_type", path)?,
        assigned_individual_id: pal_instance_id(assigned_individual_id, path)?,
        state: u8_at(value, "state", path)?,
        fixed: bool_or_u32(value.get("fixed").unwrap_or(&Value::Bool(false)), path)?,
        trailing_bytes: trailing4(value, path)?,
        multi_type,
    })
}

/// The path-keyed adapter table. Returns `Ok(None)` for a path Palworld does not
/// type, which the caller reports as an error naming that path.
pub fn game_struct_from_json(
    path: &str,
    value: &Value,
) -> Result<Option<PalStruct<SaveGameArchiveType<Palworld>>>> {
    Ok(Some(match path {
        "worldSaveData.BaseCampSaveData.RawData" => {
            PalStruct::BaseCamp(base_camp(value, path)?.into())
        }
        "worldSaveData.MapObjectSaveData.Model.BuildProcess.RawData" => {
            PalStruct::BuildProcess(from_value(value, path)?)
        }
        "worldSaveData.MapObjectSaveData.Model.Connector.RawData" => {
            PalStruct::Connector(from_value(value, path)?)
        }
        "worldSaveData.ItemContainerSaveData.RawData" => {
            PalStruct::ItemContainer(from_value(value, path)?)
        }
        "worldSaveData.ItemContainerSaveData.Slots.RawData" => {
            PalStruct::ItemContainerSlots(from_value(value, path)?)
        }
        "worldSaveData.CharacterContainerSaveData.Slots.RawData" => {
            PalStruct::CharacterContainer(from_value(value, path)?)
        }
        "worldSaveData.MapObjectSaveData.Model.RawData" => {
            PalStruct::MapModel(map_model(value, path)?.into())
        }
        "worldSaveData.BaseCampSaveData.WorkerDirector.RawData" => {
            PalStruct::WorkerDirector(worker_director(value, path)?.into())
        }
        "worldSaveData.BaseCampSaveData.WorkCollection.RawData" => {
            PalStruct::WorkCollection(work_collection(value, path)?)
        }
        "worldSaveData.MapObjectSaveData.ConcreteModel.ModuleMap.RawData" => {
            PalStruct::MapConcreteModelModule(concrete_model_module(value, path)?)
        }
        "worldSaveData.MapObjectSaveData.ConcreteModel.RawData" => {
            PalStruct::MapConcreteModel(concrete_model(value, path)?.into())
        }
        "worldSaveData.CharacterSaveParameterMap.RawData" => {
            PalStruct::CharacterData(character_data(value, path)?)
        }
        "worldSaveData.DynamicItemSaveData.RawData" => {
            PalStruct::DynamicItem(dynamic_item(value, path)?.into())
        }
        "worldSaveData.WorkSaveData.RawData" => PalStruct::Work(work(value, path)?.into()),
        "worldSaveData.WorkSaveData.WorkAssignMap.RawData" => {
            PalStruct::WorkAssign(work_assign(value, path)?)
        }
        _ => return Ok(None),
    }))
}

/// The bytes-hook counterpart of [`game_struct_from_json`]: paths whose decoded
/// dialect payload must round-trip to opaque bytes rather than a [`PalStruct`].
pub fn game_bytes_from_json(path: &str, value: &Value) -> Result<Option<Vec<u8>>> {
    Ok(match path {
        "worldSaveData.BaseCampSaveData.ModuleMap.RawData" => {
            Some(base_camp_module_bytes(value, path)?)
        }
        _ => None,
    })
}
