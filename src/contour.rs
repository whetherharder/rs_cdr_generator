// Строгий разбор вложенного YAML-конфига контура (meta.*, network.*, subscribers.*),
// как в demo/generator/demo-cdr.yaml batch-scoring-installer (эталон, не править).
//
// Понимаем ТОЛЬКО секции, перечисленные в задании этапа 2:
// meta.seed, meta.time_range.start/end, subscribers.total_count,
// subscribers.profiles, network.elements, network.cells. Внутри них — строго
// (`deny_unknown_fields`), незнакомый ключ там — это опечатка или чужая схема,
// и по нему падаем, а не молчим. Остальные секции конфига (events, anomalies,
// special_events, vendor_extensions, meta.output, meta.parallelism,
// subscribers.contact_book и т.п.) вне области этого этапа — читаем как
// непрозрачный YAML и не проверяем: падать на них означало бы требовать
// от контурного конфига знать о нашей внутренней реализации.
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct ContourConfig {
    pub meta: MetaSection,
    pub network: NetworkSection,
    pub subscribers: SubscribersSection,
    // Остальные топ-уровневые секции (events, anomalies, special_events,
    // vendor_extensions, interactive_overrides) — вне области этапа.
    #[serde(flatten)]
    pub _rest: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
pub struct MetaSection {
    pub seed: u64,
    pub time_range: TimeRange,
    // output/parallelism/time_step_seconds — вне области этапа 2.
    #[serde(flatten)]
    pub _rest: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeRange {
    pub start: String,
    pub end: String,
}

#[derive(Debug, Deserialize)]
pub struct NetworkSection {
    pub elements: Vec<NetworkElementCfg>,
    pub cells: CellsSection,
    // operator, auto_generate верхнего уровня network — вне области этапа.
    #[serde(flatten)]
    pub _rest: HashMap<String, serde_yaml::Value>,
}

/// Поля — как в питоновской assets.generator.generate_network_elements
/// (network.elements[] эталонного YAML).
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NetworkElementCfg {
    pub id: String,
    #[serde(rename = "type")]
    pub ne_type: String,
    #[serde(default)]
    pub vendor: String,
    #[serde(default)]
    pub address: String,
    #[serde(default)]
    pub serves_tacs: Vec<u32>,
    #[serde(default)]
    pub serves_apns: Vec<String>,
    #[serde(default)]
    pub produces: Vec<String>,
    #[serde(default)]
    pub extensions_template: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
pub struct CellsSection {
    #[serde(default)]
    pub items: Vec<CellItemCfg>,
    // source/file_path — вне области этапа (только inline-соты читаем).
    #[serde(flatten)]
    pub _rest: HashMap<String, serde_yaml::Value>,
}

/// Поля — как в network.cells.items[] эталонного YAML (assets.generator.Cell).
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CellItemCfg {
    pub cell_id: u32,
    pub tac: u32,
    #[serde(default)]
    pub ecgi: String,
    #[serde(default)]
    pub lat: f64,
    #[serde(default)]
    pub lon: f64,
    #[serde(default)]
    pub azimuth: i32,
    #[serde(default)]
    pub sector: i32,
    #[serde(rename = "type", default)]
    pub cell_type: String,
    #[serde(default)]
    pub capacity: String,
    #[serde(default)]
    pub neighbors: Vec<u32>,
}

#[derive(Debug, Deserialize)]
pub struct SubscribersSection {
    pub total_count: usize,
    // Интенсивности трафика (daily_rates.*.params.lambda) и веса профилей
    // читаем типизированно — они напрямую управляют объёмом и структурой
    // трафика (см. ProfileCfg ниже). hourly_weights/day_of_week_multipliers/
    // mobility остаются непрозрачным YAML внутри ProfileCfg (`_rest`) —
    // распределение по часам суток в эту правку не входит, применяются
    // только суточные интенсивности и веса профилей.
    #[serde(default)]
    pub profiles: Vec<ProfileCfg>,
    // imsi_prefix/msisdn_prefix/contact_book/external_numbers — вне области.
    #[serde(flatten)]
    pub _rest: HashMap<String, serde_yaml::Value>,
}

impl SubscribersSection {
    /// Имена профилей — для контрольного вывода (см. main.rs).
    pub fn profile_names(&self) -> Vec<String> {
        self.profiles.iter().map(|p| p.name.clone()).collect()
    }
}

/// Профиль абонента (`subscribers.profiles[]` контурного YAML) — вес
/// профиля в популяции и суточные интенсивности (пуассоновский параметр
/// `lambda`) по пяти типам событий, как у питона (`engine/runner.py:
/// 172-187`, `_ProfileCache`).
#[derive(Debug, Deserialize, Clone)]
pub struct ProfileCfg {
    pub name: String,
    #[serde(default)]
    pub weight: f64,
    pub daily_rates: DailyRatesCfg,
    // hourly_weights/day_of_week_multipliers/mobility/imei_tac_pool/
    // rat_preference/description — вне области этой правки.
    #[serde(flatten)]
    pub _rest: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DailyRatesCfg {
    pub mo_call: RateCfg,
    pub mo_sms: RateCfg,
    pub data_session: RateCfg,
    pub mt_call: RateCfg,
    pub mt_sms: RateCfg,
}

/// `{type: poisson, params: {lambda: N}}` — тип распределения (`type`)
/// сейчас не используется (в конфиге контура везде `poisson`, как
/// и у rust-сэмплера `EventCountSampler`), читаем только `lambda`.
#[derive(Debug, Deserialize, Clone)]
pub struct RateCfg {
    #[serde(default)]
    pub r#type: String,
    pub params: HashMap<String, f64>,
}

impl RateCfg {
    pub fn lambda(&self) -> f64 {
        self.params.get("lambda").copied().unwrap_or(0.0)
    }
}

/// Пытается разобрать YAML как вложенный конфиг контура. Если верхнего
/// уровня нет секций meta/network/subscribers — это не наш формат
/// (вероятно, старый плоский конфиг), возвращаем None молча: решение,
/// каким конфигом считать файл, остаётся за load_config. Если секции
/// есть, но внутри — незнакомый ключ или несовпадающий тип — падаем
/// с понятной ошибкой serde_yaml (файл, путь ключа, ожидаемый тип).
pub fn try_parse_contour_config(raw: &serde_yaml::Value) -> Option<anyhow::Result<ContourConfig>> {
    let map = raw.as_mapping()?;
    let has_any = ["meta", "network", "subscribers"]
        .iter()
        .any(|k| map.contains_key(&serde_yaml::Value::String(k.to_string())));
    if !has_any {
        return None;
    }
    Some(
        serde_yaml::from_value::<ContourConfig>(raw.clone())
            .map_err(|e| anyhow::anyhow!("вложенный конфиг контура не разобран: {}", e)),
    )
}
