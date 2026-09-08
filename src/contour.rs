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
    // events.voice.success_rate/events.sms.delivery_success_rate — читаем
    // типизированно (эта правка): доля несостоявшихся звонков/недоставленных
    // SMS напрямую задаёт асимметрию mo_call/mt_call (см. README, «известное
    // ограничение»). Остальное внутри events (duration/failure_causes/data.*)
    // и остальные топ-уровневые секции (anomalies, special_events,
    // vendor_extensions, interactive_overrides) — вне области этапа.
    #[serde(default)]
    pub events: Option<EventsSection>,
    #[serde(flatten)]
    pub _rest: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
pub struct EventsSection {
    #[serde(default)]
    pub voice: Option<VoiceEventsCfg>,
    #[serde(default)]
    pub sms: Option<SmsEventsCfg>,
    #[serde(default)]
    pub data: Option<DataEventsCfg>,
    #[serde(flatten)]
    pub _rest: HashMap<String, serde_yaml::Value>,
}

/// `events.data.volume_uplink`/`volume_downlink` — объём data-сессии
/// читаем типизированно (эта правка): раньше объём разыгрывался по
/// зашитой в generators.rs таблице средних на RAT (Normal, 1-12 МБ), а не
/// по lognormal из конфига (mu/sigma по конфигу демо — 10.4/12.5, sigma
/// 0.8) — прямая причина укороченного вывода (см. README, −28.6% байт).
/// `profile_volume_multipliers` остаётся вне области: у rust-`Subscriber`
/// нет поля профиля на уровне генерации data-сессии (это отдельная работа).
#[derive(Debug, Deserialize)]
pub struct DataEventsCfg {
    pub volume_uplink: LognormalVolumeCfg,
    pub volume_downlink: LognormalVolumeCfg,
    #[serde(flatten)]
    pub _rest: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
pub struct LognormalVolumeCfg {
    pub distribution: LognormalDistCfg,
    #[serde(default)]
    pub min_bytes: f64,
}

#[derive(Debug, Deserialize)]
pub struct LognormalDistCfg {
    #[serde(rename = "type", default)]
    pub kind: String,
    pub params: LognormalParamsCfg,
}

#[derive(Debug, Deserialize)]
pub struct LognormalParamsCfg {
    pub mu: f64,
    pub sigma: f64,
}

#[derive(Debug, Deserialize)]
pub struct VoiceEventsCfg {
    pub success_rate: f64,
    #[serde(flatten)]
    pub _rest: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
pub struct SmsEventsCfg {
    pub delivery_success_rate: f64,
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
    // Книга контактов и пул внешних номеров — читаем типизированно (этой
    // правкой): они напрямую задают, к кому уходят звонки/SMS абонента
    // (EXTERNAL_CALL_RATIO и параметры Zipf были зашиты в contact_book.rs
    // независимо от того, что написано здесь).
    #[serde(default)]
    pub contact_book: Option<ContactBookCfg>,
    #[serde(default)]
    pub external_numbers: Option<ExternalNumbersCfg>,
    // imsi_prefix/msisdn_prefix — вне области.
    #[serde(flatten)]
    pub _rest: HashMap<String, serde_yaml::Value>,
}

impl SubscribersSection {
    /// Имена профилей — для контрольного вывода (см. main.rs).
    pub fn profile_names(&self) -> Vec<String> {
        self.profiles.iter().map(|p| p.name.clone()).collect()
    }
}

/// `subscribers.contact_book` контурного YAML — параметры постоянной книги
/// контактов (`degree_distribution` — только `zipf`, других типов конфиг
/// контура не задаёт; `asymmetric`/`intra_profile_bias` пока не читаем —
/// это отдельная работа за пределами этой правки, применяется только то,
/// что напрямую объясняет измеренные расхождения: степень круга и доля
/// внешних/повторных звонков).
#[derive(Debug, Deserialize, Clone)]
pub struct ContactBookCfg {
    #[serde(default)]
    pub avg_contacts: f64,
    pub degree_distribution: DegreeDistributionCfg,
    #[serde(default)]
    pub repeat_call_probability: f64,
    pub external_call_ratio: f64,
    // asymmetric/intra_profile_bias — вне области этой правки.
    #[serde(flatten)]
    pub _rest: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DegreeDistributionCfg {
    #[serde(rename = "type", default)]
    pub kind: String,
    pub params: DegreeParamsCfg,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DegreeParamsCfg {
    #[serde(default = "default_zipf_a")]
    pub a: f64,
    pub min: usize,
    pub max: usize,
}

fn default_zipf_a() -> f64 {
    2.0
}

/// `subscribers.external_numbers` — пул номеров "на сторону". `count`
/// в rust не используется впрямую (пул генерируется по требуемым парам
/// на лету, как и раньше), но `prefixes` со своими весами — используется:
/// раньше внешний номер собирался из равновероятного списка префиксов
/// без веса, теперь веса из конфига учитываются.
#[derive(Debug, Deserialize, Clone)]
pub struct ExternalNumbersCfg {
    #[serde(default)]
    pub count: usize,
    #[serde(default)]
    pub prefixes: Vec<ExternalPrefixCfg>,
    #[serde(flatten)]
    pub _rest: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExternalPrefixCfg {
    pub prefix: String,
    #[serde(default)]
    pub weight: f64,
    #[serde(default)]
    pub label: String,
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
