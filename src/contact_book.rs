// Постоянная книга контактов абонента.
//
// Зачем: без неё собеседник звонка выбирается заново случайно на каждый
// вызов (см. docs/contact-graph-and-memory-2026-09-08.md, задача 1) —
// круга общения у абонента нет, вес контакта размазан почти идеально
// ровно (99.6% пар — 2..5 звонков), граф симметричен на 100%, устойчивость
// круга между половинами периода — 0.002 вместо питоновских 0.632.
// Потребитель данных (`CircleTop`, `CircleAvgWeighted`, History-трансформеры)
// строит фичи по кругу общения — на таком графе они дают шум.
//
// Образец — `demo/generator/upstream/cdr_generator/assets/contact_book.py`
// (batch-scoring-installer, только чтение). Алгоритм НЕ скопирован буквально:
// питон для КАЖДОГО из N абонентов строит список кандидатов и весов длиной
// N-1 (`contact_book.py:82,96-100`) — O(N²) по времени и памяти, и это
// именно то, что ограничивает питон по числу абонентов. Здесь книга строится
// за O(N·k), где k — размер круга конкретного абонента (единицы-десятки):
// каждому абоненту сэмплируются k контактов напрямую из общего пула
// (`rand::seq::index::sample`, без перебора остальных N-1 кандидатов).
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand::seq::index::sample;
use std::collections::HashMap;

/// Параметры книги — те же значения, что в питоновском конфиге
/// (`cdr_generator_config.yaml:315-321`, секция `contact_book`). Числа не
/// подгонялись под наш прогон — взяты как есть у образца.
pub const ZIPF_A: f64 = 2.0;
pub const MIN_DEGREE: usize = 3;
pub const MAX_DEGREE: usize = 100;
/// Доля звонков "на сторону" (внешний номер, не абонент сети).
pub const EXTERNAL_CALL_RATIO: f64 = 0.15;
/// Доля оставшихся (после внешних) звонков — в свой круг общения.
pub const REPEAT_CALL_PROBABILITY: f64 = 0.6;
/// Порог roll < CONTACT_THRESHOLD → тир 2 (книга контактов); формула та же,
/// что в `b_party.py:101`: ext_ratio + (1 - ext_ratio) * repeat_prob.
pub const CONTACT_THRESHOLD: f64 = EXTERNAL_CALL_RATIO
    + (1.0 - EXTERNAL_CALL_RATIO) * REPEAT_CALL_PROBABILITY;

/// Книга контактов на весь прогон: msisdn → список msisdn его круга.
///
/// Асимметрия не подгоняется отдельным шагом — она получается сама, потому
/// что книга каждого абонента сэмплируется независимо от книг остальных
/// (как и у питона при `asymmetric: true`, дефолт конфига): то, что B попал
/// в круг A, почти не влияет на то, попадёт ли A в круг B.
pub struct ContactBook {
    books: HashMap<u64, Vec<u64>>,
}

impl ContactBook {
    /// `book_seed` — ОТДЕЛЬНЫЙ от seed воркера/дня/куска пула. Книга обязана
    /// быть детерминирована от seed прогона и состава абонентов и НЕ зависеть
    /// ни от периода дат, ни от числа воркеров (иначе параллелизм по датам
    /// пересобирал бы разным суткам разные круги — тот же класс требования,
    /// что и к самому составу абонентов). Поэтому книга строится РОВНО ОДИН
    /// РАЗ в main.rs, до разбиения на work item'ы (день × кусок пула), и
    /// раздаётся всем воркерам через Arc — как и общий пул `all_msisdns`.
    pub fn build(all_msisdns: &[u64], book_seed: u64) -> Self {
        let n = all_msisdns.len();
        let mut books: HashMap<u64, Vec<u64>> = HashMap::with_capacity(n);
        if n < 2 {
            for &m in all_msisdns {
                books.insert(m, Vec::new());
            }
            return ContactBook { books };
        }

        let effective_max = MAX_DEGREE.min(n - 1);
        let effective_min = MIN_DEGREE.min(effective_max).max(1);

        let mut rng = StdRng::seed_from_u64(book_seed);

        for (idx, &msisdn) in all_msisdns.iter().enumerate() {
            let degree = sample_zipf_degree(ZIPF_A, effective_min, effective_max, &mut rng)
                .min(n - 1);
            if degree == 0 {
                books.insert(msisdn, Vec::new());
                continue;
            }

            // O(k): сэмплируем индексы напрямую из пула размера n, не строя
            // список кандидатов длиной n-1 (в отличие от contact_book.py:82).
            // Просим на один индекс больше, чтобы после отсева "себя" всё
            // равно осталось ровно `degree` контактов почти всегда.
            let want = (degree + 1).min(n);
            let mut idxs = sample(&mut rng, n, want).into_vec();
            idxs.retain(|&i| i != idx);
            idxs.truncate(degree);

            let contacts: Vec<u64> = idxs.iter().map(|&i| all_msisdns[i]).collect();
            books.insert(msisdn, contacts);
        }

        ContactBook { books }
    }

    /// Круг общения абонента; пусто — либо degree=0 выпал, либо msisdn
    /// в книгу не попал (не должно случаться при полном `all_msisdns`).
    pub fn contacts_of(&self, msisdn: u64) -> &[u64] {
        self.books.get(&msisdn).map(|v| v.as_slice()).unwrap_or(&[])
    }
}

/// Портировано из `contact_book.py::_sample_zipf_degree` — та же формула
/// клампинга Zipf, чтобы разброс размера круга (а значит и хвост веса пар,
/// см. врезку выше) воспроизводил характер питоновского, а не только среднее.
fn sample_zipf_degree(a: f64, min_val: usize, max_val: usize, rng: &mut StdRng) -> usize {
    if max_val <= min_val {
        return min_val;
    }
    let a = if a <= 1.0 { 1.01 } else { a };

    for _ in 0..100 {
        let mut u: f64 = rng.gen();
        if u == 0.0 {
            u = 1e-10;
        }
        let raw = (1.0 - u).powf(-1.0 / (a - 1.0));
        let degree = min_val as i64 + raw as i64 - 1;
        let degree = degree.max(min_val as i64) as usize;
        if degree <= max_val {
            return degree;
        }
    }
    min_val
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_from_seed_and_population() {
        let pool: Vec<u64> = (1000..1500).collect();
        let a = ContactBook::build(&pool, 42);
        let b = ContactBook::build(&pool, 42);
        for &m in &pool {
            assert_eq!(a.contacts_of(m), b.contacts_of(m));
        }
    }

    #[test]
    fn independent_of_caller_supplied_period_or_worker_axis() {
        // Книга не принимает на вход ни день, ни номер воркера — доказательство
        // архитектурное (сигнатура build), но тест фиксирует хотя бы то, что
        // один и тот же (пул, seed) при разных количествах повторных вызовов
        // build (эмулируя "разные дни зовут build с тем же seed") даёт то же
        // самое дерево контактов.
        let pool: Vec<u64> = (1..300).collect();
        let first = ContactBook::build(&pool, 7);
        for _ in 0..5 {
            let again = ContactBook::build(&pool, 7);
            for &m in &pool {
                assert_eq!(first.contacts_of(m), again.contacts_of(m));
            }
        }
    }

    #[test]
    fn degree_within_bounds_and_no_self_contact() {
        let pool: Vec<u64> = (1..2000).collect();
        let book = ContactBook::build(&pool, 1);
        for &m in &pool {
            let c = book.contacts_of(m);
            assert!(c.len() <= MAX_DEGREE);
            assert!(!c.contains(&m));
        }
    }

    #[test]
    fn memory_is_o_of_n_times_k_not_n_squared() {
        // Не измеряет RSS (это делает офлайн-профиль, см. README/отчёт) —
        // проверяет структурный инвариант: суммарная длина книг растёт
        // линейно с N при фиксированных параметрах Zipf, а не квадратично.
        let small: Vec<u64> = (1..1000).collect();
        let large: Vec<u64> = (1..8000).collect();
        let book_small = ContactBook::build(&small, 99);
        let book_large = ContactBook::build(&large, 99);
        let total_small: usize = small.iter().map(|m| book_small.contacts_of(*m).len()).sum();
        let total_large: usize = large.iter().map(|m| book_large.contacts_of(*m).len()).sum();
        // 8x абонентов не должно дать далеко за 8x суммарных контактов
        // (при квадратичном росте было бы ~64x).
        assert!(total_large < total_small * 20, "{total_small} -> {total_large}");
    }
}
