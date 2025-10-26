// Subscriber identity management: MSISDN, IMSI, IMEI, MCCMNC
use rand::Rng;
use rand::rngs::StdRng;

#[derive(Debug, Clone, Copy)]
pub struct Subscriber {
    pub msisdn: u64,  // Numeric MSISDN (e.g., 31612345678)
    pub imsi: u64,    // Numeric IMSI
    pub mccmnc: u32,  // Numeric MCCMNC (e.g., 20408)
    pub imei: u64,    // Numeric IMEI (15 digits fits in u64)
}

#[derive(Debug, Clone)]
pub struct Contacts {
    pub pool: Vec<usize>,  // Indices of contacts within the shard
    pub probs: Vec<f64>,   // Zipf-like probabilities
}

/// Generate a valid 15-digit IMEI with Luhn checksum
/// Format: TAC (8 digits) + SNR (6 digits) + check digit
/// Returns numeric IMEI as u64
pub fn gen_imei(rng: &mut StdRng) -> u64 {
    // Generate first 14 digits
    let tac = rng.gen_range(10_000_000u64..100_000_000u64); // 8 digits
    let snr = rng.gen_range(100_000u64..1_000_000u64);      // 6 digits
    let base = tac * 1_000_000 + snr;

    // Calculate Luhn check digit
    let mut total = 0;
    let mut temp = base;
    for i in 0..14 {
        let mut d = (temp % 10) as i32;
        temp /= 10;
        if i % 2 == 1 {  // Every second digit from right
            d *= 2;
            if d > 9 {
                d -= 9;
            }
        }
        total += d;
    }

    let check = (10 - (total % 10)) % 10;
    base * 10 + check as u64
}

/// Build stable subscriber identities
/// Each subscriber gets consistent MSISDN ↔ IMSI ↔ MCCMNC ↔ IMEI
/// Note: prefixes and mccmnc_pool are now expected to be numeric strings
pub fn build_subscribers(
    n_users: usize,
    prefixes: &[String],
    mccmnc_pool: &[String],
    rng: &mut StdRng,
) -> Vec<Subscriber> {
    let mut subs = Vec::with_capacity(n_users);

    for _ in 0..n_users {
        // Parse prefix to u64 and append subscriber number
        let prefix_str = &prefixes[rng.gen_range(0..prefixes.len())];
        let prefix_num: u64 = prefix_str.parse().unwrap_or(31612);
        let subscriber_number = rng.gen_range(0..10_000_000u64);
        let msisdn = prefix_num * 10_000_000 + subscriber_number;

        // Parse MCCMNC to u32 and append MSIN
        let mccmnc_str = &mccmnc_pool[rng.gen_range(0..mccmnc_pool.len())];
        let mccmnc: u32 = mccmnc_str.parse().unwrap_or(20408);
        let msin = rng.gen_range(0..10_000_000_000u64);  // 10 digits
        let imsi = (mccmnc as u64) * 10_000_000_000 + msin;

        let imei = gen_imei(rng);

        subs.push(Subscriber {
            msisdn,
            imsi,
            mccmnc,
            imei,
        });
    }

    subs
}

/// Build contact networks with Zipf-like distribution
/// Users call their close contacts more frequently
pub fn build_contacts(
    n_users: usize,
    avg_contacts: usize,
    rng: &mut StdRng,
) -> Vec<Contacts> {
    use rand_distr::{Normal, Distribution};

    let mut contacts_list = Vec::with_capacity(n_users);
    let normal = Normal::new(avg_contacts as f64, avg_contacts as f64 * 0.3).unwrap();

    for _ in 0..n_users {
        // Sample number of contacts
        let n_contacts_f = normal.sample(rng);
        let mut n_contacts = n_contacts_f.max(0.0) as usize;
        n_contacts = n_contacts.min(n_users.saturating_sub(1));

        if n_contacts == 0 {
            contacts_list.push(Contacts {
                pool: Vec::new(),
                probs: Vec::new(),
            });
            continue;
        }

        // Random sample of contact indices
        let mut pool: Vec<usize> = (0..n_users).collect();
        // Fisher-Yates shuffle for random sampling
        for i in (n_contacts..pool.len()).rev() {
            let j = rng.gen_range(0..=i);
            pool.swap(i, j);
        }
        pool.truncate(n_contacts);

        // Zipf-like distribution for contact frequencies
        // More frequently contacted people have higher weights
        let weights: Vec<f64> = (0..n_contacts)
            .map(|rank| 1.0 / (rank + 1) as f64)
            .collect();
        let total: f64 = weights.iter().sum();
        let probs: Vec<f64> = weights.iter().map(|w| w / total).collect();

        contacts_list.push(Contacts { pool, probs });
    }

    contacts_list
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn test_gen_imei() {
        let mut rng = StdRng::seed_from_u64(42);
        let imei = gen_imei(&mut rng);
        // IMEI should be 15 digits (fits in u64)
        assert!(imei >= 100_000_000_000_000);
        assert!(imei < 1_000_000_000_000_000);
    }

    #[test]
    fn test_build_subscribers() {
        let mut rng = StdRng::seed_from_u64(42);
        let prefixes = vec!["31612".to_string(), "31613".to_string()];
        let mccmnc_pool = vec!["20408".to_string(), "20416".to_string()];

        let subs = build_subscribers(10, &prefixes, &mccmnc_pool, &mut rng);
        assert_eq!(subs.len(), 10);

        for sub in &subs {
            // Check IMEI is 15 digits
            assert!(sub.imei >= 100_000_000_000_000);
            assert!(sub.imei < 1_000_000_000_000_000);
            // Check MSISDN starts with 316
            assert!(sub.msisdn >= 31612_0000000 && sub.msisdn < 31614_0000000);
            // Check MCCMNC is valid
            assert!(sub.mccmnc == 20408 || sub.mccmnc == 20416);
        }
    }

    #[test]
    fn test_build_contacts() {
        let mut rng = StdRng::seed_from_u64(42);
        let contacts = build_contacts(100, 30, &mut rng);
        assert_eq!(contacts.len(), 100);

        for c in &contacts {
            if !c.pool.is_empty() {
                assert_eq!(c.pool.len(), c.probs.len());
                let sum: f64 = c.probs.iter().sum();
                assert!((sum - 1.0).abs() < 0.001);
            }
        }
    }
}
