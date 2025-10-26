// Cell tower (base station) generation and management
use csv::{Writer, Reader};
use rand::Rng;
use rand::rngs::StdRng;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::f64::consts::PI;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cell {
    pub cell_id: u32,
    pub lat: f64,
    pub lon: f64,
    pub rat: String,
}

fn deg_per_km_lat() -> f64 {
    1.0 / 111.0
}

fn deg_per_km_lon(lat_deg: f64) -> f64 {
    1.0 / (111.320 * lat_deg.to_radians().cos()).max(1e-9)
}

/// Generate cell towers distributed in a circular area
/// Uses sqrt for uniform distribution in circle
pub fn generate_cells(
    n_cells: usize,
    center_lat: f64,
    center_lon: f64,
    radius_km: f64,
    seed: u64,
) -> Vec<Cell> {
    let mut rng = StdRng::seed_from_u64(seed);
    let rats = vec!["WCDMA", "LTE", "NR"];
    let rat_weights = [0.3, 0.5, 0.2]; // 3G, 4G, 5G distribution

    let lat_step = deg_per_km_lat();
    let lon_step = deg_per_km_lon(center_lat);

    let mut cells = Vec::with_capacity(n_cells);

    for cid in 1..=n_cells {
        // Uniform distribution in circle: sqrt for radius
        let r = radius_km * rng.gen::<f64>().sqrt();
        let theta = rng.gen::<f64>() * 2.0 * PI;

        let dlat = r * theta.sin() * lat_step;
        let dlon = r * theta.cos() * lon_step;

        let lat = center_lat + dlat;
        let lon = center_lon + dlon;

        // Weighted random choice for RAT
        let rat_choice = rng.gen::<f64>();
        let rat = if rat_choice < rat_weights[0] {
            rats[0]
        } else if rat_choice < rat_weights[0] + rat_weights[1] {
            rats[1]
        } else {
            rats[2]
        };

        cells.push(Cell {
            cell_id: cid as u32,
            lat: (lat * 1_000_000.0).round() / 1_000_000.0,
            lon: (lon * 1_000_000.0).round() / 1_000_000.0,
            rat: rat.to_string(),
        });
    }

    cells
}

/// Create cells.csv if it doesn't exist, return path
pub fn ensure_cells_catalog(
    out_dir: &Path,
    n_cells: usize,
    center_lat: f64,
    center_lon: f64,
    radius_km: f64,
    seed: u64,
) -> anyhow::Result<PathBuf> {
    std::fs::create_dir_all(out_dir)?;
    let cells_path = out_dir.join("cells.csv");

    if !cells_path.exists() {
        let cells = generate_cells(n_cells, center_lat, center_lon, radius_km, seed);
        let mut wtr = Writer::from_path(&cells_path)?;

        wtr.write_record(&["cell_id", "lat", "lon", "rat"])?;
        for c in cells {
            wtr.write_record(&[
                c.cell_id.to_string(),
                c.lat.to_string(),
                c.lon.to_string(),
                c.rat,
            ])?;
        }
        wtr.flush()?;
    }

    Ok(cells_path)
}

/// Load cells catalog and return:
/// - List of all cell IDs
/// - HashMap mapping RAT -> list of cell IDs
pub fn load_cells_catalog(cells_path: &Path) -> anyhow::Result<(Vec<u32>, HashMap<String, Vec<u32>>)> {
    let mut cells = Vec::new();
    let mut by_rat: HashMap<String, Vec<u32>> = HashMap::new();

    let mut rdr = Reader::from_path(cells_path)?;
    for result in rdr.deserialize() {
        let cell: Cell = result?;
        cells.push(cell.cell_id);
        by_rat
            .entry(cell.rat.clone())
            .or_insert_with(Vec::new)
            .push(cell.cell_id);
    }

    Ok((cells, by_rat))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_cells() {
        let cells = generate_cells(100, 52.37, 4.895, 50.0, 42);
        assert_eq!(cells.len(), 100);

        for cell in &cells {
            assert!(cell.cell_id > 0);
            assert!(cell.lat > 50.0 && cell.lat < 55.0);
            assert!(cell.lon > 2.0 && cell.lon < 8.0);
            assert!(["WCDMA", "LTE", "NR"].contains(&cell.rat.as_str()));
        }
    }

    #[test]
    fn test_ensure_and_load_cells() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = ensure_cells_catalog(dir.path(), 50, 52.37, 4.895, 10.0, 123).unwrap();
        assert!(path.exists());

        let (cells, by_rat) = load_cells_catalog(&path).unwrap();
        assert_eq!(cells.len(), 50);
        assert!(!by_rat.is_empty());
    }
}
