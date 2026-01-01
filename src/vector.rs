use anyhow::{Result, anyhow};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

pub struct VectorIndex {
    dims: usize,
    path: PathBuf,
    index: Index,
    doc_id_set: HashSet<u64>,
}

impl VectorIndex {
    pub fn open_or_create(dir: &Path, dimensions: usize) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let index_path = dir.join("usearch.index");
        let ids_path = dir.join("doc_ids.bin");

        // Check if existing index has different dimensions
        if index_path.exists() {
            let existing = Index::new(&IndexOptions::default())?;
            existing.load(index_path.to_str().ok_or_else(|| anyhow!("invalid path"))?)?;
            if existing.dimensions() != dimensions {
                // Dimension mismatch, remove old files
                let _ = fs::remove_file(&index_path);
                let _ = fs::remove_file(&ids_path);
            }
        }

        let options = IndexOptions {
            dimensions,
            metric: MetricKind::Cos,
            quantization: ScalarKind::F32,
            ..IndexOptions::default()
        };

        let index = Index::new(&options)?;

        let doc_id_set = if index_path.exists() {
            index.load(index_path.to_str().ok_or_else(|| anyhow!("invalid path"))?)?;
            if ids_path.exists() {
                load_doc_ids(&ids_path)?
            } else {
                HashSet::new()
            }
        } else {
            index.reserve(10000)?;
            HashSet::new()
        };

        Ok(Self {
            dims: dimensions,
            path: dir.to_path_buf(),
            index,
            doc_id_set,
        })
    }

    pub fn open(dir: &Path) -> Result<Self> {
        let index_path = dir.join("usearch.index");
        let ids_path = dir.join("doc_ids.bin");

        if !index_path.exists() {
            return Err(anyhow!("vector index not found"));
        }

        let index = Index::new(&IndexOptions::default())?;
        index.load(index_path.to_str().ok_or_else(|| anyhow!("invalid path"))?)?;

        let doc_id_set = if ids_path.exists() {
            load_doc_ids(&ids_path)?
        } else {
            HashSet::new()
        };

        Ok(Self {
            dims: index.dimensions(),
            path: dir.to_path_buf(),
            index,
            doc_id_set,
        })
    }

    pub fn add(&mut self, doc_id: u64, embedding: &[f32]) -> Result<()> {
        if embedding.len() != self.dims {
            return Err(anyhow!(
                "embedding dimensions mismatch: expected {}, got {}",
                self.dims,
                embedding.len()
            ));
        }
        if !self.doc_id_set.insert(doc_id) {
            return Ok(());
        }

        // Expand capacity if needed
        if self.index.size() >= self.index.capacity() {
            let new_capacity = (self.index.capacity() * 2).max(10000);
            self.index.reserve(new_capacity)?;
        }

        self.index.add(doc_id, embedding)?;
        Ok(())
    }

    pub fn search(&self, embedding: &[f32], limit: usize) -> Result<Vec<(u64, f32)>> {
        if embedding.len() != self.dims {
            return Err(anyhow!(
                "embedding dimensions mismatch: expected {}, got {}",
                self.dims,
                embedding.len()
            ));
        }
        if self.index.size() == 0 {
            return Ok(Vec::new());
        }

        let results = self.index.search(embedding, limit)?;
        Ok(results.keys.into_iter().zip(results.distances).collect())
    }

    pub fn save(&self) -> Result<()> {
        let index_path = self.path.join("usearch.index");
        let ids_path = self.path.join("doc_ids.bin");

        // Save index
        self.index
            .save(index_path.to_str().ok_or_else(|| anyhow!("invalid path"))?)?;

        // Save doc_ids
        save_doc_ids(&ids_path, &self.doc_id_set)?;

        Ok(())
    }

    pub fn contains(&self, doc_id: u64) -> bool {
        self.doc_id_set.contains(&doc_id)
    }

    #[allow(dead_code)]
    pub fn dimensions(&self) -> usize {
        self.dims
    }
}

fn load_doc_ids(path: &Path) -> Result<HashSet<u64>> {
    let bytes = fs::read(path)?;
    let ids: Vec<u64> = bytes
        .chunks_exact(8)
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
        .collect();
    Ok(ids.into_iter().collect())
}

fn save_doc_ids(path: &Path, ids: &HashSet<u64>) -> Result<()> {
    let mut bytes = Vec::with_capacity(ids.len() * 8);
    for id in ids {
        bytes.extend_from_slice(&id.to_le_bytes());
    }
    let tmp = path.with_extension("bin.tmp");
    fs::write(&tmp, &bytes)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_vector(dims: usize, seed: f32) -> Vec<f32> {
        (0..dims).map(|i| (i as f32 + seed).sin()).collect()
    }

    #[test]
    fn test_create_and_add() {
        let tmp = TempDir::new().unwrap();
        let mut idx = VectorIndex::open_or_create(tmp.path(), 64).unwrap();

        let v1 = make_vector(64, 1.0);
        idx.add(1, &v1).unwrap();

        assert!(idx.contains(1));
        assert!(!idx.contains(2));
        assert_eq!(idx.dimensions(), 64);
    }

    #[test]
    fn test_duplicate_add_ignored() {
        let tmp = TempDir::new().unwrap();
        let mut idx = VectorIndex::open_or_create(tmp.path(), 64).unwrap();

        let v1 = make_vector(64, 1.0);
        idx.add(1, &v1).unwrap();
        idx.add(1, &v1).unwrap(); // duplicate

        assert!(idx.contains(1));
    }

    #[test]
    fn test_dimension_mismatch_error() {
        let tmp = TempDir::new().unwrap();
        let mut idx = VectorIndex::open_or_create(tmp.path(), 64).unwrap();

        let wrong_dims = make_vector(32, 1.0);
        let result = idx.add(1, &wrong_dims);
        assert!(result.is_err());
    }

    #[test]
    fn test_search_empty_index() {
        let tmp = TempDir::new().unwrap();
        let idx = VectorIndex::open_or_create(tmp.path(), 64).unwrap();

        let query = make_vector(64, 1.0);
        let results = idx.search(&query, 10).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_returns_nearest() {
        let tmp = TempDir::new().unwrap();
        let mut idx = VectorIndex::open_or_create(tmp.path(), 64).unwrap();

        let v1 = make_vector(64, 1.0);
        let v2 = make_vector(64, 2.0);
        let v3 = make_vector(64, 3.0);

        idx.add(1, &v1).unwrap();
        idx.add(2, &v2).unwrap();
        idx.add(3, &v3).unwrap();

        // Search with v1 as query, should return v1 first (distance ~0)
        let results = idx.search(&v1, 3).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 1); // v1 should be first match
        assert!(results[0].1 < 0.01); // distance should be near zero
    }

    #[test]
    fn test_save_and_reload() {
        let tmp = TempDir::new().unwrap();

        // Create and populate index
        {
            let mut idx = VectorIndex::open_or_create(tmp.path(), 64).unwrap();
            let v1 = make_vector(64, 1.0);
            let v2 = make_vector(64, 2.0);
            idx.add(100, &v1).unwrap();
            idx.add(200, &v2).unwrap();
            idx.save().unwrap();
        }

        // Reload and verify
        {
            let idx = VectorIndex::open(tmp.path()).unwrap();
            assert!(idx.contains(100));
            assert!(idx.contains(200));
            assert!(!idx.contains(300));
            assert_eq!(idx.dimensions(), 64);

            // Verify search still works
            let query = make_vector(64, 1.0);
            let results = idx.search(&query, 2).unwrap();
            assert_eq!(results.len(), 2);
            assert_eq!(results[0].0, 100);
        }
    }

    #[test]
    fn test_open_nonexistent_fails() {
        let tmp = TempDir::new().unwrap();
        let result = VectorIndex::open(tmp.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_dimension_change_resets_index() {
        let tmp = TempDir::new().unwrap();

        // Create index with 64 dims
        {
            let mut idx = VectorIndex::open_or_create(tmp.path(), 64).unwrap();
            let v = make_vector(64, 1.0);
            idx.add(1, &v).unwrap();
            idx.save().unwrap();
        }

        // Reopen with different dims, should reset
        {
            let idx = VectorIndex::open_or_create(tmp.path(), 128).unwrap();
            assert!(!idx.contains(1)); // old data should be gone
            assert_eq!(idx.dimensions(), 128);
        }
    }

    #[test]
    fn test_search_with_limit() {
        let tmp = TempDir::new().unwrap();
        let mut idx = VectorIndex::open_or_create(tmp.path(), 64).unwrap();

        for i in 0..10 {
            let v = make_vector(64, i as f32);
            idx.add(i, &v).unwrap();
        }

        let query = make_vector(64, 0.0);
        let results = idx.search(&query, 3).unwrap();
        assert_eq!(results.len(), 3);
    }
}
