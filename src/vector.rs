use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VectorMetadata {
    #[serde(default)]
    format_version: u32,
    dimensions: usize,
    model: Option<String>,
    index_file: String,
    ids_file: String,
}

pub struct VectorIndex {
    dims: usize,
    model: Option<String>,
    path: PathBuf,
    index: Index,
    vector_id_set: HashSet<u64>,
    needs_backfill: bool,
}

impl VectorIndex {
    pub fn reset(dir: &Path) -> Result<()> {
        if dir.exists() {
            fs::remove_dir_all(dir)?;
        }
        fs::create_dir_all(dir)?;
        Ok(())
    }

    pub fn open_or_create(dir: &Path, dimensions: usize, model: Option<&str>) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let index_path = dir.join("usearch.index");
        let ids_path = dir.join("vector_ids.bin");
        let meta_path = dir.join("meta.json");
        let mut needs_backfill = false;
        let model = model.map(str::to_string);

        // Check if existing index has different dimensions or an incompatible embedding model.
        if index_path.exists() {
            let existing = Index::new(&IndexOptions::default())?;
            existing.load(index_path.to_str().ok_or_else(|| anyhow!("invalid path"))?)?;
            let existing_meta = load_metadata_if_exists(&meta_path)?;
            let format_mismatch = existing_meta
                .as_ref()
                .is_none_or(|meta| meta.format_version != 2);
            let model_mismatch = match (&existing_meta, &model) {
                (Some(meta), Some(model)) => meta.model.as_deref() != Some(model),
                (None, Some(_)) => true,
                _ => false,
            };
            if existing.dimensions() != dimensions || model_mismatch || format_mismatch {
                // Dimension or model mismatch; remove the old vector store and backfill.
                let _ = fs::remove_file(&index_path);
                let _ = fs::remove_file(&ids_path);
                let _ = fs::remove_file(&meta_path);
                needs_backfill = true;
            }
        }

        let options = IndexOptions {
            dimensions,
            metric: MetricKind::Cos,
            quantization: ScalarKind::F32,
            ..IndexOptions::default()
        };

        let index = Index::new(&options)?;

        let vector_id_set = if index_path.exists() {
            index.load(index_path.to_str().ok_or_else(|| anyhow!("invalid path"))?)?;
            if ids_path.exists() {
                load_vector_ids(&ids_path)?
            } else {
                HashSet::new()
            }
        } else {
            index.reserve(10000)?;
            needs_backfill = true;
            HashSet::new()
        };

        Ok(Self {
            dims: dimensions,
            model,
            path: dir.to_path_buf(),
            index,
            vector_id_set,
            needs_backfill,
        })
    }

    pub fn open(dir: &Path) -> Result<Self> {
        let index_path = dir.join("usearch.index");
        let ids_path = dir.join("vector_ids.bin");
        let meta_path = dir.join("meta.json");

        if !index_path.exists() {
            return Err(anyhow!("vector index not found"));
        }

        let index = Index::new(&IndexOptions::default())?;
        index.load(index_path.to_str().ok_or_else(|| anyhow!("invalid path"))?)?;

        let metadata = load_metadata_if_exists(&meta_path)?;
        if metadata
            .as_ref()
            .is_none_or(|meta| meta.format_version != 2)
        {
            return Err(anyhow!("vector index format is stale"));
        }
        let vector_id_set = if ids_path.exists() {
            load_vector_ids(&ids_path)?
        } else {
            HashSet::new()
        };
        let model = metadata.and_then(|meta| meta.model);

        Ok(Self {
            dims: index.dimensions(),
            model,
            path: dir.to_path_buf(),
            index,
            vector_id_set,
            needs_backfill: false,
        })
    }

    pub fn add(&mut self, vector_id: u64, embedding: &[f32]) -> Result<()> {
        if embedding.len() != self.dims {
            return Err(anyhow!(
                "embedding dimensions mismatch: expected {}, got {}",
                self.dims,
                embedding.len()
            ));
        }
        if !self.vector_id_set.insert(vector_id) {
            return Ok(());
        }

        // Expand capacity if needed
        if self.index.size() >= self.index.capacity() {
            let new_capacity = (self.index.capacity() * 2).max(10000);
            self.index.reserve(new_capacity)?;
        }

        self.index.add(vector_id, embedding)?;
        Ok(())
    }

    pub fn remove(&mut self, vector_id: u64) -> Result<bool> {
        if !self.vector_id_set.remove(&vector_id) {
            return Ok(false);
        }
        self.index.remove(vector_id)?;
        Ok(true)
    }

    pub fn remove_many(&mut self, vector_ids: impl IntoIterator<Item = u64>) -> Result<usize> {
        let mut removed = 0;
        for vector_id in vector_ids {
            removed += usize::from(self.remove(vector_id)?);
        }
        Ok(removed)
    }

    pub fn vector_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.vector_id_set.iter().copied()
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
        let ids_path = self.path.join("vector_ids.bin");
        let meta_path = self.path.join("meta.json");

        // Save index
        self.index
            .save(index_path.to_str().ok_or_else(|| anyhow!("invalid path"))?)?;

        save_vector_ids(&ids_path, &self.vector_id_set)?;
        save_metadata(
            &meta_path,
            &VectorMetadata {
                format_version: 2,
                dimensions: self.dims,
                model: self.model.clone(),
                index_file: "usearch.index".to_string(),
                ids_file: "vector_ids.bin".to_string(),
            },
        )?;

        Ok(())
    }

    pub fn contains(&self, vector_id: u64) -> bool {
        self.vector_id_set.contains(&vector_id)
    }

    pub fn len(&self) -> usize {
        self.index.size()
    }

    pub fn is_empty(&self) -> bool {
        self.index.size() == 0
    }

    pub fn vector_id_count(&self) -> usize {
        self.vector_id_set.len()
    }

    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    pub fn needs_backfill(&self) -> bool {
        self.needs_backfill
    }

    #[allow(dead_code)]
    pub fn dimensions(&self) -> usize {
        self.dims
    }
}

fn load_metadata(path: &Path) -> Result<VectorMetadata> {
    let data = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&data)?)
}

fn load_metadata_if_exists(path: &Path) -> Result<Option<VectorMetadata>> {
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(load_metadata(path)?))
}

fn save_metadata(path: &Path, metadata: &VectorMetadata) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_string_pretty(metadata)?)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

fn load_vector_ids(path: &Path) -> Result<HashSet<u64>> {
    let bytes = fs::read(path)?;
    let ids: Vec<u64> = bytes
        .chunks_exact(8)
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
        .collect();
    Ok(ids.into_iter().collect())
}

fn save_vector_ids(path: &Path, ids: &HashSet<u64>) -> Result<()> {
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
        let mut idx = VectorIndex::open_or_create(tmp.path(), 64, Some("test")).unwrap();

        let v1 = make_vector(64, 1.0);
        idx.add(1, &v1).unwrap();

        assert!(idx.contains(1));
        assert!(!idx.contains(2));
        assert_eq!(idx.dimensions(), 64);
    }

    #[test]
    fn reset_removes_all_existing_vector_state() {
        let tmp = TempDir::new().unwrap();
        let mut idx = VectorIndex::open_or_create(tmp.path(), 64, Some("test")).unwrap();
        idx.add(1, &make_vector(64, 1.0)).unwrap();
        idx.save().unwrap();
        assert!(tmp.path().join("usearch.index").exists());

        VectorIndex::reset(tmp.path()).unwrap();

        assert!(tmp.path().exists());
        assert!(!tmp.path().join("usearch.index").exists());
        assert!(!tmp.path().join("doc_ids.bin").exists());
        assert!(!tmp.path().join("vector_ids.bin").exists());
        assert!(!tmp.path().join("meta.json").exists());
    }

    #[test]
    fn test_duplicate_add_ignored() {
        let tmp = TempDir::new().unwrap();
        let mut idx = VectorIndex::open_or_create(tmp.path(), 64, Some("test")).unwrap();

        let v1 = make_vector(64, 1.0);
        idx.add(1, &v1).unwrap();
        idx.add(1, &v1).unwrap(); // duplicate

        assert!(idx.contains(1));
    }

    #[test]
    fn remove_deletes_vector_and_persisted_id() {
        let tmp = TempDir::new().unwrap();
        let mut idx = VectorIndex::open_or_create(tmp.path(), 64, Some("test")).unwrap();
        idx.add(1, &make_vector(64, 1.0)).unwrap();
        idx.add(2, &make_vector(64, 2.0)).unwrap();

        assert!(idx.remove(1).unwrap());
        assert!(!idx.remove(1).unwrap());
        idx.save().unwrap();

        let reopened = VectorIndex::open(tmp.path()).unwrap();
        assert!(!reopened.contains(1));
        assert!(reopened.contains(2));
        assert_eq!(reopened.len(), 1);
    }

    #[test]
    fn test_dimension_mismatch_error() {
        let tmp = TempDir::new().unwrap();
        let mut idx = VectorIndex::open_or_create(tmp.path(), 64, Some("test")).unwrap();

        let wrong_dims = make_vector(32, 1.0);
        let result = idx.add(1, &wrong_dims);
        assert!(result.is_err());
    }

    #[test]
    fn test_search_empty_index() {
        let tmp = TempDir::new().unwrap();
        let idx = VectorIndex::open_or_create(tmp.path(), 64, Some("test")).unwrap();

        let query = make_vector(64, 1.0);
        let results = idx.search(&query, 10).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_returns_nearest() {
        let tmp = TempDir::new().unwrap();
        let mut idx = VectorIndex::open_or_create(tmp.path(), 64, Some("test")).unwrap();

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
            let mut idx = VectorIndex::open_or_create(tmp.path(), 64, Some("test")).unwrap();
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
    fn test_save_writes_model_metadata() {
        let tmp = TempDir::new().unwrap();

        {
            let mut idx = VectorIndex::open_or_create(tmp.path(), 64, Some("bge")).unwrap();
            idx.add(100, &make_vector(64, 1.0)).unwrap();
            idx.save().unwrap();
        }

        let metadata: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(tmp.path().join("meta.json")).unwrap())
                .unwrap();
        assert_eq!(metadata["dimensions"], 64);
        assert_eq!(metadata["format_version"], 2);
        assert_eq!(metadata["model"], "bge");
        assert_eq!(metadata["index_file"], "usearch.index");
        assert_eq!(metadata["ids_file"], "vector_ids.bin");
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
            let mut idx = VectorIndex::open_or_create(tmp.path(), 64, Some("test")).unwrap();
            let v = make_vector(64, 1.0);
            idx.add(1, &v).unwrap();
            idx.save().unwrap();
        }

        // Reopen with different dims, should reset
        {
            let idx = VectorIndex::open_or_create(tmp.path(), 128, Some("test")).unwrap();
            assert!(!idx.contains(1)); // old data should be gone
            assert_eq!(idx.dimensions(), 128);
        }
    }

    #[test]
    fn test_model_change_resets_index() {
        let tmp = TempDir::new().unwrap();

        {
            let mut idx = VectorIndex::open_or_create(tmp.path(), 64, Some("alpha")).unwrap();
            let v = make_vector(64, 1.0);
            idx.add(1, &v).unwrap();
            idx.save().unwrap();
        }

        {
            let idx = VectorIndex::open_or_create(tmp.path(), 64, Some("beta")).unwrap();
            assert!(!idx.contains(1));
            assert_eq!(idx.dimensions(), 64);
            assert_eq!(idx.model(), Some("beta"));
            assert!(idx.needs_backfill());
        }
    }

    #[test]
    fn test_missing_model_metadata_resets_when_model_specified() {
        let tmp = TempDir::new().unwrap();

        {
            let mut idx = VectorIndex::open_or_create(tmp.path(), 64, Some("alpha")).unwrap();
            let v = make_vector(64, 1.0);
            idx.add(1, &v).unwrap();
            idx.save().unwrap();
        }

        fs::remove_file(tmp.path().join("meta.json")).unwrap();

        {
            let idx = VectorIndex::open_or_create(tmp.path(), 64, Some("alpha")).unwrap();
            assert!(!idx.contains(1));
            assert_eq!(idx.model(), Some("alpha"));
            assert!(idx.needs_backfill());
        }
    }

    #[test]
    fn test_corrupt_model_metadata_errors() {
        let tmp = TempDir::new().unwrap();

        {
            let mut idx = VectorIndex::open_or_create(tmp.path(), 64, Some("alpha")).unwrap();
            idx.add(1, &make_vector(64, 1.0)).unwrap();
            idx.save().unwrap();
        }

        fs::write(tmp.path().join("meta.json"), "{").unwrap();

        assert!(VectorIndex::open(tmp.path()).is_err());
        assert!(VectorIndex::open_or_create(tmp.path(), 64, Some("alpha")).is_err());
    }

    #[test]
    fn test_open_exposes_model_metadata_for_compatibility_checks() {
        let tmp = TempDir::new().unwrap();

        {
            let mut idx = VectorIndex::open_or_create(tmp.path(), 64, Some("alpha")).unwrap();
            idx.add(1, &make_vector(64, 1.0)).unwrap();
            idx.save().unwrap();
        }

        let idx = VectorIndex::open(tmp.path()).unwrap();
        assert_eq!(idx.model(), Some("alpha"));
        assert_eq!(idx.dimensions(), 64);
    }

    #[test]
    fn test_search_with_limit() {
        let tmp = TempDir::new().unwrap();
        let mut idx = VectorIndex::open_or_create(tmp.path(), 64, Some("test")).unwrap();

        for i in 0..10 {
            let v = make_vector(64, i as f32);
            idx.add(i, &v).unwrap();
        }

        let query = make_vector(64, 0.0);
        let results = idx.search(&query, 3).unwrap();
        assert_eq!(results.len(), 3);
    }
}
