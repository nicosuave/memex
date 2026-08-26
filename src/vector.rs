use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::Component;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

const CURRENT_GENERATION_FILE: &str = "current.json";
const GENERATIONS_DIR: &str = "generations";
static GENERATION_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VectorGenerationPointer {
    version: u32,
    generation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VectorMetadata {
    dimensions: usize,
    model: Option<String>,
    index_file: String,
    ids_file: String,
    #[serde(default)]
    vector_count: Option<usize>,
}

pub struct VectorIndex {
    dims: usize,
    model: Option<String>,
    root: PathBuf,
    active_path: Option<PathBuf>,
    index: Index,
    doc_id_set: HashSet<u64>,
    needs_backfill: bool,
}

pub(crate) struct StagedVectorGeneration {
    root: PathBuf,
    generation: String,
    cleanup_on_drop: bool,
}

impl StagedVectorGeneration {
    pub(crate) fn publish(mut self) -> Result<()> {
        // Once publication starts, keep the generation even on error: current.json may have
        // changed before the error was reported. A later successful save collects it safely.
        self.cleanup_on_drop = false;
        atomic_write_json(
            &self.root.join(CURRENT_GENERATION_FILE),
            &VectorGenerationPointer {
                version: 1,
                generation: self.generation.clone(),
            },
        )?;
        sync_directory(&self.root)?;
        cleanup_inactive_generations(&self.root, &self.generation);
        cleanup_legacy_files(&self.root);
        Ok(())
    }
}

impl Drop for StagedVectorGeneration {
    fn drop(&mut self) {
        if self.cleanup_on_drop {
            let _ = fs::remove_dir_all(self.root.join(GENERATIONS_DIR).join(&self.generation));
        }
    }
}

#[derive(Debug, Clone)]
pub struct VectorInventory {
    pub dimensions: usize,
    pub model: Option<String>,
    pub doc_ids: HashSet<u64>,
    pub vector_count: usize,
    pub index_bytes: u64,
    pub ids_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveStorage {
    path: PathBuf,
    generation: Option<String>,
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
        let model = model.map(str::to_string);
        if let Some(storage) = active_storage(dir)? {
            let existing = Self::open_active_snapshot(dir, storage)?;
            let model_matches = match model.as_deref() {
                Some(model) => existing.model() == Some(model),
                None => true,
            };
            if existing.dimensions() == dimensions && model_matches {
                return Ok(existing);
            }
        }

        Self::empty(dir, dimensions, model)
    }

    /// Start a complete replacement without deleting the active generation.
    pub(crate) fn empty_replacement(
        dir: &Path,
        dimensions: usize,
        model: Option<&str>,
    ) -> Result<Self> {
        fs::create_dir_all(dir)?;
        Self::empty(dir, dimensions, model.map(str::to_string))
    }

    pub fn open(dir: &Path) -> Result<Self> {
        let storage = active_storage(dir)?.ok_or_else(|| anyhow!("vector index not found"))?;
        Self::open_active_snapshot(dir, storage)
    }

    fn open_active_snapshot(root: &Path, mut storage: ActiveStorage) -> Result<Self> {
        loop {
            match Self::open_from_storage(root, &storage) {
                Ok(snapshot) => return Ok(snapshot),
                Err(load_error) => {
                    // A publisher may replace current.json and collect the old generation between
                    // our pointer read and opening its files. Retry only if the pointer changed.
                    let refreshed = active_storage(root)?;
                    if refreshed.as_ref().is_some_and(|active| active != &storage) {
                        storage = refreshed.expect("checked above");
                        continue;
                    }
                    if refreshed.is_none() {
                        return Err(anyhow!("vector index not found"));
                    }
                    return Err(load_error);
                }
            }
        }
    }

    fn open_from_storage(root: &Path, storage: &ActiveStorage) -> Result<Self> {
        let index_path = storage.path.join("usearch.index");
        let ids_path = storage.path.join("doc_ids.bin");
        let index = Index::new(&IndexOptions::default())?;
        index
            .load(path_str(&index_path)?)
            .with_context(|| format!("load vector index {}", index_path.display()))?;

        let ids_bytes = fs::read(&ids_path)
            .with_context(|| format!("read vector ID sidecar {}", ids_path.display()))?;
        let doc_id_set = decode_doc_ids(&ids_bytes, &ids_path)?;
        let metadata = load_storage_metadata(storage)?;
        validate_snapshot(&index, &doc_id_set, metadata.as_ref(), &storage.path)?;
        let model = metadata.and_then(|meta| meta.model);

        Ok(Self {
            dims: index.dimensions(),
            model,
            root: root.to_path_buf(),
            active_path: Some(storage.path.clone()),
            index,
            doc_id_set,
            needs_backfill: false,
        })
    }

    fn empty(root: &Path, dimensions: usize, model: Option<String>) -> Result<Self> {
        let options = IndexOptions {
            dimensions,
            metric: MetricKind::Cos,
            quantization: ScalarKind::F32,
            ..IndexOptions::default()
        };

        let index = Index::new(&options)?;
        index.reserve(10000)?;

        Ok(Self {
            dims: dimensions,
            model,
            root: root.to_path_buf(),
            active_path: None,
            index,
            doc_id_set: HashSet::new(),
            needs_backfill: true,
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

    /// Search until `limit` accepted candidates are found or the vector inventory is exhausted.
    pub(crate) fn search_filtered(
        &self,
        embedding: &[f32],
        limit: usize,
        accept: impl FnMut(u64) -> Result<bool>,
    ) -> Result<Vec<(u64, f32)>> {
        if limit == 0 || self.is_empty() {
            return Ok(Vec::new());
        }
        if embedding.len() != self.dims {
            return Err(anyhow!(
                "embedding dimensions mismatch: expected {}, got {}",
                self.dims,
                embedding.len()
            ));
        }

        let accept = RefCell::new(accept);
        let failure = RefCell::new(None);
        let matches = self
            .index
            .filtered_search(embedding, limit.min(self.len()), |doc_id| {
                if failure.borrow().is_some() {
                    return true;
                }
                match accept.borrow_mut()(doc_id) {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        *failure.borrow_mut() = Some(error);
                        true
                    }
                }
            })?;
        if let Some(error) = failure.into_inner() {
            return Err(error);
        }
        Ok(matches.keys.into_iter().zip(matches.distances).collect())
    }

    pub fn save(&self) -> Result<()> {
        self.stage()?.publish()
    }

    pub(crate) fn stage(&self) -> Result<StagedVectorGeneration> {
        fs::create_dir_all(&self.root)?;
        let generations = self.root.join(GENERATIONS_DIR);
        fs::create_dir_all(&generations)?;
        let generation = unique_generation_name();
        let temporary = generations.join(format!(".{generation}.tmp"));
        let final_path = generations.join(&generation);
        fs::create_dir(&temporary)?;

        let write_result = (|| -> Result<StagedVectorGeneration> {
            let index_path = temporary.join("usearch.index");
            let ids_path = temporary.join("doc_ids.bin");
            let meta_path = temporary.join("meta.json");
            self.index.save(path_str(&index_path)?)?;
            save_doc_ids(&ids_path, &self.doc_id_set)?;
            save_metadata(
                &meta_path,
                &VectorMetadata {
                    dimensions: self.dims,
                    model: self.model.clone(),
                    index_file: "usearch.index".to_string(),
                    ids_file: "doc_ids.bin".to_string(),
                    vector_count: Some(self.index.size()),
                },
            )?;
            sync_file(&index_path)?;
            sync_file(&ids_path)?;
            sync_file(&meta_path)?;

            let storage = ActiveStorage {
                path: temporary.clone(),
                generation: Some(generation.clone()),
            };
            drop(Self::open_from_storage(&self.root, &storage)?);
            sync_directory(&temporary)?;
            fs::rename(&temporary, &final_path)?;
            sync_directory(&generations)?;
            Ok(StagedVectorGeneration {
                root: self.root.clone(),
                generation: generation.clone(),
                cleanup_on_drop: true,
            })
        })();

        if write_result.is_err() {
            let _ = fs::remove_dir_all(&temporary);
        }
        write_result
    }

    pub(crate) fn retain_doc_ids(&mut self, retained: &HashSet<u64>) -> Result<()> {
        let removed: Vec<_> = self.doc_id_set.difference(retained).copied().collect();
        for doc_id in removed {
            self.index.remove(doc_id)?;
            self.doc_id_set.remove(&doc_id);
        }
        Ok(())
    }

    pub fn exists(dir: &Path) -> bool {
        dir.join(CURRENT_GENERATION_FILE).exists() || dir.join("usearch.index").exists()
    }

    pub fn inventory(dir: &Path) -> Result<Option<VectorInventory>> {
        if !Self::exists(dir) {
            return Ok(None);
        }
        let index = Self::open(dir)?;
        let storage = index
            .active_path
            .as_ref()
            .expect("opened vector index has active storage")
            .clone();
        let index_bytes = fs::metadata(storage.join("usearch.index"))?.len();
        let ids_bytes = fs::metadata(storage.join("doc_ids.bin"))?.len();
        let vector_count = index.index.size();
        Ok(Some(VectorInventory {
            dimensions: index.dims,
            model: index.model,
            doc_ids: index.doc_id_set,
            vector_count,
            index_bytes,
            ids_bytes,
        }))
    }

    pub fn contains(&self, doc_id: u64) -> bool {
        self.doc_id_set.contains(&doc_id)
    }

    pub fn len(&self) -> usize {
        self.index.size()
    }

    pub fn is_empty(&self) -> bool {
        self.index.size() == 0
    }

    pub fn doc_id_count(&self) -> usize {
        self.doc_id_set.len()
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

fn active_storage(root: &Path) -> Result<Option<ActiveStorage>> {
    let pointer_path = root.join(CURRENT_GENERATION_FILE);
    match fs::read(&pointer_path) {
        Ok(bytes) => {
            let pointer: VectorGenerationPointer =
                serde_json::from_slice(&bytes).with_context(|| {
                    format!("read vector generation pointer {}", pointer_path.display())
                })?;
            if pointer.version != 1 || !is_safe_generation_name(&pointer.generation) {
                return Err(anyhow!(
                    "invalid vector generation pointer at {}",
                    pointer_path.display()
                ));
            }
            return Ok(Some(ActiveStorage {
                path: root.join(GENERATIONS_DIR).join(&pointer.generation),
                generation: Some(pointer.generation),
            }));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!("read vector generation pointer {}", pointer_path.display())
            });
        }
    }

    if root.join("usearch.index").exists() {
        Ok(Some(ActiveStorage {
            path: root.to_path_buf(),
            generation: None,
        }))
    } else {
        Ok(None)
    }
}

fn is_safe_generation_name(name: &str) -> bool {
    let mut components = Path::new(name).components();
    matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
}

fn unique_generation_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let sequence = GENERATION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("generation-{nanos}-{}-{sequence}", std::process::id())
}

fn path_str(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| anyhow!("invalid path: {}", path.display()))
}

fn sync_file(path: &Path) -> Result<()> {
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

fn atomic_write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("path has no parent: {}", path.display()))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(&serde_json::to_vec_pretty(value)?)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

fn cleanup_inactive_generations(root: &Path, active: &str) {
    let generations = root.join(GENERATIONS_DIR);
    let Ok(entries) = fs::read_dir(generations) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let known_generation = name.starts_with("generation-");
        let abandoned_write = name.starts_with(".generation-") && name.ends_with(".tmp");
        if name != active && (known_generation || abandoned_write) {
            let _ = fs::remove_dir_all(entry.path());
        }
    }
}

fn cleanup_legacy_files(root: &Path) {
    for name in ["usearch.index", "doc_ids.bin", "meta.json"] {
        let _ = fs::remove_file(root.join(name));
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
    fs::write(path, serde_json::to_vec_pretty(metadata)?)?;
    Ok(())
}

fn load_storage_metadata(storage: &ActiveStorage) -> Result<Option<VectorMetadata>> {
    let path = storage.path.join("meta.json");
    let metadata = if storage.generation.is_some() {
        let metadata = load_metadata(&path)
            .with_context(|| format!("read vector metadata sidecar {}", path.display()))?;
        if metadata.vector_count.is_none() {
            return Err(anyhow!(
                "vector metadata {} is missing vector_count",
                path.display()
            ));
        }
        Some(metadata)
    } else {
        load_metadata_if_exists(&path)?
    };
    if let Some(metadata) = metadata.as_ref() {
        validate_metadata_layout(metadata, &storage.path)?;
    }
    Ok(metadata)
}

fn decode_doc_ids(bytes: &[u8], path: &Path) -> Result<HashSet<u64>> {
    let (chunks, remainder) = bytes.as_chunks::<8>();
    if !remainder.is_empty() {
        return Err(anyhow!(
            "invalid vector ID sidecar {}: {} bytes is not a multiple of 8",
            path.display(),
            bytes.len()
        ));
    }
    let mut ids = HashSet::with_capacity(bytes.len() / 8);
    for chunk in chunks {
        let id = u64::from_le_bytes(*chunk);
        if !ids.insert(id) {
            return Err(anyhow!(
                "invalid vector ID sidecar {}: duplicate document ID {id}",
                path.display()
            ));
        }
    }
    Ok(ids)
}

fn validate_snapshot(
    index: &Index,
    doc_ids: &HashSet<u64>,
    metadata: Option<&VectorMetadata>,
    storage: &Path,
) -> Result<()> {
    if let Some(metadata) = metadata {
        if metadata.dimensions != index.dimensions() {
            return Err(anyhow!(
                "vector metadata dimension mismatch in {}: metadata has {}, index has {}",
                storage.join("meta.json").display(),
                metadata.dimensions,
                index.dimensions()
            ));
        }
        if let Some(recorded_count) = metadata.vector_count
            && recorded_count != index.size()
        {
            return Err(anyhow!(
                "vector metadata count mismatch in {}: metadata records {}, index has {}",
                storage.join("meta.json").display(),
                recorded_count,
                index.size()
            ));
        }
    }

    if doc_ids.len() != index.size() {
        return Err(anyhow!(
            "vector sidecar cardinality mismatch in {}: doc_ids.bin has {} IDs, usearch.index has {} vectors",
            storage.display(),
            doc_ids.len(),
            index.size()
        ));
    }
    if let Some(missing) = doc_ids.iter().find(|doc_id| !index.contains(**doc_id)) {
        return Err(anyhow!(
            "vector sidecar identity mismatch in {}: document ID {missing} is absent from usearch.index",
            storage.display()
        ));
    }
    Ok(())
}

fn validate_metadata_layout(metadata: &VectorMetadata, storage: &Path) -> Result<()> {
    if metadata.index_file != "usearch.index" || metadata.ids_file != "doc_ids.bin" {
        return Err(anyhow!(
            "invalid vector metadata {}: expected usearch.index and doc_ids.bin",
            storage.join("meta.json").display()
        ));
    }
    Ok(())
}

fn save_doc_ids(path: &Path, ids: &HashSet<u64>) -> Result<()> {
    let mut bytes = Vec::with_capacity(ids.len() * 8);
    for id in ids {
        bytes.extend_from_slice(&id.to_le_bytes());
    }
    fs::write(path, &bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_vector(dims: usize, seed: f32) -> Vec<f32> {
        (0..dims).map(|i| (i as f32 + seed).sin()).collect()
    }

    fn active_dir(root: &Path) -> PathBuf {
        active_storage(root).unwrap().unwrap().path
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
        assert!(tmp.path().join(CURRENT_GENERATION_FILE).exists());

        VectorIndex::reset(tmp.path()).unwrap();

        assert!(tmp.path().exists());
        assert!(!tmp.path().join(CURRENT_GENERATION_FILE).exists());
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
    fn filtered_search_deepens_past_rejected_nearest_ids() {
        let tmp = TempDir::new().unwrap();
        let mut idx = VectorIndex::open_or_create(tmp.path(), 64, Some("test")).unwrap();
        let query = make_vector(64, 1.0);
        idx.add(1, &query).unwrap();
        idx.add(2, &make_vector(64, 2.0)).unwrap();
        idx.add(3, &make_vector(64, 3.0)).unwrap();

        let results = idx
            .search_filtered(&query, 1, |doc_id| Ok(doc_id == 3))
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 3);
    }

    #[test]
    fn filtered_search_propagates_filter_errors() {
        let tmp = TempDir::new().unwrap();
        let mut idx = VectorIndex::open_or_create(tmp.path(), 64, Some("test")).unwrap();
        let query = make_vector(64, 1.0);
        idx.add(1, &query).unwrap();

        let error = idx
            .search_filtered(&query, 1, |_| Err(anyhow!("filter failed")))
            .unwrap_err();

        assert_eq!(error.to_string(), "filter failed");
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
            let pointer: VectorGenerationPointer = serde_json::from_slice(
                &fs::read(tmp.path().join(CURRENT_GENERATION_FILE)).unwrap(),
            )
            .unwrap();
            assert_eq!(pointer.version, 1);
            assert!(
                tmp.path()
                    .join(GENERATIONS_DIR)
                    .join(pointer.generation)
                    .join("usearch.index")
                    .is_file()
            );
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

        let metadata: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(active_dir(tmp.path()).join("meta.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(metadata["dimensions"], 64);
        assert_eq!(metadata["model"], "bge");
        assert_eq!(metadata["index_file"], "usearch.index");
        assert_eq!(metadata["ids_file"], "doc_ids.bin");
        assert_eq!(metadata["vector_count"], 1);
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
            assert!(!idx.contains(1));
            assert_eq!(idx.dimensions(), 128);
        }

        assert!(VectorIndex::open(tmp.path()).unwrap().contains(1));
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

        let active = VectorIndex::open(tmp.path()).unwrap();
        assert!(active.contains(1));
        assert_eq!(active.model(), Some("alpha"));
    }

    #[test]
    fn explicit_replacement_keeps_active_until_publish() {
        let tmp = TempDir::new().unwrap();
        let mut active = VectorIndex::open_or_create(tmp.path(), 4, Some("test")).unwrap();
        active.add(1, &make_vector(4, 1.0)).unwrap();
        active.save().unwrap();

        let mut replacement = VectorIndex::empty_replacement(tmp.path(), 4, Some("test")).unwrap();
        replacement.add(2, &make_vector(4, 2.0)).unwrap();
        assert!(VectorIndex::open(tmp.path()).unwrap().contains(1));

        let staged = replacement.stage().unwrap();
        assert!(VectorIndex::open(tmp.path()).unwrap().contains(1));
        staged.publish().unwrap();
        let published = VectorIndex::open(tmp.path()).unwrap();
        assert!(!published.contains(1));
        assert!(published.contains(2));
    }

    #[test]
    fn dropping_unpublished_generation_removes_staged_files() {
        let tmp = TempDir::new().unwrap();
        let mut active = VectorIndex::open_or_create(tmp.path(), 4, Some("test")).unwrap();
        active.add(1, &make_vector(4, 1.0)).unwrap();
        active.save().unwrap();

        let mut replacement = VectorIndex::empty_replacement(tmp.path(), 4, Some("test")).unwrap();
        replacement.add(2, &make_vector(4, 2.0)).unwrap();
        let staged = replacement.stage().unwrap();
        assert_eq!(
            fs::read_dir(tmp.path().join(GENERATIONS_DIR))
                .unwrap()
                .count(),
            2
        );

        drop(staged);

        assert_eq!(
            fs::read_dir(tmp.path().join(GENERATIONS_DIR))
                .unwrap()
                .count(),
            1
        );
        assert!(VectorIndex::open(tmp.path()).unwrap().contains(1));
    }

    #[test]
    fn missing_generation_metadata_is_rejected() {
        let tmp = TempDir::new().unwrap();

        {
            let mut idx = VectorIndex::open_or_create(tmp.path(), 64, Some("alpha")).unwrap();
            let v = make_vector(64, 1.0);
            idx.add(1, &v).unwrap();
            idx.save().unwrap();
        }

        fs::remove_file(active_dir(tmp.path()).join("meta.json")).unwrap();

        assert!(VectorIndex::open(tmp.path()).is_err());
        assert!(VectorIndex::open_or_create(tmp.path(), 64, Some("alpha")).is_err());
    }

    #[test]
    fn test_corrupt_model_metadata_errors() {
        let tmp = TempDir::new().unwrap();

        {
            let mut idx = VectorIndex::open_or_create(tmp.path(), 64, Some("alpha")).unwrap();
            idx.add(1, &make_vector(64, 1.0)).unwrap();
            idx.save().unwrap();
        }

        fs::write(active_dir(tmp.path()).join("meta.json"), "{").unwrap();

        assert!(VectorIndex::open(tmp.path()).is_err());
        assert!(VectorIndex::open_or_create(tmp.path(), 64, Some("alpha")).is_err());
    }

    #[test]
    fn failed_generation_does_not_replace_active() {
        let tmp = TempDir::new().unwrap();
        let mut idx = VectorIndex::open_or_create(tmp.path(), 4, Some("test")).unwrap();
        idx.add(1, &make_vector(4, 1.0)).unwrap();
        idx.save().unwrap();
        let original_pointer = fs::read(tmp.path().join(CURRENT_GENERATION_FILE)).unwrap();

        idx.doc_id_set.insert(2);
        let error = idx.save().expect_err("invalid sidecar must not publish");

        assert!(error.to_string().contains("cardinality mismatch"));
        assert_eq!(
            fs::read(tmp.path().join(CURRENT_GENERATION_FILE)).unwrap(),
            original_pointer
        );
        let active = VectorIndex::open(tmp.path()).unwrap();
        assert!(active.contains(1));
        assert!(!active.contains(2));
    }

    #[test]
    fn sidecar_and_metadata_count_must_match_index() {
        let sidecar = TempDir::new().unwrap();
        let mut idx = VectorIndex::open_or_create(sidecar.path(), 4, Some("test")).unwrap();
        idx.add(1, &make_vector(4, 1.0)).unwrap();
        idx.save().unwrap();
        fs::write(
            active_dir(sidecar.path()).join("doc_ids.bin"),
            [1_u64.to_le_bytes(), 2_u64.to_le_bytes()].concat(),
        )
        .unwrap();
        assert!(
            VectorIndex::open(sidecar.path())
                .err()
                .expect("sidecar mismatch")
                .to_string()
                .contains("cardinality mismatch")
        );

        let count = TempDir::new().unwrap();
        let mut idx = VectorIndex::open_or_create(count.path(), 4, Some("test")).unwrap();
        idx.add(1, &make_vector(4, 1.0)).unwrap();
        idx.save().unwrap();
        let metadata_path = active_dir(count.path()).join("meta.json");
        let mut metadata = load_metadata(&metadata_path).unwrap();
        metadata.vector_count = Some(2);
        save_metadata(&metadata_path, &metadata).unwrap();
        assert!(
            VectorIndex::open(count.path())
                .err()
                .expect("metadata count mismatch")
                .to_string()
                .contains("metadata count mismatch")
        );
    }

    #[test]
    fn metadata_dimensions_must_match_index() {
        let tmp = TempDir::new().unwrap();
        let mut idx = VectorIndex::open_or_create(tmp.path(), 4, Some("test")).unwrap();
        idx.add(1, &make_vector(4, 1.0)).unwrap();
        idx.save().unwrap();
        let metadata_path = active_dir(tmp.path()).join("meta.json");
        let mut metadata = load_metadata(&metadata_path).unwrap();
        metadata.dimensions = 8;
        save_metadata(&metadata_path, &metadata).unwrap();

        assert!(
            VectorIndex::open(tmp.path())
                .err()
                .expect("metadata dimension mismatch")
                .to_string()
                .contains("dimension mismatch")
        );
    }

    #[test]
    fn legacy_flat_store_migrates_without_reembedding() {
        let tmp = TempDir::new().unwrap();
        let mut idx = VectorIndex::open_or_create(tmp.path(), 4, Some("test")).unwrap();
        idx.add(1, &make_vector(4, 1.0)).unwrap();
        idx.save().unwrap();
        let generation = active_dir(tmp.path());
        for name in ["usearch.index", "doc_ids.bin", "meta.json"] {
            fs::rename(generation.join(name), tmp.path().join(name)).unwrap();
        }
        fs::remove_file(tmp.path().join(CURRENT_GENERATION_FILE)).unwrap();
        fs::remove_dir_all(tmp.path().join(GENERATIONS_DIR)).unwrap();

        let legacy = VectorIndex::open_or_create(tmp.path(), 4, Some("test")).unwrap();
        assert!(legacy.contains(1));
        assert!(!legacy.needs_backfill());
        legacy.save().unwrap();

        assert!(tmp.path().join(CURRENT_GENERATION_FILE).is_file());
        assert!(!tmp.path().join("usearch.index").exists());
        assert!(VectorIndex::open(tmp.path()).unwrap().contains(1));
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
