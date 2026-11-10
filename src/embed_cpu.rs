//! CPU-only ONNX Runtime sessions for fastembed models.
//!
//! ort sets an automatic device policy that prefers an NPU on every new session, and
//! ONNX Runtime applies that policy whenever no execution provider factory has been
//! registered. ort's CPU provider never registers one, so a fastembed session built
//! for `execution_provider = "cpu"` could still be placed on CoreML on Apple Silicon.
//! This builds the session with [`AutoDevicePolicy::PreferCPU`] and otherwise mirrors
//! fastembed's `TextEmbedding::embed`, reusing its output selection and pooling.

use anyhow::{Context, Result, anyhow};
use fastembed::{EmbeddingModel, OutputKey, Pooling, SingleBatchOutput, TextEmbedding};
use hf_hub::api::sync::{ApiBuilder, ApiRepo};
use ndarray::Array2;
use ort::session::Session;
use ort::session::builder::{AutoDevicePolicy, GraphOptimizationLevel};
use ort::value::Value;
use std::path::{Path, PathBuf};
use tokenizers::{AddedToken, PaddingParams, PaddingStrategy, Tokenizer, TruncationParams};

/// fastembed's default `max_length` for text models.
const MAX_LENGTH: usize = 512;

/// fastembed's default output precedence for text models.
const OUTPUT_TYPE_PRECEDENCE: &[OutputKey] = &[
    OutputKey::OnlyOne,
    OutputKey::ByName("text_embeds"),
    OutputKey::ByName("last_hidden_state"),
    OutputKey::ByName("sentence_embedding"),
];

pub(crate) struct CpuTextEmbedding {
    tokenizer: Tokenizer,
    session: Session,
    need_token_type_ids: bool,
    pooling: Option<Pooling>,
    output_key: Option<OutputKey>,
}

impl CpuTextEmbedding {
    pub(crate) fn try_new(model: &EmbeddingModel) -> Result<Self> {
        let info = TextEmbedding::get_model_info(model)?;
        let repo = model_repo(&info.model_code)?;
        let model_file = repo
            .get(&info.model_file)
            .with_context(|| format!("failed to retrieve {}", info.model_file))?;
        for file in &info.additional_files {
            repo.get(file)
                .with_context(|| format!("failed to retrieve {file}"))?;
        }

        Self::from_files(
            &model_file,
            |name| Ok(repo.get(name)?),
            TextEmbedding::get_default_pooling_method(model),
            info.output_key.clone(),
        )
    }

    /// Build from an ONNX model file plus a lookup for its tokenizer files.
    fn from_files(
        model_file: &Path,
        file: impl Fn(&str) -> Result<PathBuf>,
        pooling: Option<Pooling>,
        output_key: Option<OutputKey>,
    ) -> Result<Self> {
        let threads = std::thread::available_parallelism()?.get();
        let session = Session::builder()?
            .with_auto_device(AutoDevicePolicy::PreferCPU)
            .map_err(|err| anyhow!("{err}"))?
            .with_optimization_level(GraphOptimizationLevel::Level3)
            .map_err(|err| anyhow!("{err}"))?
            .with_intra_threads(threads)
            .map_err(|err| anyhow!("{err}"))?
            .commit_from_file(model_file)?;
        let need_token_type_ids = session
            .inputs()
            .iter()
            .any(|input| input.name() == "token_type_ids");

        Ok(Self {
            tokenizer: load_tokenizer(file)?,
            session,
            need_token_type_ids,
            pooling,
            output_key,
        })
    }

    pub(crate) fn embed(&mut self, texts: &[&str], batch_size: usize) -> Result<Vec<Vec<f32>>> {
        let mut embeddings = Vec::with_capacity(texts.len());
        for batch in texts.chunks(batch_size.max(1)) {
            let output = self.run_batch(batch)?;
            let pooled = match &self.output_key {
                Some(key) => output.select_and_pool_output(&key, self.pooling.clone())?,
                None => {
                    output.select_and_pool_output(&OUTPUT_TYPE_PRECEDENCE, self.pooling.clone())?
                }
            };
            for row in pooled.rows() {
                let row = row
                    .as_slice()
                    .ok_or_else(|| anyhow!("failed to convert embedding row to slice"))?;
                embeddings.push(normalize(row));
            }
        }
        Ok(embeddings)
    }

    fn run_batch(&mut self, batch: &[&str]) -> Result<SingleBatchOutput> {
        let encodings = self
            .tokenizer
            .encode_batch(batch.to_vec(), true)
            .map_err(|err| anyhow!("failed to encode the batch: {err}"))?;
        let encoding_length = encodings
            .first()
            .ok_or_else(|| anyhow!("tokenizer returned empty encodings"))?
            .len();
        let shape = (batch.len(), encoding_length);

        let mut ids = Vec::with_capacity(shape.0 * shape.1);
        let mut mask = Vec::with_capacity(shape.0 * shape.1);
        let mut type_ids = Vec::with_capacity(shape.0 * shape.1);
        for encoding in &encodings {
            ids.extend(encoding.get_ids().iter().map(|&x| x as i64));
            mask.extend(encoding.get_attention_mask().iter().map(|&x| x as i64));
            type_ids.extend(encoding.get_type_ids().iter().map(|&x| x as i64));
        }
        let attention_mask_array = Array2::from_shape_vec(shape, mask)?;

        let mut inputs = ort::inputs![
            "input_ids" => Value::from_array(Array2::from_shape_vec(shape, ids)?)?,
            "attention_mask" => Value::from_array(attention_mask_array.clone())?,
        ];
        if self.need_token_type_ids {
            inputs.push((
                "token_type_ids".into(),
                Value::from_array(Array2::from_shape_vec(shape, type_ids)?)?.into(),
            ));
        }

        let outputs = self
            .session
            .run(inputs)?
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect();
        Ok(SingleBatchOutput {
            outputs,
            attention_mask_array,
        })
    }
}

/// Open the model repo in the same Hugging Face cache fastembed uses.
fn model_repo(model_code: &str) -> Result<ApiRepo> {
    let cache_dir = std::env::var("HF_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(fastembed::get_cache_dir()));
    let endpoint =
        std::env::var("HF_ENDPOINT").unwrap_or_else(|_| "https://huggingface.co".to_string());
    let api = ApiBuilder::new()
        .with_cache_dir(cache_dir)
        .with_endpoint(endpoint)
        .with_progress(false)
        .build()?;
    Ok(api.model(model_code.to_string()))
}

/// Same padding, truncation and special tokens as fastembed's `load_tokenizer`.
fn load_tokenizer(file: impl Fn(&str) -> Result<PathBuf>) -> Result<Tokenizer> {
    let read_json = |name: &str| -> Result<serde_json::Value> {
        let bytes = std::fs::read(file(name)?)?;
        serde_json::from_slice(&bytes).with_context(|| format!("failed to parse {name}"))
    };
    let config = read_json("config.json")?;
    let special_tokens_map = read_json("special_tokens_map.json")?;
    let tokenizer_config = read_json("tokenizer_config.json")?;
    let mut tokenizer = Tokenizer::from_file(file("tokenizer.json")?)
        .map_err(|err| anyhow!("failed to load tokenizer.json: {err}"))?;

    // Some models store a model_max_length that only fits in an f64.
    let model_max_length = tokenizer_config["model_max_length"]
        .as_f64()
        .ok_or_else(|| anyhow!("tokenizer_config.json is missing a numeric `model_max_length`"))?
        as f32;
    let max_length = MAX_LENGTH.min(model_max_length as usize);
    let pad_id = config["pad_token_id"].as_u64().unwrap_or(0) as u32;
    let pad_token = tokenizer_config["pad_token"]
        .as_str()
        .ok_or_else(|| anyhow!("tokenizer_config.json is missing a string `pad_token`"))?
        .to_string();

    tokenizer
        .with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::BatchLongest,
            pad_token,
            pad_id,
            ..Default::default()
        }))
        .with_truncation(Some(TruncationParams {
            max_length,
            ..Default::default()
        }))
        .map_err(|err| anyhow!("{err}"))?;

    if let serde_json::Value::Object(entries) = special_tokens_map {
        for value in entries.values() {
            if let Some(content) = value.as_str() {
                tokenizer.add_special_tokens(&[AddedToken {
                    content: content.into(),
                    special: true,
                    ..Default::default()
                }]);
            } else if let (
                Some(content),
                Some(single_word),
                Some(lstrip),
                Some(rstrip),
                Some(normalized),
            ) = (
                value["content"].as_str(),
                value["single_word"].as_bool(),
                value["lstrip"].as_bool(),
                value["rstrip"].as_bool(),
                value["normalized"].as_bool(),
            ) {
                tokenizer.add_special_tokens(&[AddedToken {
                    content: content.into(),
                    special: true,
                    single_word,
                    lstrip,
                    rstrip,
                    normalized,
                }]);
            }
        }
    }
    Ok(tokenizer)
}

/// L2 normalization with fastembed's epsilon.
fn normalize(v: &[f32]) -> Vec<f32> {
    let norm = v.iter().map(|val| val * val).sum::<f32>().sqrt();
    v.iter().map(|&val| val / (norm + 1e-12)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastembed::{InitOptionsUserDefined, TokenizerFiles, UserDefinedEmbeddingModel};

    const TEXTS: &[&str] = &[
        "hello world",
        "memex search runs embeddings on the cpu even when the text is long",
        "unknown words here",
        "",
    ];

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/embed_cpu")
            .join(name)
    }

    fn fastembed_reference(pooling: Pooling) -> TextEmbedding {
        let read = |name| std::fs::read(fixture(name)).expect("read fixture");
        let model = UserDefinedEmbeddingModel::new(
            read("model.onnx"),
            TokenizerFiles {
                tokenizer_file: read("tokenizer.json"),
                config_file: read("config.json"),
                special_tokens_map_file: read("special_tokens_map.json"),
                tokenizer_config_file: read("tokenizer_config.json"),
            },
        )
        .with_pooling(pooling);
        TextEmbedding::try_new_from_user_defined(model, InitOptionsUserDefined::default())
            .expect("fastembed reference model")
    }

    fn assert_matches_fastembed(pooling: Pooling) {
        let mut cpu = CpuTextEmbedding::from_files(
            &fixture("model.onnx"),
            |name| Ok(fixture(name)),
            Some(pooling.clone()),
            None,
        )
        .expect("cpu model");
        let mut reference = fastembed_reference(pooling);
        for batch_size in [1, 2, TEXTS.len()] {
            let ours = cpu.embed(TEXTS, batch_size).expect("cpu embed");
            let theirs = reference
                .embed(TEXTS, Some(batch_size))
                .expect("fastembed embed");
            assert_eq!(ours.len(), theirs.len());
            for (a, b) in ours.iter().zip(&theirs) {
                assert_eq!(a.len(), b.len());
                for (x, y) in a.iter().zip(b) {
                    assert!((x - y).abs() < 1e-6, "{a:?} != {b:?}");
                }
            }
        }
    }

    #[test]
    fn mean_pooling_matches_fastembed() {
        assert_matches_fastembed(Pooling::Mean);
    }

    #[test]
    fn cls_pooling_matches_fastembed() {
        assert_matches_fastembed(Pooling::Cls);
    }
}
