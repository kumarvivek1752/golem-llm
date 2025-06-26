#[allow(unused_imports)]
use golem_tts::golem::tts::types::{
    AudioConfig, AudioFormat, SynthesisMetadata, TextInput, TtsError,
};

#[allow(unused_imports)]
use reqwest::{Client, Method, Response};
use std::env;

// interface types {
//     /// Comprehensive error types covering all TTS operations
//     variant tts-error {
//         /// Input validation errors
//         invalid-text(string),
//         text-too-long(u32),
//         invalid-ssml(string),
//         unsupported-language(string),

//         /// Voice and model errors
//         voice-not-found(string),
//         model-not-found(string),
//         voice-unavailable(string),

//         /// Authentication and authorization
//         unauthorized(string),
//         access-denied(string),

//         /// Resource and quota limits
//         quota-exceeded(quota-info),
//         rate-limited(u32),
//         insufficient-credits,

//         /// Operation errors
//         synthesis-failed(string),
//         unsupported-operation(string),
//         invalid-configuration(string),

//         /// Service errors
//         service-unavailable(string),
//         network-error(string),
//         internal-error(string),

//         /// Storage errors (for async operations)
//         invalid-storage-location(string),
//         storage-access-denied(string),
//     }

const BASE_URL: &str = "https://api.elevenlabs.io/v1";

#[allow(dead_code)]
pub struct ElevenLabsClient {
    api_key: String,
    model_version: Option<String>,
    default_voice_id: Option<String>,
}

#[allow(dead_code)]
impl ElevenLabsClient {
    pub fn new() -> Result<Self, TtsError> {
        let api_key = env::var("ELEVENLABS_API_KEY").map_err(|_| {
            TtsError::InvalidConfiguration("ELEVENLABS_API_KEY not set".to_string())
        })?;
        let model_version = env::var("ELEVENLABS_MODEL_VERSION").ok();
        let default_voice_id = env::var("ELEVENLABS_DEFAULT_VOICE_ID").ok();
        Ok(Self {
            api_key,
            model_version,
            default_voice_id,
        })
    }
    pub fn list_voices(&self) -> Result<Vec<String>, TtsError> {
        let url = format!("{}/voices", BASE_URL);
        let client = Client::new();
        let response: Response = client
            .get(&url)
            .header("xi-api-key", &self.api_key)
            .send()
            .map_err(|e| TtsError::NetworkError(e.to_string()))?;
        if response.status().is_success() {
            let voices: Vec<String> = response
                .json()
                .map_err(|e| TtsError::SynthesisFailed(e.to_string()))?;
            Ok(voices)
        } else {
            let error_message = response
                .text()
                .unwrap_or_else(|_| "Unknown error".to_string());
            Err(TtsError::SynthesisFailed(error_message))
        }
    }
}
