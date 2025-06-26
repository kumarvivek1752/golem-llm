#[allow(unused_imports)]
use golem_tts::exports::golem::tts::synthesis::{
    AudioConfig, Guest as SynthesisGuest, SynthesisOptions,
};
use golem_tts::exports::golem::tts::voices::{TtsError, VoiceBorrow};
#[allow(unused_imports)]
use golem_tts::golem::tts::types::{LanguageCode, SynthesisResult, TextInput};

struct ElevenLabsTtsComponent;

impl SynthesisGuest for ElevenLabsTtsComponent {
    fn synthesize(
        _text_input: TextInput,
        _voice: VoiceBorrow<'_>,
        _options: Option<SynthesisOptions>,
    ) -> Result<SynthesisResult, TtsError> {
        todo!()
    }

    fn synthesize_batch(
        _text_inputs: Vec<TextInput>,
        _voice: VoiceBorrow<'_>,
        _options: Option<SynthesisOptions>,
    ) -> Result<Vec<SynthesisResult>, TtsError> {
        todo!()
    }

    fn get_timing_marks(
        _text_input: TextInput,
        _voice: VoiceBorrow<'_>,
    ) -> Result<Vec<golem_tts::golem::tts::types::TimingInfo>, TtsError> {
        todo!()
    }

    fn validate_input(
        _text_input: TextInput,
        _voice: VoiceBorrow<'_>,
    ) -> Result<golem_tts::golem::tts::synthesis::ValidationResult, TtsError> {
        todo!()
    }
}
