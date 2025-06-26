use wit_bindgen::generate;

generate!({
    path: "../wit/golem-tts/",
    world: "tts-library",
    generate_all,
    generate_unused_types: true,
    pub_export_macro: true,
});

pub use crate::exports::golem;

use std::cell::RefCell;
use std::str::FromStr;

pub struct LoggingState {
    logging_initialized: bool,
}

impl LoggingState {
    pub fn init(&mut self) {
        if !self.logging_initialized {
            let _ = wasi_logger::Logger::install();
            let max_level: log::LevelFilter =
                log::LevelFilter::from_str(&std::env::var("GOLEM_TTS_LOG").unwrap_or_default())
                    .unwrap_or(log::LevelFilter::Info);
            log::set_max_level(max_level);
            self.logging_initialized = true;
        }
    }
}

thread_local! {
    /// This holds the state of our application.
    pub static LOGGING_STATE: RefCell<LoggingState> = const { RefCell::new(LoggingState {
        logging_initialized: false,
    }) };
}

pub use __export_tts_library_impl as export_tts;
