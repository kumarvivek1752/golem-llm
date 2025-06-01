use wit_bindgen::generate;

generate!({
    path: "../wit/golem-tts/",
    world: "tts-library",
    generate_all,
    generate_unused_types: true,
    pub_export_macro: true
});

pub use __export_tts_library_impl as export_tts;
