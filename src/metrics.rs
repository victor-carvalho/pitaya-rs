// use crate::ServerKind;
// use std::collections::HashMap;

// struct Reporter {
//     server_kind: ServerKind,
//     game: String,
//     count_reporters_map: HashMap<String, prometheus::CounterVec>,
//     histogram_reporters_map: HashMap<String, prometheus::HistogramVec>,
//     gauge_reporters_map: HashMap<String, prometheus::GaugeVec>,
//     additional_labels: HashMap<String, String>,
// }

// impl Reporter {
//     pub fn new(
//         server_kind: ServerKind,
//         game: String,
//         const_labels: HashMap<String, String>,
//     ) -> Self {
//         Self {
//             server_kind,
//             game,
//             count_reporters_map: HashMap::new(),
//             histogram_reporters_map: HashMap::new(),
//             gauge_reporters_map: HashMap::new(),
//             additional_labels: HashMap::new(),
//         }
//     }
// }
