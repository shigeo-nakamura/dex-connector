#[derive(Clone, Debug)]
pub struct DexWebSocket {
    endpoint: String,
}

impl DexWebSocket {
    pub fn new(endpoint: String) -> Self {
        DexWebSocket { endpoint }
    }
}
