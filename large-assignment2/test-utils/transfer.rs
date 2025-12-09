pub struct PacketBuilder {
    state: Vec<u8>,
}

impl Default for PacketBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PacketBuilder {
    #[allow(clippy::must_use_candidate)]
    pub fn new() -> Self {
        PacketBuilder { state: Vec::new() }
    }

    pub fn add_u32(&mut self, v: u32) {
        self.state.extend_from_slice(&u32::to_be_bytes(v));
    }

    pub fn add_u64(&mut self, v: u64) {
        self.state.extend_from_slice(&u64::to_be_bytes(v));
    }

    pub fn add_u8(&mut self, v: u8) {
        self.state.extend_from_slice(&u8::to_be_bytes(v));
    }

    pub fn add_slice(&mut self, v: &[u8]) {
        self.state.extend_from_slice(v);
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        self.state.as_slice()
    }

    pub fn update_size(&mut self) {
        let data_len = self.state.len();
        self.state[..8].copy_from_slice(&u64::to_be_bytes((data_len - 8) as u64));
    }
}
