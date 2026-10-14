pub struct HealthStatus {
    pub healthy: bool,
}

impl HealthStatus {
    pub fn new() -> Self {
        Self { healthy: true }
    }
}
