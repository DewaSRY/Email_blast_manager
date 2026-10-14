pub struct EmailValidation {
    pub email: String,
    pub is_valid: bool,
}

impl EmailValidation {
    pub fn new(email: String) -> Self {
        let is_valid =
            email.split_once('@').is_some_and(|(local, domain)| {
                !local.is_empty()
                    && domain.contains('.')
                    && !domain.starts_with('.')
                    && !domain.ends_with('.')
            });

        Self { email, is_valid }
    }
}