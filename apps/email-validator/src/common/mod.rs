use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiErrorItem {
    pub field: String,
    pub reason: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ApiErrorPayload {
    pub errors: Vec<ApiErrorItem>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ApiResponse<T: ToSchema> {
    pub data: Option<T>,
    pub message: String,
    pub code: u16,
}

impl<T: ToSchema> ApiResponse<T> {
    pub fn ok(data: T, message: impl Into<String>) -> Self {
        Self {
            data: Some(data),
            message: message.into(),
            code: StatusCode::OK.as_u16(),
        }
    }

    #[allow(dead_code)]
    pub fn created(data: T, message: impl Into<String>) -> Self {
        Self {
            data: Some(data),
            message: message.into(),
            code: StatusCode::CREATED.as_u16(),
        }
    }

    pub fn with_status(data: Option<T>, status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            data,
            message: message.into(),
            code: status.as_u16(),
        }
    }

    #[allow(dead_code)]
    pub fn page(
        items: Vec<T>,
        page: u32,
        page_size: u32,
        total: u64,
        message: impl Into<String>,
    ) -> ApiResponse<PageData<T>> {
        ApiResponse::ok(
            PageData {
                items,
                page,
                page_size,
                total,
            },
            message,
        )
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[allow(dead_code)]
pub struct PageData<T: ToSchema> {
    pub items: Vec<T>,
    pub page: u32,
    pub page_size: u32,
    pub total: u64,
}

#[derive(Debug, Clone)]
pub struct AppError {
    status: StatusCode,
    message: String,
    field_errors: Vec<ApiErrorItem>,
}

impl AppError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            field_errors: Vec::new(),
        }
    }

    #[allow(dead_code)]
    pub fn validation_error(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            message: "Validation failed".to_string(),
            field_errors: vec![ApiErrorItem {
                field: field.into(),
                reason: reason.into(),
            }],
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let payload = ApiResponse::with_status(
            if self.field_errors.is_empty() {
                None
            } else {
                Some(ApiErrorPayload {
                    errors: self.field_errors,
                })
            },
            self.status,
            self.message,
        );

        (self.status, Json(payload)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::{ApiErrorPayload, ApiResponse, AppError};

    #[test]
    fn success_response_uses_the_documented_envelope() {
        let response = ApiResponse::ok(serde_json::json!({"status": "valid"}), "OK");
        let value = serde_json::to_value(response).unwrap();

        assert_eq!(value["data"]["status"], "valid");
        assert_eq!(value["message"], "OK");
        assert_eq!(value["code"], 200);
    }

    #[test]
    fn error_response_contains_field_details_inside_data() {
        let error = AppError::validation_error("email", "invalid format");
        let response = ApiResponse::with_status(
            Some(ApiErrorPayload {
                errors: error.field_errors,
            }),
            StatusCode::UNPROCESSABLE_ENTITY,
            "Validation failed",
        );
        let value = serde_json::to_value(response).unwrap();

        assert_eq!(value["data"]["errors"][0]["field"], "email");
        assert_eq!(value["data"]["errors"][0]["reason"], "invalid format");
        assert_eq!(value["code"], 422);
    }

    #[test]
    fn empty_error_data_serializes_as_null() {
        let response = ApiResponse::<String>::with_status(None, StatusCode::NOT_FOUND, "Not found");
        let value = serde_json::to_value(response).unwrap();

        assert!(value["data"].is_null());
        assert_eq!(value["code"], 404);
    }

    #[test]
    fn created_and_paged_responses_keep_the_shared_envelope() {
        let created = ApiResponse::created("id-1", "Created");
        assert_eq!(created.code, StatusCode::CREATED.as_u16());

        let page = ApiResponse::page(vec!["a", "b"], 1, 50, 2, "OK");
        assert_eq!(page.data.unwrap().items, vec!["a", "b"]);
    }
}
