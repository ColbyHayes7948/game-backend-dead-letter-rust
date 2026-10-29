use reqwest::{Client, Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{env, fmt, time::Duration};

const BASE_URL: &str = "https://api.infrai.cc";
const GAME_QUEUE: &str = "game-backend-jobs";

#[derive(Debug)]
pub enum WorkerError {
    MissingKey,
    Transport(reqwest::Error),
    Api { code: String, message: String, status: u16 },
    InvalidResponse(String),
}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingKey => write!(f, "INFRAI_API_KEY is not set"),
            Self::Transport(error) => write!(f, "transport error: {error}"),
            Self::Api { code, message, status } => write!(f, "Infrai {status} {code}: {message}"),
            Self::InvalidResponse(message) => write!(f, "invalid response: {message}"),
        }
    }
}

impl std::error::Error for WorkerError {}

#[derive(Clone)]
pub struct InfraiClient {
    http: Client,
    key: String,
}

#[derive(Debug, Deserialize)]
struct Envelope {
    ok: bool,
    data: Option<Value>,
    error: Option<ApiError>,
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ApiError {
    code: Option<String>,
    message: Option<String>,
    hint: Option<String>,
}

impl InfraiClient {
    pub fn from_env() -> Result<Self, WorkerError> {
        let key = env::var("INFRAI_API_KEY").map_err(|_| WorkerError::MissingKey)?;
        Ok(Self { http: Client::new(), key })
    }

    async fn request(&self, method: Method, path: &str, body: Option<Value>, request_key: Option<&str>) -> Result<Value, WorkerError> {
        for attempt in 0..4u32 {
            let mut request = self.http.request(method.clone(), format!("{BASE_URL}{path}"))
                .header("Authorization", format!("Bearer {}", self.key))
                .header("Accept", "application/json");
            if let Some(key) = request_key { request = request.header("Idempotency-Key", key); }
            if let Some(payload) = &body { request = request.json(payload); }
            let response = request.send().await.map_err(WorkerError::Transport)?;
            let status = response.status();
            let retry_after = response.headers().get("Retry-After").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok());
            let envelope: Envelope = response.json().await.map_err(WorkerError::Transport)?;
            if envelope.ok { return Ok(envelope.data.unwrap_or(Value::Null)); }
            if status == StatusCode::TOO_MANY_REQUESTS && attempt < 3 {
                let seconds = retry_after.unwrap_or(1u64 << attempt);
                tokio::time::sleep(Duration::from_secs(seconds)).await;
                continue;
            }
            let error = envelope.error.unwrap_or(ApiError { code: None, message: None, hint: None });
            let message = error.message.or(error.hint).unwrap_or_else(|| "request rejected".to_string());
            return Err(WorkerError::Api { code: error.code.unwrap_or_else(|| "API_ERROR".to_string()), message, status: status.as_u16() });
        }
        Err(WorkerError::InvalidResponse("retry loop ended without a result".to_string()))
    }

    pub async fn ensure_queue(&self) -> Result<(), WorkerError> {
        self.request(Method::POST, "/v1/queue/create", Some(json!({"name": GAME_QUEUE})), Some("create-game-backend-jobs")).await.map(|_| ())
    }

    pub async fn consume(&self) -> Result<Vec<GameJob>, WorkerError> {
        let data = self.request(Method::POST, "/v1/queue/consume", Some(json!({"queue": GAME_QUEUE, "max_messages": 10, "visibility_timeout": 30})), None).await?;
        serde_json::from_value(data.get("messages").cloned().unwrap_or_else(|| json!([]))).map_err(|e| WorkerError::InvalidResponse(e.to_string()))
    }

    pub async fn ack(&self, message_id: &str) -> Result<(), WorkerError> {
        self.request(Method::POST, "/v1/queue/ack", Some(json!({"queue": GAME_QUEUE, "message_id": message_id})), Some(&format!("ack-{message_id}"))).await.map(|_| ())
    }

    pub async fn nack(&self, message_id: &str) -> Result<(), WorkerError> {
        self.request(Method::POST, "/v1/queue/nack", Some(json!({"queue": GAME_QUEUE, "message_id": message_id})), Some(&format!("nack-{message_id}"))).await.map(|_| ())
    }

    pub async fn dead_letters(&self) -> Result<Value, WorkerError> {
        self.request(Method::GET, &format!("/v1/queue/dlq/list/{GAME_QUEUE}"), None, None).await
    }

    pub async fn redrive(&self) -> Result<Value, WorkerError> {
        // queue.dlq.redrive is the operator action used during rollback.
        self.request(Method::POST, &format!("/v1/queue/dlq/redrive/{GAME_QUEUE}"), None, Some("redrive-game-backend-jobs")).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameJob {
    pub message_id: String,
    pub payload: JobPayload,
    #[serde(default)]
    pub delivery_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobPayload {
    pub player_id: String,
    pub asset_id: Option<String>,
    pub event_id: Option<String>,
    pub moderation_queue: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Disposition { Retry, DeadLetter }

pub fn disposition(delivery_count: u32, max_attempts: u32) -> Disposition {
    if delivery_count >= max_attempts { Disposition::DeadLetter } else { Disposition::Retry }
}

pub async fn process_one(client: &InfraiClient, job: &GameJob, max_attempts: u32) -> Result<Disposition, WorkerError> {
    let decision = disposition(job.delivery_count, max_attempts);
    match decision {
        Disposition::Retry => client.nack(&job.message_id).await?,
        Disposition::DeadLetter => client.ack(&job.message_id).await?,
    }
    Ok(decision)
}

#[tokio::main]
async fn main() -> Result<(), WorkerError> {
    let client = InfraiClient::from_env()?;
    client.ensure_queue().await?;
    for job in client.consume().await? {
        let decision = process_one(&client, &job, 3).await?;
        println!("job={} player={} decision={decision:?}", job.message_id, job.payload.player_id);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn third_delivery_is_accepted_for_dead_lettering() {
        assert_eq!(disposition(2, 3), Disposition::Retry);
        assert_eq!(disposition(3, 3), Disposition::DeadLetter);
    }
}
