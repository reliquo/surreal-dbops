use axum::{routing::post, Json, Router};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info};

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionReviewRequest {
    pub api_version: String,
    pub kind: String,
    pub request: Option<AdmissionRequest>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionRequest {
    pub uid: String,
    pub name: String,
    pub namespace: String,
    pub operation: String,
    pub user_info: UserInfo,
    pub object: Option<serde_json::Value>,
    pub old_object: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UserInfo {
    pub username: String,
    pub groups: Option<Vec<String>>,
    pub uid: Option<String>,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionReviewResponse {
    pub api_version: String,
    pub kind: String,
    pub response: AdmissionResponse,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionResponse {
    pub uid: String,
    pub allowed: bool,
    pub patch: Option<String>,
    pub patch_type: Option<String>,
    pub status: Option<StatusMessage>,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct StatusMessage {
    pub message: String,
}

/// Start the HTTPS admission webhook server.
pub async fn start_webhook_server(
    addr: SocketAddr,
    cert_path: &Path,
    key_path: &Path,
) -> anyhow::Result<()> {
    // 1. Load TLS Certificates
    let config = load_tls_config(cert_path, key_path)?;
    let acceptor = TlsAcceptor::from(Arc::new(config));

    // 2. Build Router
    let app = Router::new().route("/mutate", post(mutate_handler));

    // 3. Bind TCP Listener
    let listener = TcpListener::bind(addr).await?;
    info!("Admission Webhook server listening on HTTPS: {}", addr);

    // 4. Accept HTTPS connections loop
    loop {
        let (stream, peer_addr) = match listener.accept().await {
            Ok(val) => val,
            Err(e) => {
                error!("Failed to accept TCP connection: {}", e);
                continue;
            }
        };

        let acceptor = acceptor.clone();
        let app = app.clone();

        tokio::spawn(async move {
            match acceptor.accept(stream).await {
                Ok(tls_stream) => {
                    let io = hyper_util::rt::TokioIo::new(tls_stream);
                    let service = hyper_util::service::TowerToHyperService::new(app);
                    let _ = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection(io, service)
                    .await;
                }
                Err(e) => {
                    error!(
                        "Failed to establish TLS handshake with peer {}: {}",
                        peer_addr, e
                    );
                }
            }
        });
    }
}

/// Webhook endpoint for mutating Rollout resources.
pub async fn mutate_handler(
    Json(payload): Json<AdmissionReviewRequest>,
) -> Json<AdmissionReviewResponse> {
    let req = match payload.request {
        Some(ref r) => r,
        None => {
            return Json(AdmissionReviewResponse {
                api_version: payload.api_version,
                kind: payload.kind,
                response: AdmissionResponse {
                    uid: "unknown".to_string(),
                    allowed: true,
                    patch: None,
                    patch_type: None,
                    status: Some(StatusMessage {
                        message: "No request body found".to_string(),
                    }),
                },
            });
        }
    };

    let uid = req.uid.clone();
    info!(
        "Received admission review mutation request for resource: {}/{}",
        req.namespace, req.name
    );

    let mut response = AdmissionResponse {
        uid: uid.clone(),
        allowed: true,
        patch: None,
        patch_type: None,
        status: None,
    };

    // 1. Inspect Rollout annotations for approval flag
    if let (Some(obj), Some(old_obj)) = (&req.object, &req.old_object) {
        let old_approved = is_approved(old_obj);
        let new_approved = is_approved(obj);

        // Transition detected: approved changed from false/missing to true
        if !old_approved && new_approved {
            let approver = &req.user_info.username;
            let timestamp = Utc::now().to_rfc3339();

            info!(
                "Rollout approved by user: {} (generating JSON patch)",
                approver
            );

            // Construct JSON patch
            let patch_ops = generate_patch(obj, approver, &timestamp);
            let patch_json = serde_json::to_string(&patch_ops).unwrap_or_default();

            response.patch = Some(BASE64.encode(patch_json.as_bytes()));
            response.patch_type = Some("JSONPatch".to_string());
        }
    }

    Json(AdmissionReviewResponse {
        api_version: payload.api_version,
        kind: payload.kind,
        response,
    })
}

fn is_approved(obj: &serde_json::Value) -> bool {
    obj.pointer("/metadata/annotations/surreal-dbops.reliquo.io~1approved")
        .and_then(|val| val.as_str())
        .map(|s| s == "true")
        .unwrap_or(false)
}

fn generate_patch(
    obj: &serde_json::Value,
    approver: &str,
    timestamp: &str,
) -> Vec<serde_json::Value> {
    let mut patch = Vec::new();

    // Check if the annotations object already exists and is a valid object
    let has_annotations = obj
        .pointer("/metadata/annotations")
        .map(|v| v.is_object())
        .unwrap_or(false);

    if !has_annotations {
        // Create annotations block
        patch.push(json!({
            "op": "add",
            "path": "/metadata/annotations",
            "value": {}
        }));
    }

    // In JSON Patch, '/' is escaped as '~1'
    patch.push(json!({
        "op": "add",
        "path": "/metadata/annotations/surreal-dbops.reliquo.io~1approved-by",
        "value": approver
    }));

    patch.push(json!({
        "op": "add",
        "path": "/metadata/annotations/surreal-dbops.reliquo.io~1approved-at",
        "value": timestamp
    }));

    patch
}

fn load_tls_config(cert_path: &Path, key_path: &Path) -> anyhow::Result<ServerConfig> {
    let cert_file = File::open(cert_path)?;
    let mut reader = BufReader::new(cert_file);
    let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;

    let key_file = File::open(key_path)?;
    let mut reader = BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut reader)?
        .ok_or_else(|| anyhow::anyhow!("No private key found in key file"))?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok(config)
}
