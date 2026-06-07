#[cfg(test)]
mod tests {
    use axum::{routing::get, Router, Json};
    use serde_json::json;
    use std::collections::BTreeMap;
    use kube::{Client, config::Config};

    use surreal_dbops::crd::{ValueOrRefSource, ValueFromSource, SecretKeySelector, ConfigMapKeySelector, Schema, SchemaSpec};
    use surreal_dbops::controller::utils::{resolve_value, resolve_and_interpolate_schema};

    async fn setup_mock_client() -> (Client, tokio::task::JoinHandle<()>) {
        let app = Router::new()
            .route("/api/v1/namespaces/test-ns/secrets/test-secret", get(|| async {
                Json(json!({
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": { "name": "test-secret", "namespace": "test-ns" },
                    "data": { "key": "dGVzdC1zZWNyZXQtdmFsdWU=" } // base64 for "test-secret-value"
                }))
            }))
            .route("/api/v1/namespaces/test-ns/configmaps/test-cm", get(|| async {
                Json(json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": { "name": "test-cm", "namespace": "test-ns" },
                    "data": { "key": "DEFINE ACCESS users WITH JWT KEY ${JWT_KEY} ENV ${ENVIRONMENT};" }
                }))
            }));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        
        let server_handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let mut config = Config::new(format!("http://127.0.0.1:{}", port).parse().unwrap());
        config.default_namespace = "test-ns".to_string();
        let client = Client::try_from(config).unwrap();

        (client, server_handle)
    }

    #[tokio::test]
    async fn test_resolve_raw_value() {
        let (client, _server) = setup_mock_client().await;

        let source = ValueOrRefSource {
            value: Some("raw-value".to_string()),
            value_from: None,
        };

        let result = resolve_value(&client, &source, "test-ns").await.unwrap();
        assert_eq!(result, "raw-value");
    }

    #[tokio::test]
    async fn test_resolve_secret_value() {
        let (client, _server) = setup_mock_client().await;

        let source = ValueOrRefSource {
            value: None,
            value_from: Some(ValueFromSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: "test-secret".to_string(),
                    key: "key".to_string(),
                }),
                config_map_ref: None,
            }),
        };

        let result = resolve_value(&client, &source, "test-ns").await.unwrap();
        assert_eq!(result, "test-secret-value");
    }

    #[tokio::test]
    async fn test_resolve_config_map_value() {
        let (client, _server) = setup_mock_client().await;

        let source = ValueOrRefSource {
            value: None,
            value_from: Some(ValueFromSource {
                secret_key_ref: None,
                config_map_ref: Some(ConfigMapKeySelector {
                    name: "test-cm".to_string(),
                    key: "key".to_string(),
                }),
            }),
        };

        let result = resolve_value(&client, &source, "test-ns").await.unwrap();
        assert_eq!(result, "DEFINE ACCESS users WITH JWT KEY ${JWT_KEY} ENV ${ENVIRONMENT};");
    }

    #[tokio::test]
    async fn test_resolve_and_interpolate_schema() {
        let (client, _server) = setup_mock_client().await;

        let mut variables = BTreeMap::new();
        variables.insert(
            "JWT_KEY".to_string(),
            ValueOrRefSource {
                value: None,
                value_from: Some(ValueFromSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: "test-secret".to_string(),
                        key: "key".to_string(),
                    }),
                    config_map_ref: None,
                }),
            },
        );
        variables.insert(
            "ENVIRONMENT".to_string(),
            ValueOrRefSource {
                value: Some("dev".to_string()),
                value_from: None,
            },
        );

        let schema = Schema::new(
            "test-schema",
            SchemaSpec {
                revision_history_limit: Some(10),
                concurrency_limit: Some(50),
                require_approval: None,
                schema: ValueOrRefSource {
                    value: None,
                    value_from: Some(ValueFromSource {
                        secret_key_ref: None,
                        config_map_ref: Some(ConfigMapKeySelector {
                            name: "test-cm".to_string(),
                            key: "key".to_string(),
                        }),
                    }),
                },
                variables: Some(variables),
            },
        );

        let result = resolve_and_interpolate_schema(&client, &schema, "test-ns").await.unwrap();
        assert_eq!(
            result,
            "DEFINE ACCESS users WITH JWT KEY test-secret-value ENV dev;"
        );
    }
}
