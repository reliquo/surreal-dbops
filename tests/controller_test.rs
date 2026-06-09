#[cfg(test)]
mod controller_tests {
    use axum::{
        extract::{Path, State},
        routing::{delete, get, patch, post},
        Json, Router,
    };
    use kube::{config::Config, Api, Client};
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use surreal_dbops::controller::{database, instance, namespace, rollout, schema, Context};
    use surreal_dbops::crd::*;
    use surreal_dbops::webhook::server::{
        mutate_handler, AdmissionRequest, AdmissionReviewRequest, UserInfo,
    };

    #[derive(Clone, Default)]
    struct MockState {
        instances: Arc<Mutex<HashMap<String, Value>>>,
        namespaces: Arc<Mutex<HashMap<String, Value>>>,
        databases: Arc<Mutex<HashMap<String, Value>>>,
        schemas: Arc<Mutex<HashMap<String, Value>>>,
        rollouts: Arc<Mutex<HashMap<String, Value>>>,
        secrets: Arc<Mutex<HashMap<String, Value>>>,
        configmaps: Arc<Mutex<HashMap<String, Value>>>,
    }

    async fn setup_mock_k8s_server() -> (Client, MockState, tokio::task::JoinHandle<()>) {
        let state = MockState::default();

        fn patch_metadata(val: &mut Value, patch: &Value) {
            if let Some(patch_meta) = patch.get("metadata") {
                if let Some(existing_meta) = val.get_mut("metadata") {
                    if let Some(existing_obj) = existing_meta.as_object_mut() {
                        if let Some(patch_obj) = patch_meta.as_object() {
                            for (k, v) in patch_obj {
                                if k == "annotations" {
                                    if existing_obj.get("annotations").is_none() {
                                        existing_obj.insert("annotations".to_string(), json!({}));
                                    }
                                    let existing_ann = existing_obj
                                        .get_mut("annotations")
                                        .unwrap()
                                        .as_object_mut()
                                        .unwrap();
                                    if let Some(obj) = v.as_object() {
                                        for (ak, av) in obj {
                                            existing_ann.insert(ak.clone(), av.clone());
                                        }
                                    }
                                } else {
                                    existing_obj.insert(k.clone(), v.clone());
                                }
                            }
                        }
                    }
                }
            }
        }

        let app = Router::new()
            // Instances
            .route("/apis/surrealdb.reliquo.io/v1alpha1/namespaces/:namespace/instances/:name", get(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>| async move {
                    let map = s.instances.lock().unwrap();
                    map.get(&name).cloned().map(Json).ok_or(axum::http::StatusCode::NOT_FOUND)
                }
            ))
            .route("/apis/surrealdb.reliquo.io/v1alpha1/namespaces/:namespace/instances/:name/status", patch(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>, Json(patch): Json<Value>| async move {
                    let mut map = s.instances.lock().unwrap();
                    if let Some(val) = map.get_mut(&name) {
                        if let Some(status) = patch.get("status") {
                            val["status"] = status.clone();
                        }
                        Ok(Json(val.clone()))
                    } else {
                        Err(axum::http::StatusCode::NOT_FOUND)
                    }
                }
            ))

            // Namespaces
            .route("/apis/surrealdb.reliquo.io/v1alpha1/namespaces/:namespace/namespaces/:name", get(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>| async move {
                    let map = s.namespaces.lock().unwrap();
                    map.get(&name).cloned().map(Json).ok_or(axum::http::StatusCode::NOT_FOUND)
                }
            ).patch(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>, Json(patch): Json<Value>| async move {
                    let mut map = s.namespaces.lock().unwrap();
                    if let Some(val) = map.get_mut(&name) {
                        patch_metadata(val, &patch);
                        Ok(Json(val.clone()))
                    } else {
                        Err(axum::http::StatusCode::NOT_FOUND)
                    }
                }
            ))
            .route("/apis/surrealdb.reliquo.io/v1alpha1/namespaces/:namespace/namespaces/:name/status", patch(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>, Json(patch): Json<Value>| async move {
                    let mut map = s.namespaces.lock().unwrap();
                    if let Some(val) = map.get_mut(&name) {
                        if let Some(status) = patch.get("status") {
                            val["status"] = status.clone();
                        }
                        Ok(Json(val.clone()))
                    } else {
                        Err(axum::http::StatusCode::NOT_FOUND)
                    }
                }
            ))

            // Databases
            .route("/apis/surrealdb.reliquo.io/v1alpha1/namespaces/:namespace/databases/:name", get(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>| async move {
                    let map = s.databases.lock().unwrap();
                    map.get(&name).cloned().map(Json).ok_or(axum::http::StatusCode::NOT_FOUND)
                }
            ).patch(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>, Json(patch): Json<Value>| async move {
                    let mut map = s.databases.lock().unwrap();
                    if let Some(val) = map.get_mut(&name) {
                        patch_metadata(val, &patch);
                        Ok(Json(val.clone()))
                    } else {
                        Err(axum::http::StatusCode::NOT_FOUND)
                    }
                }
            ))
            .route("/apis/surrealdb.reliquo.io/v1alpha1/namespaces/:namespace/databases/:name/status", patch(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>, Json(patch): Json<Value>| async move {
                    let mut map = s.databases.lock().unwrap();
                    if let Some(val) = map.get_mut(&name) {
                        if let Some(status) = patch.get("status") {
                            let existing_status = val.get_mut("status");
                            if let Some(existing) = existing_status {
                                if let Some(obj) = existing.as_object_mut() {
                                    if let Some(patch_obj) = status.as_object() {
                                        for (k, v) in patch_obj {
                                            obj.insert(k.clone(), v.clone());
                                        }
                                    }
                                }
                            } else {
                                val["status"] = status.clone();
                            }
                        }
                        Ok(Json(val.clone()))
                    } else {
                        Err(axum::http::StatusCode::NOT_FOUND)
                    }
                }
            ))
            .route("/apis/surrealdb.reliquo.io/v1alpha1/databases", get(
                |State(s): State<MockState>| async move {
                    let map = s.databases.lock().unwrap();
                    let items: Vec<Value> = map.values().cloned().collect();
                    Json(json!({
                        "apiVersion": "surrealdb.reliquo.io/v1alpha1",
                        "kind": "DatabaseList",
                        "metadata": {},
                        "items": items
                    }))
                }
            ))

            // Schemas
            .route("/apis/surrealdb.reliquo.io/v1alpha1/namespaces/:namespace/schemas/:name", get(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>| async move {
                    let map = s.schemas.lock().unwrap();
                    map.get(&name).cloned().map(Json).ok_or(axum::http::StatusCode::NOT_FOUND)
                }
            ))
            .route("/apis/surrealdb.reliquo.io/v1alpha1/namespaces/:namespace/schemas/:name/status", patch(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>, Json(patch): Json<Value>| async move {
                    let mut map = s.schemas.lock().unwrap();
                    if let Some(val) = map.get_mut(&name) {
                        if let Some(status) = patch.get("status") {
                            val["status"] = status.clone();
                        }
                        Ok(Json(val.clone()))
                    } else {
                        Err(axum::http::StatusCode::NOT_FOUND)
                    }
                }
            ))

            // Rollouts
            .route("/apis/surrealdb.reliquo.io/v1alpha1/namespaces/:namespace/rollouts/:name", get(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>| async move {
                    let map = s.rollouts.lock().unwrap();
                    map.get(&name).cloned().map(Json).ok_or(axum::http::StatusCode::NOT_FOUND)
                }
            ))
            .route("/apis/surrealdb.reliquo.io/v1alpha1/namespaces/:namespace/rollouts/:name/status", patch(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>, Json(patch): Json<Value>| async move {
                    let mut map = s.rollouts.lock().unwrap();
                    if let Some(val) = map.get_mut(&name) {
                        if let Some(status) = patch.get("status") {
                            val["status"] = status.clone();
                        }
                        Ok(Json(val.clone()))
                    } else {
                        Err(axum::http::StatusCode::NOT_FOUND)
                    }
                }
            ))
            .route("/apis/surrealdb.reliquo.io/v1alpha1/namespaces/:namespace/rollouts/:name", patch(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>, Json(patch): Json<Value>| async move {
                    let mut map = s.rollouts.lock().unwrap();
                    if let Some(val) = map.get_mut(&name) {
                        patch_metadata(val, &patch);
                        Ok(Json(val.clone()))
                    } else {
                        Err(axum::http::StatusCode::NOT_FOUND)
                    }
                }
            ))
            .route("/apis/surrealdb.reliquo.io/v1alpha1/namespaces/:namespace/rollouts", post(
                |Path(_): Path<String>, State(s): State<MockState>, Json(rollout): Json<Value>| async move {
                    let name = rollout["metadata"]["name"].as_str().unwrap().to_string();
                    let mut map = s.rollouts.lock().unwrap();
                    map.insert(name, rollout.clone());
                    Json(rollout)
                }
            ).get(
                |Path(_): Path<String>, State(s): State<MockState>| async move {
                    let map = s.rollouts.lock().unwrap();
                    let items: Vec<Value> = map.values().cloned().collect();
                    Json(json!({
                        "apiVersion": "surrealdb.reliquo.io/v1alpha1",
                        "kind": "RolloutList",
                        "metadata": {},
                        "items": items
                    }))
                }
            ))
            .route("/apis/surrealdb.reliquo.io/v1alpha1/namespaces/:namespace/rollouts/:name", delete(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>| async move {
                    let mut map = s.rollouts.lock().unwrap();
                    map.remove(&name).map(Json).ok_or(axum::http::StatusCode::NOT_FOUND)
                }
            ))

            // Secrets and ConfigMaps
            .route("/api/v1/namespaces/:namespace/secrets/:name", get(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>| async move {
                    let map = s.secrets.lock().unwrap();
                    map.get(&name).cloned().map(Json).ok_or(axum::http::StatusCode::NOT_FOUND)
                }
            ))
            .route("/api/v1/namespaces/:namespace/configmaps/:name", get(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>| async move {
                    let map = s.configmaps.lock().unwrap();
                    map.get(&name).cloned().map(Json).ok_or(axum::http::StatusCode::NOT_FOUND)
                }
            ))
            .with_state(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let mut config = Config::new(format!("http://127.0.0.1:{}", port).parse().unwrap());
        config.default_namespace = "test-ns".to_string();
        let client = Client::try_from(config).unwrap();

        (client, state, handle)
    }

    fn mock_instance_val(name: &str, endpoint: &str) -> Value {
        json!({
            "apiVersion": "surrealdb.reliquo.io/v1alpha1",
            "kind": "Instance",
            "metadata": {
                "name": name,
                "namespace": "test-ns",
                "generation": 1,
                "uid": format!("{}-uid", name)
            },
            "spec": {
                "connectionString": { "value": endpoint },
                "username": { "value": "root" },
                "password": { "value": "rootpassword" }
            }
        })
    }

    fn mock_namespace_val(name: &str, instance_name: &str) -> Value {
        json!({
            "apiVersion": "surrealdb.reliquo.io/v1alpha1",
            "kind": "Namespace",
            "metadata": {
                "name": name,
                "namespace": "test-ns",
                "generation": 1,
                "uid": format!("{}-uid", name)
            },
            "spec": {
                "instanceRef": { "name": instance_name }
            }
        })
    }

    fn mock_database_val(name: &str, ns_name: &str, schema_name: &str) -> Value {
        json!({
            "apiVersion": "surrealdb.reliquo.io/v1alpha1",
            "kind": "Database",
            "metadata": {
                "name": name,
                "namespace": "test-ns",
                "generation": 1,
                "uid": format!("{}-uid", name)
            },
            "spec": {
                "namespaceRef": { "name": ns_name },
                "schemaRef": { "name": schema_name }
            }
        })
    }

    fn mock_schema_val(name: &str, schema_text: &str) -> Value {
        json!({
            "apiVersion": "surrealdb.reliquo.io/v1alpha1",
            "kind": "Schema",
            "metadata": {
                "name": name,
                "namespace": "test-ns",
                "generation": 1,
                "uid": format!("{}-uid", name)
            },
            "spec": {
                "revisionHistoryLimit": 5,
                "concurrencyLimit": 10,
                "requireApproval": "destructive",
                "schema": { "value": schema_text }
            }
        })
    }

    fn mock_rollout_val(name: &str, schema_name: &str, generation: i64) -> Value {
        json!({
            "apiVersion": "surrealdb.reliquo.io/v1alpha1",
            "kind": "Rollout",
            "metadata": {
                "name": name,
                "namespace": "test-ns",
                "generation": 1,
                "uid": format!("{}-uid", name)
            },
            "spec": {
                "schemaRef": { "name": schema_name },
                "generation": generation
            }
        })
    }

    #[tokio::test]
    async fn test_controllers_suite() {
        let (client, state, _server) = setup_mock_k8s_server().await;
        let ctx = Arc::new(Context {
            client: client.clone(),
        });

        // 1. Instance Test
        {
            let inst_val = mock_instance_val("instinstance", "mem://");
            state
                .instances
                .lock()
                .unwrap()
                .insert("instinstance".to_string(), inst_val);

            let api: Api<Instance> = Api::namespaced(client.clone(), "test-ns");
            let inst = api.get("instinstance").await.unwrap();
            let result = instance::reconcile(Arc::new(inst), ctx.clone()).await;
            assert!(result.is_ok());

            let updated_map = state.instances.lock().unwrap();
            let updated_inst = updated_map.get("instinstance").unwrap();
            let status = updated_inst.get("status").expect("status to be populated");
            assert_eq!(status["connected"].as_bool(), Some(true));
        }

        // 2. Namespace Test
        {
            let mut inst_val = mock_instance_val("instns", "mem://");
            inst_val["status"] = json!({ "connected": true, "observedGeneration": 1 });
            state
                .instances
                .lock()
                .unwrap()
                .insert("instns".to_string(), inst_val);

            let ns_val = mock_namespace_val("nsns", "instns");
            state
                .namespaces
                .lock()
                .unwrap()
                .insert("nsns".to_string(), ns_val);

            let api: Api<Namespace> = Api::namespaced(client.clone(), "test-ns");
            let ns = api.get("nsns").await.unwrap();
            let result = namespace::reconcile(Arc::new(ns), ctx.clone()).await;
            assert!(result.is_ok());
            let updated_map = state.namespaces.lock().unwrap();
            let updated_ns = updated_map.get("nsns").unwrap();
            let status = updated_ns.get("status").expect("status to be populated");
            if status["created"].as_bool() != Some(true) {
                panic!("Namespace reconciliation failed with status: {:#?}", status);
            }
        }

        // 3. Database Test
        {
            let mut inst_val = mock_instance_val("instdb", "mem://");
            inst_val["status"] = json!({ "connected": true, "observedGeneration": 1 });
            state
                .instances
                .lock()
                .unwrap()
                .insert("instdb".to_string(), inst_val);

            let mut ns_val = mock_namespace_val("nsdb", "instdb");
            ns_val["status"] = json!({ "created": true, "observedGeneration": 1 });
            state
                .namespaces
                .lock()
                .unwrap()
                .insert("nsdb".to_string(), ns_val);

            let db_val = mock_database_val("dbdb", "nsdb", "schemadb");
            state
                .databases
                .lock()
                .unwrap()
                .insert("dbdb".to_string(), db_val);

            let api: Api<Database> = Api::namespaced(client.clone(), "test-ns");
            let db = api.get("dbdb").await.unwrap();
            let result = database::reconcile(Arc::new(db), ctx.clone()).await;
            assert!(result.is_ok());

            let updated_map = state.databases.lock().unwrap();
            let updated_db = updated_map.get("dbdb").unwrap();
            let status = updated_db.get("status").expect("status to be populated");
            if status["created"].as_bool() != Some(true) {
                panic!("Database reconciliation failed with status: {:#?}", status);
            }
        }

        // 4. Schema Test
        {
            let schema_val = mock_schema_val("schematest", "DEFINE TABLE user SCHEMAFULL;");
            state
                .schemas
                .lock()
                .unwrap()
                .insert("schematest".to_string(), schema_val);

            let api: Api<Schema> = Api::namespaced(client.clone(), "test-ns");
            let schema = api.get("schematest").await.unwrap();
            let result = schema::reconcile(Arc::new(schema), ctx.clone()).await;
            assert!(result.is_ok());

            let rollouts = state.rollouts.lock().unwrap();
            let rollout_name = "schematest-rollout-1";
            let r = rollouts
                .get(rollout_name)
                .expect("Rollout should be created");
            assert_eq!(r["spec"]["generation"].as_i64(), Some(1));
            assert_eq!(r["spec"]["schemaRef"]["name"].as_str(), Some("schematest"));
        }

        // 5. Rollout Safe Test
        {
            let mut inst_val = mock_instance_val("instsafe", "mem://");
            inst_val["status"] = json!({ "connected": true, "observedGeneration": 1 });
            state
                .instances
                .lock()
                .unwrap()
                .insert("instsafe".to_string(), inst_val);

            let mut ns_val = mock_namespace_val("nssafe", "instsafe");
            ns_val["status"] = json!({ "created": true, "observedGeneration": 1 });
            state
                .namespaces
                .lock()
                .unwrap()
                .insert("nssafe".to_string(), ns_val);

            let mut schema_val = mock_schema_val(
                "schemasafe",
                "DEFINE TABLE user SCHEMAFULL; DEFINE FIELD name ON TABLE user TYPE string;",
            );
            schema_val["status"] = json!({
                "currentVersionHash": "sha256:somehash",
                "activeRolloutName": "rolloutsafe",
                "observedGeneration": 1
            });
            state
                .schemas
                .lock()
                .unwrap()
                .insert("schemasafe".to_string(), schema_val);

            let mut db_val = mock_database_val("dbsafe", "nssafe", "schemasafe");
            db_val["status"] = json!({ "created": true, "observedGeneration": 1 });
            state
                .databases
                .lock()
                .unwrap()
                .insert("dbsafe".to_string(), db_val);

            let rollout_val = mock_rollout_val("rolloutsafe", "schemasafe", 1);
            state
                .rollouts
                .lock()
                .unwrap()
                .insert("rolloutsafe".to_string(), rollout_val);

            let api: Api<Rollout> = Api::namespaced(client.clone(), "test-ns");
            let rollout = api.get("rolloutsafe").await.unwrap();
            let result = rollout::reconcile(Arc::new(rollout), ctx.clone()).await;
            assert!(result.is_ok());

            let rollouts = state.rollouts.lock().unwrap();
            let r = rollouts.get("rolloutsafe").unwrap();
            if r["status"]["phase"].as_str() != Some("Completed") {
                panic!("Rollout phase is not Completed: {:#?}", r["status"]);
            }
            assert_eq!(r["status"]["destructive"].as_bool(), Some(false));
        }

        // 6. Rollout Destructive Test
        {
            let mut inst_val = mock_instance_val("instdest", "mem://");
            inst_val["status"] = json!({ "connected": true, "observedGeneration": 1 });
            state
                .instances
                .lock()
                .unwrap()
                .insert("instdest".to_string(), inst_val);

            let mut ns_val = mock_namespace_val("nsdest", "instdest");
            ns_val["status"] = json!({ "created": true, "observedGeneration": 1 });
            state
                .namespaces
                .lock()
                .unwrap()
                .insert("nsdest".to_string(), ns_val);

            let mut db_val = mock_database_val("dbdest", "nsdest", "schemadest");
            db_val["status"] = json!({ "created": true, "observedGeneration": 1 });
            state
                .databases
                .lock()
                .unwrap()
                .insert("dbdest".to_string(), db_val);

            // First apply the base schema containing fields using the same connection client
            let db_client =
                surreal_dbops::surreal::connect_instance("mem://", "root", "rootpassword")
                    .await
                    .unwrap();
            db_client.use_ns("nsdest").use_db("dbdest").await.unwrap();
            db_client
                .query("DEFINE TABLE user SCHEMAFULL; DEFINE FIELD name ON TABLE user TYPE string;")
                .await
                .unwrap();

            // Target Schema removes field `name` -> Destructive change!
            let mut schema_val = mock_schema_val("schemadest", "DEFINE TABLE user SCHEMAFULL;");
            schema_val["status"] = json!({
                "currentVersionHash": "sha256:destructivehash",
                "activeRolloutName": "rolloutdest",
                "observedGeneration": 1
            });
            state
                .schemas
                .lock()
                .unwrap()
                .insert("schemadest".to_string(), schema_val);

            let rollout_val = mock_rollout_val("rolloutdest", "schemadest", 1);
            state
                .rollouts
                .lock()
                .unwrap()
                .insert("rolloutdest".to_string(), rollout_val);

            let api: Api<Rollout> = Api::namespaced(client.clone(), "test-ns");
            let rollout = api.get("rolloutdest").await.unwrap();
            let result = rollout::reconcile(Arc::new(rollout), ctx.clone()).await;
            assert!(result.is_ok());

            {
                let rollouts = state.rollouts.lock().unwrap();
                let r = rollouts.get("rolloutdest").unwrap();
                if r["status"]["phase"].as_str() != Some("Blocked") {
                    panic!("Expected Blocked phase, but got: {:#?}", r["status"]);
                }
                assert_eq!(r["status"]["destructive"].as_bool(), Some(true));
                assert_eq!(r["status"]["approved"].as_bool(), Some(false));
                assert!(r["status"]["diff"]
                    .as_str()
                    .unwrap()
                    .contains("REMOVE FIELD name ON TABLE user"));
            }

            // Simulate Approving Rollout
            let patch_json = json!({
                "metadata": {
                    "annotations": {
                        "database.reliquo.io/approved": "true",
                        "database.reliquo.io/approved-by": "admin-user",
                        "database.reliquo.io/approved-at": "2026-06-07T08:00:00Z"
                    }
                }
            });
            api.patch(
                "rolloutdest",
                &kube::api::PatchParams::default(),
                &kube::api::Patch::Merge(&patch_json),
            )
            .await
            .unwrap();

            // Reconcile Rollout again -> Expected: Completed
            let rollout_approved = api.get("rolloutdest").await.unwrap();
            let result = rollout::reconcile(Arc::new(rollout_approved), ctx.clone()).await;
            assert!(result.is_ok());

            {
                let rollouts = state.rollouts.lock().unwrap();
                let r = rollouts.get("rolloutdest").unwrap();
                assert_eq!(r["status"]["phase"].as_str(), Some("Completed"));
                assert_eq!(r["status"]["approved"].as_bool(), Some(true));
                assert_eq!(r["status"]["approvedBy"].as_str(), Some("admin-user"));
            }
        }
    }

    #[test]
    fn test_webhook_admission_mutation_handler() {
        let old_obj = json!({
            "metadata": {
                "name": "my-rollout",
                "namespace": "test-ns",
                "annotations": {}
            }
        });
        let new_obj = json!({
            "metadata": {
                "name": "my-rollout",
                "namespace": "test-ns",
                "annotations": {
                    "database.reliquo.io/approved": "true"
                }
            }
        });

        let payload = AdmissionReviewRequest {
            api_version: "admission.k8s.io/v1".to_string(),
            kind: "AdmissionReview".to_string(),
            request: Some(AdmissionRequest {
                uid: "req-uid".to_string(),
                name: "my-rollout".to_string(),
                namespace: "test-ns".to_string(),
                operation: "UPDATE".to_string(),
                user_info: UserInfo {
                    username: "dev-person".to_string(),
                    groups: None,
                    uid: None,
                },
                object: Some(new_obj),
                old_object: Some(old_obj),
            }),
        };

        let Json(response_payload) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(mutate_handler(Json(payload)));

        let response = response_payload.response;
        assert!(response.allowed);
        assert!(response.patch.is_some());
        assert_eq!(response.patch_type.as_deref(), Some("JSONPatch"));

        let patch_bytes = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            response.patch.unwrap(),
        )
        .unwrap();
        let patch_val: Value = serde_json::from_slice(&patch_bytes).unwrap();
        let patch_array = patch_val.as_array().unwrap();

        let has_approved_by = patch_array.iter().any(|op| {
            op["op"] == "add"
                && op["path"] == "/metadata/annotations/database.reliquo.io~1approved-by"
                && op["value"] == "dev-person"
        });
        assert!(has_approved_by, "Patch must record the user username");

        let has_approved_at = patch_array.iter().any(|op| {
            op["op"] == "add"
                && op["path"]
                    .as_str()
                    .unwrap()
                    .contains("database.reliquo.io~1approved-at")
        });
        assert!(has_approved_at, "Patch must record approval time");
    }
}
