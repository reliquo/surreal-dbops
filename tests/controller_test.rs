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
            .route("/apis/surreal-dbops.reliquo.io/v1alpha1/namespaces/:namespace/instances/:name", get(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>| async move {
                    let map = s.instances.lock().unwrap();
                    map.get(&name).cloned().map(Json).ok_or(axum::http::StatusCode::NOT_FOUND)
                }
            ))
            .route("/apis/surreal-dbops.reliquo.io/v1alpha1/namespaces/:namespace/instances/:name/status", patch(
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
            .route("/apis/surreal-dbops.reliquo.io/v1alpha1/namespaces/:namespace/namespaces/:name", get(
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
            .route("/apis/surreal-dbops.reliquo.io/v1alpha1/namespaces/:namespace/namespaces/:name/status", patch(
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
            .route("/apis/surreal-dbops.reliquo.io/v1alpha1/namespaces/:namespace/databases/:name", get(
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
            .route("/apis/surreal-dbops.reliquo.io/v1alpha1/namespaces/:namespace/databases/:name/status", patch(
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
            .route("/apis/surreal-dbops.reliquo.io/v1alpha1/databases", get(
                |State(s): State<MockState>| async move {
                    let map = s.databases.lock().unwrap();
                    let items: Vec<Value> = map.values().cloned().collect();
                    Json(json!({
                        "apiVersion": "surreal-dbops.reliquo.io/v1alpha1",
                        "kind": "DatabaseList",
                        "metadata": {},
                        "items": items
                    }))
                }
            ))

            // Schemas
            .route("/apis/surreal-dbops.reliquo.io/v1alpha1/namespaces/:namespace/schemas/:name", get(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>| async move {
                    let map = s.schemas.lock().unwrap();
                    map.get(&name).cloned().map(Json).ok_or(axum::http::StatusCode::NOT_FOUND)
                }
            ))
            .route("/apis/surreal-dbops.reliquo.io/v1alpha1/namespaces/:namespace/schemas/:name/status", patch(
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
            .route("/apis/surreal-dbops.reliquo.io/v1alpha1/namespaces/:namespace/rollouts/:name", get(
                |Path((_, name)): Path<(String, String)>, State(s): State<MockState>| async move {
                    let map = s.rollouts.lock().unwrap();
                    map.get(&name).cloned().map(Json).ok_or(axum::http::StatusCode::NOT_FOUND)
                }
            ))
            .route("/apis/surreal-dbops.reliquo.io/v1alpha1/namespaces/:namespace/rollouts/:name/status", patch(
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
            .route("/apis/surreal-dbops.reliquo.io/v1alpha1/namespaces/:namespace/rollouts/:name", patch(
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
            .route("/apis/surreal-dbops.reliquo.io/v1alpha1/namespaces/:namespace/rollouts", post(
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
                        "apiVersion": "surreal-dbops.reliquo.io/v1alpha1",
                        "kind": "RolloutList",
                        "metadata": {},
                        "items": items
                    }))
                }
            ))
            .route("/apis/surreal-dbops.reliquo.io/v1alpha1/namespaces/:namespace/rollouts/:name", delete(
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
            "apiVersion": "surreal-dbops.reliquo.io/v1alpha1",
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
        mock_namespace_val_with_spec_name(name, instance_name, None)
    }

    fn mock_namespace_val_with_spec_name(
        name: &str,
        instance_name: &str,
        spec_name: Option<&str>,
    ) -> Value {
        let mut val = json!({
            "apiVersion": "surreal-dbops.reliquo.io/v1alpha1",
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
        });
        if let Some(s_name) = spec_name {
            val["spec"]["name"] = json!(s_name);
        }
        val
    }

    fn mock_database_val(name: &str, ns_name: &str, schema_name: &str) -> Value {
        mock_database_val_with_spec_name(name, ns_name, schema_name, None)
    }

    fn mock_database_val_with_spec_name(
        name: &str,
        ns_name: &str,
        schema_name: &str,
        spec_name: Option<&str>,
    ) -> Value {
        let mut val = json!({
            "apiVersion": "surreal-dbops.reliquo.io/v1alpha1",
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
        });
        if let Some(s_name) = spec_name {
            val["spec"]["name"] = json!(s_name);
        }
        val
    }

    fn mock_schema_val(name: &str, schema_text: &str) -> Value {
        json!({
            "apiVersion": "surreal-dbops.reliquo.io/v1alpha1",
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
        mock_rollout_val_with_desired_schema(name, schema_name, generation, None)
    }

    fn mock_rollout_val_with_desired_schema(
        name: &str,
        schema_name: &str,
        generation: i64,
        desired_schema: Option<&str>,
    ) -> Value {
        json!({
            "apiVersion": "surreal-dbops.reliquo.io/v1alpha1",
            "kind": "Rollout",
            "metadata": {
                "name": name,
                "namespace": "test-ns",
                "generation": 1,
                "uid": format!("{}-uid", name)
            },
            "spec": {
                "schemaRef": { "name": schema_name },
                "generation": generation,
                "desiredSchema": desired_schema.unwrap_or("")
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

        // 2b. Namespace Test with User Credentials
        {
            let mut inst_val = mock_instance_val("instns-users", "mem://");
            inst_val["status"] = json!({ "connected": true, "observedGeneration": 1 });
            state
                .instances
                .lock()
                .unwrap()
                .insert("instns-users".to_string(), inst_val);

            let mut ns_val = mock_namespace_val("nsns-users", "instns-users");
            ns_val["spec"]["userCredentials"] = json!([
                {
                    "username": { "value": "ns_admin" },
                    "password": { "value": "nspassword" },
                    "roles": ["OWNER"]
                }
            ]);
            state
                .namespaces
                .lock()
                .unwrap()
                .insert("nsns-users".to_string(), ns_val);

            let api: Api<Namespace> = Api::namespaced(client.clone(), "test-ns");
            let ns = api.get("nsns-users").await.unwrap();
            let result = namespace::reconcile(Arc::new(ns), ctx.clone()).await;
            assert!(result.is_ok());

            let updated_map = state.namespaces.lock().unwrap();
            let updated_ns = updated_map.get("nsns-users").unwrap();
            let status = updated_ns.get("status").expect("status to be populated");
            if status["created"].as_bool() != Some(true) {
                panic!("Namespace reconciliation failed with status: {:#?}", status);
            }

            // Connect to SurrealDB directly to verify user is created
            let db = surreal_dbops::surreal::connect_instance("mem://", "root", "rootpassword")
                .await
                .unwrap();
            let mut response = db
                .query("USE NS `nsns-users`; INFO FOR NS;")
                .await
                .unwrap()
                .check()
                .unwrap();
            let ns_info: Option<Value> = response.take(1usize).unwrap();
            let ns_info = ns_info.unwrap_or_default();
            let users = ns_info.get("users").expect("users field to be present");
            assert!(
                users.get("ns_admin").is_some()
                    || users.as_object().unwrap().keys().any(|k| k == "ns_admin"),
                "User ns_admin not found in NS info. Live: {:?}",
                users
            );
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
            assert_eq!(
                r["spec"]["desiredSchema"].as_str(),
                Some("DEFINE TABLE user SCHEMAFULL;")
            );
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
                        "surreal-dbops.reliquo.io/approved": "true",
                        "surreal-dbops.reliquo.io/approved-by": "admin-user",
                        "surreal-dbops.reliquo.io/approved-at": "2026-06-07T08:00:00Z"
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

        // 7. Rollout blocks when diff cannot be computed
        {
            let mut inst_val = mock_instance_val("instdifferr", "http://127.0.0.1:1");
            inst_val["status"] = json!({ "connected": true, "observedGeneration": 1 });
            state
                .instances
                .lock()
                .unwrap()
                .insert("instdifferr".to_string(), inst_val);

            let mut ns_val = mock_namespace_val("nsdifferr", "instdifferr");
            ns_val["status"] = json!({ "created": true, "observedGeneration": 1 });
            state
                .namespaces
                .lock()
                .unwrap()
                .insert("nsdifferr".to_string(), ns_val);

            let mut db_val = mock_database_val("dbdifferr", "nsdifferr", "schemadifferr");
            db_val["status"] = json!({ "created": true, "observedGeneration": 1 });
            state
                .databases
                .lock()
                .unwrap()
                .insert("dbdifferr".to_string(), db_val);

            let mut schema_val = mock_schema_val("schemadifferr", "DEFINE TABLE user SCHEMAFULL;");
            schema_val["status"] = json!({
                "currentVersionHash": "sha256:differrhash",
                "activeRolloutName": "rolloutdifferr",
                "observedGeneration": 1
            });
            state
                .schemas
                .lock()
                .unwrap()
                .insert("schemadifferr".to_string(), schema_val);

            let rollout_val = mock_rollout_val("rolloutdifferr", "schemadifferr", 1);
            state
                .rollouts
                .lock()
                .unwrap()
                .insert("rolloutdifferr".to_string(), rollout_val);

            let api: Api<Rollout> = Api::namespaced(client.clone(), "test-ns");
            let rollout = api.get("rolloutdifferr").await.unwrap();
            let result = rollout::reconcile(Arc::new(rollout), ctx.clone()).await;
            assert!(result.is_ok());

            let rollouts = state.rollouts.lock().unwrap();
            let r = rollouts.get("rolloutdifferr").unwrap();
            assert_eq!(r["status"]["phase"].as_str(), Some("Blocked"));
            assert_eq!(r["status"]["destructive"].as_bool(), Some(true));
            assert_eq!(
                r["status"]["conditions"][0]["reason"].as_str(),
                Some("DiffUnavailable")
            );
        }

        // 8. Rollout must use cached desired schema snapshot, not current Schema spec
        {
            let mut inst_val = mock_instance_val("instcache", "mem://");
            inst_val["status"] = json!({ "connected": true, "observedGeneration": 1 });
            state
                .instances
                .lock()
                .unwrap()
                .insert("instcache".to_string(), inst_val);

            let mut ns_val = mock_namespace_val("nscache", "instcache");
            ns_val["status"] = json!({ "created": true, "observedGeneration": 1 });
            state
                .namespaces
                .lock()
                .unwrap()
                .insert("nscache".to_string(), ns_val);

            let mut db_val = mock_database_val("dbcache", "nscache", "schemacache");
            db_val["status"] = json!({ "created": true, "observedGeneration": 1 });
            state
                .databases
                .lock()
                .unwrap()
                .insert("dbcache".to_string(), db_val);

            // Live DB currently contains table+field.
            let db_client =
                surreal_dbops::surreal::connect_instance("mem://", "root", "rootpassword")
                    .await
                    .unwrap();
            db_client.use_ns("nscache").use_db("dbcache").await.unwrap();
            db_client
                .query("DEFINE TABLE user SCHEMAFULL; DEFINE FIELD name ON TABLE user TYPE string;")
                .await
                .unwrap();

            // Current Schema spec has advanced and removed field `name`.
            let mut schema_val = mock_schema_val("schemacache", "DEFINE TABLE user SCHEMAFULL;");
            schema_val["status"] = json!({
                "currentVersionHash": "sha256:newhash",
                "activeRolloutName": "rolloutcache",
                "observedGeneration": 2
            });
            state
                .schemas
                .lock()
                .unwrap()
                .insert("schemacache".to_string(), schema_val);

            // Rollout generation 1 must keep using its cached desired schema (with field).
            let rollout_val = mock_rollout_val_with_desired_schema(
                "rolloutcache",
                "schemacache",
                1,
                Some("DEFINE TABLE user SCHEMAFULL; DEFINE FIELD name ON TABLE user TYPE string;"),
            );
            state
                .rollouts
                .lock()
                .unwrap()
                .insert("rolloutcache".to_string(), rollout_val);

            let api: Api<Rollout> = Api::namespaced(client.clone(), "test-ns");
            let rollout = api.get("rolloutcache").await.unwrap();
            let result = rollout::reconcile(Arc::new(rollout), ctx.clone()).await;
            assert!(result.is_ok());

            let rollouts = state.rollouts.lock().unwrap();
            let r = rollouts.get("rolloutcache").unwrap();
            assert_eq!(r["status"]["phase"].as_str(), Some("Completed"));
            assert_eq!(r["status"]["destructive"].as_bool(), Some(false));
            let diff = r["status"]["diff"].as_str().unwrap_or("");
            assert!(
                !diff.contains("REMOVE FIELD name ON TABLE user"),
                "rollout used current Schema instead of cached desired schema: {}",
                diff
            );
        }

        // 9. Database reconcile triggers latest rollout generation
        {
            let mut inst_val = mock_instance_val("insttrigger", "mem://");
            inst_val["status"] = json!({ "connected": true, "observedGeneration": 1 });
            state
                .instances
                .lock()
                .unwrap()
                .insert("insttrigger".to_string(), inst_val);

            let mut ns_val = mock_namespace_val("nstrigger", "insttrigger");
            ns_val["status"] = json!({ "created": true, "observedGeneration": 1 });
            state
                .namespaces
                .lock()
                .unwrap()
                .insert("nstrigger".to_string(), ns_val);

            let mut schema_val = mock_schema_val("schematrigger", "DEFINE TABLE user SCHEMAFULL;");
            schema_val["metadata"]["generation"] = json!(2);
            schema_val["status"] = json!({
                "currentVersionHash": "sha256:triggerhash",
                "activeRolloutName": "schematrigger-rollout-2",
                "observedGeneration": 2
            });
            state
                .schemas
                .lock()
                .unwrap()
                .insert("schematrigger".to_string(), schema_val);

            let rollout_val = mock_rollout_val_with_desired_schema(
                "schematrigger-rollout-2",
                "schematrigger",
                2,
                Some("DEFINE TABLE user SCHEMAFULL;"),
            );
            state
                .rollouts
                .lock()
                .unwrap()
                .insert("schematrigger-rollout-2".to_string(), rollout_val);

            let db_val = mock_database_val("dbtrigger", "nstrigger", "schematrigger");
            state
                .databases
                .lock()
                .unwrap()
                .insert("dbtrigger".to_string(), db_val);

            let api: Api<Database> = Api::namespaced(client.clone(), "test-ns");
            let db = api.get("dbtrigger").await.unwrap();
            let result = database::reconcile(Arc::new(db), ctx.clone()).await;
            assert!(result.is_ok());

            let initial_marker = {
                let rollouts = state.rollouts.lock().unwrap();
                let r = rollouts.get("schematrigger-rollout-2").unwrap();
                r["metadata"]["annotations"]["surreal-dbops.reliquo.io/triggered-by-database"]
                    .as_str()
                    .map(|s| s.to_string())
                    .expect("latest rollout was not triggered by database reconcile")
            };

            // Reconcile the same DB again. Trigger marker should be unchanged,
            // proving we do not patch rollout annotations repeatedly.
            let db_again = api.get("dbtrigger").await.unwrap();
            let result = database::reconcile(Arc::new(db_again), ctx.clone()).await;
            assert!(result.is_ok());

            let rollouts = state.rollouts.lock().unwrap();
            let r = rollouts.get("schematrigger-rollout-2").unwrap();
            let marker_after = r["metadata"]["annotations"]
                ["surreal-dbops.reliquo.io/triggered-by-database"]
                .as_str()
                .expect("trigger marker should still exist")
                .to_string();
            assert_eq!(
                marker_after, initial_marker,
                "database reconcile should not keep mutating rollout trigger annotation"
            );
        }

        // 10. Completed rollout does not mutate or run reconciliation when up-to-date
        {
            let mut inst_val = mock_instance_val("instshort", "mem://");
            inst_val["status"] = json!({ "connected": true, "observedGeneration": 1 });
            state
                .instances
                .lock()
                .unwrap()
                .insert("instshort".to_string(), inst_val);

            let mut ns_val = mock_namespace_val("nsshort", "instshort");
            ns_val["status"] = json!({ "created": true, "observedGeneration": 1 });
            state
                .namespaces
                .lock()
                .unwrap()
                .insert("nsshort".to_string(), ns_val);

            let mut db_val = mock_database_val("dbshort", "nsshort", "schemashort");
            db_val["status"] = json!({
                "created": true,
                "observedGeneration": 1,
                "appliedSchemaGeneration": 1
            });
            state
                .databases
                .lock()
                .unwrap()
                .insert("dbshort".to_string(), db_val);

            let mut rollout_val = mock_rollout_val_with_desired_schema(
                "rolloutshort",
                "schemashort",
                1,
                Some("DEFINE TABLE user SCHEMAFULL;"),
            );
            rollout_val["status"] = json!({
                "phase": "Completed",
                "diff": "",
                "destructive": false,
                "affectedDatabases": 1,
                "appliedDatabases": 1,
                "failedDatabases": 0,
                "approved": false,
                "conditions": []
            });
            state
                .rollouts
                .lock()
                .unwrap()
                .insert("rolloutshort".to_string(), rollout_val);

            let mut schema_val = mock_schema_val("schemashort", "DEFINE TABLE user SCHEMAFULL;");
            schema_val["status"] = json!({
                "currentVersionHash": "sha256:somehash",
                "activeRolloutName": "rolloutshort",
                "observedGeneration": 1
            });
            state
                .schemas
                .lock()
                .unwrap()
                .insert("schemashort".to_string(), schema_val);

            let api: Api<Rollout> = Api::namespaced(client.clone(), "test-ns");
            let rollout = api.get("rolloutshort").await.unwrap();

            // Reconciling the completed rollout should immediately return Ok(Action::await_change())
            let result = rollout::reconcile(Arc::new(rollout), ctx.clone()).await;
            assert!(result.is_ok());

            // Reconciling should not mutate the rollout status (it short-circuited early)
            let rollouts = state.rollouts.lock().unwrap();
            let r = rollouts.get("rolloutshort").unwrap();
            assert_eq!(r["status"]["phase"].as_str(), Some("Completed"));
            assert_eq!(r["status"]["affectedDatabases"].as_u64(), Some(1));

            // Also verify that a database with a newer generation (e.g. 2) is also ignored by rollout 1
            drop(rollouts);
            let mut db_updated = state
                .databases
                .lock()
                .unwrap()
                .get("dbshort")
                .unwrap()
                .clone();
            db_updated["status"]["appliedSchemaGeneration"] = json!(2);
            state
                .databases
                .lock()
                .unwrap()
                .insert("dbshort".to_string(), db_updated);

            let rollout_again = api.get("rolloutshort").await.unwrap();
            let result_again = rollout::reconcile(Arc::new(rollout_again), ctx.clone()).await;
            assert!(result_again.is_ok());

            let rollouts = state.rollouts.lock().unwrap();
            let r = rollouts.get("rolloutshort").unwrap();
            assert_eq!(r["status"]["phase"].as_str(), Some("Completed"));
        }

        // 11. Namespace Test with Custom spec.name
        {
            let mut inst_val = mock_instance_val("instnscustom", "mem://");
            inst_val["status"] = json!({ "connected": true, "observedGeneration": 1 });
            state
                .instances
                .lock()
                .unwrap()
                .insert("instnscustom".to_string(), inst_val);

            let ns_val = mock_namespace_val_with_spec_name(
                "nsnscustom",
                "instnscustom",
                Some("my-custom-ns-name"),
            );
            state
                .namespaces
                .lock()
                .unwrap()
                .insert("nsnscustom".to_string(), ns_val);

            let api: Api<Namespace> = Api::namespaced(client.clone(), "test-ns");
            let ns = api.get("nsnscustom").await.unwrap();
            let result = namespace::reconcile(Arc::new(ns), ctx.clone()).await;
            assert!(result.is_ok());
            let updated_map = state.namespaces.lock().unwrap();
            let updated_ns = updated_map.get("nsnscustom").unwrap();
            let status = updated_ns.get("status").expect("status to be populated");
            assert_eq!(status["created"].as_bool(), Some(true));
        }

        // 12. Database Test with Custom spec.name
        {
            let mut inst_val = mock_instance_val("instdbcustom", "mem://");
            inst_val["status"] = json!({ "connected": true, "observedGeneration": 1 });
            state
                .instances
                .lock()
                .unwrap()
                .insert("instdbcustom".to_string(), inst_val);

            let mut ns_val = mock_namespace_val_with_spec_name(
                "nsdbcustom",
                "instdbcustom",
                Some("my-custom-ns-name"),
            );
            ns_val["status"] = json!({ "created": true, "observedGeneration": 1 });
            state
                .namespaces
                .lock()
                .unwrap()
                .insert("nsdbcustom".to_string(), ns_val);

            let db_val = mock_database_val_with_spec_name(
                "dbdbcustom",
                "nsdbcustom",
                "schemadbcustom",
                Some("my-custom-db-name"),
            );
            state
                .databases
                .lock()
                .unwrap()
                .insert("dbdbcustom".to_string(), db_val);

            let api: Api<Database> = Api::namespaced(client.clone(), "test-ns");
            let db = api.get("dbdbcustom").await.unwrap();
            let result = database::reconcile(Arc::new(db), ctx.clone()).await;
            assert!(result.is_ok());

            let updated_map = state.databases.lock().unwrap();
            let updated_db = updated_map.get("dbdbcustom").unwrap();
            let status = updated_db.get("status").expect("status to be populated");
            assert_eq!(status["created"].as_bool(), Some(true));
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
                    "surreal-dbops.reliquo.io/approved": "true"
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
                && op["path"] == "/metadata/annotations/surreal-dbops.reliquo.io~1approved-by"
                && op["value"] == "dev-person"
        });
        assert!(has_approved_by, "Patch must record the user username");

        let has_approved_at = patch_array.iter().any(|op| {
            op["op"] == "add"
                && op["path"]
                    .as_str()
                    .unwrap()
                    .contains("surreal-dbops.reliquo.io~1approved-at")
        });
        assert!(has_approved_at, "Patch must record approval time");
    }
}
