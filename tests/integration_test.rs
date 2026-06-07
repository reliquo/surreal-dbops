#[cfg(test)]
mod integration_tests {
    use std::collections::BTreeMap;
    use std::time::Duration;
    use kube::{Api, Client, Resource, ResourceExt};
    use kube::api::{PostParams, PatchParams, Patch};
    use k8s_openapi::api::core::v1::{Namespace as K8sNamespace, Secret, ConfigMap};
    use serde_json::json;
    use tokio::time::sleep;

    use surreal_dbops::crd::{Instance, InstanceSpec, Namespace, NamespaceSpec, Database, DatabaseSpec, Schema, SchemaSpec, Rollout, ValueOrRefSource, ValueFromSource, SecretKeySelector, ConfigMapKeySelector, LocalObjectReference, ApprovalPolicy};
    use surreal_dbops::surreal::connect_instance;

    const TEST_NAMESPACE: &str = "test-ns-dbops";

    async fn ensure_namespace(client: &Client) {
        let ns_api: Api<K8sNamespace> = Api::all(client.clone());
        let test_ns = K8sNamespace {
            metadata: kube::api::ObjectMeta {
                name: Some(TEST_NAMESPACE.to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let _ = ns_api.create(&PostParams::default(), &test_ns).await;
        // Wait a brief moment for namespace initialization
        sleep(Duration::from_secs(2)).await;
    }

    async fn create_root_secret(client: &Client) {
        let secret_api: Api<Secret> = Api::namespaced(client.clone(), TEST_NAMESPACE);
        let secret = Secret {
            metadata: kube::api::ObjectMeta {
                name: Some("surrealdb-root".to_string()),
                namespace: Some(TEST_NAMESPACE.to_string()),
                ..Default::default()
            },
            data: Some(BTreeMap::from([
                ("password".to_string(), k8s_openapi::ByteString("rootpassword".as_bytes().to_vec())),
            ])),
            ..Default::default()
        };
        let _ = secret_api.create(&PostParams::default(), &secret).await;
    }

    #[tokio::test]
    async fn test_surreal_dbops_operator_lifecycle() {
        let client = Client::try_default().await.expect("Failed to create K8s client");
        ensure_namespace(&client).await;
        create_root_secret(&client).await;

        let instance_api: Api<Instance> = Api::namespaced(client.clone(), TEST_NAMESPACE);
        let ns_api: Api<Namespace> = Api::namespaced(client.clone(), TEST_NAMESPACE);
        let db_api: Api<Database> = Api::namespaced(client.clone(), TEST_NAMESPACE);
        let schema_api: Api<Schema> = Api::namespaced(client.clone(), TEST_NAMESPACE);
        let rollout_api: Api<Rollout> = Api::namespaced(client.clone(), TEST_NAMESPACE);

        // ==============================================================================
        // 1. Create Instance CRD
        // ==============================================================================
        println!("Creating Instance CRD...");
        let instance_spec = InstanceSpec {
            connection_string: ValueOrRefSource {
                value: Some("http://surrealdb.reliquo-system.svc.cluster.local:8000".to_string()),
                value_from: None,
            },
            username: ValueOrRefSource {
                value: Some("root".to_string()),
                value_from: None,
            },
            password: ValueOrRefSource {
                value: None,
                value_from: Some(ValueFromSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: "surrealdb-root".to_string(),
                        key: "password".to_string(),
                    }),
                    config_map_ref: None,
                }),
            },
        };
        let instance = Instance::new("main-cluster", instance_spec);
        let _ = instance_api.create(&PostParams::default(), &instance).await.unwrap();

        // Wait for Instance status ready
        println!("Waiting for Instance status.connected = true...");
        let mut ready = false;
        for _ in 0..10 {
            if let Ok(inst) = instance_api.get("main-cluster").await {
                if let Some(status) = inst.status {
                    if status.connected {
                        ready = true;
                        break;
                    }
                }
            }
            sleep(Duration::from_secs(3)).await;
        }
        assert!(ready, "Instance failed to reach connected status");

        // ==============================================================================
        // 2. Create Namespace CRD
        // ==============================================================================
        println!("Creating Namespace CRD...");
        let ns_spec = NamespaceSpec {
            instance_ref: LocalObjectReference {
                name: "main-cluster".to_string(),
                namespace: Some(TEST_NAMESPACE.to_string()),
            },
        };
        let namespace = Namespace::new("test-ns-surreal", ns_spec);
        let _ = ns_api.create(&PostParams::default(), &namespace).await.unwrap();

        println!("Waiting for Namespace status.created = true...");
        ready = false;
        for _ in 0..10 {
            if let Ok(n) = ns_api.get("test-ns-surreal").await {
                if let Some(status) = n.status {
                    if status.created {
                        ready = true;
                        break;
                    }
                }
            }
            sleep(Duration::from_secs(3)).await;
        }
        assert!(ready, "Namespace failed to reach created status");

        // Connect directly to SurrealDB via local port-forward to verify namespace was created
        let db_client = connect_instance("http://localhost:8000", "root", "rootpassword").await
            .expect("Failed to connect to SurrealDB from test host");

        let ns_check = db_client.query("USE NS `test-ns-surreal`; INFO FOR NS;").await;
        assert!(ns_check.is_ok(), "Namespace test-ns-surreal was not created in SurrealDB");

        // ==============================================================================
        // 3. Create ConfigMap and Schema CRD (Non-destructive)
        // ==============================================================================
        println!("Creating ConfigMap for Schema...");
        let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), TEST_NAMESPACE);
        let cm = ConfigMap {
            metadata: kube::api::ObjectMeta {
                name: Some("schemas".to_string()),
                namespace: Some(TEST_NAMESPACE.to_string()),
                ..Default::default()
            },
            data: Some(BTreeMap::from([
                ("project.surql".to_string(), "DEFINE TABLE person SCHEMAFULL; DEFINE FIELD name ON TABLE person TYPE string;".to_string()),
            ])),
            ..Default::default()
        };
        let _ = cm_api.create(&PostParams::default(), &cm).await.unwrap();

        println!("Creating Schema CRD...");
        let schema_spec = SchemaSpec {
            revision_history_limit: Some(5),
            concurrency_limit: Some(10),
            require_approval: Some(ApprovalPolicy::Destructive),
            schema: ValueOrRefSource {
                value: None,
                value_from: Some(ValueFromSource {
                    secret_key_ref: None,
                    config_map_ref: Some(ConfigMapKeySelector {
                        name: "schemas".to_string(),
                        key: "project.surql".to_string(),
                    }),
                }),
            },
            variables: None,
        };
        let schema = Schema::new("project-schema", schema_spec);
        let _ = schema_api.create(&PostParams::default(), &schema).await.unwrap();

        // ==============================================================================
        // 4. Create Database CRD
        // ==============================================================================
        println!("Creating Database CRD...");
        let db_spec = DatabaseSpec {
            namespace_ref: LocalObjectReference {
                name: "test-ns-surreal".to_string(),
                namespace: Some(TEST_NAMESPACE.to_string()),
            },
            schema_ref: LocalObjectReference {
                name: "project-schema".to_string(),
                namespace: Some(TEST_NAMESPACE.to_string()),
            },
        };
        let database = Database::new("project-db", db_spec);
        let _ = db_api.create(&PostParams::default(), &database).await.unwrap();

        println!("Waiting for Database status.created = true...");
        ready = false;
        for _ in 0..10 {
            if let Ok(d) = db_api.get("project-db").await {
                if let Some(status) = d.status {
                    if status.created {
                        ready = true;
                        break;
                    }
                }
            }
            sleep(Duration::from_secs(3)).await;
        }
        assert!(ready, "Database failed to reach created status");

        // Wait for Rollout (gen 1) completion
        println!("Waiting for Rollout project-schema-rollout-1 to complete...");
        ready = false;
        for _ in 0..10 {
            if let Ok(r) = rollout_api.get("project-schema-rollout-1").await {
                if let Some(status) = r.status {
                    if status.phase.as_deref() == Some("Completed") {
                        ready = true;
                        break;
                    }
                }
            }
            sleep(Duration::from_secs(3)).await;
        }
        assert!(ready, "Rollout 1 failed to reach Completed phase");

        // Verify table and field exist in SurrealDB
        db_client.query("USE NS `test-ns-surreal`; USE DB `project-db`;").await.unwrap();
        let table_check = db_client.query("INFO FOR TABLE person;").await;
        assert!(table_check.is_ok(), "Table person was not created in database");

        // ==============================================================================
        // 5. Destructive Rollout (Modify schema to remove field 'name')
        // ==============================================================================
        println!("Modifying ConfigMap to trigger destructive schema update...");
        let cm_patch = json!({
            "data": {
                "project.surql": "DEFINE TABLE person SCHEMAFULL;"
            }
        });
        cm_api.patch("schemas", &PatchParams::default(), &Patch::Merge(&cm_patch)).await.unwrap();

        // Increment Schema spec to trigger reconcile
        let schema_patch = json!({
            "spec": {
                "revisionHistoryLimit": 6
            }
        });
        schema_api.patch("project-schema", &PatchParams::default(), &Patch::Merge(&schema_patch)).await.unwrap();

        // Wait for Rollout (gen 2) creation and transition to Blocked
        println!("Waiting for Rollout project-schema-rollout-2 to reach Blocked phase...");
        ready = false;
        for _ in 0..10 {
            if let Ok(r) = rollout_api.get("project-schema-rollout-2").await {
                if let Some(status) = r.status {
                    if status.phase.as_deref() == Some("Blocked") && status.destructive {
                        ready = true;
                        break;
                    }
                }
            }
            sleep(Duration::from_secs(3)).await;
        }
        assert!(ready, "Rollout 2 did not transition to Blocked phase on destructive change");

        // Verify field 'name' STILL exists in SurrealDB (migration blocked)
        let info_res = db_client.query("USE NS `test-ns-surreal`; USE DB `project-db`; INFO FOR TABLE person;").await.unwrap();
        let info_str = format!("{:?}", info_res);
        assert!(info_str.contains("name"), "Field 'name' was prematurely removed before approval");

        // ==============================================================================
        // 6. Approve Destructive Rollout
        // ==============================================================================
        println!("Approving destructive Rollout 2 via annotation...");
        let approval_patch = json!({
            "metadata": {
                "annotations": {
                    "database.reliquo.io/approved": "true"
                }
            }
        });
        rollout_api.patch("project-schema-rollout-2", &PatchParams::default(), &Patch::Merge(&approval_patch)).await.unwrap();

        // Wait for Rollout 2 completion
        println!("Waiting for Rollout 2 to complete after approval...");
        ready = false;
        for _ in 0..15 {
            if let Ok(r) = rollout_api.get("project-schema-rollout-2").await {
                if let Some(status) = r.status {
                    if status.phase.as_deref() == Some("Completed") {
                        ready = true;
                        break;
                    }
                }
            }
            sleep(Duration::from_secs(3)).await;
        }
        assert!(ready, "Rollout 2 failed to complete after approval");

        // Verify Mutating Admission Webhook injected approved-by metadata
        let final_rollout = rollout_api.get("project-schema-rollout-2").await.unwrap();
        let annotations = final_rollout.metadata.annotations.unwrap_or_default();
        assert!(annotations.contains_key("database.reliquo.io/approved-by"), "Webhook failed to inject approved-by annotation");
        assert!(annotations.contains_key("database.reliquo.io/approved-at"), "Webhook failed to inject approved-at annotation");

        // Verify field 'name' is now removed in SurrealDB
        let final_info_res = db_client.query("USE NS `test-ns-surreal`; USE DB `project-db`; INFO FOR TABLE person;").await.unwrap();
        let final_info_str = format!("{:?}", final_info_res);
        assert!(!final_info_str.contains("name"), "Field 'name' was not removed after rollout completed");

        println!("=== Integration Test Completed Successfully! ===");
    }
}
