use std::sync::Arc;
use std::net::SocketAddr;
use std::path::Path;
use kube::{Api, Client};
use kube::runtime::Controller;
use kube::api::ListParams;
use futures::StreamExt;
use tracing::{info, error, Level};
use tracing_subscriber::FmtSubscriber;

use surreal_dbops::crd::{Instance, Namespace, Database, Schema, Rollout};
use surreal_dbops::controller::{Context, instance, namespace, database, schema, rollout};
use surreal_dbops::webhook::start_webhook_server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Check if --crdgen was passed
    if std::env::args().any(|arg| arg == "--crdgen") {
        use kube::CustomResourceExt;
        let crds = vec![
            Instance::crd(),
            Namespace::crd(),
            Database::crd(),
            Schema::crd(),
            Rollout::crd(),
        ];
        for crd in crds {
            let yaml = serde_yaml::to_string(&crd)?;
            println!("---\n{}", yaml);
        }
        return Ok(());
    }

    // 1. Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting surreal-dbops operator...");

    // 2. Initialize Kubernetes client
    let client = Client::try_default().await?;
    let context = Arc::new(Context { client: client.clone() });

    // 3. Initialize APIs
    let instances: Api<Instance> = Api::all(client.clone());
    let namespaces: Api<Namespace> = Api::all(client.clone());
    let databases: Api<Database> = Api::all(client.clone());
    let schemas: Api<Schema> = Api::all(client.clone());
    let rollouts: Api<Rollout> = Api::all(client.clone());

    // 4. Start Webhook Server (if certs and key are provided)
    let enable_webhook = std::env::var("ENABLE_WEBHOOK").unwrap_or_else(|_| "false".to_string()) == "true";
    if enable_webhook {
        let port = std::env::var("WEBHOOK_PORT").unwrap_or_else(|_| "8443".to_string())
            .parse::<u16>()?;
        let cert_path_str = std::env::var("TLS_CERT_PATH").unwrap_or_else(|_| "/tls/tls.crt".to_string());
        let key_path_str = std::env::var("TLS_KEY_PATH").unwrap_or_else(|_| "/tls/tls.key".to_string());
        
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        let cert_path = Path::new(&cert_path_str).to_path_buf();
        let key_path = Path::new(&key_path_str).to_path_buf();

        tokio::spawn(async move {
            if let Err(e) = start_webhook_server(addr, &cert_path, &key_path).await {
                error!("Webhook server failed to start: {}", e);
            }
        });
    }

    // 5. Start Controller reconciliation loops
    let instance_handle = tokio::spawn({
        let context = context.clone();
        async move {
            info!("Starting Instance controller...");
            Controller::new(instances, kube::runtime::watcher::Config::default())
                .run(instance::reconcile, instance::error_policy, context)
                .for_each(|res| async move {
                    if let Err(e) = res {
                        error!("Instance controller reconciliation error: {:?}", e);
                    }
                })
                .await;
        }
    });

    let namespace_handle = tokio::spawn({
        let context = context.clone();
        async move {
            info!("Starting Namespace controller...");
            Controller::new(namespaces, kube::runtime::watcher::Config::default())
                .run(namespace::reconcile, namespace::error_policy, context)
                .for_each(|res| async move {
                    if let Err(e) = res {
                        error!("Namespace controller reconciliation error: {:?}", e);
                    }
                })
                .await;
        }
    });

    let database_handle = tokio::spawn({
        let context = context.clone();
        async move {
            info!("Starting Database controller...");
            Controller::new(databases, kube::runtime::watcher::Config::default())
                .run(database::reconcile, database::error_policy, context)
                .for_each(|res| async move {
                    if let Err(e) = res {
                        error!("Database controller reconciliation error: {:?}", e);
                    }
                })
                .await;
        }
    });

    let schema_handle = tokio::spawn({
        let context = context.clone();
        async move {
            info!("Starting Schema controller...");
            Controller::new(schemas, kube::runtime::watcher::Config::default())
                .run(schema::reconcile, schema::error_policy, context)
                .for_each(|res| async move {
                    if let Err(e) = res {
                        error!("Schema controller reconciliation error: {:?}", e);
                    }
                })
                .await;
        }
    });

    let rollout_handle = tokio::spawn({
        let context = context.clone();
        async move {
            info!("Starting Rollout controller...");
            Controller::new(rollouts, kube::runtime::watcher::Config::default())
                .run(rollout::reconcile, rollout::error_policy, context)
                .for_each(|res| async move {
                    if let Err(e) = res {
                        error!("Rollout controller reconciliation error: {:?}", e);
                    }
                })
                .await;
        }
    });

    // Keep running all controllers
    let _ = tokio::join!(
        instance_handle,
        namespace_handle,
        database_handle,
        schema_handle,
        rollout_handle
    );

    Ok(())
}
