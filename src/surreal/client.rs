use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;
use tokio::sync::OnceCell;

static MEM_CLIENT: OnceCell<Surreal<Any>> = OnceCell::const_new();

/// Connects to a SurrealDB instance using root credentials.
pub async fn connect_instance(
    endpoint: &str,
    username: &str,
    password: &str,
) -> Result<Surreal<Any>, surrealdb::Error> {
    if endpoint.starts_with("mem://") {
        let client = MEM_CLIENT.get_or_init(|| async {
            connect("mem://").await.expect("Failed to initialize in-memory SurrealDB")
        }).await;
        return Ok(client.clone());
    }

    let db = connect(endpoint).await?;
    db.signin(Root {
        username: username.to_string(),
        password: password.to_string(),
    }).await?;
    Ok(db)
}
