#[cfg(test)]
mod tests {
    use surreal_dbops::surreal::diff::{parse_schema, SchemaObjectType};

    #[test]
    fn test_parse_schema_tables_and_fields() {
        let schema_text = "
            -- This is a comment
            DEFINE TABLE account SCHEMAFULL;
            
            # Another comment
            DEFINE FIELD email ON TABLE account TYPE string ASSERT string::is::email($value);
            
            DEFINE INDEX email_idx ON TABLE account FIELDS email UNIQUE;
            
            // Scope definition
            DEFINE ACCESS users ON DATABASE TYPE RECORD
              SIGNUP ( CREATE account SET email = $email )
              SIGNIN ( SELECT * FROM account WHERE email = $email )
            ;
        ";

        let objects = parse_schema(schema_text);

        // Verify table was parsed
        let table_key = "table:account";
        assert!(objects.contains_key(table_key), "Should contain table key");
        let table = objects.get(table_key).unwrap();
        assert_eq!(table.name, "account");
        assert_eq!(table.object_type, SchemaObjectType::Table);
        assert!(table.definition.contains("DEFINE TABLE account SCHEMAFULL"));

        // Verify field was parsed
        let field_key = "field:account.email";
        assert!(objects.contains_key(field_key), "Should contain field key");
        let field = objects.get(field_key).unwrap();
        assert_eq!(field.name, "email");
        assert_eq!(
            field.object_type,
            SchemaObjectType::Field {
                table: "account".to_string()
            }
        );

        // Verify index was parsed
        let index_key = "index:account.email_idx";
        assert!(objects.contains_key(index_key), "Should contain index key");
        let index = objects.get(index_key).unwrap();
        assert_eq!(index.name, "email_idx");
        assert_eq!(
            index.object_type,
            SchemaObjectType::Index {
                table: "account".to_string()
            }
        );

        // Verify access was parsed
        let access_key = "access:users";
        assert!(
            objects.contains_key(access_key),
            "Should contain access key"
        );
        let access = objects.get(access_key).unwrap();
        assert_eq!(access.name, "users");
        assert_eq!(access.object_type, SchemaObjectType::Access);
    }

    #[tokio::test]
    async fn test_db_info_deserialization() {
        let db = match surrealdb::engine::any::connect("http://localhost:8000").await {
            Ok(db) => db,
            Err(e) => {
                println!("Skipping integration-style diff test: local SurrealDB not reachable ({:?})", e);
                return;
            }
        };
        if let Err(e) = db.signin(surrealdb::opt::auth::Root {
            username: "root".to_string(),
            password: "rootpassword".to_string(),
        }).await {
            println!("Skipping integration-style diff test: signin failed ({:?})", e);
            return;
        }
        
        let _ = db.query("DEFINE NAMESPACE `test-ns-surreal`; DEFINE DATABASE `project-db`;").await;
        if let Err(e) = db.use_ns("test-ns-surreal").use_db("project-db").await {
            println!("Skipping integration-style diff test: use_ns/db failed ({:?})", e);
            return;
        }
        
        // Define some schema
        let _ = db.query("DEFINE TABLE person SCHEMAFULL; DEFINE FIELD name ON TABLE person TYPE string;").await.unwrap();

        // Test compute_diff
        let desired_schema = "DEFINE TABLE person SCHEMAFULL;";
        let res = surreal_dbops::surreal::diff::compute_diff(&db, desired_schema).await;
        println!("=== DEBUG: compute_diff result: {:?}", res);
        let (diff, destructive) = res.expect("Failed to compute diff!");
        assert!(destructive);
        assert!(!diff.is_empty());
        assert!(diff[0].contains("REMOVE FIELD name ON TABLE person"));
    }
}
