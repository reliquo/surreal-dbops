use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use surrealdb_types::SurrealValue;
use tracing::{debug, info};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SchemaObjectType {
    Table,
    Field { table: String },
    Index { table: String },
    Event { table: String },
    Access,
}

impl SchemaObjectType {
    pub fn priority(&self) -> u8 {
        match self {
            SchemaObjectType::Table => 0,
            SchemaObjectType::Field { .. } => 1,
            SchemaObjectType::Index { .. } => 2,
            SchemaObjectType::Event { .. } => 3,
            SchemaObjectType::Access => 4,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SchemaObject {
    pub object_type: SchemaObjectType,
    pub name: String,
    pub definition: String,
}

impl SchemaObject {
    pub fn key(&self) -> String {
        match &self.object_type {
            SchemaObjectType::Table => format!("table:{}", self.name),
            SchemaObjectType::Field { table } => format!("field:{}.{}", table, self.name),
            SchemaObjectType::Index { table } => format!("index:{}.{}", table, self.name),
            SchemaObjectType::Event { table } => format!("event:{}.{}", table, self.name),
            SchemaObjectType::Access => format!("access:{}", self.name),
        }
    }
}

/// A simplified SurrealQL parser that extracts DEFINE statements from a schema text.
pub fn parse_schema(schema_text: &str) -> BTreeMap<String, SchemaObject> {
    let mut objects = BTreeMap::new();

    // Split by semicolons, taking care of comments and basic strings
    let mut statements = Vec::new();
    let mut current_stmt = String::new();
    let mut in_double_quote = false;
    let mut in_single_quote = false;
    let mut in_comment = false;
    let mut chars = schema_text.chars().peekable();

    while let Some(c) = chars.next() {
        if in_comment {
            if c == '\n' {
                in_comment = false;
            }
            continue;
        }

        // Detect single-line comments (-- or // or #)
        if !in_double_quote && !in_single_quote {
            if c == '#' {
                in_comment = true;
                continue;
            }
            if c == '-' && chars.peek() == Some(&'-') {
                in_comment = true;
                let _ = chars.next();
                continue;
            }
            if c == '/' && chars.peek() == Some(&'/') {
                in_comment = true;
                let _ = chars.next();
                continue;
            }
        }

        if c == '"' && !in_single_quote {
            in_double_quote = !in_double_quote;
        }
        if c == '\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
        }

        if c == ';' && !in_double_quote && !in_single_quote {
            let stmt = current_stmt.trim().to_string();
            if !stmt.is_empty() {
                statements.push(stmt);
            }
            current_stmt.clear();
        } else {
            current_stmt.push(c);
        }
    }
    let stmt = current_stmt.trim().to_string();
    if !stmt.is_empty() {
        statements.push(stmt);
    }

    // Process each statement to extract definitions
    for stmt in statements {
        let normalized = stmt.to_uppercase();
        if !normalized.starts_with("DEFINE") {
            continue;
        }

        // Split tokens
        let tokens: Vec<&str> = stmt.split_whitespace().collect();
        if tokens.len() < 3 {
            continue;
        }

        let obj_type = tokens[1].to_uppercase();
        let obj_name = tokens[2].replace(":", ""); // Clean record prefix if present

        let schema_obj = if obj_type == "TABLE" {
            Some(SchemaObject {
                object_type: SchemaObjectType::Table,
                name: obj_name.to_lowercase(),
                definition: stmt.clone(),
            })
        } else if obj_type == "FIELD" {
            // Format: DEFINE FIELD name ON [TABLE] table ...
            let table_index = tokens.iter().position(|&t| t.to_uppercase() == "ON");
            if let Some(idx) = table_index {
                let next_token = tokens
                    .get(idx + 1)
                    .map(|t| t.to_uppercase())
                    .unwrap_or_default();
                let table_name = if next_token == "TABLE" {
                    tokens
                        .get(idx + 2)
                        .map(|t| t.replace(";", ""))
                        .unwrap_or_default()
                } else {
                    tokens
                        .get(idx + 1)
                        .map(|t| t.replace(";", ""))
                        .unwrap_or_default()
                };
                Some(SchemaObject {
                    object_type: SchemaObjectType::Field {
                        table: table_name.to_lowercase(),
                    },
                    name: obj_name.to_lowercase(),
                    definition: stmt.clone(),
                })
            } else {
                None
            }
        } else if obj_type == "INDEX" {
            let table_index = tokens.iter().position(|&t| t.to_uppercase() == "ON");
            if let Some(idx) = table_index {
                let next_token = tokens
                    .get(idx + 1)
                    .map(|t| t.to_uppercase())
                    .unwrap_or_default();
                let table_name = if next_token == "TABLE" {
                    tokens
                        .get(idx + 2)
                        .map(|t| t.replace(";", ""))
                        .unwrap_or_default()
                } else {
                    tokens
                        .get(idx + 1)
                        .map(|t| t.replace(";", ""))
                        .unwrap_or_default()
                };
                Some(SchemaObject {
                    object_type: SchemaObjectType::Index {
                        table: table_name.to_lowercase(),
                    },
                    name: obj_name.to_lowercase(),
                    definition: stmt.clone(),
                })
            } else {
                None
            }
        } else if obj_type == "EVENT" {
            let table_index = tokens.iter().position(|&t| t.to_uppercase() == "ON");
            if let Some(idx) = table_index {
                let next_token = tokens
                    .get(idx + 1)
                    .map(|t| t.to_uppercase())
                    .unwrap_or_default();
                let table_name = if next_token == "TABLE" {
                    tokens
                        .get(idx + 2)
                        .map(|t| t.replace(";", ""))
                        .unwrap_or_default()
                } else {
                    tokens
                        .get(idx + 1)
                        .map(|t| t.replace(";", ""))
                        .unwrap_or_default()
                };
                Some(SchemaObject {
                    object_type: SchemaObjectType::Event {
                        table: table_name.to_lowercase(),
                    },
                    name: obj_name.to_lowercase(),
                    definition: stmt.clone(),
                })
            } else {
                None
            }
        } else if obj_type == "ACCESS" || obj_type == "SCOPE" {
            Some(SchemaObject {
                object_type: SchemaObjectType::Access,
                name: obj_name.to_lowercase(),
                definition: stmt.clone(),
            })
        } else {
            None
        };

        if let Some(obj) = schema_obj {
            objects.insert(obj.key(), obj);
        }
    }

    objects
}

#[derive(Serialize, Deserialize, SurrealValue, Debug, Default)]
struct InfoDbResponse {
    tables: Option<BTreeMap<String, String>>,
    scopes: Option<BTreeMap<String, String>>,
    accesses: Option<BTreeMap<String, String>>,
}

#[derive(Serialize, Deserialize, SurrealValue, Debug, Default)]
struct InfoTableResponse {
    fields: Option<BTreeMap<String, String>>,
    indexes: Option<BTreeMap<String, String>>,
    events: Option<BTreeMap<String, String>>,
}

fn make_overwrite(definition: &str) -> String {
    let trimmed = definition.trim();
    let upper = trimmed.to_uppercase();
    if upper.starts_with("DEFINE TABLE ") {
        format!("{} OVERWRITE{}", &trimmed[..12], &trimmed[12..])
    } else if upper.starts_with("DEFINE FIELD ") {
        format!("{} OVERWRITE{}", &trimmed[..12], &trimmed[12..])
    } else if upper.starts_with("DEFINE INDEX ") {
        format!("{} OVERWRITE{}", &trimmed[..12], &trimmed[12..])
    } else if upper.starts_with("DEFINE EVENT ") {
        format!("{} OVERWRITE{}", &trimmed[..12], &trimmed[12..])
    } else if upper.starts_with("DEFINE ACCESS ") {
        format!("{} OVERWRITE{}", &trimmed[..13], &trimmed[13..])
    } else if upper.starts_with("DEFINE SCOPE ") {
        format!("{} OVERWRITE{}", &trimmed[..12], &trimmed[12..])
    } else {
        trimmed.to_string()
    }
}

/// Introspects a SurrealDB database schema and compares it against the desired schema to produce a list of diff statements.
pub async fn compute_diff(
    db: &Surreal<Any>,
    desired_schema_text: &str,
) -> Result<(Vec<String>, bool), surrealdb::Error> {
    let desired = parse_schema(desired_schema_text);
    let mut diff_statements = Vec::new();
    let mut contains_destructive = false;

    // 1. Query INFO FOR DB
    let mut response = db.query("INFO FOR DB;").await?;
    let db_info: Option<InfoDbResponse> = response.take(0)?;
    let db_info = db_info.unwrap_or_default();

    let db_tables = db_info.tables.unwrap_or_default();
    let db_scopes = db_info.scopes.unwrap_or_default();
    let db_accesses = db_info.accesses.unwrap_or_default();

    tracing::info!(
        "compute_diff: desired keys: {:?}",
        desired.keys().collect::<Vec<_>>()
    );
    tracing::info!(
        "compute_diff: live tables: {:?}",
        db_tables.keys().collect::<Vec<_>>()
    );
    tracing::info!("compute_diff: live tables raw: {:?}", db_tables);

    let mut live_keys = BTreeSet::new();

    // Process tables
    for (table_name, table_def) in db_tables {
        let table_key = format!("table:{}", table_name);
        live_keys.insert(table_key.clone());

        // Check Table fields, indexes, events
        let table_info_query = format!("INFO FOR TABLE {};", table_name);
        if let Ok(mut t_response) = db.query(&table_info_query).await {
            let table_info: Option<InfoTableResponse> = t_response.take(0)?;
            let table_info = table_info.unwrap_or_default();
            tracing::info!("compute_diff: table {} info: {:?}", table_name, table_info);

            // Fields
            if let Some(fields) = table_info.fields {
                for (field_name, field_def) in fields {
                    let field_key = format!("field:{}.{}", table_name, field_name);
                    live_keys.insert(field_key.clone());

                    if !desired.contains_key(&field_key) {
                        diff_statements.push(format!(
                            "REMOVE FIELD {} ON TABLE {};",
                            field_name, table_name
                        ));
                        contains_destructive = true;
                    } else if desired.get(&field_key).unwrap().definition.trim() != field_def.trim()
                    {
                        diff_statements.push(format!(
                            "{};",
                            make_overwrite(&desired.get(&field_key).unwrap().definition)
                        ));
                    }
                }
            }

            // Indexes
            if let Some(indexes) = table_info.indexes {
                for (index_name, index_def) in indexes {
                    let index_key = format!("index:{}.{}", table_name, index_name);
                    live_keys.insert(index_key.clone());

                    if !desired.contains_key(&index_key) {
                        diff_statements.push(format!(
                            "REMOVE INDEX {} ON TABLE {};",
                            index_name, table_name
                        ));
                        contains_destructive = true;
                    } else if desired.get(&index_key).unwrap().definition.trim() != index_def.trim()
                    {
                        diff_statements.push(format!(
                            "{};",
                            make_overwrite(&desired.get(&index_key).unwrap().definition)
                        ));
                    }
                }
            }

            // Events
            if let Some(events) = table_info.events {
                for (event_name, event_def) in events {
                    let event_key = format!("event:{}.{}", table_name, event_name);
                    live_keys.insert(event_key.clone());

                    if !desired.contains_key(&event_key) {
                        diff_statements.push(format!(
                            "REMOVE EVENT {} ON TABLE {};",
                            event_name, table_name
                        ));
                        contains_destructive = true;
                    } else if desired.get(&event_key).unwrap().definition.trim() != event_def.trim()
                    {
                        diff_statements.push(format!(
                            "{};",
                            make_overwrite(&desired.get(&event_key).unwrap().definition)
                        ));
                    }
                }
            }
        }

        // Table itself
        if !desired.contains_key(&table_key) {
            diff_statements.push(format!("REMOVE TABLE {};", table_name));
            contains_destructive = true;
        } else if desired.get(&table_key).unwrap().definition.trim() != table_def.trim() {
            diff_statements.push(format!(
                "{};",
                make_overwrite(&desired.get(&table_key).unwrap().definition)
            ));
        }
    }

    // Process scopes/accesses
    let merged_access = db_scopes.into_iter().chain(db_accesses.into_iter());
    for (access_name, access_def) in merged_access {
        let access_key = format!("access:{}", access_name);
        live_keys.insert(access_key.clone());

        if !desired.contains_key(&access_key) {
            diff_statements.push(format!("REMOVE ACCESS {} ON DATABASE;", access_name)); // or REMOVE SCOPE depending on version
            contains_destructive = true;
        } else if desired.get(&access_key).unwrap().definition.trim() != access_def.trim() {
            diff_statements.push(format!(
                "{};",
                make_overwrite(&desired.get(&access_key).unwrap().definition)
            ));
        }
    }

    // 2. Add new objects from desired schema that don't exist in live database
    let mut new_objects: Vec<&SchemaObject> = desired
        .values()
        .filter(|obj| !live_keys.contains(&obj.key()))
        .collect();
    new_objects.sort_by_key(|obj| obj.object_type.priority());
    for obj in new_objects {
        diff_statements.push(format!("{};", obj.definition));
    }

    Ok((diff_statements, contains_destructive))
}
