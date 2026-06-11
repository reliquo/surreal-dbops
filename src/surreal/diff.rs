use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use surrealdb_types::SurrealValue;

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
    let upper = trimmed.to_ascii_uppercase();

    if !upper.starts_with("DEFINE ") {
        return trimmed.to_string();
    }

    let mut parts = trimmed.splitn(3, char::is_whitespace);
    let Some(_define_kw) = parts.next() else {
        return trimmed.to_string();
    };
    let Some(kind) = parts.next() else {
        return trimmed.to_string();
    };
    let Some(rest) = parts.next() else {
        return trimmed.to_string();
    };

    let rest = rest.trim_start();
    let rest_upper = rest.to_ascii_uppercase();

    if rest_upper.starts_with("OVERWRITE") {
        return trimmed.to_string();
    }

    // IF NOT EXISTS and OVERWRITE are mutually exclusive: replace INE with OVERWRITE.
    let rewritten_rest = if rest_upper.starts_with("IF NOT EXISTS") {
        rest["IF NOT EXISTS".len()..].trim_start()
    } else {
        rest
    };

    if rewritten_rest.is_empty() {
        format!("DEFINE {} OVERWRITE", kind)
    } else {
        format!("DEFINE {} OVERWRITE {}", kind, rewritten_rest)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CanonicalTableDef {
    name: String,
    schema_mode: Option<String>,
    table_type: Option<String>,
    permissions: Option<String>,
}

fn parse_canonical_table_definition(definition: &str) -> Option<CanonicalTableDef> {
    let cleaned = definition.trim().trim_end_matches(';');
    let tokens: Vec<String> = cleaned
        .split_whitespace()
        .map(|t| t.trim_end_matches(';').to_uppercase())
        .collect();

    if tokens.len() < 3 || tokens[0] != "DEFINE" || tokens[1] != "TABLE" {
        return None;
    }

    let mut idx = 2;
    if tokens.get(idx).is_some_and(|t| t == "OVERWRITE") {
        idx += 1;
    }

    let name = tokens.get(idx)?.clone();
    idx += 1;

    let mut schema_mode: Option<String> = None;
    let mut table_type: Option<String> = None;
    let mut permissions: Option<String> = None;

    while idx < tokens.len() {
        match tokens[idx].as_str() {
            "TYPE" => {
                if let Some(next) = tokens.get(idx + 1) {
                    table_type = Some(next.clone());
                    idx += 2;
                } else {
                    idx += 1;
                }
            }
            "SCHEMAFULL" | "SCHEMALESS" => {
                schema_mode = Some(tokens[idx].clone());
                idx += 1;
            }
            "PERMISSIONS" => {
                let rest = tokens[idx + 1..].join(" ");
                permissions = Some(rest);
                break;
            }
            _ => {
                idx += 1;
            }
        }
    }

    Some(CanonicalTableDef {
        name,
        schema_mode,
        table_type,
        permissions,
    })
}

fn table_definitions_equivalent(desired: &str, live: &str) -> bool {
    match (
        parse_canonical_table_definition(desired),
        parse_canonical_table_definition(live),
    ) {
        (Some(desired_def), Some(live_def)) => {
            if desired_def.name != live_def.name {
                return false;
            }
            if desired_def.schema_mode.is_some()
                && desired_def.schema_mode.as_ref() != live_def.schema_mode.as_ref()
            {
                return false;
            }
            if desired_def.table_type.is_some()
                && desired_def.table_type.as_ref() != live_def.table_type.as_ref()
            {
                return false;
            }
            if desired_def.permissions.is_some()
                && desired_def.permissions.as_ref() != live_def.permissions.as_ref()
            {
                return false;
            }
            true
        }
        _ => desired.trim() == live.trim(),
    }
}

fn parse_canonical_field_definition(definition: &str) -> Option<String> {
    let cleaned = definition.trim().trim_end_matches(';');
    let tokens: Vec<&str> = cleaned.split_whitespace().collect();

    if tokens.len() < 5
        || !tokens[0].eq_ignore_ascii_case("DEFINE")
        || !tokens[1].eq_ignore_ascii_case("FIELD")
    {
        return None;
    }

    let mut idx = 2;
    if tokens
        .get(idx)
        .is_some_and(|t| t.eq_ignore_ascii_case("OVERWRITE"))
    {
        idx += 1;
    }

    let field_name = tokens.get(idx)?.trim_end_matches(';').to_lowercase();
    idx += 1;

    if !tokens
        .get(idx)
        .is_some_and(|t| t.eq_ignore_ascii_case("ON"))
    {
        return None;
    }
    idx += 1;

    if tokens
        .get(idx)
        .is_some_and(|t| t.eq_ignore_ascii_case("TABLE"))
    {
        idx += 1;
    }

    let table_name = tokens.get(idx)?.trim_end_matches(';').to_lowercase();
    idx += 1;

    let mut body_tokens: Vec<String> = tokens[idx..]
        .iter()
        .map(|t| t.trim_end_matches(';').to_string())
        .collect();
    let has_permissions = body_tokens
        .iter()
        .any(|t| t.eq_ignore_ascii_case("PERMISSIONS"));

    // SurrealDB defaults FIELD permissions to FULL when omitted.
    if !has_permissions {
        body_tokens.push("PERMISSIONS".to_string());
        body_tokens.push("FULL".to_string());
    }

    let body = body_tokens.join(" ");
    if body.is_empty() {
        Some(format!("DEFINE FIELD {} ON {}", field_name, table_name))
    } else {
        Some(format!(
            "DEFINE FIELD {} ON {} {}",
            field_name, table_name, body
        ))
    }
}

fn field_definitions_equivalent(desired: &str, live: &str) -> bool {
    match (
        parse_canonical_field_definition(desired),
        parse_canonical_field_definition(live),
    ) {
        (Some(desired_def), Some(live_def)) => desired_def.eq_ignore_ascii_case(&live_def),
        _ => desired.trim() == live.trim(),
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
                    } else if !field_definitions_equivalent(
                        &desired.get(&field_key).unwrap().definition,
                        &field_def,
                    ) {
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
        } else if !table_definitions_equivalent(
            &desired.get(&table_key).unwrap().definition,
            &table_def,
        ) {
            diff_statements.push(format!(
                "{};",
                make_overwrite(&desired.get(&table_key).unwrap().definition)
            ));
        }
    }

    // Process scopes/accesses
    let merged_access = db_scopes.into_iter().chain(db_accesses);
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

#[cfg(test)]
mod tests {
    use super::{field_definitions_equivalent, make_overwrite, table_definitions_equivalent};

    #[test]
    fn table_definitions_treat_default_type_and_permissions_as_equivalent() {
        let desired = "DEFINE TABLE person SCHEMAFULL";
        let live = "DEFINE TABLE person TYPE NORMAL SCHEMAFULL PERMISSIONS NONE";
        assert!(table_definitions_equivalent(desired, live));
    }

    #[test]
    fn table_definitions_treat_omitted_schema_clauses_as_wildcards() {
        let desired = "DEFINE TABLE person";
        let live = "DEFINE TABLE person TYPE RELATION SCHEMALESS PERMISSIONS FULL";
        assert!(table_definitions_equivalent(desired, live));
    }

    #[test]
    fn table_definitions_detect_schema_mode_change() {
        let desired = "DEFINE TABLE person SCHEMALESS";
        let live = "DEFINE TABLE person TYPE NORMAL SCHEMAFULL PERMISSIONS NONE";
        assert!(!table_definitions_equivalent(desired, live));
    }

    #[test]
    fn table_definitions_detect_non_default_permissions_change() {
        let desired = "DEFINE TABLE person SCHEMAFULL PERMISSIONS FULL";
        let live = "DEFINE TABLE person TYPE NORMAL SCHEMAFULL PERMISSIONS NONE";
        assert!(!table_definitions_equivalent(desired, live));
    }

    #[test]
    fn field_definitions_treat_missing_permissions_full_as_equivalent() {
        let desired = "DEFINE FIELD name ON TABLE person TYPE string";
        let live = "DEFINE FIELD name ON person TYPE string PERMISSIONS FULL";
        assert!(field_definitions_equivalent(desired, live));
    }

    #[test]
    fn field_definitions_detect_non_default_permissions_change() {
        let desired = "DEFINE FIELD name ON TABLE person TYPE string";
        let live = "DEFINE FIELD name ON person TYPE string PERMISSIONS NONE";
        assert!(!field_definitions_equivalent(desired, live));
    }

    #[test]
    fn make_overwrite_supports_any_define_kind() {
        let stmt = "DEFINE MODULE mod::math AS f\"math:/math.surli\"";
        assert_eq!(
            make_overwrite(stmt),
            "DEFINE MODULE OVERWRITE mod::math AS f\"math:/math.surli\""
        );
    }

    #[test]
    fn make_overwrite_replaces_if_not_exists() {
        let stmt = "DEFINE TABLE IF NOT EXISTS person SCHEMAFULL";
        assert_eq!(
            make_overwrite(stmt),
            "DEFINE TABLE OVERWRITE person SCHEMAFULL"
        );
    }

    #[test]
    fn make_overwrite_is_noop_for_non_define() {
        let stmt = "REMOVE FIELD name ON TABLE person";
        assert_eq!(make_overwrite(stmt), stmt);
    }
}
