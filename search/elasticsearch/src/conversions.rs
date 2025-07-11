use crate::client::{
    ElasticsearchHit, ElasticsearchMappings, ElasticsearchQuery, ElasticsearchSearchResponse,
    ElasticsearchSettings, ElasticsearchTotal,
};
use golem_search::golem::search::types::{
    Doc, FieldType, HighlightConfig, Schema, SchemaField, SearchConfig, SearchHit, SearchQuery,
    SearchResults,
};
use serde_json::{json, Map, Value};
use std::collections::HashMap;

/// Convert a Golem Doc to an Elasticsearch document Value
pub fn doc_to_elasticsearch_document(doc: Doc) -> Result<Value, String> {
    // Validate document ID length (Elasticsearch limit is 512 bytes)
    if doc.id.len() > 512 {
        return Err(format!("Document ID too long: {} bytes (max 512)", doc.id.len()));
    }
    
    let content: Value = serde_json::from_str(&doc.content)
        .map_err(|e| format!("Invalid JSON in document content: {}", e))?;
    
    // Create a document that includes the ID
    let document = match content {
        Value::Object(mut obj) => {
            obj.insert("id".to_string(), Value::String(doc.id));
            Value::Object(obj)
        }
        _ => {
            let mut obj = Map::new();
            obj.insert("id".to_string(), Value::String(doc.id));
            obj.insert("content".to_string(), content);
            Value::Object(obj)
        }
    };
    
    Ok(document)
}

/// Convert an Elasticsearch document to a Golem Doc
pub fn elasticsearch_document_to_doc(id: String, source: Value) -> Doc {
    let content = serde_json::to_string(&source).unwrap_or_else(|_| "{}".to_string());
    Doc { id, content }
}

/// Convert a Golem SearchQuery to an Elasticsearch query
pub fn search_query_to_elasticsearch_query(query: SearchQuery) -> ElasticsearchQuery {
    let mut es_query = ElasticsearchQuery {
        query: None,
        from: query.offset,
        size: query.per_page,
        sort: None,
        highlight: None,
        aggs: None,
        _source: None,
    };

    // Build the main query
    let mut bool_query = json!({
        "bool": {
            "must": [],
            "filter": []
        }
    });

    // Add text search if provided
    if let Some(q) = query.q {
        if !q.trim().is_empty() {
            bool_query["bool"]["must"]
                .as_array_mut()
                .unwrap()
                .push(json!({
                    "multi_match": {
                        "query": q,
                        "type": "best_fields",
                        "fields": ["*"]
                    }
                }));
        }
    }

    // Add filters
    for filter in query.filters {
        if let Ok(filter_value) = serde_json::from_str::<Value>(&filter) {
            // JSON filter
            bool_query["bool"]["filter"]
                .as_array_mut()
                .unwrap()
                .push(filter_value);
        } else if filter.contains(':') {
            // Simple field:value syntax
            let parts: Vec<&str> = filter.splitn(2, ':').collect();
            if parts.len() == 2 {
                let field = parts[0].trim();
                let value = parts[1].trim();
                bool_query["bool"]["filter"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({
                        "term": {
                            field: value
                        }
                    }));
            }
        } else if filter.contains('=') {
            // field = "value" syntax (Meilisearch style)
            let parts: Vec<&str> = filter.splitn(2, '=').collect();
            if parts.len() == 2 {
                let field = parts[0].trim();
                let value = parts[1].trim().trim_matches('"').trim_matches('\'');
                bool_query["bool"]["filter"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({
                        "term": {
                            field: value
                        }
                    }));
            }
        } else {
            // Fallback: treat as status filter
            bool_query["bool"]["filter"]
                .as_array_mut()
                .unwrap()
                .push(json!({
                    "term": {
                        "status": filter
                    }
                }));
        }
    }

    // Only set query if we have conditions
    if !bool_query["bool"]["must"].as_array().unwrap().is_empty()
        || !bool_query["bool"]["filter"].as_array().unwrap().is_empty()
    {
        es_query.query = Some(bool_query);
    } else {
        // Match all if no specific query
        es_query.query = Some(json!({
            "match_all": {}
        }));
    }

    // Add sorting
    if !query.sort.is_empty() {
        let mut sort_array = Vec::new();
        for sort_field in query.sort {
            if let Some(colon_pos) = sort_field.find(':') {
                let field = &sort_field[..colon_pos];
                let direction = &sort_field[colon_pos + 1..];
                let order = if direction == "desc" { "desc" } else { "asc" };
                sort_array.push(json!({
                    field: {
                        "order": order
                    }
                }));
            } else if sort_field.starts_with('-') {
                // Descending order (alternative syntax)
                let field = &sort_field[1..];
                sort_array.push(json!({
                    field: {
                        "order": "desc"
                    }
                }));
            } else {
                // Ascending order
                sort_array.push(json!({
                    sort_field: {
                        "order": "asc"
                    }
                }));
            }
        }
        es_query.sort = Some(sort_array);
    }

    // Add highlighting
    if let Some(highlight_config) = query.highlight {
        let mut highlight = json!({
            "fields": {}
        });

        // Set highlight fields
        for field in highlight_config.fields {
            highlight["fields"][field] = json!({});
        }

        // Set pre/post tags
        if let Some(pre_tag) = highlight_config.pre_tag {
            highlight["pre_tags"] = json!([pre_tag]);
        }
        if let Some(post_tag) = highlight_config.post_tag {
            highlight["post_tags"] = json!([post_tag]);
        }

        // Set fragment size
        if let Some(max_length) = highlight_config.max_length {
            highlight["fragment_size"] = json!(max_length);
        }

        es_query.highlight = Some(highlight);
    }

    // Add aggregations for facets
    if !query.facets.is_empty() {
        let mut aggs = json!({});
        for facet in query.facets {
            aggs[&facet] = json!({
                "terms": {
                    "field": format!("{}.keyword", facet),
                    "size": 10
                }
            });
        }
        es_query.aggs = Some(aggs);
    }

    // Handle search configuration
    if let Some(config) = query.config {
        // Add source filtering for attributes to retrieve
        if !config.attributes_to_retrieve.is_empty() {
            es_query._source = Some(json!(config.attributes_to_retrieve));
        }

        // Apply boost fields
        if !config.boost_fields.is_empty() && es_query.query.is_some() {
            if let Some(query_obj) = es_query.query.as_mut() {
                // Modify multi_match to include boosted fields
                if let Some(multi_match) = query_obj
                    .get_mut("bool")
                    .and_then(|b| b.get_mut("must"))
                    .and_then(|m| m.as_array_mut())
                    .and_then(|arr| arr.first_mut())
                    .and_then(|first| first.get_mut("multi_match"))
                {
                    let mut boosted_fields = Vec::new();
                    for (field, boost) in config.boost_fields {
                        boosted_fields.push(format!("{}^{}", field, boost));
                    }
                    multi_match["fields"] = json!(boosted_fields);
                }
            }
        }
    }

    es_query
}

/// Convert Elasticsearch search response to Golem SearchResults
pub fn elasticsearch_response_to_search_results(
    response: ElasticsearchSearchResponse,
) -> SearchResults {
    let hits = response
        .hits
        .hits
        .into_iter()
        .map(elasticsearch_hit_to_search_hit)
        .collect();

    let total = match response.hits.total.relation.as_str() {
        "eq" => Some(response.hits.total.value),
        "gte" => Some(response.hits.total.value), // At least this many
        _ => None,
    };

    SearchResults {
        total,
        page: None, // Elasticsearch uses from/size, not page-based pagination
        per_page: None,
        hits,
        facets: response.aggregations.map(|aggs| {
            serde_json::to_string(&aggs).unwrap_or_else(|_| "{}".to_string())
        }),
        took_ms: Some(response.took),
    }
}

/// Convert an Elasticsearch hit to a Golem SearchHit
fn elasticsearch_hit_to_search_hit(hit: ElasticsearchHit) -> SearchHit {
    let content = hit.source.map(|source| {
        serde_json::to_string(&source).unwrap_or_else(|_| "{}".to_string())
    });

    let highlights = hit.highlight.map(|highlight| {
        serde_json::to_string(&highlight).unwrap_or_else(|_| "{}".to_string())
    });

    SearchHit {
        id: hit.id,
        score: hit.score,
        content,
        highlights,
    }
}

/// Convert a Golem Schema to Elasticsearch settings with mappings
pub fn schema_to_elasticsearch_settings(schema: Schema) -> ElasticsearchSettings {
    let mut properties = Map::new();

    for field in schema.fields {
        let mut field_mapping = Map::new();

        // Map field types
        match field.field_type {
            FieldType::Text => {
                field_mapping.insert("type".to_string(), Value::String("text".to_string()));
                // Add keyword subfield for exact matching, sorting, and aggregations
                field_mapping.insert("fields".to_string(), json!({
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }));
            }
            FieldType::Keyword => {
                field_mapping.insert("type".to_string(), Value::String("keyword".to_string()));
            }
            FieldType::Integer => {
                field_mapping.insert("type".to_string(), Value::String("integer".to_string()));
            }
            FieldType::Float => {
                field_mapping.insert("type".to_string(), Value::String("float".to_string()));
            }
            FieldType::Boolean => {
                field_mapping.insert("type".to_string(), Value::String("boolean".to_string()));
            }
            FieldType::Date => {
                field_mapping.insert("type".to_string(), Value::String("date".to_string()));
            }
            FieldType::GeoPoint => {
                field_mapping.insert("type".to_string(), Value::String("geo_point".to_string()));
            }
        }

        // Handle indexing
        if !field.index {
            field_mapping.insert("index".to_string(), Value::Bool(false));
        }

        properties.insert(field.name, Value::Object(field_mapping));
    }

    // Add strict mapping for common fields to ensure proper sorting
    properties.insert("year".to_string(), json!({
        "type": "integer"
    }));
    properties.insert("id".to_string(), json!({
        "type": "keyword"
    }));

    let mappings = ElasticsearchMappings {
        properties: Some(properties),
        dynamic: Some(true), // Allow dynamic fields by default
    };

    ElasticsearchSettings {
        mappings: Some(mappings),
        settings: None,
    }
}

/// Convert Elasticsearch mappings to a Golem Schema
pub fn elasticsearch_mappings_to_schema(mappings: Value, index_name: &str) -> Schema {
    let mut fields = Vec::new();

    if let Some(index_mappings) = mappings.get(index_name) {
        if let Some(properties) = index_mappings
            .get("mappings")
            .and_then(|m| m.get("properties"))
            .and_then(|p| p.as_object())
        {
            for (field_name, field_def) in properties {
                if let Some(field_type_str) = field_def.get("type").and_then(|t| t.as_str()) {
                    let field_type = match field_type_str {
                        "text" => FieldType::Text,
                        "keyword" => FieldType::Keyword,
                        "integer" | "long" | "short" | "byte" => FieldType::Integer,
                        "float" | "double" | "half_float" | "scaled_float" => FieldType::Float,
                        "boolean" => FieldType::Boolean,
                        "date" => FieldType::Date,
                        "geo_point" => FieldType::GeoPoint,
                        _ => FieldType::Text, // Default to text for unknown types
                    };

                    let index = field_def
                        .get("index")
                        .and_then(|i| i.as_bool())
                        .unwrap_or(true);

                    fields.push(SchemaField {
                        name: field_name.clone(),
                        field_type,
                        required: false, // Elasticsearch doesn't have required fields in mappings
                        facet: field_type == FieldType::Keyword, // Keywords can be used for faceting
                        sort: field_type != FieldType::Text, // Text fields typically can't be sorted directly
                        index,
                    });
                }
            }
        }
    }

    Schema {
        fields,
        primary_key: Some("id".to_string()), // Default to 'id' as primary key
    }
}

/// Create a retry query for failed/partial search operations
pub fn create_retry_query(original_query: &SearchQuery, partial_hits: &[SearchHit]) -> SearchQuery {
    let mut retry_query = original_query.clone();
    
    // If we have partial results, adjust the offset to continue from where we left off
    if !partial_hits.is_empty() {
        let current_offset = original_query.offset.unwrap_or(0);
        let received_count = partial_hits.len() as u32;
        retry_query.offset = Some(current_offset + received_count);
    }
    
    retry_query
}

/// Build Elasticsearch bulk operations string for multiple documents
pub fn build_bulk_operations(index_name: &str, docs: &[Doc], operation: &str) -> Result<String, String> {
    let mut bulk_ops = String::new();
    
    for doc in docs {
        // Add the operation line
        let action = json!({
            operation: {
                "_index": index_name,
                "_id": doc.id
            }
        });
        bulk_ops.push_str(&serde_json::to_string(&action).map_err(|e| e.to_string())?);
        bulk_ops.push('\n');
        
        // Add the document source (except for delete operations)
        if operation != "delete" {
            let document = doc_to_elasticsearch_document(doc.clone())?;
            bulk_ops.push_str(&serde_json::to_string(&document).map_err(|e| e.to_string())?);
            bulk_ops.push('\n');
        }
    }
    
    Ok(bulk_ops)
}

/// Build bulk delete operations string
pub fn build_bulk_delete_operations(index_name: &str, ids: &[String]) -> Result<String, String> {
    let mut bulk_ops = String::new();
    
    for id in ids {
        let action = json!({
            "delete": {
                "_index": index_name,
                "_id": id
            }
        });
        bulk_ops.push_str(&serde_json::to_string(&action).map_err(|e| e.to_string())?);
        bulk_ops.push('\n');
    }
    
    Ok(bulk_ops)
}
