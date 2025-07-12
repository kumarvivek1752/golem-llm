use crate::client::{
    OpenSearchMappings, OpenSearchQuery, OpenSearchSearchResponse, OpenSearchSettings,
};
use golem_search::golem::search::types::{
    Doc, FieldType, Schema, SchemaField, SearchHit, SearchQuery, SearchResults,
};
use serde_json::{Map, Value};
use std::collections::HashMap;

pub fn doc_to_opensearch_document(doc: Doc) -> Result<Value, String> {
    let mut opensearch_doc = Map::new();

    opensearch_doc.insert("id".to_string(), Value::String(doc.id));

    match serde_json::from_str::<Value>(&doc.content) {
        Ok(Value::Object(content_map)) => {
            for (key, value) in content_map {
                opensearch_doc.insert(key, value);
            }
        }
        Ok(other_value) => {
            opensearch_doc.insert("content".to_string(), other_value);
        }
        Err(_) => {
            opensearch_doc.insert("content".to_string(), Value::String(doc.content));
        }
    }

    Ok(Value::Object(opensearch_doc))
}

pub fn opensearch_document_to_doc(document: Value) -> Doc {
    let mut doc_map = match document {
        Value::Object(map) => map,
        other => {
            let mut map = Map::new();
            map.insert("content".to_string(), other);
            map
        }
    };

    let id = doc_map
        .remove("id")
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "unknown".to_string());

    let content =
        serde_json::to_string(&Value::Object(doc_map)).unwrap_or_else(|_| "{}".to_string());

    Doc { id, content }
}

pub fn search_query_to_opensearch_request(query: SearchQuery) -> OpenSearchQuery {
    let mut opensearch_query = OpenSearchQuery {
        query: None,
        from: query.offset,
        size: query.per_page,
        sort: None,
        highlight: None,
        aggs: None,
        _source: None,
    };

    if let Some(q) = query.q {
        if q.trim().is_empty() {
            opensearch_query.query = Some(serde_json::json!({
                "match_all": {}
            }));
        } else {
            opensearch_query.query = Some(serde_json::json!({
                "multi_match": {
                    "query": q,
                    "type": "best_fields",
                    "fields": ["*"]
                }
            }));
        }
    } else {
        opensearch_query.query = Some(serde_json::json!({
            "match_all": {}
        }));
    }

    if !query.filters.is_empty() {
        let mut bool_query = serde_json::json!({
            "bool": {
                "must": opensearch_query.query.take(),
                "filter": []
            }
        });

        for filter in query.filters {
            if let Some((field, value)) = filter.split_once(':') {
                bool_query["bool"]["filter"]
                    .as_array_mut()
                    .unwrap()
                    .push(serde_json::json!({
                        "term": {
                            field: value
                        }
                    }));
            } else {
                bool_query["bool"]["filter"]
                    .as_array_mut()
                    .unwrap()
                    .push(serde_json::json!({
                        "query_string": {
                            "query": filter
                        }
                    }));
            }
        }

        opensearch_query.query = Some(bool_query);
    }

    if !query.sort.is_empty() {
        let mut sort_array = Vec::new();
        for sort_field in query.sort {
            if let Some(field) = sort_field.strip_prefix('-') {
                let mut sort_obj = Map::new();
                sort_obj.insert(field.to_string(), serde_json::json!({ "order": "desc" }));
                sort_array.push(Value::Object(sort_obj));
            } else if let Some((field, order)) = sort_field.split_once(':') {
                let order = if order.to_lowercase() == "desc" {
                    "desc"
                } else {
                    "asc"
                };
                let mut sort_obj = Map::new();
                sort_obj.insert(field.to_string(), serde_json::json!({ "order": order }));
                sort_array.push(Value::Object(sort_obj));
            } else {
                let mut sort_obj = Map::new();
                sort_obj.insert(sort_field, serde_json::json!({ "order": "asc" }));
                sort_array.push(Value::Object(sort_obj));
            }
        }
        opensearch_query.sort = Some(sort_array);
    }

    if let Some(highlight_config) = query.highlight {
        let mut highlight = serde_json::json!({
            "fields": {}
        });

        if !highlight_config.fields.is_empty() {
            for field in highlight_config.fields {
                highlight["fields"][field] = serde_json::json!({});
            }
        } else {
            highlight["fields"]["*"] = serde_json::json!({});
        }

        if let Some(pre_tag) = highlight_config.pre_tag {
            highlight["pre_tags"] = serde_json::json!([pre_tag]);
        }

        if let Some(post_tag) = highlight_config.post_tag {
            highlight["post_tags"] = serde_json::json!([post_tag]);
        }

        if let Some(max_length) = highlight_config.max_length {
            highlight["fragment_size"] = serde_json::json!(max_length);
        }

        opensearch_query.highlight = Some(highlight);
    }

    if !query.facets.is_empty() {
        let mut aggs = Map::new();
        for facet in query.facets {
            let field_name = if facet == "year" {
                facet.clone()
            } else {
                format!("{}.keyword", facet)
            };

            aggs.insert(
                format!("{}_terms", facet),
                serde_json::json!({
                    "terms": {
                        "field": field_name,
                        "size": 100
                    }
                }),
            );
        }
        opensearch_query.aggs = Some(Value::Object(aggs));
    }

    opensearch_query
}

pub fn opensearch_response_to_search_results(response: OpenSearchSearchResponse) -> SearchResults {
    let hits: Vec<SearchHit> = response
        .hits
        .hits
        .into_iter()
        .map(|hit| {
            let mut highlights = HashMap::new();
            if let Some(highlight) = hit.highlight {
                if let Value::Object(highlight_map) = highlight {
                    for (field, values) in highlight_map {
                        if let Value::Array(values_array) = values {
                            let highlight_strings: Vec<String> = values_array
                                .into_iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect();
                            if !highlight_strings.is_empty() {
                                highlights.insert(field, highlight_strings);
                            }
                        }
                    }
                }
            }

            let content = hit.source.unwrap_or_else(|| serde_json::json!({}));
            let content_str = serde_json::to_string(&content).unwrap_or_else(|_| "{}".to_string());

            SearchHit {
                id: hit.id,
                score: hit.score,
                content: Some(content_str),
                highlights: Some(
                    serde_json::to_string(&highlights).unwrap_or_else(|_| "{}".to_string()),
                ),
            }
        })
        .collect();

    let total = response.hits.total.value;

    let facets = response
        .aggregations
        .map(|aggs| {
            let mut facet_map = HashMap::new();
            if let Value::Object(aggs_map) = aggs {
                for (key, value) in aggs_map {
                    if key.ends_with("_terms") {
                        let facet_name = key.strip_suffix("_terms").unwrap_or(&key);
                        if let Some(buckets) = value.get("buckets") {
                            if let Value::Array(buckets_array) = buckets {
                                let facet_values: Vec<String> = buckets_array
                                    .iter()
                                    .filter_map(|bucket| {
                                        bucket
                                            .get("key")
                                            .and_then(|k| k.as_str().map(|s| s.to_string()))
                                    })
                                    .collect();
                                if !facet_values.is_empty() {
                                    facet_map.insert(facet_name.to_string(), facet_values);
                                }
                            }
                        }
                    }
                }
            }
            facet_map
        })
        .unwrap_or_default();

    SearchResults {
        total: Some(total),
        page: None,     // OpenSearch uses offset/size, not page numbers
        per_page: None, // We'll calculate this from the request
        hits,
        facets: Some(serde_json::to_string(&facets).unwrap_or_else(|_| "{}".to_string())),
        took_ms: Some(response.took),
    }
}

pub fn schema_to_opensearch_settings(schema: Schema) -> OpenSearchSettings {
    let mut properties = Map::new();

    for field in schema.fields {
        let mut field_mapping = Map::new();

        let opensearch_type = match field.field_type {
            FieldType::Text => "text",
            FieldType::Keyword => "keyword",
            FieldType::Integer => "integer",
            FieldType::Float => "float",
            FieldType::Boolean => "boolean",
            FieldType::Date => "date",
            FieldType::GeoPoint => "geo_point",
        };
        field_mapping.insert(
            "type".to_string(),
            Value::String(opensearch_type.to_string()),
        );

        if field.field_type == FieldType::Text {
            field_mapping.insert(
                "analyzer".to_string(),
                Value::String("standard".to_string()),
            );
        }

        properties.insert(field.name, Value::Object(field_mapping));
    }

    let mappings = OpenSearchMappings {
        properties: Some(properties),
        dynamic: Some(true),
    };

    let mut index_settings = Map::new();
    index_settings.insert(
        "number_of_shards".to_string(),
        Value::Number(serde_json::Number::from(1)),
    );
    index_settings.insert(
        "number_of_replicas".to_string(),
        Value::Number(serde_json::Number::from(0)),
    );

    OpenSearchSettings {
        mappings: Some(mappings),
        settings: Some(index_settings),
    }
}

pub fn opensearch_mappings_to_schema(
    mappings_response: Value,
    primary_key: Option<String>,
) -> Schema {
    let mut fields = Vec::new();

    if let Value::Object(indices) = mappings_response {
        for (_, index_info) in indices {
            if let Some(mappings) = index_info.get("mappings") {
                if let Some(properties) = mappings.get("properties") {
                    if let Value::Object(props) = properties {
                        for (field_name, field_def) in props {
                            if let Value::Object(field_map) = field_def {
                                let field_type = field_map
                                    .get("type")
                                    .and_then(|t| t.as_str())
                                    .map(|type_str| match type_str {
                                        "text" => FieldType::Text,
                                        "keyword" => FieldType::Keyword,
                                        "integer" | "long" | "short" | "byte" => FieldType::Integer,
                                        "float" | "double" | "half_float" | "scaled_float" => {
                                            FieldType::Float
                                        }
                                        "boolean" => FieldType::Boolean,
                                        "date" => FieldType::Date,
                                        "geo_point" => FieldType::GeoPoint,
                                        _ => FieldType::Text,
                                    })
                                    .unwrap_or(FieldType::Text);

                                fields.push(SchemaField {
                                    name: field_name.clone(),
                                    field_type,
                                    required: false,
                                    facet: field_type == FieldType::Keyword,
                                    sort: true,
                                    index: true,
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    Schema {
        fields,
        primary_key: Some(primary_key.unwrap_or_else(|| "id".to_string())),
    }
}

pub fn create_retry_query(original_query: &SearchQuery, partial_hits: &[SearchHit]) -> SearchQuery {
    let mut retry_query = original_query.clone();

    let current_offset = retry_query.offset.unwrap_or(0);
    let hits_received = partial_hits.len() as u32;
    retry_query.offset = Some(current_offset + hits_received);

    retry_query
}
