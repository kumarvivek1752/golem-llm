use crate::client::{
    CollectionField, CollectionSchema, SearchHit as TypesenseSearchHit,
    SearchQuery as TypesenseSearchQuery, SearchResponse, TypesenseDocument,
};
use golem_search::golem::search::types::{
    Doc, FieldType, Schema, SchemaField, SearchHit, SearchQuery, SearchResults,
};
use serde_json::{Map, Value};

pub fn doc_to_typesense_document(doc: Doc) -> Result<TypesenseDocument, String> {
    let mut fields: Map<String, Value> = serde_json::from_str(&doc.content)
        .map_err(|e| format!("Failed to parse document content as JSON: {}", e))?;

    fields.insert("id".to_string(), Value::String(doc.id));

    Ok(TypesenseDocument { fields })
}

pub fn _typesense_document_to_doc(doc: TypesenseDocument) -> Doc {
    let mut fields = doc.fields;

    let id = fields
        .remove("id")
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "unknown".to_string());

    let content = serde_json::to_string(&fields).unwrap_or_else(|_| "{}".to_string());

    Doc { id, content }
}

pub fn search_query_to_typesense_query(query: SearchQuery) -> TypesenseSearchQuery {
    let mut typesense_query = TypesenseSearchQuery {
        q: query.q.unwrap_or_else(|| "*".to_string()),
        query_by: Some("title,author,description,genre".to_string()),
        filter_by: None,
        sort_by: None,
        facet_by: None,
        max_facet_values: None,
        page: query.page,
        per_page: query.per_page,
        offset: query.offset,
        include_fields: None,
        exclude_fields: None,
        highlight_full_fields: None,
        highlight_affix_num_tokens: None,
        highlight_start_tag: None,
        highlight_end_tag: None,
        snippet_threshold: None,
        num_typos: None,
        min_len_1typo: None,
        min_len_2typo: None,
        typo_tokens_threshold: None,
        drop_tokens_threshold: None,
        pinned_hits: None,
        hidden_hits: None,
        group_by: None,
        group_limit: None,
        limit_hits: None,
        search_cutoff_ms: None,
        exhaustive_search: None,
        use_cache: None,
        cache_ttl: None,
        pre_segmented_query: None,
        enable_overrides: None,
        prioritize_exact_match: None,
        prioritize_token_position: None,
        max_candidates: None,
    };

    if !query.filters.is_empty() {
        typesense_query.filter_by = Some(query.filters.join(" && "));
    }

    if !query.sort.is_empty() {
        typesense_query.sort_by = Some(query.sort.join(","));
    }

    if !query.facets.is_empty() {
        typesense_query.facet_by = Some(query.facets.join(","));
    }

    if let Some(highlight) = query.highlight {
        if let Some(pre_tag) = highlight.pre_tag {
            typesense_query.highlight_start_tag = Some(pre_tag);
        }
        if let Some(post_tag) = highlight.post_tag {
            typesense_query.highlight_end_tag = Some(post_tag);
        }
        if let Some(max_length) = highlight.max_length {
            typesense_query.snippet_threshold = Some(max_length);
        }

        if !highlight.fields.is_empty() {
            typesense_query.highlight_full_fields = Some(highlight.fields.join(","));
        }
    }

    if let Some(config) = query.config {
        if let Some(timeout_ms) = config.timeout_ms {
            typesense_query.search_cutoff_ms = Some(timeout_ms);
        }

        if !config.attributes_to_retrieve.is_empty() {
            typesense_query.include_fields = Some(config.attributes_to_retrieve.join(","));
        }

        if !config.boost_fields.is_empty() {
            let mut query_by_fields = Vec::new();
            for (field, boost) in config.boost_fields {
                query_by_fields.push(format!("{}:{}", field, boost));
            }
            typesense_query.query_by = Some(query_by_fields.join(","));
        }

        if let Some(typo_tolerance) = config.typo_tolerance {
            if typo_tolerance {
                typesense_query.num_typos = Some("2".to_string()); // Allow up to 2 typos
            } else {
                typesense_query.num_typos = Some("0".to_string()); // No typos allowed
            }
        }

        if let Some(exact_match_boost) = config.exact_match_boost {
            typesense_query.prioritize_exact_match = Some(exact_match_boost > 0.0);
        }

        // Parse provider-specific parameters
        if let Some(provider_params) = config.provider_params {
            if let Ok(params_map) = serde_json::from_str::<Map<String, Value>>(&provider_params) {
                if let Some(exhaustive_search) = params_map
                    .get("exhaustive_search")
                    .and_then(|v| v.as_bool())
                {
                    typesense_query.exhaustive_search = Some(exhaustive_search);
                }
                if let Some(use_cache) = params_map.get("use_cache").and_then(|v| v.as_bool()) {
                    typesense_query.use_cache = Some(use_cache);
                }
                if let Some(cache_ttl) = params_map.get("cache_ttl").and_then(|v| v.as_u64()) {
                    typesense_query.cache_ttl = Some(cache_ttl as u32);
                }
                if let Some(group_by) = params_map.get("group_by").and_then(|v| v.as_str()) {
                    typesense_query.group_by = Some(group_by.to_string());
                }
                if let Some(group_limit) = params_map.get("group_limit").and_then(|v| v.as_u64()) {
                    typesense_query.group_limit = Some(group_limit as u32);
                }
                if let Some(max_facet_values) =
                    params_map.get("max_facet_values").and_then(|v| v.as_u64())
                {
                    typesense_query.max_facet_values = Some(max_facet_values as u32);
                }
                if let Some(limit_hits) = params_map.get("limit_hits").and_then(|v| v.as_u64()) {
                    typesense_query.limit_hits = Some(limit_hits as u32);
                }
                if let Some(prioritize_token_position) = params_map
                    .get("prioritize_token_position")
                    .and_then(|v| v.as_bool())
                {
                    typesense_query.prioritize_token_position = Some(prioritize_token_position);
                }
                if let Some(max_candidates) =
                    params_map.get("max_candidates").and_then(|v| v.as_u64())
                {
                    typesense_query.max_candidates = Some(max_candidates as u32);
                }
                if let Some(drop_tokens_threshold) = params_map
                    .get("drop_tokens_threshold")
                    .and_then(|v| v.as_u64())
                {
                    typesense_query.drop_tokens_threshold = Some(drop_tokens_threshold as u32);
                }
                if let Some(typo_tokens_threshold) = params_map
                    .get("typo_tokens_threshold")
                    .and_then(|v| v.as_u64())
                {
                    typesense_query.typo_tokens_threshold = Some(typo_tokens_threshold as u32);
                }
                if let Some(min_len_1typo) =
                    params_map.get("min_len_1typo").and_then(|v| v.as_u64())
                {
                    typesense_query.min_len_1typo = Some(min_len_1typo as u32);
                }
                if let Some(min_len_2typo) =
                    params_map.get("min_len_2typo").and_then(|v| v.as_u64())
                {
                    typesense_query.min_len_2typo = Some(min_len_2typo as u32);
                }
                if let Some(enable_overrides) =
                    params_map.get("enable_overrides").and_then(|v| v.as_bool())
                {
                    typesense_query.enable_overrides = Some(enable_overrides);
                }
                if let Some(pre_segmented_query) = params_map
                    .get("pre_segmented_query")
                    .and_then(|v| v.as_bool())
                {
                    typesense_query.pre_segmented_query = Some(pre_segmented_query);
                }
                if let Some(pinned_hits) = params_map.get("pinned_hits").and_then(|v| v.as_str()) {
                    typesense_query.pinned_hits = Some(pinned_hits.to_string());
                }
                if let Some(hidden_hits) = params_map.get("hidden_hits").and_then(|v| v.as_str()) {
                    typesense_query.hidden_hits = Some(hidden_hits.to_string());
                }
                if let Some(exclude_fields) =
                    params_map.get("exclude_fields").and_then(|v| v.as_str())
                {
                    typesense_query.exclude_fields = Some(exclude_fields.to_string());
                }
                if let Some(highlight_affix_num_tokens) = params_map
                    .get("highlight_affix_num_tokens")
                    .and_then(|v| v.as_u64())
                {
                    typesense_query.highlight_affix_num_tokens =
                        Some(highlight_affix_num_tokens as u32);
                }
            }
        }
    }

    typesense_query
}

pub fn typesense_response_to_search_results(response: SearchResponse) -> SearchResults {
    let hits = response
        .hits
        .into_iter()
        .map(typesense_hit_to_search_hit)
        .collect();

    let facets = response.facet_counts.map(|facet_counts| {
        let facets_map: Map<String, Value> = facet_counts
            .into_iter()
            .map(|facet_count| {
                let values: Map<String, Value> = facet_count
                    .counts
                    .into_iter()
                    .map(|facet_value| {
                        (
                            facet_value.value.as_str().unwrap_or("unknown").to_string(),
                            Value::Number(serde_json::Number::from(facet_value.count)),
                        )
                    })
                    .collect();
                (facet_count.field_name, Value::Object(values))
            })
            .collect();
        serde_json::to_string(&facets_map).unwrap_or_default()
    });

    SearchResults {
        total: Some(response.found),
        page: Some(response.page),
        per_page: Some(response.request_params.per_page),
        hits,
        facets,
        took_ms: Some(response.search_time_ms),
    }
}

pub fn typesense_hit_to_search_hit(hit: TypesenseSearchHit) -> SearchHit {
    let mut document = hit.document;

    let (id, content) =
        if let Some(nested_doc) = document.get("document").and_then(|v| v.as_object()) {
            let id = nested_doc
                .get("id")
                .and_then(|v| v.as_str().map(|s| s.to_string()))
                .unwrap_or_else(|| "unknown".to_string());

            let mut content_doc = nested_doc.clone();
            content_doc.remove("id");
            let content = serde_json::to_string(&content_doc).unwrap_or_else(|_| "{}".to_string());

            (id, content)
        } else {
            let id = document
                .remove("id")
                .and_then(|v| v.as_str().map(|s| s.to_string()))
                .unwrap_or_else(|| "unknown".to_string());

            let content = serde_json::to_string(&document).unwrap_or_else(|_| "{}".to_string());

            (id, content)
        };

    let score = hit.text_match.map(|tm| tm as f64 / 1000000.0); // Typesense uses large integers for text_match

    let highlights = hit
        .highlight
        .or_else(|| {
            hit.highlights.map(|h_array| {
                let mut highlight_map = serde_json::Map::new();
                for h in h_array {
                    if let Some(obj) = h.as_object() {
                        if let (Some(field), Some(snippet)) = (obj.get("field"), obj.get("snippet"))
                        {
                            if let (Some(field_str), Some(snippet_str)) =
                                (field.as_str(), snippet.as_str())
                            {
                                highlight_map.insert(
                                    field_str.to_string(),
                                    serde_json::Value::String(snippet_str.to_string()),
                                );
                            }
                        }
                    }
                }
                highlight_map
            })
        })
        .map(|h| serde_json::to_string(&h).unwrap_or_default());

    SearchHit {
        id,
        score,
        content: Some(content),
        highlights,
    }
}

pub fn schema_to_typesense_schema(schema: Schema, collection_name: &str) -> CollectionSchema {
    let fields: Vec<CollectionField> = schema
        .fields
        .iter()
        .map(|f| schema_field_to_collection_field(f.clone()))
        .collect();

    let default_sorting_field = schema
        .fields
        .iter()
        .find(|f| f.sort && f.name != "id" && f.required)
        .map(|f| f.name.clone());

    CollectionSchema {
        name: collection_name.to_string(),
        fields,
        default_sorting_field,
        enable_nested_fields: None,
        token_separators: None,
        symbols_to_index: None,
    }
}

pub fn schema_field_to_collection_field(field: SchemaField) -> CollectionField {
    let field_type = match field.field_type {
        FieldType::Text => "string",
        FieldType::Keyword => "string",
        FieldType::Integer => "int32",
        FieldType::Float => "float",
        FieldType::Boolean => "bool",
        FieldType::Date => "int64",
        FieldType::GeoPoint => "geopoint",
    }
    .to_string();

    CollectionField {
        name: field.name,
        field_type,
        facet: Some(field.facet),
        index: Some(field.index),
        sort: Some(field.sort),
        optional: Some(!field.required),
    }
}

pub fn _typesense_schema_to_schema(schema: CollectionSchema) -> Schema {
    let fields = schema
        .fields
        .into_iter()
        .map(collection_field_to_schema_field)
        .collect();

    Schema {
        fields,
        primary_key: schema.default_sorting_field,
    }
}

pub fn collection_field_to_schema_field(field: CollectionField) -> SchemaField {
    let field_type = match field.field_type.as_str() {
        "string" => FieldType::Text,
        "int32" | "int64" => FieldType::Integer,
        "float" => FieldType::Float,
        "bool" => FieldType::Boolean,
        "geopoint" => FieldType::GeoPoint,
        _ => FieldType::Text,
    };

    SchemaField {
        name: field.name,
        field_type,
        required: !field.optional.unwrap_or(false),
        facet: field.facet.unwrap_or(false),
        sort: field.sort.unwrap_or(false),
        index: field.index.unwrap_or(true),
    }
}
