use crate::client::{AlgoliaObject, SearchQuery as AlgoliaSearchQuery, SearchResponse, SearchHit as AlgoliaSearchHit, IndexSettings};
use golem_search::golem::search::types::{
    Doc, SearchQuery, SearchResults, SearchHit, Schema, SchemaField, FieldType
};
use serde_json::{Map, Value};

pub fn doc_to_algolia_object(doc: Doc) -> Result<AlgoliaObject, String> {
    let content: Value = serde_json::from_str(&doc.content)
        .map_err(|e| format!("Failed to parse document content as JSON: {}", e))?;

    Ok(AlgoliaObject {
        object_id: Some(doc.id),
        content,
    })
}

pub fn algolia_object_to_doc(obj: AlgoliaObject) -> Doc {
    let content = serde_json::to_string(&obj.content).unwrap_or_else(|_| "{}".to_string());
    Doc {
        id: obj.object_id.unwrap_or_else(|| "unknown".to_string()),
        content,
    }
}

pub fn search_query_to_algolia_query(query: SearchQuery) -> AlgoliaSearchQuery {
    let mut algolia_query = AlgoliaSearchQuery {
        query: query.q,
        filters: None,
        numeric_filters: None,
        page: query.page,
        hits_per_page: query.per_page,
        offset: query.offset,
        length: None,
        facets: query.facets,
        attributes_to_retrieve: vec![],
        typo_tolerance: None,
        analytics: Some(false),
    };

    // Handle filters - Algolia uses the filters field for general attribute filtering
    if !query.filters.is_empty() {
        // Convert filters to Algolia format - join multiple filters with AND
        // Each filter should be in the format "attribute:value" or "attribute>value", etc.
        algolia_query.filters = Some(query.filters.join(" AND "));
    }

    // Handle sort - convert to Algolia's ranking format
    if !query.sort.is_empty() {
        // Note: Algolia handles sorting differently via index replicas or custom ranking
        // For now, we'll include this in the provider params if available
    }

    // Handle highlight configuration
    // Note: Algolia handles highlighting automatically in the index settings
    // and returns _highlightResult in search responses. Query-level highlight
    // parameters are not supported in the search API.
    if let Some(_highlight) = query.highlight {
        // Highlighting configuration would need to be set at the index level
        // For now, we acknowledge but ignore highlight settings
    }

    // Handle search config
    if let Some(config) = query.config {
        algolia_query.attributes_to_retrieve = config.attributes_to_retrieve;
        algolia_query.typo_tolerance = config.typo_tolerance;

        // Parse provider-specific parameters
        if let Some(provider_params) = config.provider_params {
            if let Ok(params_map) = serde_json::from_str::<Map<String, Value>>(&provider_params) {
                // Handle Algolia-specific parameters
                if let Some(filters) = params_map.get("filters").and_then(|v| v.as_str()) {
                    algolia_query.filters = Some(filters.to_string());
                }
                if let Some(numeric_filters) = params_map.get("numericFilters") {
                    algolia_query.numeric_filters = Some(numeric_filters.clone());
                }
                if let Some(analytics) = params_map.get("analytics").and_then(|v| v.as_bool()) {
                    algolia_query.analytics = Some(analytics);
                }
                // Note: facetFilters are not supported in this implementation as they're not in the SearchQuery struct
            }
        }
    }

    algolia_query
}

pub fn algolia_response_to_search_results(response: SearchResponse) -> SearchResults {
    let hits = response.hits.into_iter().map(algolia_hit_to_search_hit).collect();

    SearchResults {
        total: Some(response.nb_hits),
        page: Some(response.page),
        per_page: Some(response.hits_per_page),
        hits,
        facets: response.facets.map(|f| serde_json::to_string(&f).unwrap_or_default()),
        took_ms: Some(response.processing_time_ms),
    }
}

pub fn algolia_hit_to_search_hit(hit: AlgoliaSearchHit) -> SearchHit {
    let highlights = hit.highlight_result
        .map(|h| serde_json::to_string(&h).unwrap_or_default());

    // Extract score from ranking info if available
    let score = hit.ranking_info
        .as_ref()
        .map(|info| info.user_score as f64);

    SearchHit {
        id: hit.object_id,
        score,
        content: Some(serde_json::to_string(&hit.content).unwrap_or_else(|_| "{}".to_string())),
        highlights,
    }
}

pub fn schema_to_algolia_settings(schema: Schema) -> IndexSettings {
    let mut settings = IndexSettings::default();

    for field in schema.fields {
        match field.field_type {
            FieldType::Text => {
                if field.index {
                    settings.searchable_attributes.push(field.name.clone());
                }
                if field.facet {
                    settings.attributes_for_faceting.push(format!("filterOnly({})", field.name));
                }
            }
            FieldType::Keyword => {
                if field.facet {
                    settings.attributes_for_faceting.push(field.name.clone());
                }
                if field.index {
                    settings.searchable_attributes.push(field.name.clone());
                }
            }
            FieldType::Integer | FieldType::Float => {
                if field.facet {
                    settings.attributes_for_faceting.push(field.name.clone());
                }
            }
            FieldType::Boolean => {
                if field.facet {
                    settings.attributes_for_faceting.push(field.name.clone());
                }
            }
            FieldType::Date => {
                if field.facet {
                    settings.attributes_for_faceting.push(field.name.clone());
                }
            }
            FieldType::GeoPoint => {
                // Algolia has built-in geo support
                if field.facet {
                    settings.attributes_for_faceting.push(field.name.clone());
                }
            }
        }

        // Handle sorting - in Algolia, sorting is done via custom ranking
        if field.sort {
            settings.custom_ranking.push(format!("desc({})", field.name));
        }
    }

    settings
}

pub fn algolia_settings_to_schema(settings: IndexSettings) -> Schema {
    let mut fields = Vec::new();

    // Convert searchable attributes to text fields
    for attr in settings.searchable_attributes {
        fields.push(SchemaField {
            name: attr,
            field_type: FieldType::Text,
            required: false,
            facet: false,
            sort: false,
            index: true,
        });
    }

    // Convert faceting attributes to faceted fields
    for attr in settings.attributes_for_faceting {
        // Remove Algolia-specific prefixes like "filterOnly(field)"
        let field_name = if attr.starts_with("filterOnly(") && attr.ends_with(')') {
            attr.trim_start_matches("filterOnly(").trim_end_matches(')')
        } else if attr.starts_with("searchable(") && attr.ends_with(')') {
            attr.trim_start_matches("searchable(").trim_end_matches(')')
        } else {
            &attr
        };

        // Check if field already exists
        if let Some(existing_field) = fields.iter_mut().find(|f| f.name == field_name) {
            existing_field.facet = true;
        } else {
            fields.push(SchemaField {
                name: field_name.to_string(),
                field_type: FieldType::Keyword, // Default for faceting
                required: false,
                facet: true,
                sort: false,
                index: false,
            });
        }
    }

    // Convert custom ranking to sortable fields
    for ranking_rule in settings.custom_ranking {
        if let Some(field_name) = extract_field_from_ranking(&ranking_rule) {
            if let Some(existing_field) = fields.iter_mut().find(|f| f.name == field_name) {
                existing_field.sort = true;
            } else {
                fields.push(SchemaField {
                    name: field_name,
                    field_type: FieldType::Integer, // Default for sorting
                    required: false,
                    facet: false,
                    sort: true,
                    index: false,
                });
            }
        }
    }

    Schema {
        fields,
        primary_key: None, // Algolia doesn't support primary keys in settings
    }
}

fn extract_field_from_ranking(ranking_rule: &str) -> Option<String> {
    // Extract field name from ranking rules like "desc(field)", "asc(field)"
    if let Some(start) = ranking_rule.find('(') {
        if let Some(end) = ranking_rule.rfind(')') {
            if start < end {
                return Some(ranking_rule[start + 1..end].to_string());
            }
        }
    }
    None
}

pub fn create_retry_query(original_query: &SearchQuery, partial_hits: &[SearchHit]) -> SearchQuery {
    let mut retry_query = original_query.clone();
    
    if !partial_hits.is_empty() {
        // For Algolia, we adjust the page or offset to continue pagination
        if let Some(current_page) = retry_query.page {
            retry_query.page = Some(current_page + 1);
        } else if let Some(current_offset) = retry_query.offset {
            let hits_received = partial_hits.len() as u32;
            retry_query.offset = Some(current_offset + hits_received);
        } else {
            // If no pagination was set, start from where we left off
            retry_query.offset = Some(partial_hits.len() as u32);
        }
    }
    
    retry_query
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use golem_search::golem::search::types::HighlightConfig;

//     #[test]
//     fn test_doc_to_algolia_object() {
//         let doc = Doc {
//             id: "test-id".to_string(),
//             content: r#"{"title": "Test Document", "content": "This is a test"}"#.to_string(),
//         };

//         let algolia_obj = doc_to_algolia_object(doc).unwrap();
//         assert_eq!(algolia_obj.object_id, Some("test-id".to_string()));
//         assert_eq!(algolia_obj.content["title"], "Test Document");
//     }

//     #[test]
//     fn test_search_query_conversion() {
//         let search_query = SearchQuery {
//             q: Some("test query".to_string()),
//             filters: vec!["category:electronics".to_string(), "price:>100".to_string()],
//             sort: vec!["price:desc".to_string()],
//             facets: vec!["category".to_string(), "brand".to_string()],
//             page: Some(1),
//             per_page: Some(20),
//             offset: None,
//             highlight: Some(HighlightConfig {
//                 fields: vec!["title".to_string()],
//                 pre_tag: Some("<em>".to_string()),
//                 post_tag: Some("</em>".to_string()),
//                 max_length: Some(200),
//             }),
//             config: None,
//         };

//         let algolia_query = search_query_to_algolia_query(search_query);
//         assert_eq!(algolia_query.query, Some("test query".to_string()));
//         assert_eq!(algolia_query.facets, vec!["category".to_string(), "brand".to_string()]);
//         // Note: Highlight parameters are handled at the index level, not in search queries
//     }

//     #[test]
//     fn test_schema_conversion() {
//         let schema = Schema {
//             fields: vec![
//                 SchemaField {
//                     name: "title".to_string(),
//                     field_type: FieldType::Text,
//                     required: true,
//                     facet: false,
//                     sort: false,
//                     index: true,
//                 },
//                 SchemaField {
//                     name: "category".to_string(),
//                     field_type: FieldType::Keyword,
//                     required: false,
//                     facet: true,
//                     sort: false,
//                     index: false,
//                 },
//                 SchemaField {
//                     name: "price".to_string(),
//                     field_type: FieldType::Float,
//                     required: false,
//                     facet: false,
//                     sort: true,
//                     index: false,
//                 },
//             ],
//             primary_key: Some("id".to_string()),
//         };

//         let settings = schema_to_algolia_settings(schema);
//         assert!(settings.searchable_attributes.contains(&"title".to_string()));
//         assert!(settings.attributes_for_faceting.contains(&"category".to_string()));
//         assert!(settings.custom_ranking.contains(&"desc(price)".to_string()));
//         // Note: Algolia doesn't support primary_key in settings
//     }
// }
