use golem_search::golem::search::types::{
    Doc, SearchQuery, SearchResults, SearchHit, Schema, SchemaField, FieldType
};
use crate::client::{
    MeilisearchDocument, MeilisearchSearchRequest, MeilisearchSearchResponse, MeilisearchSettings
};
use serde_json::{Value as JsonValue, Map as JsonMap};
use std::collections::HashMap;

pub fn doc_to_meilisearch_document(doc: Doc) -> Result<MeilisearchDocument, String> {
    let mut meilisearch_doc = JsonMap::new();
    
    meilisearch_doc.insert("id".to_string(), JsonValue::String(doc.id.clone()));
    
    if let Ok(content_value) = serde_json::from_str::<JsonValue>(&doc.content) {
        if let JsonValue::Object(content_map) = content_value {
            for (key, value) in content_map {
                meilisearch_doc.insert(key, value);
            }
        }
    }
    
    Ok(meilisearch_doc)
}

pub fn meilisearch_document_to_doc(mut doc: MeilisearchDocument) -> Doc {

    let id = doc.remove("id")
        .and_then(|v| match v {
            JsonValue::String(s) => Some(s),
            JsonValue::Number(n) => Some(n.to_string()),
            _ => None,
        })
        .unwrap_or_else(|| "unknown".to_string());
    
    let content = serde_json::to_string(&JsonValue::Object(doc))
        .unwrap_or_else(|_| "{}".to_string());
    
    Doc { id, content }
}

pub fn search_query_to_meilisearch_request(query: SearchQuery) -> MeilisearchSearchRequest {
    let mut request = MeilisearchSearchRequest {
        q: query.q,
        offset: query.offset,
        limit: query.per_page,
        filter: None,
        facets: if query.facets.is_empty() { None } else { Some(query.facets) },
        sort: if query.sort.is_empty() { None } else { Some(query.sort) },
        attributes_to_retrieve: query.config.as_ref()
            .and_then(|c| serde_json::from_str::<JsonValue>(&c.provider_params.as_ref()?)
                .ok()
                .and_then(|v| v.get("attributes_to_retrieve")
                    .and_then(|a| a.as_array())
                    .map(|arr| arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()))),
        attributes_to_highlight: None,
        attributes_to_crop: None,
        crop_length: None,
        show_matches_position: None,
        matching_strategy: None,
        show_ranking_score: None,
    };

    if !query.filters.is_empty() {
        request.filter = Some(convert_filters_to_meilisearch(query.filters));
    }

    request
}

pub fn meilisearch_response_to_search_results(response: MeilisearchSearchResponse) -> SearchResults {
    let hits: Vec<SearchHit> = response.hits
        .into_iter()
        .map(|doc| {
            let converted_doc = meilisearch_document_to_doc(doc.clone());
            SearchHit {
                id: converted_doc.id,
                score: None, 
                content: Some(converted_doc.content),
                highlights: None, 
            }
        })
        .collect();

    SearchResults {
        total: Some(response.estimated_total_hits),
        page: None, // We'd need to calculate this from offset and limit
        per_page: Some(response.limit),
        hits,
        facets: response.facet_distribution.map(|facets| serde_json::to_string(&facets).unwrap_or_default()),
        took_ms: Some(response.processing_time_ms),
    }
}

pub fn schema_to_meilisearch_settings(schema: Schema) -> MeilisearchSettings {
    let mut settings = MeilisearchSettings::default();
    
    let mut searchable_attributes = Vec::new();
    let mut filterable_attributes = Vec::new();
    let mut sortable_attributes = Vec::new();
    
    for field in schema.fields {

        if field.index {
            searchable_attributes.push(field.name.clone());
        }
        
        if field.facet {
            filterable_attributes.push(field.name.clone());
        }
        
        if field.sort {
            sortable_attributes.push(field.name.clone());
        }
    }
    
    if !searchable_attributes.is_empty() {
        settings.searchable_attributes = Some(searchable_attributes);
    }
    
    if !filterable_attributes.is_empty() {
        settings.filterable_attributes = Some(filterable_attributes);
    }
    
    if !sortable_attributes.is_empty() {
        settings.sortable_attributes = Some(sortable_attributes);
    }
    
    settings
}


pub fn meilisearch_settings_to_schema(settings: MeilisearchSettings) -> Schema {
    let mut fields = Vec::new();
    

    let mut field_names = std::collections::BTreeSet::new();
    
    if let Some(searchable) = &settings.searchable_attributes {
        field_names.extend(searchable.iter().cloned());
    }
    if let Some(filterable) = &settings.filterable_attributes {
        field_names.extend(filterable.iter().cloned());
    }
    if let Some(sortable) = &settings.sortable_attributes {
        field_names.extend(sortable.iter().cloned());
    }
    if let Some(displayed) = &settings.displayed_attributes {
        field_names.extend(displayed.iter().cloned());
    }
    

    if field_names.is_empty() {

        fields.push(SchemaField {
            name: "*".to_string(),
            field_type: FieldType::Text,
            required: false,
            facet: false,
            sort: false,
            index: true,
        });
    } else {
        for field_name in field_names {
            let index = settings.searchable_attributes
                .as_ref()
                .map(|attrs| attrs.contains(&field_name))
                .unwrap_or(true);
            
            let facet = settings.filterable_attributes
                .as_ref()
                .map(|attrs| attrs.contains(&field_name))
                .unwrap_or(false);
            
            let sort = settings.sortable_attributes
                .as_ref()
                .map(|attrs| attrs.contains(&field_name))
                .unwrap_or(false);
            
            fields.push(SchemaField {
                name: field_name,
                field_type: FieldType::Text,
                required: false,
                facet,
                sort,
                index,
            });
        }
    }
    
    Schema {
        fields,
        primary_key: None,
    }
}


pub fn create_retry_query(original_query: &SearchQuery, partial_hits: &[SearchHit]) -> SearchQuery {
    let mut retry_query = original_query.clone();
    

    let current_offset = original_query.offset.unwrap_or(0);
    let per_page = original_query.per_page.unwrap_or(20);
    let hits_received = partial_hits.len() as u32;
    

    if hits_received < per_page {
        retry_query.offset = Some(current_offset + hits_received);
    } else {
        retry_query.offset = Some(current_offset + per_page);
    }
    
    retry_query
}

fn convert_filters_to_meilisearch(filters: Vec<String>) -> String {
    // Join multiple filters with AND
    // In Meilisearch, filter syntax supports expressions like:
    // "genre = horror AND year > 2000"
    // "color = red OR color = blue"
    filters.join(" AND ")
}

// for later development :-
fn _convert_meilisearch_facets_to_golem(facets: JsonMap<String, JsonValue>) -> HashMap<String, HashMap<String, u64>> {
    let mut result = HashMap::new();
    
    for (facet_name, facet_value) in facets {
        if let JsonValue::Object(facet_map) = facet_value {
            let mut facet_counts = HashMap::new();
            for (value, count) in facet_map {
                if let JsonValue::Number(n) = count {
                    if let Some(count_u64) = n.as_u64() {
                        facet_counts.insert(value, count_u64);
                    }
                }
            }
            result.insert(facet_name, facet_counts);
        }
    }
    
    result
}
