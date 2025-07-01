use golem_search::error::{search_error_from_status, from_reqwest_error};
use golem_search::golem::search::types::SearchError;
use log::trace;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, Response};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// The Typesense Search API client for managing collections and performing search operations.
///
/// Based on https://typesense.org/docs/latest/api/
pub struct TypesenseSearchApi {
    api_key: String,
    client: Client,
    base_url: String,
}

impl TypesenseSearchApi {
    pub fn new(api_key: String, base_url: String) -> Self {
        let client = Client::builder()
            .build()
            .expect("Failed to initialize HTTP client");
        
        Self {
            api_key,
            client,
            base_url,
        }
    }

    fn create_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("X-TYPESENSE-API-KEY", HeaderValue::from_str(&self.api_key).unwrap());
        headers.insert("Content-Type", HeaderValue::from_static("application/json"));
        headers
    }

    pub fn create_collection(&self, collection_name: &str, schema: &CollectionSchema) -> Result<CreateCollectionResponse, SearchError> {
        trace!("Creating collection: {collection_name}");
        
        let url = format!("{}/collections", self.base_url);
        
        let response: Response = self
            .client
            .post(&url)
            .headers(self.create_headers())
            .json(schema)
            .send()
            .map_err(|e| from_reqwest_error("Failed to create collection", e))?;

        parse_response(response)
    }

    pub fn delete_collection(&self, collection_name: &str) -> Result<DeleteCollectionResponse, SearchError> {
        trace!("Deleting collection: {collection_name}");
        
        let url = format!("{}/collections/{}", self.base_url, collection_name);
        
        let response: Response = self
            .client
            .delete(&url)
            .headers(self.create_headers())
            .send()
            .map_err(|e| from_reqwest_error("Failed to delete collection", e))?;

        parse_response(response)
    }

    pub fn list_collections(&self) -> Result<ListCollectionsResponse, SearchError> {
        trace!("Listing collections");
        
        let url = format!("{}/collections", self.base_url);
        
        let response: Response = self
            .client
            .get(&url)
            .headers(self.create_headers())
            .send()
            .map_err(|e| from_reqwest_error("Failed to list collections", e))?;

        parse_response(response)
    }

    pub fn index_document(&self, collection_name: &str, document: &TypesenseDocument) -> Result<IndexDocumentResponse, SearchError> {
        trace!("Indexing document to collection: {collection_name}");
        
        let url = format!("{}/collections/{}/documents", self.base_url, collection_name);
        
        let response: Response = self
            .client
            .post(&url)
            .headers(self.create_headers())
            .json(document)
            .send()
            .map_err(|e| from_reqwest_error("HTTP request failed", e))?;

        parse_response(response)
    }

    pub fn index_documents(&self, collection_name: &str, documents: &[TypesenseDocument]) -> Result<IndexDocumentsResponse, SearchError> {
        trace!("Indexing {} documents to collection: {collection_name}", documents.len());
        
        let url = format!("{}/collections/{}/documents/import", self.base_url, collection_name);
        
        // Typesense expects newline-delimited JSON for bulk import
        let ndjson = documents.iter()
            .map(|doc| serde_json::to_string(doc).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n");
        
        let response: Response = self
            .client
            .post(&url)
            .headers(self.create_headers())
            .header("Content-Type", "text/plain")
            .body(ndjson)
            .send()
            .map_err(|e| from_reqwest_error("HTTP request failed", e))?;

        parse_response(response)
    }

    pub fn upsert_document(&self, collection_name: &str, document: &TypesenseDocument) -> Result<UpsertDocumentResponse, SearchError> {
        trace!("Upserting document to collection: {collection_name}");
        
        let url = format!("{}/collections/{}/documents?action=upsert", self.base_url, collection_name);
        
        let response: Response = self
            .client
            .post(&url)
            .headers(self.create_headers())
            .json(document)
            .send()
            .map_err(|e| from_reqwest_error("HTTP request failed", e))?;

        parse_response(response)
    }

    pub fn delete_document(&self, collection_name: &str, document_id: &str) -> Result<DeleteDocumentResponse, SearchError> {
        trace!("Deleting document {document_id} from collection: {collection_name}");
        
        let url = format!("{}/collections/{}/documents/{}", self.base_url, collection_name, document_id);
        
        let response: Response = self
            .client
            .delete(&url)
            .headers(self.create_headers())
            .send()
            .map_err(|e| from_reqwest_error("HTTP request failed", e))?;

        parse_response(response)
    }

    pub fn delete_documents_by_query(&self, collection_name: &str, filter_by: &str) -> Result<DeleteDocumentsResponse, SearchError> {
        trace!("Deleting documents from collection: {collection_name} with filter: {filter_by}");
        
        let url = format!("{}/collections/{}/documents?filter_by={}", self.base_url, collection_name, filter_by);
        
        let response: Response = self
            .client
            .delete(&url)
            .headers(self.create_headers())
            .send()
            .map_err(|e| from_reqwest_error("HTTP request failed", e))?;

        parse_response(response)
    }

    pub fn search(&self, collection_name: &str, query: &SearchQuery) -> Result<SearchResponse, SearchError> {
        trace!("Searching collection: {collection_name}");
        
        let url = format!("{}/collections/{}/documents/search", self.base_url, collection_name);
        
        let response: Response = self
            .client
            .get(&url)
            .headers(self.create_headers())
            .query(&query)
            .send()
            .map_err(|e| from_reqwest_error("HTTP request failed", e))?;

        parse_response(response)
    }

    pub fn multi_search(&self, searches: &MultiSearchQuery) -> Result<MultiSearchResponse, SearchError> {
        trace!("Performing multi-search");
        
        let url = format!("{}/multi_search", self.base_url);
        
        let response: Response = self
            .client
            .post(&url)
            .headers(self.create_headers())
            .json(searches)
            .send()
            .map_err(|e| from_reqwest_error("HTTP request failed", e))?;

        parse_response(response)
    }
}

fn parse_response<T: DeserializeOwned + Debug>(response: Response) -> Result<T, SearchError> {
    let status = response.status();
    
    if !status.is_success() {
        return Err(search_error_from_status(status));
    }
    
    let body = response.text()
        .map_err(|e| from_reqwest_error("Failed to read response body", e))?;
    
    trace!("Response body: {}", body);
    
    serde_json::from_str(&body)
        .map_err(|e| SearchError::Internal(format!("Failed to parse response: {}", e)))
}

// Typesense API Types

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionSchema {
    pub name: String,
    pub fields: Vec<CollectionField>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_sorting_field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_nested_fields: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_separators: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbols_to_index: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionField {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facet: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypesenseDocument {
    #[serde(flatten)]
    pub fields: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQuery {
    pub q: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facet_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_facet_values: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub per_page: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_fields: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude_fields: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub highlight_full_fields: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub highlight_affix_num_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub highlight_start_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub highlight_end_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet_threshold: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_typos: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_len_1typo: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_len_2typo: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub typo_tokens_threshold: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drop_tokens_threshold: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pinned_hits: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hidden_hits: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_hits: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_cutoff_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exhaustive_search: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_cache: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_ttl: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pre_segmented_query: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_overrides: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prioritize_exact_match: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prioritize_token_position: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_candidates: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiSearchQuery {
    pub searches: Vec<MultiSearchRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiSearchRequest {
    pub collection: String,
    #[serde(flatten)]
    pub query: SearchQuery,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    pub facet_counts: Option<Vec<FacetCount>>,
    pub found: u32,
    pub found_docs: Option<u32>,
    pub out_of: u32,
    pub page: u32,
    pub request_params: RequestParams,
    pub search_time_ms: u32,
    pub search_cutoff: Option<bool>,
    pub hits: Vec<SearchHit>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHit {
    #[serde(flatten)]
    pub document: serde_json::Map<String, serde_json::Value>,
    pub highlights: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(rename = "text_match")]
    pub text_match: Option<u64>,
    #[serde(rename = "text_match_info")]
    pub text_match_info: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FacetCount {
    pub field_name: String,
    pub counts: Vec<FacetValue>,
    pub stats: Option<FacetStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FacetValue {
    pub count: u32,
    pub highlighted: Option<String>,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FacetStats {
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub sum: Option<f64>,
    pub avg: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestParams {
    pub collection_name: String,
    pub per_page: u32,
    pub q: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiSearchResponse {
    pub results: Vec<SearchResponse>,
}

// Response types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCollectionResponse {
    pub name: String,
    pub num_documents: u32,
    pub fields: Vec<CollectionField>,
    pub default_sorting_field: Option<String>,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteCollectionResponse {
    pub name: String,
    pub num_documents: u32,
    pub fields: Vec<CollectionField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListCollectionsResponse(pub Vec<CreateCollectionResponse>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDocumentResponse {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDocumentsResponse {
    pub success: bool,
    pub num_imported: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpsertDocumentResponse {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteDocumentResponse {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteDocumentsResponse {
    pub num_deleted: u32,
}
