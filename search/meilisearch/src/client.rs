use golem_search::error::{search_error_from_status, from_reqwest_error};
use golem_search::golem::search::types::SearchError;
use log::trace;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, Response};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, Map as JsonMap};
use std::fmt::Debug;

#[derive(Debug, Clone)]
pub struct MeilisearchApi {
    client: Client,
    base_url: String,
    api_key: Option<String>,
}

// Meilisearch Index object
#[derive(Debug, Serialize, Deserialize)]
pub struct MeilisearchIndex {
    pub uid: String,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    #[serde(rename = "primaryKey")]
    pub primary_key: Option<String>,
}

// Meilisearch Index list response
#[derive(Debug, Serialize, Deserialize)]
pub struct MeilisearchIndexListResponse {
    pub results: Vec<MeilisearchIndex>,
    pub offset: u32,
    pub limit: u32,
    pub total: u32,
}

// Meilisearch Index creation request
#[derive(Debug, Serialize, Deserialize)]
pub struct MeilisearchCreateIndexRequest {
    pub uid: String,
    #[serde(rename = "primaryKey", skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<String>,
}

// Meilisearch Task response (for async operations)
#[derive(Debug, Serialize, Deserialize)]
pub struct MeilisearchTask {
    #[serde(rename = "taskUid")]
    pub task_uid: u64,
    #[serde(rename = "indexUid")]
    pub index_uid: Option<String>,
    pub status: String,
    #[serde(rename = "type")]
    pub task_type: String,
    #[serde(rename = "enqueuedAt")]
    pub enqueued_at: String,
}

// Meilisearch Document
pub type MeilisearchDocument = JsonMap<String, JsonValue>;

// Meilisearch Documents response for fetching
#[derive(Debug, Serialize, Deserialize)]
pub struct MeilisearchDocumentsResponse {
    pub results: Vec<MeilisearchDocument>,
    pub offset: u32,
    pub limit: u32,
    pub total: u32,
}

// Meilisearch Document fetch request
#[derive(Debug, Serialize, Deserialize)]
pub struct MeilisearchDocumentFetchRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ids: Option<Vec<String>>,
}

// Meilisearch Search Request
#[derive(Debug, Serialize, Deserialize)]
pub struct MeilisearchSearchRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub q: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<String>>,
    #[serde(rename = "attributesToRetrieve", skip_serializing_if = "Option::is_none")]
    pub attributes_to_retrieve: Option<Vec<String>>,
    #[serde(rename = "attributesToHighlight", skip_serializing_if = "Option::is_none")]
    pub attributes_to_highlight: Option<Vec<String>>,
    #[serde(rename = "attributesToCrop", skip_serializing_if = "Option::is_none")]
    pub attributes_to_crop: Option<Vec<String>>,
    #[serde(rename = "cropLength", skip_serializing_if = "Option::is_none")]
    pub crop_length: Option<u32>,
    #[serde(rename = "showMatchesPosition", skip_serializing_if = "Option::is_none")]
    pub show_matches_position: Option<bool>,
    #[serde(rename = "matchingStrategy", skip_serializing_if = "Option::is_none")]
    pub matching_strategy: Option<String>,
    #[serde(rename = "showRankingScore", skip_serializing_if = "Option::is_none")]
    pub show_ranking_score: Option<bool>,
}

// Meilisearch Search Response
#[derive(Debug, Serialize, Deserialize)]
pub struct MeilisearchSearchResponse {
    pub hits: Vec<MeilisearchDocument>,
    pub offset: u32,
    pub limit: u32,
    #[serde(rename = "estimatedTotalHits")]
    pub estimated_total_hits: u32,
    #[serde(rename = "processingTimeMs")]
    pub processing_time_ms: u32,
    pub query: String,
    #[serde(rename = "facetDistribution", skip_serializing_if = "Option::is_none")]
    pub facet_distribution: Option<JsonMap<String, JsonValue>>,
}

// Meilisearch Settings
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct MeilisearchSettings {
    #[serde(rename = "displayedAttributes", skip_serializing_if = "Option::is_none")]
    pub displayed_attributes: Option<Vec<String>>,
    #[serde(rename = "searchableAttributes", skip_serializing_if = "Option::is_none")]
    pub searchable_attributes: Option<Vec<String>>,
    #[serde(rename = "filterableAttributes", skip_serializing_if = "Option::is_none")]
    pub filterable_attributes: Option<Vec<String>>,
    #[serde(rename = "sortableAttributes", skip_serializing_if = "Option::is_none")]
    pub sortable_attributes: Option<Vec<String>>,
    #[serde(rename = "rankingRules", skip_serializing_if = "Option::is_none")]
    pub ranking_rules: Option<Vec<String>>,
    #[serde(rename = "stopWords", skip_serializing_if = "Option::is_none")]
    pub stop_words: Option<Vec<String>>,
    #[serde(rename = "synonyms", skip_serializing_if = "Option::is_none")]
    pub synonyms: Option<JsonMap<String, JsonValue>>,
    #[serde(rename = "distinctAttribute", skip_serializing_if = "Option::is_none")]
    pub distinct_attribute: Option<String>,
    #[serde(rename = "typoTolerance", skip_serializing_if = "Option::is_none")]
    pub typo_tolerance: Option<JsonValue>,
    #[serde(rename = "faceting", skip_serializing_if = "Option::is_none")]
    pub faceting: Option<JsonValue>,
    #[serde(rename = "pagination", skip_serializing_if = "Option::is_none")]
    pub pagination: Option<JsonValue>,
}

impl MeilisearchApi {
    pub fn new(base_url: String, api_key: Option<String>) -> Self {
        let client = Client::builder()
            .build()
            .expect("Failed to initialize HTTP client");
        
        Self { client, base_url, api_key }
    }

    fn create_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(api_key) = &self.api_key {
            headers.insert("Authorization", HeaderValue::from_str(&format!("Bearer {}", api_key)).unwrap());
        }
        headers.insert("Content-Type", HeaderValue::from_static("application/json"));
        headers
    }
}

/// Helper function to parse HTTP responses and handle errors
fn parse_response<T: DeserializeOwned + Debug>(response: Response) -> Result<T, SearchError> {
    let status = response.status();
    
    if status.is_success() {
        let text = response.text()
            .map_err(|e| from_reqwest_error("Failed to read response text", e))?;
        serde_json::from_str(&text)
            .map_err(|e| SearchError::Internal(format!("Failed to parse JSON response: {}", e)))
    } else {
        Err(search_error_from_status(status))
    }
}

impl MeilisearchApi {

    // Index Management
    pub fn list_indexes(&self) -> Result<MeilisearchIndexListResponse, SearchError> {
        trace!("Listing indexes");
        
        let url = format!("{}/indexes", self.base_url);
        
        let response = self
            .client
            .get(&url)
            .headers(self.create_headers())
            .send()
            .map_err(|e| from_reqwest_error("Failed to list indexes", e))?;

        parse_response(response)
    }

    pub fn get_index(&self, index_uid: &str) -> Result<MeilisearchIndex, SearchError> {
        trace!("Getting index: {}", index_uid);
        
        let url = format!("{}/indexes/{}", self.base_url, index_uid);
        
        let response = self
            .client
            .get(&url)
            .headers(self.create_headers())
            .send()
            .map_err(|e| from_reqwest_error("Failed to get index", e))?;

        parse_response(response)
    }

    pub fn create_index(&self, request: &MeilisearchCreateIndexRequest) -> Result<MeilisearchTask, SearchError> {
        trace!("Creating index: {}", request.uid);
        
        let url = format!("{}/indexes", self.base_url);
        
        let response = self
            .client
            .post(&url)
            .headers(self.create_headers())
            .json(request)
            .send()
            .map_err(|e| from_reqwest_error("Failed to create index", e))?;

        parse_response(response)
    }

    pub fn delete_index(&self, index_uid: &str) -> Result<MeilisearchTask, SearchError> {
        trace!("Deleting index: {}", index_uid);
        
        let url = format!("{}/indexes/{}", self.base_url, index_uid);
        
        let response = self
            .client
            .delete(&url)
            .headers(self.create_headers())
            .send()
            .map_err(|e| from_reqwest_error("Failed to delete index", e))?;

        parse_response(response)
    }

    // Document Management
    pub fn get_documents(&self, index_uid: &str, request: &MeilisearchDocumentFetchRequest) -> Result<MeilisearchDocumentsResponse, SearchError> {
        trace!("Getting documents from index: {}", index_uid);
        
        let url = format!("{}/indexes/{}/documents/fetch", self.base_url, index_uid);
        
        let response = self
            .client
            .post(&url)
            .headers(self.create_headers())
            .json(request)
            .send()
            .map_err(|e| from_reqwest_error("Failed to get documents", e))?;

        parse_response(response)
    }

    pub fn get_document(&self, index_uid: &str, document_id: &str) -> Result<Option<MeilisearchDocument>, SearchError> {
        trace!("Getting document {} from index: {}", document_id, index_uid);
        
        let url = format!("{}/indexes/{}/documents/{}", self.base_url, index_uid, document_id);
        
        let response = self
            .client
            .get(&url)
            .headers(self.create_headers())
            .send()
            .map_err(|e| from_reqwest_error("Failed to get document", e))?;

        if response.status().as_u16() == 404 {
            Ok(None)
        } else {
            Ok(Some(parse_response(response)?))
        }
    }

    pub fn add_documents(&self, index_uid: &str, documents: &[MeilisearchDocument]) -> Result<MeilisearchTask, SearchError> {
        trace!("Adding {} documents to index: {}", documents.len(), index_uid);
        
        let url = format!("{}/indexes/{}/documents", self.base_url, index_uid);
        
        let response = self
            .client
            .post(&url)
            .headers(self.create_headers())
            .json(documents)
            .send()
            .map_err(|e| from_reqwest_error("Failed to add documents", e))?;

        parse_response(response)
    }

    pub fn update_documents(&self, index_uid: &str, documents: &[MeilisearchDocument]) -> Result<MeilisearchTask, SearchError> {
        trace!("Updating {} documents in index: {}", documents.len(), index_uid);
        
        let url = format!("{}/indexes/{}/documents", self.base_url, index_uid);
        
        let response = self
            .client
            .put(&url)
            .headers(self.create_headers())
            .json(documents)
            .send()
            .map_err(|e| from_reqwest_error("Failed to update documents", e))?;

        parse_response(response)
    }

    pub fn delete_document(&self, index_uid: &str, document_id: &str) -> Result<MeilisearchTask, SearchError> {
        trace!("Deleting document {} from index: {}", document_id, index_uid);
        
        let url = format!("{}/indexes/{}/documents/{}", self.base_url, index_uid, document_id);
        
        let response = self
            .client
            .delete(&url)
            .headers(self.create_headers())
            .send()
            .map_err(|e| from_reqwest_error("Failed to delete document", e))?;

        parse_response(response)
    }

    pub fn delete_documents(&self, index_uid: &str, document_ids: &[String]) -> Result<MeilisearchTask, SearchError> {
        trace!("Deleting {} documents from index: {}", document_ids.len(), index_uid);
        
        let url = format!("{}/indexes/{}/documents/delete-batch", self.base_url, index_uid);
        
        let response = self
            .client
            .post(&url)
            .headers(self.create_headers())
            .json(document_ids)
            .send()
            .map_err(|e| from_reqwest_error("Failed to delete documents", e))?;

        parse_response(response)
    }

    pub fn delete_all_documents(&self, index_uid: &str) -> Result<MeilisearchTask, SearchError> {
        trace!("Deleting all documents from index: {}", index_uid);
        
        let url = format!("{}/indexes/{}/documents", self.base_url, index_uid);
        
        let response = self
            .client
            .delete(&url)
            .headers(self.create_headers())
            .send()
            .map_err(|e| from_reqwest_error("Failed to delete all documents", e))?;

        parse_response(response)
    }

    // Search
    pub fn search(&self, index_uid: &str, request: &MeilisearchSearchRequest) -> Result<MeilisearchSearchResponse, SearchError> {
        trace!("Searching in index: {}", index_uid);
        
        let url = format!("{}/indexes/{}/search", self.base_url, index_uid);
        
        let response = self
            .client
            .post(&url)
            .headers(self.create_headers())
            .json(request)
            .send()
            .map_err(|e| from_reqwest_error("Failed to search", e))?;

        parse_response(response)
    }

    // Settings/Schema Management
    pub fn get_settings(&self, index_uid: &str) -> Result<MeilisearchSettings, SearchError> {
        trace!("Getting settings for index: {}", index_uid);
        
        let url = format!("{}/indexes/{}/settings", self.base_url, index_uid);
        
        let response = self
            .client
            .get(&url)
            .headers(self.create_headers())
            .send()
            .map_err(|e| from_reqwest_error("Failed to get settings", e))?;

        parse_response(response)
    }

    pub fn update_settings(&self, index_uid: &str, settings: &MeilisearchSettings) -> Result<MeilisearchTask, SearchError> {
        trace!("Updating settings for index: {}", index_uid);
        
        let url = format!("{}/indexes/{}/settings", self.base_url, index_uid);
        
        let response = self
            .client
            .patch(&url)
            .headers(self.create_headers())
            .json(settings)
            .send()
            .map_err(|e| from_reqwest_error("Failed to update settings", e))?;

        parse_response(response)
    }

    pub fn reset_settings(&self, index_uid: &str) -> Result<MeilisearchTask, SearchError> {
        trace!("Resetting settings for index: {}", index_uid);
        
        let url = format!("{}/indexes/{}/settings", self.base_url, index_uid);
        
        let response = self
            .client
            .delete(&url)
            .headers(self.create_headers())
            .send()
            .map_err(|e| from_reqwest_error("Failed to reset settings", e))?;

        parse_response(response)
    }

    // Task Management (for checking async operation status)
    pub fn get_task(&self, task_uid: u64) -> Result<MeilisearchTask, SearchError> {
        trace!("Getting task: {}", task_uid);
        
        let url = format!("{}/tasks/{}", self.base_url, task_uid);
        
        let response = self
            .client
            .get(&url)
            .headers(self.create_headers())
            .send()
            .map_err(|e| from_reqwest_error("Failed to get task", e))?;

        parse_response(response)
    }

    pub fn wait_for_task(&self, task_uid: u64) -> Result<(), SearchError> {
        trace!("Waiting for task: {}", task_uid);
        
        // Simple polling mechanism - in production you might want exponential backoff
        for _ in 0..30 { // Wait up to 30 seconds
            let task = self.get_task(task_uid)?;
            match task.status.as_str() {
                "succeeded" => return Ok(()),
                "failed" => return Err(SearchError::Internal(format!("Task {} failed", task_uid))),
                "canceled" => return Err(SearchError::Internal(format!("Task {} was canceled", task_uid))),
                _ => {
                    // Sleep for 1 second - this is a simple blocking wait
                    // In a real implementation, you might want to use async/await
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        }
        Err(SearchError::Internal(format!("Task {} timed out", task_uid)))
    }
}
