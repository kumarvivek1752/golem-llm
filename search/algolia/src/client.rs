use golem_search::error::{search_error_from_status, from_reqwest_error};
use golem_search::golem::search::types::SearchError;
use log::trace;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, Response};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// The Algolia Search API client for managing indices and performing search operations.
///
/// Based on https://www.algolia.com/doc/api-reference/search-api/
pub struct AlgoliaSearchApi {
    application_id: String,
    api_key: String,
    client: Client,
    base_url: String,
}

impl AlgoliaSearchApi {
    pub fn new(application_id: String, api_key: String) -> Self {
        let client = Client::builder()
            .build()
            .expect("Failed to initialize HTTP client");
        
        let base_url = format!("https://{}-dsn.algolia.net", application_id);
        
        Self {
            application_id,
            api_key,
            client,
            base_url,
        }
    }

    fn create_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("X-Algolia-Application-Id", HeaderValue::from_str(&self.application_id).unwrap());
        headers.insert("X-Algolia-API-Key", HeaderValue::from_str(&self.api_key).unwrap());
        headers.insert("Content-Type", HeaderValue::from_static("application/json"));
        headers
    }

    pub fn create_index(&self, index_name: &str, settings: Option<IndexSettings>) -> Result<CreateIndexResponse, SearchError> {
        trace!("Creating index: {index_name}");
        
        let url = format!("{}/1/indexes/{}", self.base_url, index_name);
        let body = settings.unwrap_or_default();
        
        let response: Response = self
            .client
            .put(&url)
            .headers(self.create_headers())
            .json(&body)
            .send()
            .map_err(|e| from_reqwest_error("Failed to create index", e))?;

        parse_response(response)
    }

    pub fn delete_index(&self, index_name: &str) -> Result<DeleteIndexResponse, SearchError> {
        trace!("Deleting index: {index_name}");
        
        let url = format!("{}/1/indexes/{}", self.base_url, index_name);
        
        let response: Response = self
            .client
            .delete(&url)
            .headers(self.create_headers())
            .send()
            .map_err(|e| from_reqwest_error("Failed to delete index", e))?;

        parse_response(response)
    }

    pub fn list_indexes(&self) -> Result<ListIndexesResponse, SearchError> {
        trace!("Listing indexes");
        
        let url = format!("{}/1/indexes", self.base_url);
        
        let response: Response = self
            .client
            .get(&url)
            .headers(self.create_headers())
            .send()
            .map_err(|e| from_reqwest_error("Failed to list indexes", e))?;

        parse_response(response)
    }

    pub fn save_object(&self, index_name: &str, object: &AlgoliaObject) -> Result<SaveObjectResponse, SearchError> {
        trace!("Saving object to index: {index_name}");
        
        let url = format!("{}/1/indexes/{}", self.base_url, index_name);
        
        let response: Response = self
            .client
            .post(&url)
            .headers(self.create_headers())
            .json(object)
            .send()
            .map_err(|e| from_reqwest_error("HTTP request failed", e))?;

        parse_response(response)
    }

    pub fn save_objects(&self, index_name: &str, objects: &[AlgoliaObject]) -> Result<SaveObjectsResponse, SearchError> {
        trace!("Saving {} objects to index: {index_name}", objects.len());
        
        let url = format!("{}/1/indexes/{}/batch", self.base_url, index_name);
        let batch_request = BatchRequest {
            requests: objects.iter().map(|obj| BatchOperation {
                action: "addObject".to_string(),
                body: obj.clone(),
            }).collect(),
        };
        
        let response: Response = self
            .client
            .post(&url)
            .headers(self.create_headers())
            .json(&batch_request)
            .send()
            .map_err(|e| from_reqwest_error("HTTP request failed", e))?;

        parse_response(response)
    }

    pub fn delete_object(&self, index_name: &str, object_id: &str) -> Result<DeleteObjectResponse, SearchError> {
        trace!("Deleting object {object_id} from index: {index_name}");
        
        let url = format!("{}/1/indexes/{}/{}", self.base_url, index_name, object_id);
        
        let response: Response = self
            .client
            .delete(&url)
            .headers(self.create_headers())
            .send()
            .map_err(|e| from_reqwest_error("HTTP request failed", e))?;

        parse_response(response)
    }

    pub fn delete_objects(&self, index_name: &str, object_ids: &[String]) -> Result<DeleteObjectsResponse, SearchError> {
        trace!("Deleting {} objects from index: {index_name}", object_ids.len());
        
        let url = format!("{}/1/indexes/{}/batch", self.base_url, index_name);
        let batch_request = BatchRequest {
            requests: object_ids.iter().map(|id| BatchOperation {
                action: "deleteObject".to_string(),
                body: AlgoliaObject {
                    object_id: Some(id.clone()),
                    content: serde_json::Value::Null,
                },
            }).collect(),
        };
        
        let response: Response = self
            .client
            .post(&url)
            .headers(self.create_headers())
            .json(&batch_request)
            .send()
            .map_err(|e| from_reqwest_error("HTTP request failed", e))?;

        parse_response(response)
    }

    pub fn get_object(&self, index_name: &str, object_id: &str) -> Result<Option<AlgoliaObject>, SearchError> {
        trace!("Getting object {object_id} from index: {index_name}");
        
        let url = format!("{}/1/indexes/{}/{}", self.base_url, index_name, object_id);
        
        let response: Response = self
            .client
            .get(&url)
            .headers(self.create_headers())
            .send()
            .map_err(|e| from_reqwest_error("HTTP request failed", e))?;

        if response.status().as_u16() == 404 {
            Ok(None)
        } else {
            let object: AlgoliaObject = parse_response(response)?;
            Ok(Some(object))
        }
    }

    pub fn search(&self, index_name: &str, query: &SearchQuery) -> Result<SearchResponse, SearchError> {
        trace!("Searching index {index_name} with query: {query:?}");
        
        let url = format!("{}/1/indexes/{}/query", self.base_url, index_name);
        
        let response: Response = self
            .client
            .post(&url)
            .headers(self.create_headers())
            .json(query)
            .send()
            .map_err(|e| from_reqwest_error("HTTP request failed", e))?;

        parse_response(response)
    }

    pub fn get_settings(&self, index_name: &str) -> Result<IndexSettings, SearchError> {
        trace!("Getting settings for index: {index_name}");
        
        let url = format!("{}/1/indexes/{}/settings", self.base_url, index_name);
        
        let response: Response = self
            .client
            .get(&url)
            .headers(self.create_headers())
            .send()
            .map_err(|e| from_reqwest_error("HTTP request failed", e))?;

        parse_response(response)
    }

    pub fn set_settings(&self, index_name: &str, settings: &IndexSettings) -> Result<SetSettingsResponse, SearchError> {
        trace!("Setting settings for index: {index_name}");
        
        let url = format!("{}/1/indexes/{}/settings", self.base_url, index_name);
        
        let response: Response = self
            .client
            .put(&url)
            .headers(self.create_headers())
            .json(settings)
            .send()
            .map_err(|e| from_reqwest_error("HTTP request failed", e))?;

        parse_response(response)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgoliaObject {
    #[serde(rename = "objectID")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_id: Option<String>,
    #[serde(flatten)]
    pub content: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQuery {
    pub query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facet_filters: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub numeric_filters: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
    #[serde(rename = "hitsPerPage")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hits_per_page: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length: Option<u32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub facets: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub highlight_pre_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub highlight_post_tag: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub attributes_to_retrieve: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub typo_tolerance: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub analytics: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub click_analytics: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    pub hits: Vec<SearchHit>,
    pub page: u32,
    #[serde(rename = "nbHits")]
    pub nb_hits: u32,
    #[serde(rename = "nbPages")]
    pub nb_pages: u32,
    #[serde(rename = "hitsPerPage")]
    pub hits_per_page: u32,
    #[serde(rename = "processingTimeMS")]
    pub processing_time_ms: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets_stats: Option<serde_json::Value>,
    pub exhaustive_facets_count: bool,
    pub exhaustive_nb_hits: bool,
    pub query: String,
    pub params: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHit {
    #[serde(rename = "objectID")]
    pub object_id: String,
    #[serde(rename = "_highlightResult")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub highlight_result: Option<serde_json::Value>,
    #[serde(rename = "_snippetResult")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet_result: Option<serde_json::Value>,
    #[serde(rename = "_rankingInfo")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ranking_info: Option<RankingInfo>,
    #[serde(flatten)]
    pub content: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankingInfo {
    #[serde(rename = "nbTypos")]
    pub nb_typos: u32,
    #[serde(rename = "firstMatchedWord")]
    pub first_matched_word: u32,
    #[serde(rename = "proximityDistance")]
    pub proximity_distance: u32,
    #[serde(rename = "userScore")]
    pub user_score: u32,
    #[serde(rename = "geoDistance")]
    pub geo_distance: u32,
    #[serde(rename = "geoPrecision")]
    pub geo_precision: u32,
    #[serde(rename = "nbExactWords")]
    pub nb_exact_words: u32,
    pub words: u32,
    pub filters: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexSettings {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub searchable_attributes: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub attributes_for_faceting: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub unretrievable_attributes: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub attributes_to_retrieve: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ranking: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub custom_ranking: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub replicas: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateIndexResponse {
    #[serde(rename = "taskID")]
    pub task_id: u64,
    #[serde(rename = "createdAt")]
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteIndexResponse {
    #[serde(rename = "taskID")]
    pub task_id: u64,
    #[serde(rename = "deletedAt")]
    pub deleted_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListIndexesResponse {
    pub items: Vec<IndexInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    pub entries: u64,
    #[serde(rename = "dataSize")]
    pub data_size: u64,
    #[serde(rename = "fileSize")]
    pub file_size: u64,
    #[serde(rename = "lastBuildTimeS")]
    pub last_build_time_s: u64,
    #[serde(rename = "numberOfPendingTasks")]
    pub number_of_pending_tasks: u64,
    #[serde(rename = "pendingTask")]
    pub pending_task: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaveObjectResponse {
    #[serde(rename = "taskID")]
    pub task_id: u64,
    #[serde(rename = "objectID")]
    pub object_id: String,
    #[serde(rename = "createdAt")]
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaveObjectsResponse {
    #[serde(rename = "taskID")]
    pub task_id: u64,
    #[serde(rename = "objectIDs")]
    pub object_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteObjectResponse {
    #[serde(rename = "taskID")]
    pub task_id: u64,
    #[serde(rename = "deletedAt")]
    pub deleted_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteObjectsResponse {
    #[serde(rename = "taskID")]
    pub task_id: u64,
    #[serde(rename = "objectIDs")]
    pub object_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetSettingsResponse {
    #[serde(rename = "taskID")]
    pub task_id: u64,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchRequest {
    pub requests: Vec<BatchOperation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchOperation {
    pub action: String,
    pub body: AlgoliaObject,
}

fn parse_response<T: DeserializeOwned + Debug>(response: Response) -> Result<T, SearchError> {
    let status = response.status();
    if status.is_success() {
        let body = response
            .json::<T>()
            .map_err(|e| from_reqwest_error("Failed to parse response", e))?;

        trace!("Received response from Algolia API: {body:?}");

        Ok(body)
    } else {
        let body = response
            .text()
            .map_err(|e| from_reqwest_error("Failed to read error response", e))?;

        trace!("Received {status} response from Algolia API: {body:?}");

        Err(search_error_from_status(status))
    }
}
