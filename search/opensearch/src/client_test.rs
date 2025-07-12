use base64::Engine;
use golem_search::golem::search::types::SearchError;
use reqwest::{Client, RequestBuilder, Method, Response};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::fmt::Debug;

/// OpenSearch REST API client
pub struct OpenSearchApi {
    client: Client,
    base_url: String,
    api_key: Option<String>,
    username: Option<String>,
    password: Option<String>,
}

/// OpenSearch index settings for mapping and configuration
#[derive(Debug, Serialize, Deserialize)]
pub struct OpenSearchSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mappings: Option<OpenSearchMappings>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings: Option<Map<String, Value>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OpenSearchMappings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dynamic: Option<bool>,
}

/// OpenSearch search query structure
#[derive(Debug, Serialize, Deserialize)]
pub struct OpenSearchQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub highlight: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggs: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _source: Option<Value>,
}

/// OpenSearch search response
#[derive(Debug, Deserialize)]
pub struct OpenSearchSearchResponse {
    pub took: u32,
    pub timed_out: bool,
    pub hits: OpenSearchHits,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregations: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct OpenSearchHits {
    pub total: OpenSearchTotal,
    pub max_score: Option<f64>,
    pub hits: Vec<OpenSearchHit>,
}

#[derive(Debug, Deserialize)]
pub struct OpenSearchTotal {
    pub value: u32,
    pub relation: String,
}

#[derive(Debug, Deserialize)]
pub struct OpenSearchHit {
    #[serde(rename = "_index")]
    pub index: String,
    #[serde(rename = "_id")]
    pub id: String,
    #[serde(rename = "_score")]
    pub score: Option<f64>,
    #[serde(rename = "_source")]
    pub source: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub highlight: Option<Value>,
}

/// OpenSearch bulk operation
#[derive(Debug, Serialize)]
pub struct OpenSearchBulkOperation {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<OpenSearchBulkAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create: Option<OpenSearchBulkAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update: Option<OpenSearchBulkAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete: Option<OpenSearchBulkAction>,
}

#[derive(Debug, Serialize)]
pub struct OpenSearchBulkAction {
    #[serde(rename = "_index")]
    pub index: String,
    #[serde(rename = "_id")]
    pub id: String,
}

/// OpenSearch bulk response
#[derive(Debug, Deserialize)]
pub struct OpenSearchBulkResponse {
    pub took: u32,
    pub errors: bool,
    pub items: Vec<Value>,
}

/// Response for listing indices
#[derive(Debug, Deserialize)]
pub struct OpenSearchIndexInfo {
    pub health: Option<String>,
    pub status: Option<String>,
    pub index: String,
    pub uuid: Option<String>,
    pub pri: Option<String>,
    pub rep: Option<String>,
    #[serde(rename = "docs.count")]
    pub docs_count: Option<String>,
    #[serde(rename = "docs.deleted")]
    pub docs_deleted: Option<String>,
    #[serde(rename = "store.size")]
    pub store_size: Option<String>,
    #[serde(rename = "pri.store.size")]
    pub pri_store_size: Option<String>,
}

/// Error response from OpenSearch
#[derive(Debug, Deserialize)]
pub struct OpenSearchErrorResponse {
    pub error: OpenSearchError,
}

#[derive(Debug, Deserialize)]
pub struct OpenSearchError {
    #[serde(rename = "type")]
    pub error_type: String,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u32>,
}

impl OpenSearchApi {
    /// Create a new OpenSearch API client
    pub fn new(base_url: String, username: Option<String>, password: Option<String>, api_key: Option<String>) -> Self {
        Self {
            client: Self::create_secure_client(),
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            username,
            password,
        }
    }

    /// Create a secure client with strict TLS validation (production default)
    fn create_secure_client() -> Client {
        Client::builder()
            .build()
            .expect("Failed to initialize HTTP client")
    }

    /// Create an authenticated request
    fn create_request(&self, method: &str, url: &str) -> RequestBuilder {
        let method = match method {
            "GET" => Method::GET,
            "POST" => Method::POST,
            "PUT" => Method::PUT,
            "DELETE" => Method::DELETE,
            _ => Method::GET,
        };

        println!("[OpenSearch] HTTP {} {}", method, url);

        let mut request = self.client
            .request(method, url)
            .header("Content-Type", "application/json");

        if let Some(api_key) = &self.api_key {
            request = request.header("Authorization", format!("ApiKey {}", api_key));
        } else if let (Some(username), Some(password)) = (&self.username, &self.password) {
            let credentials = base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", username, password));
            request = request.header("Authorization", format!("Basic {}", credentials));
            println!(
                "[OpenSearch] Headers: Authorization=Basic {}...",
                &credentials[..8.min(credentials.len())]
            );
        }
        
        request
    }

    /// Create an authenticated request with custom content type
    fn create_request_with_content_type(&self, method: &str, url: &str, content_type: &str) -> RequestBuilder {
        let method = match method {
            "GET" => Method::GET,
            "POST" => Method::POST,
            "PUT" => Method::PUT,
            "DELETE" => Method::DELETE,
            _ => Method::GET,
        };

        println!("[OpenSearch] HTTP {} {}", method, url);

        let mut request = self.client
            .request(method, url)
            .header("Content-Type", content_type);

        if let Some(api_key) = &self.api_key {
            request = request.header("Authorization", format!("ApiKey {}", api_key));
        } else if let (Some(username), Some(password)) = (&self.username, &self.password) {
            let credentials = base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", username, password));
            request = request.header("Authorization", format!("Basic {}", credentials));
            println!(
                "[OpenSearch] Headers: Authorization=Basic {}...",
                &credentials[..8.min(credentials.len())]
            );
        }
        
        request
    }

    /// Parse response and handle errors
    fn parse_response<T: serde::de::DeserializeOwned + Debug>(response: Response) -> Result<T, SearchError> {
        let status_code = response.status();
        
        if status_code.is_success() {
            response
                .json::<T>()
                .map_err(|e| SearchError::Internal(format!("Failed to parse response: {}", e)))
        } else {
            let error_body = response
                .text()
                .map_err(|e| SearchError::Internal(format!("Failed to read error response: {}", e)))?;
            
            if status_code == 404 {
                Err(SearchError::IndexNotFound)
            } else if status_code == 429 {
                Err(SearchError::RateLimited)
            } else {
                // Try to parse the error response
                if let Ok(error_response) = serde_json::from_str::<OpenSearchErrorResponse>(&error_body) {
                    Err(SearchError::Internal(format!("{}: {}", error_response.error.error_type, error_response.error.reason)))
                } else {
                    Err(SearchError::Internal(format!("HTTP {}: {}", status_code, error_body)))
                }
            }
        }
    }

    /// Create an index
    pub fn create_index(&self, index_name: &str, settings: Option<OpenSearchSettings>) -> Result<(), SearchError> {
        let url = format!("{}/{}", self.base_url, index_name);
        
        let response = if let Some(settings) = settings {
            self.create_request("PUT", &url)
                .json(&settings)
                .send()
                .map_err(|e| SearchError::Internal(format!("Failed to create index: {}", e)))?
        } else {
            self.create_request("PUT", &url)
                .send()
                .map_err(|e| SearchError::Internal(format!("Failed to create index: {}", e)))?
        };
        
        Self::parse_response::<serde_json::Value>(response).map(|_| ())
    }

    /// Delete an index
    pub fn delete_index(&self, index_name: &str) -> Result<(), SearchError> {
        let url = format!("{}/{}", self.base_url, index_name);
        
        let response = self.create_request("DELETE", &url)
            .send()
            .map_err(|e| SearchError::Internal(format!("Failed to delete index: {}", e)))?;
        
        Self::parse_response::<serde_json::Value>(response).map(|_| ())
    }

    /// List all indices
    pub fn list_indices(&self) -> Result<Vec<OpenSearchIndexInfo>, SearchError> {
        let url = format!("{}/_cat/indices?format=json", self.base_url);
        
        let response = self.create_request("GET", &url)
            .send()
            .map_err(|e| SearchError::Internal(format!("Failed to list indices: {}", e)))?;
        
        Self::parse_response::<Vec<OpenSearchIndexInfo>>(response)
    }

    /// Index a single document
    pub fn index_document(&self, index_name: &str, id: &str, document: &Value) -> Result<(), SearchError> {
        let url = format!("{}/{}/_doc/{}", self.base_url, index_name, id);
        
        let response = self.create_request("PUT", &url)
            .json(document)
            .send()
            .map_err(|e| SearchError::Internal(format!("Failed to index document: {}", e)))?;
        
        Self::parse_response::<serde_json::Value>(response).map(|_| ())
    }

    /// Bulk index documents
    pub fn bulk_index(&self, operations: &str) -> Result<OpenSearchBulkResponse, SearchError> {
        let url = format!("{}/_bulk", self.base_url);
        
        let response = self.create_request_with_content_type("POST", &url, "application/x-ndjson")
            .body(operations.to_string())
            .send()
            .map_err(|e| SearchError::Internal(format!("Failed to bulk index: {}", e)))?;
        
        Self::parse_response::<OpenSearchBulkResponse>(response)
    }

    /// Delete a document
    pub fn delete_document(&self, index_name: &str, id: &str) -> Result<(), SearchError> {
        let url = format!("{}/{}/_doc/{}", self.base_url, index_name, id);
        
        let response = self.create_request("DELETE", &url)
            .send()
            .map_err(|e| SearchError::Internal(format!("Failed to delete document: {}", e)))?;
        
        Self::parse_response::<serde_json::Value>(response).map(|_| ())
    }

    /// Get a document by ID
    pub fn get_document(&self, index_name: &str, id: &str) -> Result<Option<Value>, SearchError> {
        let url = format!("{}/{}/_doc/{}", self.base_url, index_name, id);
        
        let response = self.create_request("GET", &url).send();
        
        match response {
            Ok(resp) => {
                if resp.status() == 404 {
                    Ok(None)
                } else {
                    let doc: Value = Self::parse_response(resp)?;
                    // Extract the _source field
                    if let Some(source) = doc.get("_source") {
                        Ok(Some(source.clone()))
                    } else {
                        Ok(None)
                    }
                }
            }
            Err(e) => {
                // Check if it's a 404 error
                if e.to_string().contains("404") {
                    Ok(None)
                } else {
                    Err(SearchError::Internal(format!("Failed to get document: {}", e)))
                }
            }
        }
    }

    /// Search documents
    pub fn search(&self, index_name: &str, query: &OpenSearchQuery) -> Result<OpenSearchSearchResponse, SearchError> {
        let url = format!("{}/{}/_search", self.base_url, index_name);
        
        let response = self.create_request("POST", &url)
            .json(query)
            .send()
            .map_err(|e| SearchError::Internal(format!("Failed to search: {}", e)))?;
        
        Self::parse_response::<OpenSearchSearchResponse>(response)
    }

    /// Get index mappings (schema)
    pub fn get_mappings(&self, index_name: &str) -> Result<Value, SearchError> {
        let url = format!("{}/{}/_mapping", self.base_url, index_name);
        
        let response = self.create_request("GET", &url)
            .send()
            .map_err(|e| SearchError::Internal(format!("Failed to get mappings: {}", e)))?;
        
        Self::parse_response::<Value>(response)
    }

    /// Update index mappings
    pub fn put_mappings(&self, index_name: &str, mappings: &OpenSearchMappings) -> Result<(), SearchError> {
        let url = format!("{}/{}/_mapping", self.base_url, index_name);
        
        let response = self.create_request("PUT", &url)
            .json(mappings)
            .send()
            .map_err(|e| SearchError::Internal(format!("Failed to put mappings: {}", e)))?;
        
        Self::parse_response::<serde_json::Value>(response).map(|_| ())
    }
}

