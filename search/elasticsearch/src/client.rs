use golem_search::golem::search::types::{SearchError, Schema};
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

/// Elasticsearch REST API client
pub struct ElasticsearchApi {
    client: Client,
    base_url: String,
    api_key: Option<String>,
    username: Option<String>,
    password: Option<String>,
}

/// Elasticsearch index settings for mapping and configuration
#[derive(Debug, Serialize, Deserialize)]
pub struct ElasticsearchSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mappings: Option<ElasticsearchMappings>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings: Option<Map<String, Value>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ElasticsearchMappings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dynamic: Option<bool>,
}

/// Elasticsearch search query structure
#[derive(Debug, Serialize, Deserialize)]
pub struct ElasticsearchQuery {
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

/// Elasticsearch search response
#[derive(Debug, Deserialize)]
pub struct ElasticsearchSearchResponse {
    pub took: u32,
    pub timed_out: bool,
    pub hits: ElasticsearchHits,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregations: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct ElasticsearchHits {
    pub total: ElasticsearchTotal,
    pub max_score: Option<f64>,
    pub hits: Vec<ElasticsearchHit>,
}

#[derive(Debug, Deserialize)]
pub struct ElasticsearchTotal {
    pub value: u32,
    pub relation: String,
}

#[derive(Debug, Deserialize)]
pub struct ElasticsearchHit {
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

/// Elasticsearch bulk operation
#[derive(Debug, Serialize)]
pub struct ElasticsearchBulkOperation {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<ElasticsearchBulkAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create: Option<ElasticsearchBulkAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update: Option<ElasticsearchBulkAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete: Option<ElasticsearchBulkAction>,
}

#[derive(Debug, Serialize)]
pub struct ElasticsearchBulkAction {
    #[serde(rename = "_index")]
    pub index: String,
    #[serde(rename = "_id")]
    pub id: String,
}

/// Elasticsearch bulk response
#[derive(Debug, Deserialize)]
pub struct ElasticsearchBulkResponse {
    pub took: u32,
    pub errors: bool,
    pub items: Vec<Value>,
}

/// Response for listing indices
#[derive(Debug, Deserialize)]
pub struct ElasticsearchIndexInfo {
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

/// Error response from Elasticsearch
#[derive(Debug, Deserialize)]
pub struct ElasticsearchErrorResponse {
    pub error: ElasticsearchError,
}

#[derive(Debug, Deserialize)]
pub struct ElasticsearchError {
    #[serde(rename = "type")]
    pub error_type: String,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u32>,
}

impl ElasticsearchApi {
    /// Create a new Elasticsearch API client
    pub fn new(base_url: String, username: Option<String>, password: Option<String>, api_key: Option<String>) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            username,
            password,
        }
    }

    /// Add authentication headers to request builder
    fn add_auth(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(api_key) = &self.api_key {
            builder.header("Authorization", format!("ApiKey {}", api_key))
        } else if let (Some(username), Some(password)) = (&self.username, &self.password) {
            builder.basic_auth(username, Some(password))
        } else {
            builder
        }
    }

    /// Handle Elasticsearch API errors
    fn handle_error(response: Response) -> SearchError {
        let status = response.status();
        
        if status == 404 {
            return SearchError::IndexNotFound;
        }
        
        if status == 429 {
            return SearchError::RateLimited;
        }
        
        // Try to parse the error response
        if let Ok(error_response) = response.json::<ElasticsearchErrorResponse>() {
            SearchError::Internal(format!("{}: {}", error_response.error.error_type, error_response.error.reason))
        } else {
            SearchError::Internal(format!("HTTP {}: Unknown error", status))
        }
    }

    /// Create an index
    pub fn create_index(&self, index_name: &str, settings: Option<ElasticsearchSettings>) -> Result<(), SearchError> {
        let url = format!("{}/{}", self.base_url, index_name);
        
        let mut builder = self.add_auth(self.client.put(&url))
            .header("Content-Type", "application/json");
        
        if let Some(settings) = settings {
            builder = builder.json(&settings);
        }
        
        let response = builder.send().map_err(|e| SearchError::Internal(e.to_string()))?;
        
        if response.status().is_success() {
            Ok(())
        } else {
            Err(Self::handle_error(response))
        }
    }

    /// Delete an index
    pub fn delete_index(&self, index_name: &str) -> Result<(), SearchError> {
        let url = format!("{}/{}", self.base_url, index_name);
        
        let response = self.add_auth(self.client.delete(&url))
            .send()
            .map_err(|e| SearchError::Internal(e.to_string()))?;
        
        if response.status().is_success() {
            Ok(())
        } else {
            Err(Self::handle_error(response))
        }
    }

    /// List all indices
    pub fn list_indices(&self) -> Result<Vec<ElasticsearchIndexInfo>, SearchError> {
        let url = format!("{}/_cat/indices?format=json", self.base_url);
        
        let response = self.add_auth(self.client.get(&url))
            .send()
            .map_err(|e| SearchError::Internal(e.to_string()))?;
        
        if response.status().is_success() {
            response.json::<Vec<ElasticsearchIndexInfo>>()
                .map_err(|e| SearchError::Internal(format!("Failed to parse response: {}", e)))
        } else {
            Err(Self::handle_error(response))
        }
    }

    /// Index a single document
    pub fn index_document(&self, index_name: &str, id: &str, document: &Value) -> Result<(), SearchError> {
        let url = format!("{}/{}/_doc/{}", self.base_url, index_name, id);
        
        let response = self.add_auth(self.client.put(&url))
            .header("Content-Type", "application/json")
            .json(document)
            .send()
            .map_err(|e| SearchError::Internal(e.to_string()))?;
        
        if response.status().is_success() {
            Ok(())
        } else {
            Err(Self::handle_error(response))
        }
    }

    /// Bulk index documents
    pub fn bulk_index(&self, operations: &str) -> Result<ElasticsearchBulkResponse, SearchError> {
        let url = format!("{}/_bulk", self.base_url);
        
        let response = self.add_auth(self.client.post(&url))
            .header("Content-Type", "application/x-ndjson")
            .body(operations.to_string())
            .send()
            .map_err(|e| SearchError::Internal(e.to_string()))?;
        
        if response.status().is_success() {
            response.json::<ElasticsearchBulkResponse>()
                .map_err(|e| SearchError::Internal(format!("Failed to parse response: {}", e)))
        } else {
            Err(Self::handle_error(response))
        }
    }

    /// Delete a document
    pub fn delete_document(&self, index_name: &str, id: &str) -> Result<(), SearchError> {
        let url = format!("{}/{}/_doc/{}", self.base_url, index_name, id);
        
        let response = self.add_auth(self.client.delete(&url))
            .send()
            .map_err(|e| SearchError::Internal(e.to_string()))?;
        
        if response.status().is_success() {
            Ok(())
        } else {
            Err(Self::handle_error(response))
        }
    }

    /// Get a document by ID
    pub fn get_document(&self, index_name: &str, id: &str) -> Result<Option<Value>, SearchError> {
        let url = format!("{}/{}/_doc/{}", self.base_url, index_name, id);
        
        let response = self.add_auth(self.client.get(&url))
            .send()
            .map_err(|e| SearchError::Internal(e.to_string()))?;
        
        if response.status() == 404 {
            Ok(None)
        } else if response.status().is_success() {
            let doc: Value = response.json()
                .map_err(|e| SearchError::Internal(format!("Failed to parse response: {}", e)))?;
            
            // Extract the _source field
            if let Some(source) = doc.get("_source") {
                Ok(Some(source.clone()))
            } else {
                Ok(None)
            }
        } else {
            Err(Self::handle_error(response))
        }
    }

    /// Search documents
    pub fn search(&self, index_name: &str, query: &ElasticsearchQuery) -> Result<ElasticsearchSearchResponse, SearchError> {
        let url = format!("{}/{}/_search", self.base_url, index_name);
        
        let response = self.add_auth(self.client.post(&url))
            .header("Content-Type", "application/json")
            .json(query)
            .send()
            .map_err(|e| SearchError::Internal(e.to_string()))?;
        
        if response.status().is_success() {
            response.json::<ElasticsearchSearchResponse>()
                .map_err(|e| SearchError::Internal(format!("Failed to parse response: {}", e)))
        } else {
            Err(Self::handle_error(response))
        }
    }

    /// Get index mappings (schema)
    pub fn get_mappings(&self, index_name: &str) -> Result<Value, SearchError> {
        let url = format!("{}/{}/_mapping", self.base_url, index_name);
        
        let response = self.add_auth(self.client.get(&url))
            .send()
            .map_err(|e| SearchError::Internal(e.to_string()))?;
        
        if response.status().is_success() {
            response.json::<Value>()
                .map_err(|e| SearchError::Internal(format!("Failed to parse response: {}", e)))
        } else {
            Err(Self::handle_error(response))
        }
    }

    /// Update index mappings
    pub fn put_mappings(&self, index_name: &str, mappings: &ElasticsearchMappings) -> Result<(), SearchError> {
        let url = format!("{}/{}/_mapping", self.base_url, index_name);
        
        let response = self.add_auth(self.client.put(&url))
            .header("Content-Type", "application/json")
            .json(mappings)
            .send()
            .map_err(|e| SearchError::Internal(e.to_string()))?;
        
        if response.status().is_success() {
            Ok(())
        } else {
            Err(Self::handle_error(response))
        }
    }
}
