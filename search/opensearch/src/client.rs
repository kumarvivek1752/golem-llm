use golem_search::error::{from_reqwest_error, internal_error, search_error_from_status};
use golem_search::golem::search::types::SearchError;
use log::trace;
use reqwest::{Client, Method, RequestBuilder, Response};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::fmt::Debug;

/// The OpenSearch API client for managing indices and performing search
/// Based on the OpenSearch REST API
#[derive(Clone)]
pub struct OpenSearchApi {
    client: Client,
    base_url: String,
    api_key: Option<String>,
    username: Option<String>,
    password: Option<String>,
}

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

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct OpenSearchSearchResponse {
    pub took: u32,
    pub timed_out: bool,
    pub hits: OpenSearchHits,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregations: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct OpenSearchHits {
    pub total: OpenSearchTotal,
    pub max_score: Option<f64>,
    pub hits: Vec<OpenSearchHit>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct OpenSearchTotal {
    pub value: u32,
    pub relation: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
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

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct OpenSearchBulkResponse {
    pub took: u32,
    pub errors: bool,
    pub items: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
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

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct OpenSearchErrorResponse {
    pub error: OpenSearchError,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct OpenSearchError {
    #[serde(rename = "type")]
    pub error_type: String,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u32>,
}

impl OpenSearchApi {
    pub fn new(
        base_url: String,
        username: Option<String>,
        password: Option<String>,
        api_key: Option<String>,
    ) -> Self {
        let client = Client::builder()
            .build()
            .expect("Failed to initialize HTTP client");

        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            username,
            password,
        }
    }

    fn create_request(&self, method: Method, url: &str) -> RequestBuilder {
        let mut builder = self
            .client
            .request(method, url)
            .header("Content-Type", "application/json");

        // Add authentication
        if let Some(api_key) = &self.api_key {
            builder = builder.header("Authorization", format!("ApiKey {}", api_key));
        } else if let (Some(username), Some(password)) = (&self.username, &self.password) {
            builder = builder.basic_auth(username, Some(password));
        }

        builder
    }

    fn create_request_with_content_type(
        &self,
        method: Method,
        url: &str,
        content_type: &str,
    ) -> RequestBuilder {
        let mut builder = self
            .client
            .request(method, url)
            .header("Content-Type", content_type);

        // Add authentication
        if let Some(api_key) = &self.api_key {
            builder = builder.header("Authorization", format!("ApiKey {}", api_key));
        } else if let (Some(username), Some(password)) = (&self.username, &self.password) {
            builder = builder.basic_auth(username, Some(password));
        }

        builder
    }

    pub fn create_index(
        &self,
        index_name: &str,
        settings: Option<OpenSearchSettings>,
    ) -> Result<(), SearchError> {
        trace!("Creating index: {index_name}");

        let url = format!("{}/{}", self.base_url, index_name);

        let mut request = self.create_request(Method::PUT, &url);

        if let Some(settings) = settings {
            request = request.json(&settings);
        }

        let response = request
            .send()
            .map_err(|e| internal_error(format!("Failed to create index: {}", e)))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(search_error_from_status(response.status()))
        }
    }

    pub fn delete_index(&self, index_name: &str) -> Result<(), SearchError> {
        trace!("Deleting index: {index_name}");

        let url = format!("{}/{}", self.base_url, index_name);

        let response = self
            .create_request(Method::DELETE, &url)
            .send()
            .map_err(|e| internal_error(format!("Failed to delete index: {}", e)))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(search_error_from_status(response.status()))
        }
    }

    pub fn list_indices(&self) -> Result<Vec<OpenSearchIndexInfo>, SearchError> {
        trace!("Listing indices");

        let url = format!("{}/_cat/indices?format=json", self.base_url);

        let response = self
            .create_request(Method::GET, &url)
            .send()
            .map_err(|e| internal_error(format!("Failed to list indices: {}", e)))?;

        parse_response(response)
    }

    pub fn index_document(
        &self,
        index_name: &str,
        id: &str,
        document: &Value,
    ) -> Result<(), SearchError> {
        trace!("Indexing document {id} in index: {index_name}");

        let url = format!("{}/{}/_doc/{}", self.base_url, index_name, id);

        let response = self
            .create_request(Method::PUT, &url)
            .json(document)
            .send()
            .map_err(|e| internal_error(format!("Failed to index document: {}", e)))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(search_error_from_status(response.status()))
        }
    }

    pub fn bulk_index(&self, operations: &str) -> Result<OpenSearchBulkResponse, SearchError> {
        trace!("Performing bulk index operation");

        let url = format!("{}/_bulk", self.base_url);

        let response = self
            .create_request_with_content_type(Method::POST, &url, "application/x-ndjson")
            .body(operations.to_string())
            .send()
            .map_err(|e| internal_error(format!("Failed to perform bulk operation: {}", e)))?;

        parse_response(response)
    }

    pub fn delete_document(&self, index_name: &str, id: &str) -> Result<(), SearchError> {
        trace!("Deleting document {id} from index: {index_name}");

        let url = format!("{}/{}/_doc/{}", self.base_url, index_name, id);

        let response = self
            .create_request(Method::DELETE, &url)
            .send()
            .map_err(|e| internal_error(format!("Failed to delete document: {}", e)))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(search_error_from_status(response.status()))
        }
    }

    pub fn get_document(&self, index_name: &str, id: &str) -> Result<Option<Value>, SearchError> {
        trace!("Getting document {id} from index: {index_name}");

        let url = format!("{}/{}/_doc/{}", self.base_url, index_name, id);

        let response = self
            .create_request(Method::GET, &url)
            .send()
            .map_err(|e| internal_error(format!("Failed to get document: {}", e)))?;

        if response.status() == 404 {
            Ok(None)
        } else if response.status().is_success() {
            let doc: Value = parse_response(response)?;
            if let Some(source) = doc.get("_source") {
                Ok(Some(source.clone()))
            } else {
                Ok(None)
            }
        } else {
            Err(search_error_from_status(response.status()))
        }
    }

    pub fn search(
        &self,
        index_name: &str,
        query: &OpenSearchQuery,
    ) -> Result<OpenSearchSearchResponse, SearchError> {
        trace!("Searching index {index_name} with query: {query:?}");

        let url = format!("{}/{}/_search", self.base_url, index_name);

        let response = self
            .create_request(Method::POST, &url)
            .json(query)
            .send()
            .map_err(|e| internal_error(format!("Failed to search: {}", e)))?;

        parse_response(response)
    }

    pub fn get_mappings(&self, index_name: &str) -> Result<Value, SearchError> {
        trace!("Getting mappings for index: {index_name}");

        let url = format!("{}/{}/_mapping", self.base_url, index_name);

        let response = self
            .create_request(Method::GET, &url)
            .send()
            .map_err(|e| internal_error(format!("Failed to get mappings: {}", e)))?;

        parse_response(response)
    }

    pub fn put_mappings(
        &self,
        index_name: &str,
        mappings: &OpenSearchMappings,
    ) -> Result<(), SearchError> {
        trace!("Putting mappings for index: {index_name}");

        let url = format!("{}/{}/_mapping", self.base_url, index_name);

        let response = self
            .create_request(Method::PUT, &url)
            .json(mappings)
            .send()
            .map_err(|e| internal_error(format!("Failed to put mappings: {}", e)))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(search_error_from_status(response.status()))
        }
    }
}

fn parse_response<T: DeserializeOwned + Debug>(response: Response) -> Result<T, SearchError> {
    let status = response.status();

    trace!("Received response from OpenSearch API: {response:?}");

    if status.is_success() {
        let body = response
            .json::<T>()
            .map_err(|err| from_reqwest_error("Failed to decode response body", err))?;

        trace!("Received response from OpenSearch API: {body:?}");

        Ok(body)
    } else {
        let error_body = response
            .text()
            .map_err(|err| from_reqwest_error("Failed to receive error response body", err))?;

        trace!("Received {status} response from OpenSearch API: {error_body:?}");

        Err(search_error_from_status(status))
    }
}
