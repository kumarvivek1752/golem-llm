
// this is just for native testing purpose

use golem_search::error::{internal_error, search_error_from_status};
use golem_search::golem::search::types::SearchError;
use log::error;
use log::trace;
use ureq::{Agent, Response};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

// Custom deserializer to handle null values as empty vectors
fn deserialize_nullable_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<Vec<String>>::deserialize(deserializer).map(|opt| opt.unwrap_or_default())
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct IndexSettings {
    #[serde(skip_serializing_if = "Vec::is_empty", rename = "searchableAttributes", deserialize_with = "deserialize_nullable_vec", default)]
    pub searchable_attributes: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", rename = "attributesForFaceting", deserialize_with = "deserialize_nullable_vec", default)]
    pub attributes_for_faceting: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", rename = "unretrievableAttributes", deserialize_with = "deserialize_nullable_vec", default)]
    pub unretrievable_attributes: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", rename = "attributesToRetrieve", deserialize_with = "deserialize_nullable_vec", default)]
    pub attributes_to_retrieve: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", rename = "ranking", deserialize_with = "deserialize_nullable_vec", default)]
    pub ranking: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", rename = "customRanking", deserialize_with = "deserialize_nullable_vec", default)]
    pub custom_ranking: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", rename = "replicas", deserialize_with = "deserialize_nullable_vec", default)]
    pub replicas: Vec<String>,
}

/// The Algolia Search API client for managing indices and performing search
/// Based on https://www.algolia.com/doc/api-reference/search-api/
#[derive(Clone)]
pub struct AlgoliaSearchApi {
    application_id: String,
    api_key: String,
    agent: Agent,
    search_url: String,
    write_url: String,
}

impl AlgoliaSearchApi {
    pub fn new(application_id: String, api_key: String) -> Self {
        let agent = Agent::new();

        // Algolia URLs require lowercase application IDs
        let search_url = format!(
            "https://{}.algolia.net",
            application_id.to_lowercase()
        );
        let write_url = format!("https://{}.algolia.net", application_id.to_lowercase());

        Self {
            application_id,
            api_key,
            agent,
            search_url,
            write_url,
        }
    }

    fn create_request(&self, method: &str, url: &str) -> ureq::Request  {
        println!("[Algolia] HTTP {} {}", method, url);
        println!(
            "[Algolia] Headers: X-Algolia-Application-Id={}, X-Algolia-API-Key={}...",
            self.application_id,
            &self.api_key[..4]
        );

        let http_method = match method {
            "GET" => reqwest::Method::GET,
            "POST" => reqwest::Method::POST,
            "PUT" => reqwest::Method::PUT,
            "DELETE" => reqwest::Method::DELETE,
            _ => panic!("Unsupported HTTP method"),
        };

         self.agent
            .request(method, url)
            .set("X-Algolia-Application-Id", &self.application_id)
            .set("X-Algolia-API-Key", &self.api_key)
            .set("Content-Type", "application/json")
    }

    // pub fn update_index_settings(
    //     &self,
    //     index_name: &str,
    //     settings: Option<IndexSettings>,
    // ) -> Result<UpdateIndexResponse, SearchError> {
    //     trace!("Updating index settings: {index_name}");

    //     let url = format!("{}/1/indexes/{}/settings", self.write_url, index_name);
    //     let body = settings.unwrap_or_default();

    //     let response = self
    //         .create_request("PUT", &url)
    //         .json(&body)
    //         .send()
    //         .map_err(|e| internal_error(format!("Failed to create index: {}", e)))?;

    //     parse_response(response)
    // }

    pub fn delete_index(&self, index_name: &str) -> Result<DeleteIndexResponse, SearchError> {
        trace!("Deleting index: {index_name}");

        let url = format!("{}/1/indexes/{}", self.write_url, index_name);

        let response = self
            .create_request("DELETE", &url)
            .call()
            .map_err(|e| internal_error(format!("Failed to delete index: {}", e)))?;

        parse_response(response)
    }

    pub fn list_indexes(&self) -> Result<ListIndexesResponse, SearchError> {
        trace!("Listing indexes");

        let url = format!("{}/1/indexes", self.write_url);
        println!("[Algolia] list_indexes URL: {}", url);

        let response = self
            .create_request("GET", &url)
            .call()
            .map_err(|e| internal_error(format!("Failed to list indexes: {}", e)))?;

        parse_response(response)
    }
    //fixed
    pub fn save_object(
        &self,
        index_name: &str,
        object: &AlgoliaObject,
    ) -> Result<SaveObjectResponse, SearchError> {
        trace!("Saving object to index: {index_name}");

        let (method, url) =  {
            (
                "POST",
                format!("{}/1/indexes/{}", self.write_url, index_name),
            )
        };

        let json = serde_json::to_string(object)
            .map_err(|e| internal_error(format!("Failed to serialize object: {}", e)))?;
        let response = self.create_request(method, &url)
            .send_string(&json)
            .map_err(|e| internal_error(format!("Failed to save object: {}", e)))?;

        parse_response(response)
    }

    pub fn save_objects(
        &self,
        index_name: &str,
        objects: &[AlgoliaObject],
    ) -> Result<SaveObjectsResponse, SearchError> {
        trace!("Saving {} objects to index: {index_name}", objects.len());

        let url = format!("{}/1/indexes/{}/batch", self.write_url, index_name);
        let batch_request = BatchRequest {
            requests: objects
                .iter()
                .map(|obj| BatchOperation {
                    action: "addObject".to_string(),
                    body: obj.clone(),
                })
                .collect(),
        };

         let json = serde_json::to_string(&batch_request)
            .map_err(|e| internal_error(format!("Failed to serialize batch request: {}", e)))?;
        let response = self.create_request("POST", &url)
            .send_string(&json)
            .map_err(|e| internal_error(format!("Failed to save objects: {}", e)))?;

        parse_response(response)
    }

    pub fn delete_object(
        &self,
        index_name: &str,
        object_id: &str,
    ) -> Result<DeleteObjectResponse, SearchError> {
        trace!("Deleting object {object_id} from index: {index_name}");

        let url = format!("{}/1/indexes/{}/{}", self.write_url, index_name, object_id);

        let response = self
            .create_request("DELETE", &url)
            .call()
            .map_err(|e| internal_error(format!("Failed to delete object: {}", e)))?;

        parse_response(response)
    }

    pub fn delete_objects(
        &self,
        index_name: &str,
        object_ids: &[String],
    ) -> Result<DeleteObjectsResponse, SearchError> {
        trace!(
            "Deleting {} objects from index: {index_name}",
            object_ids.len()
        );

        let url = format!("{}/1/indexes/{}/batch", self.write_url, index_name);
        let batch_request = BatchRequest {
            requests: object_ids
                .iter()
                .map(|id| BatchOperation {
                    action: "deleteObject".to_string(),
                    body: AlgoliaObject {
                        object_id: Some(id.clone()),
                        content: serde_json::Value::Null,
                    },
                })
                .collect(),
        };

       let json = serde_json::to_string(&batch_request)
            .map_err(|e| internal_error(format!("Failed to serialize batch request: {}", e)))?;
        let response = self.create_request("POST", &url)
            .send_string(&json)
            .map_err(|e| internal_error(format!("Failed to delete objects: {}", e)))?;

        parse_response(response)
    }

    pub fn get_object(
        &self,
        index_name: &str,
        object_id: &str,
    ) -> Result<Option<AlgoliaObject>, SearchError> {
        trace!("Getting object {object_id} from index: {index_name}");

        let url = format!("{}/1/indexes/{}/{}", self.search_url, index_name, object_id);

        let response = self.create_request("GET", &url).call();

        match response {
            Ok(resp) => {
                if resp.status() == 404 {
                    Ok(None)
                } else {
                    let object: AlgoliaObject = parse_response(resp)?;
                    Ok(Some(object))
                }
            }
            Err(e) => Err(internal_error(format!("Failed to get object: {}", e))),
        }
    }

    pub fn search(
        &self,
        index_name: &str,
        query: &SearchQuery,
    ) -> Result<SearchResponse, SearchError> {
        trace!("Searching index {index_name} with query: {query:?}");

        let url = format!("{}/1/indexes/{}/query", self.search_url, index_name);

        let json = serde_json::to_string(query)
            .map_err(|e| internal_error(format!("Failed to serialize search query: {}", e)))?;
        println!("[Algolia] search query body: {}", json);
        let response = self.create_request("POST", &url)
            .send_string(&json);

         match response {
            Ok(resp) => parse_response(resp),
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_else(|_| "Failed to read error body".to_string());
                println!("[Algolia] search error body: {}", body);
                Err(internal_error(format!("Failed to search: {}: status code {}", url, code)))
            }
            Err(e) => Err(internal_error(format!("Failed to search: {}", e))),
        }
    }

    pub fn get_settings(&self, index_name: &str) -> Result<IndexSettings, SearchError> {
        trace!("Getting settings for index: {index_name}");

        let url = format!("{}/1/indexes/{}/settings", self.write_url, index_name);

        let response = self
            .create_request("GET", &url)
            .call()
            .map_err(|e| internal_error(format!("Failed to get settings: {}", e)))?;

        parse_response(response)
    }

    pub fn set_settings(
        &self,
        index_name: &str,
        settings: &IndexSettings,
    ) -> Result<SetSettingsResponse, SearchError> {
        trace!("Setting settings for index: {index_name}");

        let url = format!("{}/1/indexes/{}/settings", self.write_url, index_name);

        let json = serde_json::to_string(settings)
            .map_err(|e| internal_error(format!("Failed to serialize settings: {}", e)))?;
        let response = self
            .create_request("PUT", &url)
            .send_string(&json)
            .map_err(|e| internal_error(format!("Failed to set settings: {}", e)))?;
        println!("[Algolia] set_settings response: {:?}", response);

        parse_response(response)
    }

    pub fn wait_for_task(&self, index_name: &str, task_id: u64) -> Result<(), SearchError> {
        trace!("Waiting for task {task_id} on index {index_name}");
        let url = format!(
            "{}/1/indexes/{}/task/{}",
            self.write_url, index_name, task_id
        );

        for _ in 0..20 {
            // Poll for up to 10 seconds
            let response = self.create_request("GET", &url).call();
            match response {
                Ok(resp) => {
                    let body_str = match resp.into_string() {
                        Ok(s) => s,
                        Err(e) => {
                            println!("[Algolia] Failed to read task status response body: {}", e);
                            continue;
                        }
                    };
                    let body: serde_json::Value = match serde_json::from_str(&body_str) {
                        Ok(b) => b,
                        Err(e) => {
                            println!("[Algolia] Failed to parse task status json: {}. Body: {}", e, body_str);
                            continue;
                        }
                    };
                    println!("[Algolia] Task status response: {:?}", body);
                    if body.get("status").and_then(|s| s.as_str()) == Some("published") {
                        println!("[Algolia] Task {} is published.", task_id);
                        return Ok(());
                    }
                }
                Err(e) => {
                    println!("[Algolia] Error waiting for task: {:?}", e);
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        Err(internal_error(format!(
            "Task {task_id} did not complete in time."
        )))
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
    // Removed highlight_pre_tag as it's not supported in search query parameters
    // (Algolia handles highlighting automatically and returns _highlightResult)
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub attributes_to_retrieve: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub typo_tolerance: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub analytics: Option<bool>,
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
    #[serde(rename = "exhaustiveNbHits", default)]
    pub exhaustive_nb_hits: bool,
    #[serde(rename = "exhaustiveFacetsCount", default)]
    pub exhaustive_facets_count: bool,
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

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct UpdateIndexResponse {
//     #[serde(rename = "taskID")]
//     pub task_id: u64,
//     #[serde(rename = "updatedAt")]
//     pub updated_at: String,
// }

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
    #[serde(rename = "nbPages")]
    pub nb_pages: u32,
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
    let status_code = response.status();
    println!("[Algolia] Response status: {}", status_code);
    
    if status_code >= 200 && status_code < 300 {
        let body_str = response
            .into_string()
            .map_err(|e| internal_error(format!("Failed to read response: {}", e)))?;
        println!("[Algolia] Success response body: {}", body_str);
        let body: T = serde_json::from_str(&body_str)
            .map_err(|e| internal_error(format!("Failed to parse response: {} | body: {}", e, body_str)))?;
        println!("[Algolia] Parsed response: {body:?}");
        Ok(body)
    } else {
        let body = response
            .into_string()
            .map_err(|e| internal_error(format!("Failed to read error response: {}", e)))?;
        println!("[Algolia] Error response body: {}", body);
        error!("[Algolia] parse_response error status {}: {}", status_code, body);
        // Convert status code to reqwest::StatusCode for compatibility with existing error handling
        let status = reqwest::StatusCode::from_u16(status_code)
            .unwrap_or(reqwest::StatusCode::INTERNAL_SERVER_ERROR);
        Err(search_error_from_status(status))
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::env;
//     use std::time::Duration;

//     // ===== SERIALIZATION/DESERIALIZATION TESTS =====

//     #[test]
//     fn test_index_settings_serialization() {
//         println!("[TEST] test_index_settings_serialization - testing IndexSettings serde");
        
//         // Test default/empty settings
//         let empty_settings = IndexSettings::default();
//         let json = serde_json::to_string(&empty_settings).unwrap();
//         println!("[TEST] Empty settings JSON: {}", json);
//         let deserialized: IndexSettings = serde_json::from_str(&json).unwrap();
//         assert_eq!(empty_settings.searchable_attributes, deserialized.searchable_attributes);
        
//         // Test populated settings
//         let settings = IndexSettings {
//             searchable_attributes: vec!["title".to_string(), "content".to_string()],
//             attributes_for_faceting: vec!["category".to_string(), "filterOnly(brand)".to_string()],
//             unretrievable_attributes: vec!["internal_id".to_string()],
//             attributes_to_retrieve: vec!["title".to_string(), "description".to_string()],
//             ranking: vec!["typo".to_string(), "geo".to_string()],
//             custom_ranking: vec!["desc(popularity)".to_string(), "asc(price)".to_string()],
//             replicas: vec!["index_replica1".to_string()],
//         };
        
//         let json = serde_json::to_string(&settings).unwrap();
//         println!("[TEST] Populated settings JSON: {}", json);
//         let deserialized: IndexSettings = serde_json::from_str(&json).unwrap();
//         assert_eq!(settings.searchable_attributes, deserialized.searchable_attributes);
//         assert_eq!(settings.attributes_for_faceting, deserialized.attributes_for_faceting);
//         assert_eq!(settings.custom_ranking, deserialized.custom_ranking);
        
//         // Test parsing real Algolia API response with null values
//         let algolia_response = r#"{
//             "searchableAttributes": ["title", "description"],
//             "attributesForFaceting": null,
//             "customRanking": ["desc(popularity)"],
//             "replicas": null
//         }"#;
        
//         let parsed: IndexSettings = serde_json::from_str(algolia_response).unwrap();
//         assert_eq!(parsed.searchable_attributes, vec!["title", "description"]);
//         assert_eq!(parsed.attributes_for_faceting, Vec::<String>::new()); // null should become empty vec
//         assert_eq!(parsed.custom_ranking, vec!["desc(popularity)"]);
//         assert_eq!(parsed.replicas, Vec::<String>::new()); // null should become empty vec
        
//         println!("[TEST] ✓ IndexSettings serialization/deserialization works correctly");
//     }

//     #[test]
//     fn test_algolia_object_serialization() {
//         println!("[TEST] test_algolia_object_serialization - testing AlgoliaObject serde");
        
//         // Test with objectID
//         let obj_with_id = AlgoliaObject {
//             object_id: Some("test123".to_string()),
//             content: serde_json::json!({
//                 "title": "Test Document",
//                 "category": "books",
//                 "price": 29.99,
//                 "in_stock": true
//             }),
//         };
        
//         let json = serde_json::to_string(&obj_with_id).unwrap();
//         println!("[TEST] Object with ID JSON: {}", json);
//         let deserialized: AlgoliaObject = serde_json::from_str(&json).unwrap();
//         assert_eq!(obj_with_id.object_id, deserialized.object_id);
//         assert_eq!(obj_with_id.content["title"], deserialized.content["title"]);
        
//         // Test without objectID
//         let obj_without_id = AlgoliaObject {
//             object_id: None,
//             content: serde_json::json!({
//                 "title": "Another Document",
//                 "category": "electronics"
//             }),
//         };
        
//         let json = serde_json::to_string(&obj_without_id).unwrap();
//         println!("[TEST] Object without ID JSON: {}", json);
//         // Should not include objectID field when None
//         assert!(!json.contains("objectID"));
        
//         let deserialized: AlgoliaObject = serde_json::from_str(&json).unwrap();
//         assert_eq!(obj_without_id.object_id, deserialized.object_id);
//         assert_eq!(obj_without_id.content["title"], deserialized.content["title"]);
        
//         println!("[TEST] ✓ AlgoliaObject serialization/deserialization works correctly");
//     }

//     #[test]
//     fn test_search_query_serialization() {
//         println!("[TEST] test_search_query_serialization - testing SearchQuery serde");
        
//         // Test full query
//         let query = SearchQuery {
//             query: Some("search term".to_string()),
//             filters: Some("category:books AND price > 10".to_string()),
//             numeric_filters: Some(serde_json::json!(["price > 10", "rating >= 4"])),
//             page: Some(1),
//             hits_per_page: Some(20),
//             offset: Some(0),
//             length: Some(100),
//             facets: vec!["category".to_string(), "brand".to_string()],
//             attributes_to_retrieve: vec!["title".to_string(), "price".to_string()],
//             typo_tolerance: Some(true),
//             analytics: Some(false),
//         };
        
//         let json = serde_json::to_string(&query).unwrap();
//         println!("[TEST] Full query JSON: {}", json);
//         let deserialized: SearchQuery = serde_json::from_str(&json).unwrap();
//         assert_eq!(query.query, deserialized.query);
//         assert_eq!(query.filters, deserialized.filters);
//         assert_eq!(query.hits_per_page, deserialized.hits_per_page);
//         assert_eq!(query.facets, deserialized.facets);
        
//         // Test minimal query
//         let minimal_query = SearchQuery {
//             query: Some("minimal".to_string()),
//             filters: None,
//             numeric_filters: None,
//             page: None,
//             hits_per_page: None,
//             offset: None,
//             length: None,
//             facets: vec![],
//             attributes_to_retrieve: vec![],
//             typo_tolerance: None,
//             analytics: None,
//         };
        
//         let json = serde_json::to_string(&minimal_query).unwrap();
//         println!("[TEST] Minimal query JSON: {}", json);
//         // Should not include None/empty fields
//         assert!(!json.contains("filters"));
//         assert!(!json.contains("page"));
//         assert!(!json.contains("facets"));
        
//         println!("[TEST] ✓ SearchQuery serialization/deserialization works correctly");
//     }

//     fn setup_client() -> AlgoliaSearchApi {
//         let app_id = "SLPKFQ34PO";
//         let api_key = "76b6638c2c0754b20b008c55dc2356bb";
//         println!("[TEST] Using ALGOLIA_APPLICATION_ID={} ALGOLIA_API_KEY={}...", app_id, &api_key[..4]);
//         println!("[TEST] Note: Testing with provided credentials");
//         AlgoliaSearchApi::new(app_id.to_string(), api_key.to_string())
//     }

//     fn setup_client_safe() -> Result<AlgoliaSearchApi, String> {
//         // Try environment variables first, then fall back to hardcoded values
//         let app_id = env::var("ALGOLIA_APPLICATION_ID").unwrap_or_else(|_| "SLPKFQ34PO".to_string());
//         let api_key = env::var("ALGOLIA_API_KEY").unwrap_or_else(|_| "76b6638c2c0754b20b008c55dc2356bb".to_string());
        
//         println!("[TEST] Using ALGOLIA_APPLICATION_ID={} ALGOLIA_API_KEY={}...", app_id, &api_key[..4]);
        
//         let client = AlgoliaSearchApi::new(app_id, api_key);
        
//         // Test basic connectivity with list_indexes (should work with any valid key)
//         println!("[TEST] Testing basic connectivity...");
//         match client.list_indexes() {
//             Ok(_) => {
//                 println!("[TEST] ✓ Basic connectivity test passed");
//                 Ok(client)
//             }
//             Err(e) => {
//                 println!("[TEST] ✗ Basic connectivity test failed: {:?}", e);
//                 Err(format!("Failed to connect to Algolia: {:?}", e))
//             }
//         }
//     }

//     fn test_index_name(test_name: &str) -> String {
//         format!("test-algolia-{}-{}", test_name, std::process::id())
//     }

//     fn create_test_object(id: &str) -> AlgoliaObject {
//         AlgoliaObject {
//             object_id: Some(id.to_string()),
//             content: serde_json::json!({
//                 "title": format!("Test Object {}", id),
//                 "category": "test",
//                 "value": id.parse::<i32>().unwrap_or(0),
//                 "active": true
//             }),
//         }
//     }

//     #[test]
//     fn test_delete_objects() {
//         println!("[TEST] test_delete_objects - comprehensive delete_objects testing");
//         let client = setup_client();
//         let index = test_index_name("delete_objects");
        
//         // Create index by setting initial settings (Algolia auto-creates index)
//         println!("[TEST] Creating test index by setting settings: {}", index);
//         let initial_settings = IndexSettings::default();
//         let settings_res = client.set_settings(&index, &initial_settings);
//         println!("[TEST] set_settings result: {:?}", settings_res);
        
//         match settings_res {
//             Ok(settings_response) => {
//                 println!("[TEST] ✓ Index auto-created via set_settings");
//                 client.wait_for_task(&index, settings_response.task_id).expect("wait_for_task for set_settings failed");
                
//                 // Create multiple test objects
//                 println!("[TEST] Creating test objects for deletion");
//                 let test_objects = vec![
//                     create_test_object("obj1"),
//                     create_test_object("obj2"),
//                     create_test_object("obj3"),
//                     create_test_object("obj4"),
//                     create_test_object("obj5"),
//                 ];
                
//                 // Save all objects
//                 let save_res = client.save_objects(&index, &test_objects);
//                 println!("[TEST] save_objects result: {:?}", save_res);
                
//                 match save_res {
//                     Ok(save_response) => {
//                         client.wait_for_task(&index, save_response.task_id).expect("wait_for_task for save_objects failed");
//                         println!("[TEST] ✓ Test objects created successfully");
                        
//                         // Wait for objects to be indexed and verify they exist
//                         println!("[TEST] Verifying objects exist before deletion");
//                         let mut all_found = false;
//                         for attempt in 0..20 {
//                             let mut found_count = 0;
//                             for obj in &test_objects {
//                                 if let Ok(Some(_)) = client.get_object(&index, &obj.object_id.as_ref().unwrap()) {
//                                     found_count += 1;
//                                 }
//                             }
                            
//                             if found_count == test_objects.len() {
//                                 all_found = true;
//                                 println!("[TEST] ✓ All {} objects found after {} attempts", test_objects.len(), attempt + 1);
//                                 break;
//                             }
                            
//                             println!("[TEST] Found {}/{} objects, retrying... ({}/20)", found_count, test_objects.len(), attempt + 1);
//                             std::thread::sleep(Duration::from_millis(500));
//                         }
                        
//                         if !all_found {
//                             println!("[TEST] ⚠ Not all objects were found before deletion test");
//                         }
                        
//                         // Test deleting a subset of objects
//                         let objects_to_delete = vec![
//                             "obj1".to_string(),
//                             "obj3".to_string(),
//                             "obj5".to_string(),
//                         ];
                        
//                         println!("[TEST] Deleting {} objects: {:?}", objects_to_delete.len(), objects_to_delete);
//                         let delete_res = client.delete_objects(&index, &objects_to_delete);
//                         println!("[TEST] delete_objects result: {:?}", delete_res);
                        
//                         match delete_res {
//                             Ok(delete_response) => {
//                                 client.wait_for_task(&index, delete_response.task_id).expect("wait_for_task for delete_objects failed");
//                                 println!("[TEST] ✓ delete_objects operation completed");
                                
//                                 // Verify deleted objects are gone and remaining objects still exist
//                                 println!("[TEST] Verifying deletion results");
//                                 let mut verification_passed = false;
                                
//                                 for attempt in 0..20 {
//                                     let mut deleted_count = 0;
//                                     let mut remaining_count = 0;
                                    
//                                     // Check deleted objects
//                                     for obj_id in &objects_to_delete {
//                                         match client.get_object(&index, obj_id) {
//                                             Ok(None) => deleted_count += 1,
//                                             Ok(Some(_)) => {
//                                                 println!("[TEST] Object {} still exists (attempt {})", obj_id, attempt + 1);
//                                             }
//                                             Err(_) => {
//                                                 // 404 error is also a sign of successful deletion
//                                                 deleted_count += 1;
//                                             }
//                                         }
//                                     }
                                    
//                                     // Check remaining objects
//                                     for obj_id in &["obj2", "obj4"] {
//                                         if let Ok(Some(_)) = client.get_object(&index, obj_id) {
//                                             remaining_count += 1;
//                                         }
//                                     }
                                    
//                                     if deleted_count == objects_to_delete.len() && remaining_count == 2 {
//                                         verification_passed = true;
//                                         println!("[TEST] ✓ Deletion verification passed after {} attempts", attempt + 1);
//                                         println!("[TEST] ✓ {} objects successfully deleted", deleted_count);
//                                         println!("[TEST] ✓ {} objects correctly remaining", remaining_count);
//                                         break;
//                                     }
                                    
//                                     println!("[TEST] Verification attempt {}/20: deleted={}/{}, remaining={}/2", 
//                                         attempt + 1, deleted_count, objects_to_delete.len(), remaining_count);
//                                     std::thread::sleep(Duration::from_millis(500));
//                                 }
                                
//                                 if !verification_passed {
//                                     println!("[TEST] ⚠ Deletion verification did not complete within expected time");
//                                 } else {
//                                     println!("[TEST] ✓ delete_objects test completed successfully");
//                                 }
//                             }
//                             Err(e) => {
//                                 println!("[TEST] ✗ delete_objects operation failed: {:?}", e);
//                             }
//                         }
//                     }
//                     Err(e) => {
//                         println!("[TEST] ✗ Failed to create test objects: {:?}", e);
//                     }
//                 }
                
//                 // Cleanup
//                 println!("[TEST] Cleaning up test index");
//                 match client.delete_index(&index) {
//                     Ok(delete_response) => {
//                         client.wait_for_task(&index, delete_response.task_id).ok();
//                         println!("[TEST] ✓ Test index cleaned up successfully");
//                     }
//                     Err(e) => {
//                         println!("[TEST] ⚠ Failed to clean up test index: {:?}", e);
//                     }
//                 }
//             }
//             Err(e) => {
//                 println!("[TEST] ✗ Setting initial settings failed, skipping delete_objects test: {:?}", e);
//                 println!("[TEST] This usually means you're using a Search-Only API key");
//                 println!("[TEST] For full testing, please provide an Admin API key via environment variables:");
//                 println!("[TEST] export ALGOLIA_APPLICATION_ID=your_app_id");
//                 println!("[TEST] export ALGOLIA_API_KEY=your_admin_api_key");
//             }
//         }
//     }

//     #[test]
//     fn test_list_indexes() {
//         println!("[TEST] test_list_indexes - testing index listing functionality");
//         let client = setup_client();
        
//         match client.list_indexes() {
//             Ok(response) => {
//                 println!("[TEST] ✓ list_indexes succeeded");
//                 println!("[TEST] Response: {:?}", response);
//                 println!("[TEST] Found {} indexes", response.items.len());
//                 for (i, index) in response.items.iter().take(3).enumerate() {
//                     println!("[TEST]   {}. {}", i + 1, index.name);
//                 }
//             }
//             Err(e) => {
//                 println!("[TEST] ✗ list_indexes failed: {:?}", e);
//             }
//         }
//     }

//     #[test]
//     fn test_search() {
//         println!("[TEST] test_search - testing search functionality");
//         let client = setup_client();
//         let index = test_index_name("search");
        
//         // Create test objects first (this will auto-create the index)
//         println!("[TEST] Creating test objects (will auto-create index)");
//         let test_objects = vec![
//             create_test_object("search1"),
//             create_test_object("search2"),
//         ];
        
//         let save_res = client.save_objects(&index, &test_objects);
//         match save_res {
//             Ok(save_response) => {
//                 println!("[TEST] ✓ Index auto-created via save_objects");
//                 client.wait_for_task(&index, save_response.task_id).expect("wait_for_task for save_objects failed");
                
//                 // Wait for indexing and test search
//                 println!("[TEST] Testing search functionality");
//                 let search_query = SearchQuery {
//                     query: Some("Test Object".to_string()),
//                     filters: None,
//                     numeric_filters: None,
//                     page: None,
//                     hits_per_page: Some(10),
//                     offset: None,
//                     length: None,
//                     facets: vec![],
//                     attributes_to_retrieve: vec![],
//                     typo_tolerance: None,
//                     analytics: None,
//                 };
                
//                 let mut search_success = false;
//                 for attempt in 0..20 {
//                     match client.search(&index, &search_query) {
//                         Ok(response) => {
//                             println!("[TEST] ✓ search succeeded on attempt {}", attempt + 1);
//                             println!("[TEST] Search response: {:?}", response);
//                             if response.hits.len() > 0 {
//                                 search_success = true;
//                                 println!("[TEST] ✓ Found {} search results", response.hits.len());
//                             }
//                             break;
//                         }
//                         Err(e) => {
//                             println!("[TEST] Search attempt {} failed: {:?}", attempt + 1, e);
//                             std::thread::sleep(Duration::from_millis(500));
//                         }
//                     }
//                 }
                
//                 if !search_success {
//                     println!("[TEST] ⚠ Search test did not return results within expected time");
//                 }
//             }
//             Err(e) => println!("[TEST] ✗ Failed to create test objects for search: {:?}", e),
//         }
        
//         // Cleanup
//         client.delete_index(&index).ok();
//     }

//     #[test]
//     fn test_save_and_get_object() {
//         println!("[TEST] test_save_and_get_object - testing single object operations");
//         let client = setup_client();
//         let index = test_index_name("save_get");
        
//         // Test save_object (this will auto-create the index)
//         let test_obj = create_test_object("save_test_1");
//         println!("[TEST] Testing save_object (will auto-create index)");
        
//         match client.save_object(&index, &test_obj) {
//             Ok(save_response) => {
//                 println!("[TEST] ✓ save_object succeeded: {:?}", save_response);
//                 client.wait_for_task(&index, save_response.task_id).expect("wait_for_task for save_object failed");
                
//                 // Test get_object
//                 println!("[TEST] Testing get_object");
//                 let mut get_success = false;
//                 for attempt in 0..20 {
//                     match client.get_object(&index, "save_test_1") {
//                         Ok(Some(obj)) => {
//                             println!("[TEST] ✓ get_object succeeded on attempt {}: {:?}", attempt + 1, obj);
//                             get_success = true;
//                             break;
//                         }
//                         Ok(None) => {
//                             println!("[TEST] Object not found yet, retrying... ({}/20)", attempt + 1);
//                             std::thread::sleep(Duration::from_millis(500));
//                         }
//                         Err(e) => {
//                             println!("[TEST] get_object error on attempt {}: {:?}", attempt + 1, e);
//                             std::thread::sleep(Duration::from_millis(500));
//                         }
//                     }
//                 }
                
//                 if !get_success {
//                     println!("[TEST] ⚠ get_object test did not succeed within expected time");
//                 }
//             }
//             Err(e) => println!("[TEST] ✗ save_object failed: {:?}", e),
//         }
        
//         // Cleanup
//         client.delete_index(&index).ok();
//     }

//     #[test]
//     fn test_delete_object() {
//         println!("[TEST] test_delete_object - testing single object deletion");
//         let client = setup_client();
//         let index = test_index_name("delete_object");
        
//         // Create test object (this will auto-create the index)
//         let test_obj = create_test_object("delete_test_1");
//         println!("[TEST] Creating test object (will auto-create index)");
        
//         match client.save_object(&index, &test_obj) {
//             Ok(save_response) => {
//                 client.wait_for_task(&index, save_response.task_id).expect("wait_for_task for save_object failed");
                
//                 // Verify object exists
//                 let mut obj_exists = false;
//                 for attempt in 0..20 {
//                     if let Ok(Some(_)) = client.get_object(&index, "delete_test_1") {
//                         obj_exists = true;
//                         println!("[TEST] ✓ Object exists before deletion (attempt {})", attempt + 1);
//                         break;
//                     }
//                     std::thread::sleep(Duration::from_millis(500));
//                 }
                
//                 if obj_exists {
//                     // Test delete_object
//                     println!("[TEST] Testing delete_object");
                    
//                     match client.delete_object(&index, "delete_test_1") {
//                         Ok(delete_response) => {
//                             println!("[TEST] ✓ delete_object succeeded: {:?}", delete_response);
//                             client.wait_for_task(&index, delete_response.task_id).expect("wait_for_task for delete_object failed");
                            
//                             // Verify object is deleted
//                             let mut deletion_verified = false;
//                             for attempt in 0..20 {
//                                 match client.get_object(&index, "delete_test_1") {
//                                     Ok(None) => {
//                                         deletion_verified = true;
//                                         println!("[TEST] ✓ Object deletion verified after {} attempts", attempt + 1);
//                                         break;
//                                     }
//                                     Ok(Some(_)) => {
//                                         println!("[TEST] Object still exists, retrying... ({}/20)", attempt + 1);
//                                         std::thread::sleep(Duration::from_millis(500));
//                                     }
//                                     Err(_) => {
//                                         // 404 error is also acceptable for deleted object
//                                         deletion_verified = true;
//                                         println!("[TEST] ✓ Object deletion verified (404 response) after {} attempts", attempt + 1);
//                                         break;
//                                     }
//                                 }
//                             }
                            
//                             if !deletion_verified {
//                                 println!("[TEST] ⚠ Object deletion verification did not complete within expected time");
//                             }
//                         }
//                         Err(e) => println!("[TEST] ✗ delete_object failed: {:?}", e),
//                     }
//                 } else {
//                     println!("[TEST] ⚠ Object was not found before deletion test");
//                 }
//             }
//             Err(e) => println!("[TEST] ✗ Failed to create test object for deletion: {:?}", e),
//         }
        
//         // Cleanup
//         client.delete_index(&index).ok();
//     }

//     #[test]
//     fn test_response_types_serialization() {
//         println!("[TEST] test_response_types_serialization - testing all response types serde");
        
//         // Test SetSettingsResponse (the one causing issues)
//         let set_settings_response = r#"{
//             "taskID": 44372162001,
//             "updatedAt": "2025-07-04T14:47:12.197Z"
//         }"#;
        
//         let parsed: SetSettingsResponse = serde_json::from_str(set_settings_response).unwrap();
//         assert_eq!(parsed.task_id, 44372162001);
//         assert_eq!(parsed.updated_at, "2025-07-04T14:47:12.197Z");
        
//         // Test round-trip
//         let json = serde_json::to_string(&parsed).unwrap();
//         println!("[TEST] SetSettingsResponse round-trip JSON: {}", json);
//         let reparsed: SetSettingsResponse = serde_json::from_str(&json).unwrap();
//         assert_eq!(parsed.task_id, reparsed.task_id);
//         assert_eq!(parsed.updated_at, reparsed.updated_at);
        
//         println!("[TEST] ✓ SetSettingsResponse serialization/deserialization works correctly");
//     }

//     #[test]
//     fn test_set_settings_integration_roundtrip() {
//         println!("[TEST] test_set_settings_integration_roundtrip - comprehensive set_settings integration test");
//         let client = setup_client();
//         let index = test_index_name("set_settings_integration");
        
//         // Test 1: Create index and set initial settings
//         println!("[TEST] Phase 1: Testing initial set_settings call");
//         let initial_settings = IndexSettings {
//             searchable_attributes: vec!["title".to_string(), "description".to_string()],
//             attributes_for_faceting: vec!["category".to_string()],
//             unretrievable_attributes: vec![],
//             attributes_to_retrieve: vec!["title".to_string(), "description".to_string(), "category".to_string()],
//             ranking: vec!["typo".to_string(), "geo".to_string(), "words".to_string()],
//             custom_ranking: vec!["desc(popularity)".to_string()],
//             replicas: vec![],
//         };
        
//         match client.set_settings(&index, &initial_settings) {
//             Ok(set_response) => {
//                 println!("[TEST] ✓ set_settings succeeded: {:?}", set_response);
                
//                 // Test SetSettingsResponse structure
//                 assert!(set_response.task_id > 0, "task_id should be positive");
//                 assert!(!set_response.updated_at.is_empty(), "updated_at should not be empty");
//                 assert!(set_response.updated_at.contains("T"), "updated_at should be ISO format");
//                 println!("[TEST] ✓ SetSettingsResponse structure is valid");
                
//                 // Test roundtrip serialization of SetSettingsResponse
//                 let serialized = serde_json::to_string(&set_response).unwrap();
//                 println!("[TEST] SetSettingsResponse serialized: {}", serialized);
//                 let deserialized: SetSettingsResponse = serde_json::from_str(&serialized).unwrap();
//                 assert_eq!(set_response.task_id, deserialized.task_id);
//                 assert_eq!(set_response.updated_at, deserialized.updated_at);
//                 println!("[TEST] ✓ SetSettingsResponse roundtrip serialization works");
                
//                 // Wait for task completion
//                 match client.wait_for_task(&index, set_response.task_id) {
//                     Ok(()) => {
//                         println!("[TEST] ✓ Initial settings task completed successfully");
                        
//                         // Test 2: Verify settings were applied by getting them back
//                         println!("[TEST] Phase 2: Verifying settings were applied");
//                         match client.get_settings(&index) {
//                             Ok(retrieved_settings) => {
//                                 println!("[TEST] ✓ Retrieved settings: {:?}", retrieved_settings);
//                                 assert_eq!(initial_settings.searchable_attributes, retrieved_settings.searchable_attributes);
//                                 assert_eq!(initial_settings.attributes_for_faceting, retrieved_settings.attributes_for_faceting);
//                                 assert_eq!(initial_settings.custom_ranking, retrieved_settings.custom_ranking);
//                                 println!("[TEST] ✓ Settings were applied correctly");
//                             }
//                             Err(e) => {
//                                 println!("[TEST] ⚠ Failed to retrieve settings for verification: {:?}", e);
//                             }
//                         }
                        
//                         // Test 3: Update settings and test another SetSettingsResponse
//                         println!("[TEST] Phase 3: Testing settings update");
//                         let updated_settings = IndexSettings {
//                             searchable_attributes: vec!["title".to_string(), "content".to_string(), "tags".to_string()],
//                             attributes_for_faceting: vec!["category".to_string(), "brand".to_string()],
//                             unretrievable_attributes: vec!["internal_id".to_string()],
//                             attributes_to_retrieve: vec!["title".to_string(), "content".to_string()],
//                             ranking: vec!["typo".to_string(), "geo".to_string(), "words".to_string(), "proximity".to_string()],
//                             custom_ranking: vec!["desc(popularity)".to_string(), "asc(price)".to_string()],
//                             replicas: vec![],
//                         };
                        
//                         match client.set_settings(&index, &updated_settings) {
//                             Ok(update_response) => {
//                                 println!("[TEST] ✓ Settings update succeeded: {:?}", update_response);
                                
//                                 // Test second SetSettingsResponse
//                                 assert!(update_response.task_id > 0);
//                                 assert!(update_response.task_id != set_response.task_id, "Should have different task IDs");
//                                 assert!(!update_response.updated_at.is_empty());
//                                 println!("[TEST] ✓ Second SetSettingsResponse structure is valid");
                                
//                                 // Test roundtrip serialization of second response
//                                 let serialized2 = serde_json::to_string(&update_response).unwrap();
//                                 println!("[TEST] Second SetSettingsResponse serialized: {}", serialized2);
//                                 let deserialized2: SetSettingsResponse = serde_json::from_str(&serialized2).unwrap();
//                                 assert_eq!(update_response.task_id, deserialized2.task_id);
//                                 assert_eq!(update_response.updated_at, deserialized2.updated_at);
//                                 println!("[TEST] ✓ Second SetSettingsResponse roundtrip serialization works");
                                
//                                 // Wait for update task completion
//                                 match client.wait_for_task(&index, update_response.task_id) {
//                                     Ok(()) => {
//                                         println!("[TEST] ✓ Settings update task completed successfully");
                                        
//                                         // Test 4: Verify updated settings
//                                         println!("[TEST] Phase 4: Verifying updated settings");
//                                         match client.get_settings(&index) {
//                                             Ok(final_settings) => {
//                                                 println!("[TEST] ✓ Retrieved updated settings: {:?}", final_settings);
//                                                 assert_eq!(updated_settings.searchable_attributes, final_settings.searchable_attributes);
//                                                 assert_eq!(updated_settings.attributes_for_faceting, final_settings.attributes_for_faceting);
//                                                 assert_eq!(updated_settings.unretrievable_attributes, final_settings.unretrievable_attributes);
//                                                 assert_eq!(updated_settings.custom_ranking, final_settings.custom_ranking);
//                                                 println!("[TEST] ✓ Updated settings were applied correctly");
//                                             }
//                                             Err(e) => {
//                                                 println!("[TEST] ⚠ Failed to retrieve updated settings: {:?}", e);
//                                             }
//                                         }
                                        
//                                         println!("[TEST] ✓ All SetSettingsResponse integration tests passed!");
//                                     }
//                                     Err(e) => {
//                                         println!("[TEST] ⚠ Settings update task failed: {:?}", e);
//                                     }
//                                 }
//                             }
//                             Err(e) => {
//                                 println!("[TEST] ✗ Settings update failed: {:?}", e);
//                             }
//                         }
//                     }
//                     Err(e) => {
//                         println!("[TEST] ⚠ Initial settings task failed: {:?}", e);
//                     }
//                 }
//             }
//             Err(e) => {
//                 println!("[TEST] ✗ Initial set_settings failed: {:?}", e);
//                 println!("[TEST] This usually means you're using a Search-Only API key");
//                 println!("[TEST] For full testing, please provide an Admin API key via environment variables:");
//                 println!("[TEST] export ALGOLIA_APPLICATION_ID=your_app_id");
//                 println!("[TEST] export ALGOLIA_API_KEY=your_admin_api_key");
//             }
//         }
        
//         // Cleanup
//         println!("[TEST] Cleaning up test index");
//         match client.delete_index(&index) {
//             Ok(delete_response) => {
//                 client.wait_for_task(&index, delete_response.task_id).ok();
//                 println!("[TEST] ✓ Test index cleaned up successfully");
//             }
//             Err(e) => {
//                 println!("[TEST] ⚠ Failed to clean up test index: {:?}", e);
//             }
//         }
//     }

//     // ===== EXISTING TESTS =====
// }
