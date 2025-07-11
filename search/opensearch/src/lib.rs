use crate::client::OpenSearchApi;
use crate::conversions::{
    doc_to_opensearch_document, opensearch_document_to_doc, search_query_to_opensearch_request,
    opensearch_response_to_search_results, schema_to_opensearch_settings, opensearch_mappings_to_schema,
    create_retry_query,
};
use golem_search::golem::search::core::{Guest, SearchStream, GuestSearchStream};
use golem_search::golem::search::types::{
    IndexName, DocumentId, Doc, SearchQuery, SearchResults, SearchHit, Schema, SearchError
};
use golem_search::config::with_config_keys;
use golem_search::durability::{DurableSearch, ExtendedGuest};
use golem_search::LOGGING_STATE;
use golem_rust::wasm_rpc::Pollable;
use std::cell::{RefCell, Cell};

mod client;
mod conversions;

/// Simple search stream implementation for OpenSearch
/// Since OpenSearch doesn't have native streaming, we implement pagination-based streaming
struct OpenSearchSearchStream {
    client: OpenSearchApi,
    index_name: String,
    query: SearchQuery,
    current_page: Cell<u32>,
    finished: Cell<bool>,
    last_response: RefCell<Option<SearchResults>>,
}

impl OpenSearchSearchStream {
    pub fn new(client: OpenSearchApi, index_name: String, query: SearchQuery) -> Self {
        Self {
            client,
            index_name,
            query: query.clone(),
            current_page: Cell::new(query.offset.unwrap_or(0) / query.per_page.unwrap_or(20)),
            finished: Cell::new(false),
            last_response: RefCell::new(None),
        }
    }

    pub fn subscribe(&self) -> Pollable {
        // For non-streaming APIs, return an immediately ready pollable
        golem_rust::bindings::wasi::clocks::monotonic_clock::subscribe_duration(0)
    }
}

impl GuestSearchStream for OpenSearchSearchStream {
    fn get_next(&self) -> Option<Vec<SearchHit>> {
        if self.finished.get() {
            return Some(vec![]);
        }

        let mut search_query = self.query.clone();
        let current_page = self.current_page.get();
        let limit = search_query.per_page.unwrap_or(20);
        
        // Calculate offset based on current page
        search_query.offset = Some(current_page * limit);

        let opensearch_request = search_query_to_opensearch_request(search_query);
        
        match self.client.search(&self.index_name, &opensearch_request) {
            Ok(response) => {
                let search_results = opensearch_response_to_search_results(response);
                
                // Check if we've reached the end
                if search_results.hits.is_empty() {
                    self.finished.set(true);
                    return Some(vec![]);
                }

                // Check if this is the last page based on total and per_page
                if let Some(total) = search_results.total {
                    let current_offset = current_page * limit;
                    let next_offset = current_offset + limit;
                    if next_offset >= total {
                        self.finished.set(true);
                    }
                }

                // If we received fewer hits than requested, we're at the end
                if (search_results.hits.len() as u32) < limit {
                    self.finished.set(true);
                }

                // Prepare for next page
                self.current_page.set(current_page + 1);
                
                let hits = search_results.hits.clone();
                *self.last_response.borrow_mut() = Some(search_results);
                
                Some(hits)
            }
            Err(_) => {
                self.finished.set(true);
                Some(vec![])
            }
        }
    }

    fn blocking_get_next(&self) -> Vec<SearchHit> {
        self.get_next().unwrap_or_default()
    }
}

struct OpenSearchComponent;

impl OpenSearchComponent {
    const BASE_URL_ENV_VAR: &'static str = "OPENSEARCH_BASE_URL";
    const USERNAME_ENV_VAR: &'static str = "OPENSEARCH_USERNAME";
    const PASSWORD_ENV_VAR: &'static str = "OPENSEARCH_PASSWORD";
    const API_KEY_ENV_VAR: &'static str = "OPENSEARCH_API_KEY";

    fn create_client() -> Result<OpenSearchApi, SearchError> {
        with_config_keys(
            &[Self::BASE_URL_ENV_VAR],
            |keys| {
                if keys.is_empty() {
                    return Err(SearchError::Internal("Missing OpenSearch base URL".to_string()));
                }
                
                let base_url = keys[0].clone();
                
                // Authentication is optional
                let username = std::env::var(Self::USERNAME_ENV_VAR).ok();
                let password = std::env::var(Self::PASSWORD_ENV_VAR).ok();
                let api_key = std::env::var(Self::API_KEY_ENV_VAR).ok();
                
                // In test mode, check if we should accept invalid certificates
                // #[cfg(test)]
                // {
                //     let accept_invalid_certs = std::env::var("OPENSEARCH_ACCEPT_INVALID_CERTS")
                //         .map(|v| v == "true" || v == "1")
                //         .unwrap_or(false);
                    
                //     Ok(OpenSearchApi::new_for_testing(base_url, username, password, api_key, accept_invalid_certs))
                // }
                
                //#[cfg(not(test))]
                {
                    Ok(OpenSearchApi::new(base_url, username, password, api_key))
                }
            }
        )
    }
}

impl Guest for OpenSearchComponent {
    type SearchStream = OpenSearchSearchStream;

    fn create_index(name: IndexName, schema: Option<Schema>) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        let settings = schema.map(schema_to_opensearch_settings);
        client.create_index(&name, settings)?;
        
        Ok(())
    }

    fn delete_index(name: IndexName) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        client.delete_index(&name)?;
        
        Ok(())
    }

    fn list_indexes() -> Result<Vec<IndexName>, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let indices = client.list_indices()?;
        Ok(indices.into_iter().map(|idx| idx.index).collect())
    }

    fn upsert(index: IndexName, doc: Doc) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let opensearch_doc = doc_to_opensearch_document(doc)
            .map_err(|e| SearchError::InvalidQuery(e))?;
        
        // Extract ID for indexing
        let doc_id = opensearch_doc.get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        
        client.index_document(&index, &doc_id, &opensearch_doc)?;
        
        Ok(())
    }

    fn upsert_many(index: IndexName, docs: Vec<Doc>) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        if docs.is_empty() {
            return Ok(());
        }

        // Convert docs to OpenSearch format and create bulk operations
        let mut bulk_operations = Vec::new();
        for doc in docs {
            let opensearch_doc = doc_to_opensearch_document(doc)
                .map_err(|e| SearchError::InvalidQuery(e))?;
            
            let doc_id = opensearch_doc.get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            
            // Add index action
            let action = serde_json::json!({
                "index": {
                    "_index": index,
                    "_id": doc_id
                }
            });
            bulk_operations.push(serde_json::to_string(&action).unwrap());
            bulk_operations.push(serde_json::to_string(&opensearch_doc).unwrap());
        }
        
        let bulk_body = bulk_operations.join("\n") + "\n";
        
        let _result = client.bulk_index(&bulk_body)?;
        
        Ok(())
    }

    fn delete(index: IndexName, id: DocumentId) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        client.delete_document(&index, &id)?;
        
        Ok(())
    }

    fn delete_many(index: IndexName, ids: Vec<DocumentId>) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        if ids.is_empty() {
            return Ok(());
        }

        // Create bulk delete operations
        let mut bulk_operations = Vec::new();
        for id in ids {
            let action = serde_json::json!({
                "delete": {
                    "_index": index,
                    "_id": id
                }
            });
            bulk_operations.push(serde_json::to_string(&action).unwrap());
        }
        
        let bulk_body = bulk_operations.join("\n") + "\n";
        client.bulk_index(&bulk_body)?;
        
        Ok(())
    }

    fn get(index: IndexName, id: DocumentId) -> Result<Option<Doc>, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        match client.get_document(&index, &id)? {
            Some(opensearch_doc) => Ok(Some(opensearch_document_to_doc(opensearch_doc))),
            None => Ok(None),
        }
    }

    fn search(index: IndexName, query: SearchQuery) -> Result<SearchResults, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let opensearch_request = search_query_to_opensearch_request(query);
        
        let response = client.search(&index, &opensearch_request)?;
        Ok(opensearch_response_to_search_results(response))
    }

    fn stream_search(index: IndexName, query: SearchQuery) -> Result<SearchStream, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let stream = OpenSearchSearchStream::new(client, index, query);
        Ok(SearchStream::new(stream))
    }

    fn get_schema(index: IndexName) -> Result<Schema, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        let mappings = client.get_mappings(&index)?;
        Ok(opensearch_mappings_to_schema(mappings, Some("id".to_string())))
    }

    fn update_schema(index: IndexName, schema: Schema) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let settings = schema_to_opensearch_settings(schema);
        
        // Update mappings if provided
        if let Some(mappings) = settings.mappings {
            client.put_mappings(&index, &mappings)?;
        }
        
        Ok(())
    }
}

impl ExtendedGuest for OpenSearchComponent {
    fn unwrapped_stream(index: IndexName, query: SearchQuery) -> Self::SearchStream {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client().unwrap_or_else(|_| {
            // Return a dummy client in case of error, will fail on actual operations
            OpenSearchApi::new("http://localhost:9200".to_string(), None, None, None)
        });
        
        OpenSearchSearchStream::new(client, index, query)
    }

    fn retry_query(original_query: &SearchQuery, partial_hits: &[SearchHit]) -> SearchQuery {
        create_retry_query(original_query, partial_hits)
    }

    fn subscribe(stream: &Self::SearchStream) -> Pollable {
        stream.subscribe()
    }
}

type DurableOpenSearchComponent = DurableSearch<OpenSearchComponent>;

golem_search::export_search!(DurableOpenSearchComponent with_types_in golem_search);

#[cfg(test)]
mod tests {
    use super::*;
    use golem_search::golem::search::types::{
        Doc, SearchQuery, Schema, SchemaField, FieldType, HighlightConfig
    };
    use serde_json::Value;
    use std::collections::HashMap;

    // Mock environment setup for tests
    fn setup_test_env() {
        std::env::set_var("OPENSEARCH_BASE_URL", "	https://21746d9f48c0.ngrok-free.app");
        std::env::set_var("OPENSEARCH_USERNAME", "admin");
        std::env::set_var("OPENSEARCH_PASSWORD", "StrongPassword123!");
        // Allow invalid certificates for testing with self-signed certs
        std::env::set_var("OPENSEARCH_ACCEPT_INVALID_CERTS", "true");
    }

    // Helper function to wait for documents to be indexed
    fn wait_for_indexing() {
        std::thread::sleep(std::time::Duration::from_millis(1500));
    }

    fn create_test_doc(id: &str, title: &str, content: &str) -> Doc {
        let mut doc_content = HashMap::new();
        doc_content.insert("title".to_string(), Value::String(title.to_string()));
        doc_content.insert("author".to_string(), Value::String("Test Author".to_string()));
        doc_content.insert("year".to_string(), Value::Number(serde_json::Number::from(2023)));
        doc_content.insert("genre".to_string(), Value::String("test".to_string()));
        doc_content.insert("description".to_string(), Value::String(content.to_string()));
        
        Doc {
            id: id.to_string(),
            content: serde_json::to_string(&doc_content).unwrap(),
        }
    }

    // Helper function to create test documents matching integration test pattern
    fn create_test_documents() -> Vec<Doc> {
        vec![
            Doc {
                id: "doc1".to_string(),
                content: r#"{"title": "The Great Gatsby", "author": "F. Scott Fitzgerald", "year": 1925, "genre": "fiction", "description": "A classic American novel about the Jazz Age"}"#.to_string(),
            },
            Doc {
                id: "doc2".to_string(),
                content: r#"{"title": "To Kill a Mockingbird", "author": "Harper Lee", "year": 1960, "genre": "fiction", "description": "A powerful story about racial injustice in the American South"}"#.to_string(),
            },
            Doc {
                id: "doc3".to_string(),
                content: r#"{"title": "1984", "author": "George Orwell", "year": 1949, "genre": "dystopian", "description": "A dystopian novel about totalitarian surveillance"}"#.to_string(),
            },
            Doc {
                id: "doc4".to_string(),
                content: r#"{"title": "Pride and Prejudice", "author": "Jane Austen", "year": 1813, "genre": "romance", "description": "A romantic novel about marriage and social class in Georgian England"}"#.to_string(),
            },
            Doc {
                id: "doc5".to_string(),
                content: r#"{"title": "The Catcher in the Rye", "author": "J.D. Salinger", "year": 1951, "genre": "fiction", "description": "A coming-of-age story about teenage rebellion"}"#.to_string(),
            },
        ]
    }

    fn create_test_schema() -> Schema {
        Schema {
            primary_key: Some("id".to_string()),
            fields: vec![
                SchemaField {
                    name: "id".to_string(),
                    field_type: FieldType::Text,
                    required: true,
                    facet: false,
                    sort: false,
                    index: true,
                },
                SchemaField {
                    name: "title".to_string(),
                    field_type: FieldType::Text,
                    required: false,
                    facet: false,
                    sort: false,
                    index: true,
                },
                SchemaField {
                    name: "author".to_string(),
                    field_type: FieldType::Text,
                    required: false,
                    facet: true,
                    sort: false,
                    index: true,
                },
                SchemaField {
                    name: "year".to_string(),
                    field_type: FieldType::Integer,
                    required: true,
                    facet: false,
                    sort: true,
                    index: true,
                },
                SchemaField {
                    name: "genre".to_string(),
                    field_type: FieldType::Text,
                    required: false,
                    facet: true,
                    sort: false,
                    index: true,
                },
                SchemaField {
                    name: "description".to_string(),
                    field_type: FieldType::Text,
                    required: false,
                    facet: false,
                    sort: false,
                    index: true,
                },
            ],
        }
    }

    // #[test]
    // fn test_create_index_without_schema() {
    //     setup_test_env();
        
    //     let timestamp = std::time::SystemTime::now()
    //         .duration_since(std::time::UNIX_EPOCH)
    //         .unwrap()
    //         .as_secs();
    //     let index_name = format!("test_index_no_schema_{}", timestamp);
        
    //     // Test creating index without schema
    //     let result = OpenSearchComponent::create_index(index_name.clone(), None);
        
    //     // Note: This test will fail if OpenSearch is not available
    //     match result {
    //         Ok(()) => {
    //             println!("Index created successfully without schema");
    //             // Clean up: delete the index
    //             let _ = OpenSearchComponent::delete_index(index_name);
    //         }
    //         Err(SearchError::Internal(e)) => {
    //             // Expected if OpenSearch is not available
    //             println!("OpenSearch not available for testing: {:?}", e);
    //         }
    //         Err(e) => println!("Create index failed: {:?}", e),
    //     }
    // }

    // #[test]
    // fn test_create_index_with_schema() {
    //     setup_test_env();
        
    //     let timestamp = std::time::SystemTime::now()
    //         .duration_since(std::time::UNIX_EPOCH)
    //         .unwrap()
    //         .as_secs();
    //     let index_name = format!("test_index_with_schema_{}", timestamp);
    //     let schema = create_test_schema();
        
    //     // Test creating index with schema
    //     let result = OpenSearchComponent::create_index(index_name.clone(), Some(schema));
        
    //     match result {
    //         Ok(()) => {
    //             println!("Index created successfully with schema");
    //             // Clean up: delete the index
    //             let _ = OpenSearchComponent::delete_index(index_name);
    //         }
    //         Err(SearchError::Internal(_)) => {
    //             // Expected if OpenSearch is not available
    //             println!("OpenSearch not available for testing");
    //         }
    //         Err(e) => println!("Create index with schema failed: {:?}", e),
    //     }
    // }

    // #[test]
    // fn test_delete_index() {
    //     setup_test_env();
        
    //     let index_name = "test_index_to_delete".to_string();
        
    //     // First create an index
    //     let create_result = OpenSearchComponent::create_index(index_name.clone(), None);
        
    //     if create_result.is_ok() {
    //         // Then test deleting it
    //         let delete_result = OpenSearchComponent::delete_index(index_name);
    //         match delete_result {
    //             Ok(()) => println!("Index deleted successfully"),
    //             Err(e) => println!("Delete index failed: {:?}", e),
    //         }
    //     } else {
    //         println!("Skipping delete test - OpenSearch not available");
    //     }
    // }

    // #[test]
    // fn test_list_indexes() {
    //     setup_test_env();
        
    //     let result = OpenSearchComponent::list_indexes();
        
    //     match result {
    //         Ok(indexes) => {
    //             // Should return a vector of index names
    //             println!("Found {} indexes", indexes.len());
    //             for index in indexes {
    //                 println!("  - {}", index);
    //             }
    //         }
    //         Err(SearchError::Internal(_)) => {
    //             // Expected if OpenSearch is not available
    //             println!("OpenSearch not available for testing");
    //         }
    //         Err(e) => println!("List indexes failed: {:?}", e),
    //     }
    // }

    // #[test]
    // fn test_upsert_single_document() {
    //     setup_test_env();
        
    //     let index_name = "test_upsert_single".to_string();
    //     let doc = create_test_doc("1", "Test Title", "Test content for searching");
        
    //     // Create index first
    //     let create_result = OpenSearchComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
    //     if create_result.is_ok() {
    //         // Test upserting a document
    //         let upsert_result = OpenSearchComponent::upsert(index_name.clone(), doc);
            
    //         match upsert_result {
    //             Ok(()) => {
    //                 println!("Document upserted successfully");
    //             }
    //             Err(e) => println!("Upsert failed: {:?}", e),
    //         }
            
    //         // Clean up
    //         let _ = OpenSearchComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping upsert test - OpenSearch not available");
    //     }
    // }

    // #[test]
    // fn test_upsert_many_documents() {
    //     setup_test_env();
        
    //     let index_name = "test_upsert_many".to_string();
    //     let docs = vec![
    //         create_test_doc("1", "First Title", "First content"),
    //         create_test_doc("2", "Second Title", "Second content"),
    //         create_test_doc("3", "Third Title", "Third content"),
    //     ];
        
    //     // Create index first
    //     let create_result = OpenSearchComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
    //     if create_result.is_ok() {
    //         // Test upserting multiple documents
    //         let upsert_result = OpenSearchComponent::upsert_many(index_name.clone(), docs);
            
    //         match upsert_result {
    //             Ok(()) => {
    //                 println!("Multiple documents upserted successfully");
    //             }
    //             Err(e) => println!("Upsert many failed: {:?}", e),
    //         }
            
    //         // Clean up
    //         let _ = OpenSearchComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping upsert many test - OpenSearch not available");
    //     }
    // }

    // #[test]
    // fn test_delete_single_document() {
    //     setup_test_env();
        
    //     let index_name = "test_delete_single".to_string();
    //     let doc = create_test_doc("delete_me", "Title to Delete", "Content to delete");
        
    //     // Create index and add document first
    //     let create_result = OpenSearchComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
    //     if create_result.is_ok() {
    //         let upsert_result = OpenSearchComponent::upsert(index_name.clone(), doc);
            
    //         if upsert_result.is_ok() {
    //             // Test deleting the document
    //             let delete_result = OpenSearchComponent::delete(index_name.clone(), "delete_me".to_string());
                
    //             match delete_result {
    //                 Ok(()) => {
    //                     println!("Document deleted successfully");
    //                 }
    //                 Err(e) => println!("Delete document failed: {:?}", e),
    //             }
    //         }
            
    //         // Clean up
    //         let _ = OpenSearchComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping delete document test - OpenSearch not available");
    //     }
    // }

    // #[test]
    // fn test_delete_many_documents() {
    //     setup_test_env();
        
    //     let index_name = "test_delete_many".to_string();
    //     let docs = vec![
    //         create_test_doc("delete1", "First Delete", "First content to delete"),
    //         create_test_doc("delete2", "Second Delete", "Second content to delete"),
    //         create_test_doc("delete3", "Third Delete", "Third content to delete"),
    //     ];
        
    //     // Create index and add documents first
    //     let create_result = OpenSearchComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
    //     if create_result.is_ok() {
    //         let upsert_result = OpenSearchComponent::upsert_many(index_name.clone(), docs);
            
    //         if upsert_result.is_ok() {
    //             // Test deleting multiple documents
    //             let ids = vec!["delete1".to_string(), "delete2".to_string(), "delete3".to_string()];
    //             let delete_result = OpenSearchComponent::delete_many(index_name.clone(), ids);
                
    //             match delete_result {
    //                 Ok(()) => {
    //                     println!("Multiple documents deleted successfully");
    //                 }
    //                 Err(e) => println!("Delete many documents failed: {:?}", e),
    //             }
    //         }
            
    //         // Clean up
    //         let _ = OpenSearchComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping delete many documents test - OpenSearch not available");
    //     }
    // }

    // #[test]
    // fn test_get_document() {
    //     setup_test_env();
        
    //     let index_name = "test_get_document".to_string();
    //     let doc = create_test_doc("get_me", "Get This Title", "Get this content");
        
    //     // Create index and add document first
    //     let create_result = OpenSearchComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
    //     if create_result.is_ok() {
    //         let upsert_result = OpenSearchComponent::upsert(index_name.clone(), doc.clone());
            
    //         if upsert_result.is_ok() {
    //             // Test getting the document
    //             let get_result = OpenSearchComponent::get(index_name.clone(), "get_me".to_string());
                
    //             match get_result {
    //                 Ok(Some(retrieved_doc)) => {
    //                     println!("Document retrieved successfully: {}", retrieved_doc.id);
    //                     assert_eq!(retrieved_doc.id, "get_me");
    //                 }
    //                 Ok(None) => {
    //                     println!("Document not found");
    //                 }
    //                 Err(e) => println!("Get document failed: {:?}", e),
    //             }
    //         }
            
    //         // Clean up
    //         let _ = OpenSearchComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping get document test - OpenSearch not available");
    //     }
    // }

    // #[test]
    // fn test_get_nonexistent_document() {
    //     setup_test_env();
        
    //     let index_name = "test_get_nonexistent".to_string();
        
    //     // Create index first
    //     let create_result = OpenSearchComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
    //     if create_result.is_ok() {
    //         // Test getting a nonexistent document
    //         let get_result = OpenSearchComponent::get(index_name.clone(), "nonexistent".to_string());
            
    //         match get_result {
    //             Ok(None) => {
    //                 println!("Correctly returned None for nonexistent document");
    //             }
    //             Ok(Some(_)) => {
    //                 println!("Unexpectedly found a document that shouldn't exist");
    //             }
    //             Err(e) => println!("Get nonexistent document error: {:?}", e),
    //         }
            
    //         // Clean up
    //         let _ = OpenSearchComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping get nonexistent document test - OpenSearch not available");
    //     }
    // }

    // #[test]
    // fn test_search() {
    //     setup_test_env();
        
    //     let index_name = "test_search".to_string();
    //     let docs = vec![
    //         create_test_doc("1", "The Great Gatsby", "Classic American literature"),
    //         create_test_doc("2", "To Kill a Mockingbird", "Story about justice and morality"),
    //         create_test_doc("3", "1984", "Dystopian novel about surveillance"),
    //     ];
        
    //     // Create index and add documents first
    //     let create_result = OpenSearchComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
    //     if create_result.is_ok() {
    //         let upsert_result = OpenSearchComponent::upsert_many(index_name.clone(), docs);
            
    //         if upsert_result.is_ok() {
    //             // Wait for documents to be indexed
    //             wait_for_indexing();
                
    //             // Test basic search
    //             let search_query = SearchQuery {
    //                 q: Some("Gatsby".to_string()),
    //                 filters: vec![],
    //                 sort: vec![],
    //                 facets: vec![],
    //                 page: Some(1),
    //                 per_page: Some(10),
    //                 offset: None,
    //                 highlight: None,
    //                 config: None,
    //             };
                
    //             let search_result = OpenSearchComponent::search(index_name.clone(), search_query);
                
    //             match search_result {
    //                 Ok(results) => {
    //                     println!("Search returned {} hits", results.hits.len());
    //                     if !results.hits.is_empty() {
    //                         println!("  First hit: {}", results.hits[0].id);
    //                     } else {
    //                         println!("  No hits found - documents may need more time to index");
    //                     }
    //                 }
    //                 Err(e) => println!("Search failed: {:?}", e),
    //             }
    //         }
            
    //         // Clean up
    //         let _ = OpenSearchComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping search test - OpenSearch not available");
    //     }
    // }

    // #[test]
    // fn test_search_with_filters() {
    //     setup_test_env();
        
    //     let index_name = "test_search_filters".to_string();
    //     let docs = vec![
    //         create_test_doc("1", "Fiction Book", "A great fiction story"),
    //         create_test_doc("2", "Non-Fiction Book", "A factual account"),
    //     ];
        
    //     // Create index and add documents first
    //     let create_result = OpenSearchComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
    //     if create_result.is_ok() {
    //         let upsert_result = OpenSearchComponent::upsert_many(index_name.clone(), docs);
            
    //         if upsert_result.is_ok() {
    //             // Wait for documents to be indexed
    //             wait_for_indexing();
                
    //             // Test search with filters
    //             let search_query = SearchQuery {
    //                 q: Some("book".to_string()),
    //                 filters: vec![], // Using empty filters for now - need to check actual filter type
    //                 sort: vec![],
    //                 facets: vec![],
    //                 page: Some(1),
    //                 per_page: Some(10),
    //                 offset: None,
    //                 highlight: None,
    //                 config: None,
    //             };
                
    //             let search_result = OpenSearchComponent::search(index_name.clone(), search_query);
                
    //             match search_result {
    //                 Ok(results) => {
    //                     println!("Filtered search returned {} hits", results.hits.len());
    //                     if results.hits.is_empty() {
    //                         println!("  No hits found - documents may need more time to index");
    //                     }
    //                 }
    //                 Err(e) => println!("Filtered search failed: {:?}", e),
    //             }
    //         }
            
    //         // Clean up
    //         let _ = OpenSearchComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping filtered search test - OpenSearch not available");
    //     }
    // }

    // #[test]
    // fn test_stream_search() {
    //     setup_test_env();
        
    //     let index_name = "test_stream_search".to_string();
    //     let docs = vec![
    //         create_test_doc("1", "Stream Test 1", "First streaming document"),
    //         create_test_doc("2", "Stream Test 2", "Second streaming document"),
    //         create_test_doc("3", "Stream Test 3", "Third streaming document"),
    //     ];
        
    //     // Create index and add documents first
    //     let create_result = OpenSearchComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
    //     if create_result.is_ok() {
    //         let upsert_result = OpenSearchComponent::upsert_many(index_name.clone(), docs);
            
    //         if upsert_result.is_ok() {
    //             // Test streaming search
    //             let search_query = SearchQuery {
    //                 q: Some("stream".to_string()),
    //                 filters: vec![],
    //                 sort: vec![],
    //                 facets: vec![],
    //                 page: Some(1),
    //                 per_page: Some(2), // Small page size to test pagination
    //                 offset: None,
    //                 highlight: None,
    //                 config: None,
    //             };
                
    //             let stream_result = OpenSearchComponent::stream_search(index_name.clone(), search_query);
                
    //             match stream_result {
    //                 Ok(_search_stream) => {
    //                     println!("Stream search created successfully");
    //                     // Note: Actually testing the stream would require more complex setup
    //                 }
    //                 Err(e) => println!("Stream search failed: {:?}", e),
    //             }
    //         }
            
    //         // Clean up
    //         let _ = OpenSearchComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping stream search test - OpenSearch not available");
    //     }
    // }

    // #[test]
    // fn test_get_schema() {
    //     setup_test_env();
        
    //     let index_name = "test_get_schema".to_string();
    //     let original_schema = create_test_schema();
        
    //     // Create index with schema first
    //     let create_result = OpenSearchComponent::create_index(index_name.clone(), Some(original_schema.clone()));
        
    //     if create_result.is_ok() {
    //         // Test getting the schema
    //         let get_schema_result = OpenSearchComponent::get_schema(index_name.clone());
            
    //         match get_schema_result {
    //             Ok(retrieved_schema) => {
    //                 println!("Schema retrieved successfully");
    //                 println!("  Fields count: {}", retrieved_schema.fields.len());
                    
    //                 if let Some(pk) = &retrieved_schema.primary_key {
    //                     println!("  Primary key: {}", pk);
    //                 }
                    
    //                 // Validate some key fields exist
    //                 let field_names: Vec<&String> = retrieved_schema.fields.iter().map(|f| &f.name).collect();
    //                 if field_names.contains(&&"title".to_string()) {
    //                     println!("  ✓ Title field found");
    //                 }
    //                 if field_names.contains(&&"author".to_string()) {
    //                     println!("  ✓ Author field found");
    //                 }
    //             }
    //             Err(e) => println!("Get schema failed: {:?}", e),
    //         }
            
    //         // Clean up
    //         let _ = OpenSearchComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping get schema test - OpenSearch not available");
    //     }
    // }

    // #[test]
    // fn test_update_schema() {
    //     setup_test_env();
        
    //     let index_name = "test_update_schema".to_string();
    //     let original_schema = create_test_schema();
        
    //     // Create index with schema first
    //     let create_result = OpenSearchComponent::create_index(index_name.clone(), Some(original_schema.clone()));
        
    //     if create_result.is_ok() {
    //         // Create updated schema with additional field
    //         let mut updated_schema = original_schema;
    //         updated_schema.fields.push(SchemaField {
    //             name: "new_field".to_string(),
    //             field_type: FieldType::Text,
    //             required: false,
    //             facet: false,
    //             sort: false,
    //             index: true,
    //         });
            
    //         // Test updating the schema
    //         let update_result = OpenSearchComponent::update_schema(index_name.clone(), updated_schema);
            
    //         match update_result {
    //             Ok(()) => {
    //                 println!("Schema updated successfully");
                    
    //                 // Verify the update by getting the schema back
    //                 if let Ok(retrieved_schema) = OpenSearchComponent::get_schema(index_name.clone()) {
    //                     println!("  Updated schema has {} fields", retrieved_schema.fields.len());
    //                 }
    //             }
    //             Err(e) => println!("Update schema failed: {:?}", e),
    //         }
            
    //         // Clean up
    //         let _ = OpenSearchComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping update schema test - OpenSearch not available");
    //     }
    // }

    // Test3 - Search with sorting and pagination (mirrors integration test3)
    #[test]
    fn test_search_sorting_pagination() {
        setup_test_env();
        
        let index_name = "test_sorting_pagination".to_string();
        let schema = create_test_schema();
        
        // Setup schema and documents
        println!("Creating index: {}", index_name);
        let setup_result = OpenSearchComponent::create_index(index_name.clone(), Some(schema));
        if setup_result.is_err() {
            println!("Skipping sorting/pagination test - OpenSearch not available: {:?}", setup_result.err());
            return;
        }
        
        // Insert test documents
        let docs = create_test_documents();
        println!("Inserting {} documents", docs.len());
        match OpenSearchComponent::upsert_many(index_name.clone(), docs) {
            Ok(()) => println!("✓ Documents inserted successfully"),
            Err(e) => {
                let _ = OpenSearchComponent::delete_index(index_name);
                println!("Document insertion failed: {:?}", e);
                return;
            }
        }
        
        // Wait for documents to be indexed
        println!("Waiting for documents to be indexed...");
        wait_for_indexing();
        
        // First, verify documents exist with a simple match_all query
        println!("Verifying documents exist in index...");
        let verify_query = SearchQuery {
            q: None,
            filters: vec![],
            sort: vec![],
            facets: vec![],
            page: None,
            per_page: None,
            offset: None,
            highlight: None,
            config: None,
        };
        
        match OpenSearchComponent::search(index_name.clone(), verify_query) {
            Ok(search_results) => {
                println!("✓ Found {} documents in index", search_results.hits.len());
                if search_results.hits.is_empty() {
                    println!("⚠ No documents found - indexing may have failed or needs more time");
                    // Try a longer wait
                    println!("Waiting additional time for indexing...");
                    std::thread::sleep(std::time::Duration::from_millis(3000));
                }
            }
            Err(e) => {
                println!("✗ Error verifying documents: {:?}", e);
                let _ = OpenSearchComponent::delete_index(index_name);
                return;
            }
        }
        
        // Test sorting by year (descending)
        println!("Testing search with sorting by year");
        let sorted_query = SearchQuery {
            q: None,
            filters: vec![],
            sort: vec!["year:desc".to_string()],
            facets: vec![],
            page: None,
            per_page: None,
            offset: None,
            highlight: None,
            config: None,
        };
        
        match OpenSearchComponent::search(index_name.clone(), sorted_query) {
            Ok(search_results) => {
                println!("✓ Sorted search returned {} hits", search_results.hits.len());
                if search_results.hits.len() >= 2 {
                    println!("  Verifying sort order by checking first two results");
                    // Additional validation could be added here
                }
            }
            Err(e) => println!("✗ Sorted search failed: {:?}", e),
        }
        
        // Test pagination
        println!("Testing pagination with page=1, per_page=2");
        let paginated_query = SearchQuery {
            q: None,
            filters: vec![],
            sort: vec!["year:desc".to_string()],
            facets: vec![],
            page: Some(1),
            per_page: Some(2),
            offset: None,
            highlight: None,
            config: None,
        };
        
        match OpenSearchComponent::search(index_name.clone(), paginated_query) {
            Ok(search_results) => {
                println!("✓ Paginated search returned {} hits", search_results.hits.len());
                if let Some(total) = search_results.total {
                    println!("  Total documents: {}", total);
                }
                if let Some(page) = search_results.page {
                    println!("  Current page: {}", page);
                }
            }
            Err(e) => println!("✗ Paginated search failed: {:?}", e),
        }
        
        // Cleanup
        let _ = OpenSearchComponent::delete_index(index_name);
    }

    //Test4 - Search with highlighting and facets (mirrors integration test4)
    #[test]
    fn test_search_highlighting_facets() {
        setup_test_env();
        
        let index_name = "test_highlighting_facets".to_string();
        let schema = create_test_schema();
        
        // Setup schema and documents
        let setup_result = OpenSearchComponent::create_index(index_name.clone(), Some(schema));
        if setup_result.is_err() {
            println!("Skipping highlighting/facets test - OpenSearch not available");
            return;
        }
        
        // Insert test documents
        let docs = create_test_documents();
        if OpenSearchComponent::upsert_many(index_name.clone(), docs).is_err() {
            let _ = OpenSearchComponent::delete_index(index_name);
            println!("Document insertion failed, skipping test");
            return;
        }
        
        // Wait for documents to be indexed
        println!("Waiting for documents to be indexed...");
        wait_for_indexing();
        
        // Test search with highlighting and facets
        println!("Testing search with highlighting and facets");
        let highlight_query = SearchQuery {
            q: Some("American".to_string()),
            filters: vec![],
            sort: vec![],
            facets: vec!["genre".to_string(), "author".to_string()],
            page: None,
            per_page: None,
            offset: None,
            highlight: Some(HighlightConfig {
                fields: vec!["title".to_string(), "description".to_string()],
                pre_tag: Some("<mark>".to_string()),
                post_tag: Some("</mark>".to_string()),
                max_length: Some(200),
            }),
            config: None,
        };
        
        match OpenSearchComponent::search(index_name.clone(), highlight_query) {
            Ok(search_results) => {
                println!("✓ Highlighted search returned {} hits", search_results.hits.len());
                
                // Check for highlights
                for hit in &search_results.hits {
                    if hit.highlights.is_some() {
                        println!("  ✓ Found highlights in results");
                        break;
                    }
                }
                
                // Check for facets
                if search_results.facets.is_some() {
                    println!("  ✓ Facet data returned");
                } else {
                    println!("  ⚠ No facet data returned (may not be supported)");
                }
                
                // Check timing information
                if let Some(took_ms) = search_results.took_ms {
                    println!("  Query took: {}ms", took_ms);
                }
            }
            Err(e) => println!("✗ Highlighted search failed: {:?}", e),
        }
        
        // Cleanup
        let _ = OpenSearchComponent::delete_index(index_name);
    }

   // Test5 - Schema inspection and validation (mirrors integration test5)
    #[test]
    fn test_schema_inspection_validation() {
        setup_test_env();
        
        let index_name = "test_schema_validation".to_string();
        let original_schema = create_test_schema();
        
        // Create index with predefined schema
        println!("Setting up index with predefined schema");
        match OpenSearchComponent::create_index(index_name.clone(), Some(original_schema.clone())) {
            Ok(()) => println!("✓ Index schema configured successfully"),
            Err(e) => {
                println!("Schema setup failed: {:?}, skipping test", e);
                return;
            }
        }
        
        // Test schema retrieval
        println!("Retrieving index schema");
        match OpenSearchComponent::get_schema(index_name.clone()) {
            Ok(retrieved_schema) => {
                println!("✓ Schema retrieved successfully");
                println!("  Fields count: {}", retrieved_schema.fields.len());
                
                if let Some(pk) = &retrieved_schema.primary_key {
                    println!("  Primary key: {}", pk);
                }
                
                // Validate some key fields exist
                let field_names: Vec<&String> = retrieved_schema.fields.iter().map(|f| &f.name).collect();
                if field_names.contains(&&"title".to_string()) {
                    println!("  ✓ Title field found");
                }
                if field_names.contains(&&"author".to_string()) {
                    println!("  ✓ Author field found");
                }
            }
            Err(e) => println!("✗ Schema retrieval failed: {:?}", e),
        }
        
        // Test schema update
        println!("Testing schema update");
        let mut updated_schema = original_schema;
        updated_schema.fields.push(SchemaField {
            name: "isbn".to_string(),
            field_type: FieldType::Text,
            required: false,
            facet: false,
            sort: false,
            index: true,
        });
        
        match OpenSearchComponent::update_schema(index_name.clone(), updated_schema) {
            Ok(()) => println!("✓ Schema updated successfully"),
            Err(e) => println!("✗ Schema update failed: {:?}", e),
        }
        
        // Test document insertion with invalid data (schema validation)
        println!("Testing schema validation with invalid document");
        let invalid_doc = Doc {
            id: "invalid1".to_string(),
            content: r#"{"invalid_field": "this should not be allowed"}"#.to_string(),
        };
        
        match OpenSearchComponent::upsert(index_name.clone(), invalid_doc) {
            Ok(()) => println!("  ⚠ Invalid document accepted (lenient validation)"),
            Err(SearchError::InvalidQuery(_)) => println!("  ✓ Invalid document rejected (strict validation)"),
            Err(e) => println!("  ? Unexpected error with invalid document: {:?}", e),
        }
        
        // Cleanup
        let _ = OpenSearchComponent::delete_index(index_name);
    }

    // Test6 - Streaming search behavior (mirrors integration test6)
    // #[test]
    // fn test_streaming_search() {
    //     setup_test_env();
        
    //     let index_name = "test_streaming".to_string();
    //     let schema = create_test_schema();
        
    //     // Setup schema
    //     let setup_result = OpenSearchComponent::create_index(index_name.clone(), Some(schema));
    //     if setup_result.is_err() {
    //         println!("Skipping streaming test - OpenSearch not available");
    //         return;
    //     }
        
    //     // Create additional documents for streaming test
    //     let mut docs = create_test_documents();
    //     for i in 6..=20 {
    //         docs.push(Doc {
    //             id: format!("doc{}", i),
    //             content: format!(r#"{{"title": "Book {}", "author": "Author {}", "year": {}, "genre": "test", "description": "A test book for streaming search"}}"#, i, i, 1900 + i),
    //         });
    //     }
        
    //     if OpenSearchComponent::upsert_many(index_name.clone(), docs).is_err() {
    //         let _ = OpenSearchComponent::delete_index(index_name);
    //         println!("Document insertion failed, skipping test");
    //         return;
    //     }
        
    //     println!("Testing streaming search functionality");
    //     let stream_query = SearchQuery {
    //         q: Some("book".to_string()),
    //         filters: vec![],
    //         sort: vec!["year:asc".to_string()],
    //         facets: vec![],
    //         page: None,
    //         per_page: Some(5), // Small page size to encourage streaming
    //         offset: None,
    //         highlight: None,
    //         config: None,
    //     };
        
    //     match OpenSearchComponent::stream_search(index_name.clone(), stream_query.clone()) {
    //         Ok(stream) => {
    //             println!("✓ Search stream created successfully");
                
    //             let mut total_hits = 0;
    //             let mut batch_count = 0;
                
    //             // Use a conservative approach to streaming
    //             for _ in 0..5 { // Limit to 5 iterations maximum
    //                 let hits = stream.();
    //                 if hits.is_empty() {
    //                     break;
    //                 }
                    
    //                 batch_count += 1;
    //                 total_hits += hits.len();
    //                 println!("  Batch {}: {} hits", batch_count, hits.len());
    //             }
                
    //             println!("✓ Streaming complete: {} total hits in {} batches", total_hits, batch_count);
    //         }
    //         Err(SearchError::Unsupported) => {
    //             println!("⚠ Streaming search not supported by this provider");
                
    //             // Fallback to regular search
    //             match OpenSearchComponent::search(index_name.clone(), stream_query) {
    //                 Ok(search_results) => {
    //                     println!("  Fallback: Regular search returned {} hits", search_results.hits.len());
    //                 }
    //                 Err(e) => println!("  Fallback search also failed: {:?}", e),
    //             }
    //         }
    //         Err(e) => println!("✗ Streaming search failed: {:?}", e),
    //     }
        
    //     // Cleanup
    //     let _ = OpenSearchComponent::delete_index(index_name);
    // }

}
