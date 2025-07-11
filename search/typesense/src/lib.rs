use crate::client::{TypesenseSearchApi, CollectionSchema, CollectionField};
use crate::conversions::*;
use golem_search::golem::search::core::{Guest, SearchStream, GuestSearchStream};
use golem_search::golem::search::types::{
    Doc, SearchQuery, SearchResults, SearchHit, Schema, SearchError, IndexName, DocumentId
};
use golem_search::config::with_config_keys;
use golem_search::durability::{DurableSearch, ExtendedGuest};
use golem_search::LOGGING_STATE;
use golem_rust::wasm_rpc::Pollable;
use std::cell::{RefCell, Cell};

mod client;
mod conversions;

/// Simple search stream implementation for Typesense
/// Since Typesense doesn't have native streaming, we implement pagination-based streaming
struct TypesenseSearchStream {
    client: TypesenseSearchApi,
    index_name: String,
    query: SearchQuery,
    current_page: Cell<u32>,
    finished: Cell<bool>,
    last_response: RefCell<Option<SearchResults>>,
}

impl TypesenseSearchStream {
    fn new(client: TypesenseSearchApi, index_name: String, query: SearchQuery) -> Self {
        println!("[DEBUG] Creating TypesenseSearchStream for index: {}", index_name);
        println!("[DEBUG] Query details: {:?}", query);
        
        Self {
            client,
            index_name,
            query: query.clone(),
            current_page: Cell::new(query.page.unwrap_or(1)),
            finished: Cell::new(false),
            last_response: RefCell::new(None),
        }
    }

    fn subscribe(&self) -> Pollable {
        println!("[DEBUG] subscribe() called on TypesenseSearchStream");
        // For non-streaming APIs, return an immediately ready pollable
        golem_rust::bindings::wasi::clocks::monotonic_clock::subscribe_duration(0)
    }
}

/// Component implementation for Typesense
struct TypesenseComponent;

impl TypesenseComponent {
    const API_KEY_ENV_VAR: &'static str = "TYPESENSE_API_KEY";
    const BASE_URL_ENV_VAR: &'static str = "TYPESENSE_BASE_URL";

    fn create_client() -> Result<TypesenseSearchApi, SearchError> {
        println!("[DEBUG] Creating TypesenseSearchApi client");
        
        with_config_keys(
            &[Self::API_KEY_ENV_VAR, Self::BASE_URL_ENV_VAR],
            |keys| {
                println!("[DEBUG] Got config keys, length: {}", keys.len());
                
                if keys.len() != 2 {
                    println!("[DEBUG] Missing Typesense credentials: expected 2 keys, got {}", keys.len());
                    return Err(SearchError::Internal("Missing Typesense credentials".to_string()));
                }
                
                let api_key = keys[0].clone();
                let base_url = keys[1].clone();
                
                println!("[DEBUG] Creating client with base URL: {}", base_url);
                println!("[DEBUG] API key length: {}", api_key.len());
                
                Ok(TypesenseSearchApi::new(api_key, base_url))
            }
        )
    }
}

impl GuestSearchStream for TypesenseSearchStream {
    fn get_next(&self) -> Option<Vec<SearchHit>> {
        println!("[DEBUG] get_next() called on TypesenseSearchStream");
        
        if self.finished.get() {
            println!("[DEBUG] Stream already finished, returning empty result");
            return Some(vec![]);
        }

        println!("[DEBUG] Current page: {}", self.current_page.get());
        
        // Prepare query for current page
        let mut search_query = self.query.clone();
        search_query.page = Some(self.current_page.get());
        
        println!("[DEBUG] Converting search query to Typesense query");
        let typesense_query = search_query_to_typesense_query(search_query);
        println!("[DEBUG] Typesense query: {:?}", typesense_query);
        
        println!("[DEBUG] Executing search request to Typesense");
        match self.client.search(&self.index_name, &typesense_query) {
            Ok(response) => {
                println!("[DEBUG] Search response received successfully");
                let search_results = typesense_response_to_search_results(response);
                println!("[DEBUG] Parsed search results: total={:?}, hits={}", 
                    search_results.total, search_results.hits.len());
                
                // Check if we've reached the end
                let current_page = self.current_page.get();
                let per_page = self.query.per_page.unwrap_or(20);
                let total_pages = if let Some(total) = search_results.total {
                    println!("[DEBUG] Total results: {}", total);
                    (total + per_page - 1) / per_page // Ceiling division
                } else {
                    println!("[DEBUG] No total provided, assuming more pages exist");
                    current_page + 1
                };
                
                println!("[DEBUG] Current page: {}, Total pages: {}", current_page, total_pages);

                if current_page >= total_pages || search_results.hits.is_empty() {
                    println!("[DEBUG] Reached end of results, marking stream as finished");
                    self.finished.set(true);
                }

                // Prepare for next page
                self.current_page.set(current_page + 1);
                println!("[DEBUG] Next page set to: {}", self.current_page.get());
                
                let hits = search_results.hits.clone();
                *self.last_response.borrow_mut() = Some(search_results);
                
                println!("[DEBUG] Returning {} hits", hits.len());
                Some(hits)
            },
            Err(e) => {
                println!("[DEBUG] Search request failed: {:?}", e);
                self.finished.set(true);
                Some(vec![])
            }
        }
    }

    fn blocking_get_next(&self) -> Vec<SearchHit> {
        println!("[DEBUG] blocking_get_next() called on TypesenseSearchStream");
        let result = self.get_next().unwrap_or_default();
        println!("[DEBUG] blocking_get_next returning {} hits", result.len());
        result
    }
}

impl Guest for TypesenseComponent {
    type SearchStream = TypesenseSearchStream;

    fn create_index(name: IndexName, schema: Option<Schema>) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        let typesense_schema = schema
            .map(|s| schema_to_typesense_schema(s, &name))
            .unwrap_or_else(|| {
                // Create a basic schema if none provided
                CollectionSchema {
                    name: name.clone(),
                    fields: vec![
                        CollectionField {
                            name: "id".to_string(),
                            field_type: "string".to_string(),
                            facet: Some(false),
                            index: Some(true),
                            sort: Some(false),
                            optional: Some(false),
                        }
                    ],
                    default_sorting_field: None,
                    enable_nested_fields: None,
                    token_separators: None,
                    symbols_to_index: None,
                }
            });
        
        client.create_collection(&name, &typesense_schema)?;
        Ok(())
    }

    fn delete_index(name: IndexName) -> Result<(), SearchError> {
        let client = Self::create_client()?;
        client.delete_collection(&name)?;
        Ok(())
    }

    fn list_indexes() -> Result<Vec<IndexName>, SearchError> {
        let client = Self::create_client()?;
        let response = client.list_collections()?;
        Ok(response.0.into_iter().map(|collection| collection.name).collect())
    }

    fn upsert(index: IndexName, doc: Doc) -> Result<(), SearchError> {
        let client = Self::create_client()?;
        let typesense_doc = doc_to_typesense_document(doc)
            .map_err(|e| SearchError::Internal(e))?;
        client.upsert_document(&index, &typesense_doc)?;
        Ok(())
    }

    fn upsert_many(index: IndexName, docs: Vec<Doc>) -> Result<(), SearchError> {
        let client = Self::create_client()?;
        let typesense_docs: Result<Vec<_>, _> = docs.iter()
            .map(|doc| doc_to_typesense_document(doc.clone()))
            .collect();
        let typesense_docs = typesense_docs
            .map_err(|e| SearchError::Internal(e))?;
        client.index_documents(&index, &typesense_docs)?;
        Ok(())
    }

    fn delete(index: IndexName, id: DocumentId) -> Result<(), SearchError> {
        let client = Self::create_client()?;
        client.delete_document(&index, &id)?;
        Ok(())
    }

    fn delete_many(index: IndexName, ids: Vec<DocumentId>) -> Result<(), SearchError> {
        let client = Self::create_client()?;
        // Typesense doesn't have bulk delete by IDs, so we use filter_by
        let filter = format!("id:[{}]", ids.join(","));
        client.delete_documents_by_query(&index, &filter)?;
        Ok(())
    }

    fn get(index: IndexName, id: DocumentId) -> Result<Option<Doc>, SearchError> {
        let client = Self::create_client()?;
        
        // Typesense doesn't have a direct get document endpoint
        // We need to search for the specific document by ID using a filter-only search
        let query = SearchQuery {
            q: Some("*".to_string()), // Match all documents
            filters: vec![format!("id:={}", id)], // Then filter by exact ID match
            sort: vec![],
            facets: vec![],
            page: Some(1),
            per_page: Some(1),
            offset: None,
            highlight: None,
            config: None,
        };
        
        let typesense_query = search_query_to_typesense_query(query);
        let response = client.search(&index, &typesense_query)?;
        let results = typesense_response_to_search_results(response);
        
        Ok(results.hits.into_iter().next().map(|hit| Doc {
            id: hit.id,
            content: hit.content.unwrap_or_else(|| "{}".to_string()),
        }))
    }

    fn search(index: IndexName, query: SearchQuery) -> Result<SearchResults, SearchError> {
        let client = Self::create_client()?;
        let typesense_query = search_query_to_typesense_query(query);
        let response = client.search(&index, &typesense_query)?;
        Ok(typesense_response_to_search_results(response))
    }

    fn stream_search(index: IndexName, query: SearchQuery) -> Result<SearchStream, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        println!("[DEBUG] stream_search called for index: {}", index);
        println!("[DEBUG] query: {:?}", query);

        let client = Self::create_client()?;
        println!("[DEBUG] Client created successfully");
        
        let stream = TypesenseSearchStream::new(client, index, query);
        println!("[DEBUG] TypesenseSearchStream created successfully");
        
        // Debug the stream object before passing to SearchStream::new
        println!("[DEBUG] Stream object details:");
        println!("[DEBUG] - index_name: {}", stream.index_name);
        println!("[DEBUG] - current_page: {:?}", stream.current_page.get());
        println!("[DEBUG] - finished: {:?}", stream.finished.get());
        println!("[DEBUG] - query: {:?}", stream.query);
        println!("[DEBUG] - last_response: {:?}", stream.last_response.borrow());
        //println!("[DEBUG] - client is None: {:?}", stream.client);

        println!("[DEBUG] About to create SearchStream wrapper");
        
        // Try-catch equivalent in Rust - let's see if we can capture more details
        let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            SearchStream::new(stream)
        })) {
            Ok(stream) => {
                println!("[DEBUG] SearchStream wrapper created successfully");
                stream
            },
            Err(panic_info) => {
                println!("[DEBUG] PANIC caught during SearchStream::new()!");
                if let Some(s) = panic_info.downcast_ref::<&str>() {
                    println!("[DEBUG] Panic message: {}", s);
                } else if let Some(s) = panic_info.downcast_ref::<String>() {
                    println!("[DEBUG] Panic message: {}", s);
                } else {
                    println!("[DEBUG] Panic message: <unknown>");
                }
                
                // Re-panic to maintain original behavior
                std::panic::resume_unwind(panic_info);
            }
        };
        
        println!("[DEBUG] SearchStream wrapper created: {:?}", result);
        Ok(result)
    }

    fn get_schema(index: IndexName) -> Result<Schema, SearchError> {
        let client = Self::create_client()?;
        
        // Typesense doesn't have a direct get schema endpoint for collections
        // We need to get the collection info from the list
        let collections = client.list_collections()?;
        
        let collection = collections.0.into_iter()
            .find(|c| c.name == index)
            .ok_or(SearchError::IndexNotFound)?;
        
        let schema = Schema {
            fields: collection.fields.into_iter().map(collection_field_to_schema_field).collect(),
            primary_key: collection.default_sorting_field,
        };
        
        Ok(schema)
    }

    fn update_schema(index: IndexName, schema: Schema) -> Result<(), SearchError> {
        // Typesense doesn't support updating schema after collection creation
        // We need to delete and recreate the collection
        let client = Self::create_client()?;
        
        // First check if collection exists
        let collections = client.list_collections()?;
        let exists = collections.0.iter().any(|c| c.name == index);
        
        if exists {
            // Delete existing collection
            client.delete_collection(&index)?;
        }
        
        // Create new collection with updated schema
        let typesense_schema = schema_to_typesense_schema(schema, &index);
        client.create_collection(&index, &typesense_schema)?;
        
        Ok(())
    }
}



impl ExtendedGuest for TypesenseComponent {
    fn unwrapped_stream(index: IndexName, query: SearchQuery) -> Self::SearchStream {
        println!("[DEBUG] ExtendedGuest::unwrapped_stream called for index: {}", index);
        println!("[DEBUG] ExtendedGuest::unwrapped_stream query: {:?}", query);
        LOGGING_STATE.with_borrow_mut(|state| state.init());
        
        println!("[DEBUG] Creating client for unwrapped_stream");
        let client = Self::create_client().unwrap_or_else(|e| {
            println!("[DEBUG] Error creating client: {:?}", e);
            // Return a dummy client in case of error, will fail on actual operations
            TypesenseSearchApi::new("dummy".to_string(), "http://localhost:8108".to_string())
        });
        
        println!("[DEBUG] Client created for unwrapped_stream");
        
        // Use a simplified query to improve stability
        println!("[DEBUG] Creating simplified query from original: {:?}", query);
        let simplified_query = SearchQuery {
            q: query.q,
            filters: query.filters,
            sort: query.sort,
            facets: query.facets,
            page: Some(1), // Always start with page 1
            per_page: query.per_page.or(Some(20)), // Ensure we have a per_page value
            offset: None, // Don't use offset for streaming
            highlight: None, // Skip highlighting for streaming
            config: query.config,
        };
        println!("[DEBUG] Simplified query: {:?}", simplified_query);
        
        println!("[DEBUG] Creating TypesenseSearchStream for unwrapped_stream");
        let stream = TypesenseSearchStream::new(client, index, simplified_query);
        println!("[DEBUG] TypesenseSearchStream created successfully for unwrapped_stream");
        
        // Debug the stream object before returning
        println!("[DEBUG] Unwrapped stream object details:");
        println!("[DEBUG] - index_name: {}", stream.index_name);
        println!("[DEBUG] - current_page: {:?}", stream.current_page.get());
        println!("[DEBUG] - finished: {:?}", stream.finished.get());
        println!("[DEBUG] - query: {:?}", stream.query);
        
        stream
    }

    fn retry_query(original_query: &SearchQuery, partial_hits: &[SearchHit]) -> SearchQuery {
        println!("[DEBUG] retry_query called with {} partial hits", partial_hits.len());
        
        let mut retry_query = original_query.clone();
        
        // If we have partial results, we might want to exclude already seen document IDs
        // or adjust pagination to continue from where we left off
        if !partial_hits.is_empty() {
            // Adjust offset to skip already received hits
            let current_offset = original_query.offset.unwrap_or(0);
            let received_count = partial_hits.len() as u32;
            retry_query.offset = Some(current_offset + received_count);
            println!("[DEBUG] Adjusted offset to {} (was {})", retry_query.offset.unwrap(), current_offset);
        } else {
            println!("[DEBUG] No partial hits, query unchanged");
        }
        
        retry_query
    }

    fn subscribe(stream: &Self::SearchStream) -> Pollable {
        println!("[DEBUG] ExtendedGuest::subscribe called");
        let result = stream.subscribe();
        println!("[DEBUG] Subscribe returned a pollable");
        result
    }
}

// Use non-durable version for testing
// type DurableTypesenseComponent = DurableSearch<TypesenseComponent>;
// golem_search::export_search!(DurableTypesenseComponent with_types_in golem_search);

// Export the component directly without durability wrapper
golem_search::export_search!(TypesenseComponent with_types_in golem_search);

#[cfg(test)]
mod tests {
    use super::*;
    use golem_search::golem::search::types::{
        Doc, SearchQuery, Schema, SchemaField, FieldType
    };
    use serde_json::Value;
    use std::collections::HashMap;

    // Mock environment setup for tests
    fn setup_test_env() {
        std::env::set_var("TYPESENSE_API_KEY", "cwDa4QdDMhyX6gYyYZBLSFDxBedHqfBm");
        std::env::set_var("TYPESENSE_BASE_URL", "https://tw3v692qmzapneo7p-1.a1.typesense.net");
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
    //     let result = TypesenseComponent::create_index(index_name.clone(), None);
        
    //     // Note: This test will fail if Typesense is not available
    //     match result {
    //         Ok(()) => {
    //             println!("Index created successfully without schema");
    //             // Clean up: delete the index
    //             let _ = TypesenseComponent::delete_index(index_name);
    //         }
    //         Err(SearchError::Internal(_)) => {
    //             // Expected if Typesense is not available
    //             println!("Typesense not available for testing");
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
    //     let result = TypesenseComponent::create_index(index_name.clone(), Some(schema));
        
    //     match result {
    //         Ok(()) => {
    //             println!("Index created successfully with schema");
    //             // Clean up: delete the index
    //             let _ = TypesenseComponent::delete_index(index_name);
    //         }
    //         Err(SearchError::Internal(_)) => {
    //             // Expected if Typesense is not available
    //             println!("Typesense not available for testing");
    //         }
    //         Err(e) => println!("Create index with schema failed: {:?}", e),
    //     }
    // }

    // #[test]
    // fn test_delete_index() {
    //     setup_test_env();
        
    //     let index_name = "test_index_to_delete3".to_string();
        
    //     // First create an index
    //     let create_result = TypesenseComponent::create_index(index_name.clone(), None);
        
    //     if create_result.is_ok() {
    //         // Then test deleting it
    //         let delete_result = TypesenseComponent::delete_index(index_name);
    //         match delete_result {
    //             Ok(()) => println!("Index deleted successfully"),
    //             Err(e) => println!("Delete index failed: {:?}", e),
    //         }
    //     } else {
    //         println!("Skipping delete test - Typesense not available");
    //     }
    // }

    // #[test]
    // fn test_list_indexes() {
    //     setup_test_env();
        
    //     let result = TypesenseComponent::list_indexes();
        
    //     match result {
    //         Ok(indexes) => {
    //             // Should return a vector of index names (collections)
    //             println!("Found {} collections", indexes.len());
    //             for index in indexes {
    //                 println!("  - {}", index);
    //             }
    //         }
    //         Err(SearchError::Internal(_)) => {
    //             // Expected if Typesense is not available
    //             println!("Typesense not available for testing");
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
    //     let create_result = TypesenseComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
    //     if create_result.is_ok() {
    //         // Test upserting a document
    //         let upsert_result = TypesenseComponent::upsert(index_name.clone(), doc);
            
    //         match upsert_result {
    //             Ok(()) => {
    //                 println!("Document upserted successfully");
    //             }
    //             Err(e) => println!("Upsert failed: {:?}", e),
    //         }
            
    //         // Clean up
    //         let _ = TypesenseComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping upsert test - Typesense not available");
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
    //     let create_result = TypesenseComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
    //     if create_result.is_ok() {
    //         // Test upserting multiple documents
    //         let upsert_result = TypesenseComponent::upsert_many(index_name.clone(), docs);
            
    //         match upsert_result {
    //             Ok(()) => {
    //                 println!("Multiple documents upserted successfully");
    //             }
    //             Err(e) => println!("Upsert many failed: {:?}", e),
    //         }
            
    //         // Clean up
    //         let _ = TypesenseComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping upsert many test - Typesense not available");
    //     }
    // }

    // #[test]
    // fn test_delete_single_document() {
    //     setup_test_env();
        
    //     let index_name = "test_delete_single".to_string();
    //     let doc = create_test_doc("delete_me", "Title to Delete", "Content to delete");
        
    //     // Create index and add document first
    //     let create_result = TypesenseComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
    //     if create_result.is_ok() {
    //         let upsert_result = TypesenseComponent::upsert(index_name.clone(), doc);
            
    //         if upsert_result.is_ok() {
    //             // Test deleting the document
    //             let delete_result = TypesenseComponent::delete(index_name.clone(), "delete_me".to_string());
                
    //             match delete_result {
    //                 Ok(()) => {
    //                     println!("Document deleted successfully");
    //                 }
    //                 Err(e) => println!("Delete document failed: {:?}", e),
    //             }
    //         }
            
    //         // Clean up
    //         let _ = TypesenseComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping delete document test - Typesense not available");
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
    //     let create_result = TypesenseComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
    //     if create_result.is_ok() {
    //         let upsert_result = TypesenseComponent::upsert_many(index_name.clone(), docs);
            
    //         if upsert_result.is_ok() {
    //             // Test deleting multiple documents
    //             let ids = vec!["delete1".to_string(), "delete2".to_string(), "delete3".to_string()];
    //             let delete_result = TypesenseComponent::delete_many(index_name.clone(), ids);
                
    //             match delete_result {
    //                 Ok(()) => {
    //                     println!("Multiple documents deleted successfully");
    //                 }
    //                 Err(e) => println!("Delete many documents failed: {:?}", e),
    //             }
    //         }
            
    //         // Clean up
    //         let _ = TypesenseComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping delete many documents test - Typesense not available");
    //     }
    // }

    // #[test]
    // fn test_get_document() {
    //     setup_test_env();
        
    //     let index_name = "test_get_document3".to_string();
    //     let doc = create_test_doc("get_me", "Get This Title", "Get this content");
        
    //     // Create index and add document first
    //     let create_result = TypesenseComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
    //     if create_result.is_ok() {
    //         let upsert_result = TypesenseComponent::upsert(index_name.clone(), doc.clone());
            
    //         if upsert_result.is_ok() {
    //             // Test getting the document
    //             let get_result = TypesenseComponent::get(index_name.clone(), "get_me".to_string());
                
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
    //         let _ = TypesenseComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping get document test - Typesense not available");
    //     }
    // }

    // #[test]
    // fn test_get_nonexistent_document() {
    //     setup_test_env();
        
    //     let index_name = "test_get_nonexistent".to_string();
        
    //     // Create index first
    //     let create_result = TypesenseComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
    //     if create_result.is_ok() {
    //         // Test getting a nonexistent document
    //         let get_result = TypesenseComponent::get(index_name.clone(), "nonexistent".to_string());
            
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
    //         let _ = TypesenseComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping get nonexistent document test - Typesense not available");
    //     }
    // }

    #[test]
    fn test_search() {
        setup_test_env();
        
        let index_name = "test_search".to_string();
        let docs = vec![
            create_test_doc("1", "The Great Gatsby", "Classic American literature"),
            create_test_doc("2", "To Kill a Mockingbird", "Story about justice and morality"),
            create_test_doc("3", "1984", "Dystopian novel about surveillance"),
        ];
        
        // Create index and add documents first
        let create_result = TypesenseComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
        if create_result.is_ok() {
            let upsert_result = TypesenseComponent::upsert_many(index_name.clone(), docs);
            
            if upsert_result.is_ok() {
                // Test basic search
                let search_query = SearchQuery {
                    q: Some("Gatsby".to_string()),
                    filters: vec![],
                    sort: vec![],
                    facets: vec![],
                    page: Some(1),
                    per_page: Some(10),
                    offset: None,
                    highlight: None,
                    config: None,
                };
                
                let search_result = TypesenseComponent::search(index_name.clone(), search_query);
                
                match search_result {
                    Ok(results) => {
                        println!("Search returned {} hits", results.hits.len());
                        if let Some(total) = results.total {
                            println!("Total found: {}", total);
                        }
                        for hit in results.hits {
                            println!("  Hit: {} (score: {:?})", hit.id, hit.score);
                        }
                    }
                    Err(e) => println!("Search failed: {:?}", e),
                }
            }
            
            // Clean up
            let _ = TypesenseComponent::delete_index(index_name);
        } else {
            println!("Skipping search test - Typesense not available");
        }
    }

    #[test]
    fn test_search_with_filters() {
        setup_test_env();
        
        let index_name = "test_search_filters".to_string();
        let docs = vec![
            create_test_doc("1", "Fiction Book", "A great fiction story"),
            create_test_doc("2", "Non-Fiction Book", "A factual account"),
        ];
        
        // Create index and add documents first
        let create_result = TypesenseComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
        if create_result.is_ok() {
            let upsert_result = TypesenseComponent::upsert_many(index_name.clone(), docs);
            
            if upsert_result.is_ok() {
                // Test search with filters
                let search_query = SearchQuery {
                    q: Some("book".to_string()),
                    filters: vec!["genre:test".to_string()],
                    sort: vec![],
                    facets: vec![],
                    page: Some(1),
                    per_page: Some(10),
                    offset: None,
                    highlight: None,
                    config: None,
                };
                
                let search_result = TypesenseComponent::search(index_name.clone(), search_query);
                
                match search_result {
                    Ok(results) => {
                        println!("Filtered search returned {} hits", results.hits.len());
                    }
                    Err(e) => println!("Filtered search failed: {:?}", e),
                }
            }
            
            // Clean up
            let _ = TypesenseComponent::delete_index(index_name);
        } else {
            println!("Skipping filtered search test - Typesense not available");
        }
    }

    // #[test]
    // fn test_search_with_highlighting() {
    //     setup_test_env();
        
    //     let index_name = "test_search_highlight".to_string();
    //     let docs = vec![
    //         create_test_doc("1", "The Great American Novel", "An amazing story about America"),
    //     ];
        
    //     // Create index and add documents first
    //     let create_result = TypesenseComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
    //     if create_result.is_ok() {
    //         let upsert_result = TypesenseComponent::upsert_many(index_name.clone(), docs);
            
    //         if upsert_result.is_ok() {
    //             // Test search with highlighting
    //             let search_query = SearchQuery {
    //                 q: Some("American".to_string()),
    //                 filters: vec![],
    //                 sort: vec![],
    //                 facets: vec![],
    //                 page: Some(1),
    //                 per_page: Some(10),
    //                 offset: None,
    //                 highlight: Some(HighlightConfig {
    //                     fields: vec!["title".to_string(), "description".to_string()],
    //                     pre_tag: Some("<mark>".to_string()),
    //                     post_tag: Some("</mark>".to_string()),
    //                     max_length: Some(200),
    //                 }),
    //                 config: None,
    //             };
                
    //             let search_result = TypesenseComponent::search(index_name.clone(), search_query);
                
    //             match search_result {
    //                 Ok(results) => {
    //                     println!("Highlighted search returned {} hits", results.hits.len());
    //                     for hit in results.hits {
    //                         if let Some(highlights) = hit.highlights {
    //                             println!("  Highlights: {}", highlights);
    //                         }
    //                     }
    //                 }
    //                 Err(e) => println!("Highlighted search failed: {:?}", e),
    //             }
    //         }
            
    //         // Clean up
    //         let _ = TypesenseComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping highlighted search test - Typesense not available");
    //     }
    // }

    #[test]
    fn test_stream_search() {
        println!("\n[TEST] Starting test_stream_search");
        setup_test_env();
        println!("[TEST] Test environment set up");
        
        let index_name = "test_stream_search39".to_string();
        println!("[TEST] Using index: {}", index_name);
        
        let docs = vec![
            create_test_doc("1", "Book One", "First book content"),
            create_test_doc("2", "Book Two", "Second book content"),
            create_test_doc("3", "Book Three", "Third book content"),
            create_test_doc("4", "Book Four", "Fourth book content"),
            create_test_doc("5", "Book Five", "Fifth book content"),
        ];
        println!("[TEST] Created {} test documents", docs.len());
        
        // Create index and add documents first
        println!("[TEST] Creating index with schema");
        let create_result = TypesenseComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
        if create_result.is_ok() {
            println!("[TEST] Index created successfully");
            
            println!("[TEST] Upserting documents");
            let upsert_result = TypesenseComponent::upsert_many(index_name.clone(), docs);
            
            if upsert_result.is_ok() {
                println!("[TEST] Documents added successfully");
                
                // Test streaming search
                println!("[TEST] Creating search query");
                let search_query = SearchQuery {
                    q: Some("book".to_string()),
                    filters: vec![],
                    sort: vec![],
                    facets: vec![],
                    page: Some(1),
                    per_page: Some(2), // Small page size to test streaming
                    offset: None,
                    highlight: None,
                    config: None,
                };
                
                println!("[TEST] Query parameters: q={:?}, page={:?}, per_page={:?}", 
                    search_query.q, search_query.page, search_query.per_page);
                
                println!("[TEST] Calling stream_search");
                let stream_result = TypesenseComponent::stream_search(index_name.clone(), search_query);
                
                match stream_result {
                    Ok(stream) => {
                        println!("[TEST] Stream search created successfully");
                        println!("[TEST] Stream: {:?}", stream);
                        
                        // Try to get some results from the stream
                        println!("[TEST] Attempting to get next batch from stream");
                        
                        // Note: the core issue is here - we're not actually accessing the stream
                        // The SearchStream wrapper doesn't expose get_next() directly - that's handled
                        // by the WIT-generated bindings and the underlying stream implementation
                    }
                    Err(e) => println!("[TEST] Stream search failed: {:?}", e),
                }
            } else {
                println!("[TEST] Failed to upsert documents: {:?}", upsert_result);
            }
            
            // Clean up
            println!("[TEST] Cleaning up - deleting index");
            let delete_result = TypesenseComponent::delete_index(index_name);
            println!("[TEST] Delete result: {:?}", delete_result);
        } else {
            println!("[TEST] Skipping stream search test - index creation failed: {:?}", create_result);
        }
    }

    // #[test]
    // fn test_get_schema() {
    //     setup_test_env();
        
    //     let index_name = "test_get_schema1".to_string();
    //     let original_schema = create_test_schema();
        
    //     // Create index with schema first
    //     let create_result = TypesenseComponent::create_index(index_name.clone(), Some(original_schema.clone()));
        
    //     if create_result.is_ok() {
    //         // Test getting the schema
    //         let get_schema_result = TypesenseComponent::get_schema(index_name.clone());
            
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
    //         let _ = TypesenseComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping get schema test - Typesense not available");
    //     }
    // }

    // #[test]
    // fn test_update_schema() {
    //     setup_test_env();
        
    //     let index_name = "test_update_schema1".to_string();
    //     let initial_schema = create_test_schema();
        
    //     // Create index with initial schema
    //     let create_result = TypesenseComponent::create_index(index_name.clone(), Some(initial_schema));
        
    //     if create_result.is_ok() {
    //         // Create an updated schema with additional field
    //         let mut updated_schema = create_test_schema();
    //         updated_schema.fields.push(SchemaField {
    //             name: "isbn".to_string(),
    //             field_type: FieldType::Text,
    //             required: false,
    //             facet: false,
    //             sort: false,
    //             index: true,
    //         });
            
    //         // Test updating the schema
    //         let update_result = TypesenseComponent::update_schema(index_name.clone(), updated_schema);
            
    //         match update_result {
    //             Ok(()) => {
    //                 println!("Schema updated successfully");
                    
    //                 // Verify the update by getting the schema back
    //                 let get_schema_result = TypesenseComponent::get_schema(index_name.clone());
                    
    //                 match get_schema_result {
    //                     Ok(retrieved_schema) => {
    //                         println!("Updated schema retrieved: {} fields", retrieved_schema.fields.len());
    //                         // Check if the new field exists
    //                         let field_names: Vec<&String> = retrieved_schema.fields.iter().map(|f| &f.name).collect();
    //                         if field_names.contains(&&"isbn".to_string()) {
    //                             println!("  ✓ New ISBN field found in updated schema");
    //                         } else {
    //                             println!("  ✗ New ISBN field NOT found in updated schema");
    //                         }
    //                     }
    //                     Err(e) => println!("Failed to retrieve updated schema: {:?}", e),
    //                 }
    //             }
    //             Err(e) => println!("Update schema failed: {:?}", e),
    //         }
            
    //         // Clean up
    //         let _ = TypesenseComponent::delete_index(index_name);
    //     } else {
    //         println!("Skipping update schema test - Typesense not available");
    //     }
    // }

    // #[test]
    // fn test_error_handling() {
    //     // Test with invalid API key
    //     std::env::set_var("TYPESENSE_API_KEY", "invalid_key");
    //     std::env::set_var("TYPESENSE_BASE_URL", "https://tw3v692qmzapneo7p-1.a1.typesense.net");
        
    //     let result = TypesenseComponent::create_index("test".to_string(), None);
    //     match result {
    //         Err(_) => println!("✓ Correctly failed with invalid API key"),
    //         Ok(_) => println!("⚠ Unexpectedly succeeded with invalid API key"),
    //     }
        
    //     // Test with invalid base URL
    //     std::env::set_var("TYPESENSE_API_KEY", "cwDa4QdDMhyX6gYyYZBLSFDxBedHqfBm");
    //     std::env::set_var("TYPESENSE_BASE_URL", "invalid_url");
        
    //     let result = TypesenseComponent::create_index("test".to_string(), None);
    //     match result {
    //         Err(_) => println!("✓ Correctly failed with invalid URL"),
    //         Ok(_) => println!("⚠ Unexpectedly succeeded with invalid URL"),
    //     }
        
    //     // Test with missing API key
    //     std::env::remove_var("TYPESENSE_API_KEY");
    //     std::env::remove_var("TYPESENSE_BASE_URL");
        
    //     let result = TypesenseComponent::create_index("test".to_string(), None);
    //     match result {
    //         Err(_) => println!("✓ Correctly failed with missing credentials"),
    //         Ok(_) => println!("⚠ Unexpectedly succeeded with missing credentials"),
    //     }
        
    //     println!("Error handling tests completed");
    // }

    // #[test]
    // fn test_search_stream_implementation() {
    //     setup_test_env();
        
    //     // Test creating a search stream without actual Typesense connection
    //     let client = TypesenseSearchApi::new(
    //         "test_key".to_string(),
    //         "http://localhost:8108".to_string()
    //     );
    //     let query = SearchQuery {
    //         q: Some("test".to_string()),
    //         offset: Some(0),
    //         per_page: Some(5),
    //         page: Some(1),
    //         filters: vec![],
    //         facets: vec![],
    //         sort: vec![],
    //         highlight: None,
    //         config: None,
    //     };
        
    //     let stream = TypesenseSearchStream::new(client, "test_index".to_string(), query);
        
    //     // Test that the stream can be created and subscribed to
    //     let _pollable = stream.subscribe();
        
    //     // Test that get_next returns Some (even if empty due to no Typesense connection)
    //     let result = stream.get_next();
    //     assert!(result.is_some() || result.is_none()); // Either is valid for error conditions
        
    //     // Test blocking_get_next
    //     let _blocking_result = stream.blocking_get_next();
        
    //     println!("Stream implementation tests completed");
    // }
}
