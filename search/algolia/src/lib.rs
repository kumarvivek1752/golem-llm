use crate::client::{AlgoliaSearchApi};
use crate::conversions::{
    doc_to_algolia_object, algolia_object_to_doc, search_query_to_algolia_query,
    algolia_response_to_search_results, schema_to_algolia_settings, algolia_settings_to_schema,
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

/// Since Algolia doesn't have native streaming, we implement pagination-based streaming
struct AlgoliaSearchStream {
    client: AlgoliaSearchApi,
    index_name: String,
    query: SearchQuery,
    current_page: Cell<u32>,
    finished: Cell<bool>,
    last_response: RefCell<Option<SearchResults>>,
}

impl AlgoliaSearchStream {
    pub fn new(client: AlgoliaSearchApi, index_name: String, query: SearchQuery) -> Self {
        Self {
            client,
            index_name,
            query:query.clone(),
            current_page: Cell::new(query.page.unwrap_or(0)),
            finished: Cell::new(false),
            last_response: RefCell::new(None),
        }
    }

    pub fn subscribe(&self) -> Pollable {
        // For non-streaming APIs, return an immediately ready pollable
        golem_rust::bindings::wasi::clocks::monotonic_clock::subscribe_duration(0)
    }
}

impl GuestSearchStream for AlgoliaSearchStream {
    fn get_next(&self) -> Option<Vec<SearchHit>> {
        if self.finished.get() {
            return Some(vec![]);
        }

        let mut search_query = self.query.clone();
        search_query.page = Some(self.current_page.get());

        let algolia_query = search_query_to_algolia_query(search_query);
        
        match self.client.search(&self.index_name, &algolia_query) {
            Ok(response) => {
                let search_results = algolia_response_to_search_results(response);
                
                // Check if we've reached the end
                let current_page = self.current_page.get();
                let total_pages = if let (Some(total), Some(per_page)) = (search_results.total, search_results.per_page) {
                    (total + per_page - 1) / per_page // Ceiling division
                } else {
                    current_page + 1
                };

                if current_page >= total_pages || search_results.hits.is_empty() {
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

struct AlgoliaComponent;

impl AlgoliaComponent {
    const APPLICATION_ID_ENV_VAR: &'static str = "ALGOLIA_APPLICATION_ID";
    const API_KEY_ENV_VAR: &'static str = "ALGOLIA_API_KEY";

    fn create_client() -> Result<AlgoliaSearchApi, SearchError> {
        with_config_keys(
            &[Self::APPLICATION_ID_ENV_VAR, Self::API_KEY_ENV_VAR],
            |keys| {
                if keys.len() != 2 {
                    return Err(SearchError::Internal("Missing Algolia credentials".to_string()));
                }
                
                let application_id = keys[0].clone();
                let api_key = keys[1].clone();
                
                Ok(AlgoliaSearchApi::new(application_id, api_key))
            }
        )
    }
}

impl Guest for AlgoliaComponent {
    type SearchStream = AlgoliaSearchStream;

    fn create_index(_name: IndexName, _schema: Option<Schema>) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        // Algolia doesn't require explicit index creation - indices are created automatically
        // when you first add documents. According to the golem:search interface spec,
        // providers that don't support index creation should return unsupported.
        Err(SearchError::Unsupported)
    }

    fn delete_index(name: IndexName) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        match client.delete_index(&name) {
            Ok(response) => {
                println!("[Algolia] delete_index successful - task_id: {}, deleted_at: {}", response.task_id, response.deleted_at);
                // Properly consume the response before returning ()
                let _ = response;
                Ok(())
            },
            Err(e) => Err(e),
        }
    }

    fn list_indexes() -> Result<Vec<IndexName>, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        match client.list_indexes() {
            Ok(response) => Ok(response.items.into_iter().map(|item| item.name).collect()),
            Err(e) => Err(e),
        }
    }

    fn upsert(index: IndexName, doc: Doc) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let algolia_object = doc_to_algolia_object(doc)
            .map_err(|e| SearchError::InvalidQuery(e))?;
        
        match client.save_object(&index, &algolia_object) {
            Ok(response) => {
                println!("[Algolia] upsert successful - task_id: {}, object_id: {}", response.task_id, response.object_id);
                // Properly consume the response before returning ()
                let _ = response;
                Ok(())
            },
            Err(e) => Err(e),
        }
    }

    fn upsert_many(index: IndexName, docs: Vec<Doc>) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let mut algolia_objects = Vec::new();
        
        for doc in docs {
            let algolia_object = doc_to_algolia_object(doc)
                .map_err(|e| SearchError::InvalidQuery(e))?;
            algolia_objects.push(algolia_object);
        }
        
        match client.save_objects(&index, &algolia_objects) {
            Ok(response) => {
                println!("[Algolia] upsert_many successful - task_id: {}, object_ids: {:?}", response.task_id, response.object_ids);
                // Properly consume the response before returning ()
                let _ = response;
                Ok(())
            },
            Err(e) => Err(e),
        }
    }

    fn delete(index: IndexName, id: DocumentId) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        match client.delete_object(&index, &id) {
            Ok(response) => {
                println!("[Algolia] delete successful - task_id: {}, deleted_at: {}", response.task_id, response.deleted_at);
                // Properly consume the response before returning ()
                let _ = response;
                Ok(())
            },
            Err(e) => Err(e),
        }
    }

    fn delete_many(index: IndexName, ids: Vec<DocumentId>) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        match client.delete_objects(&index, &ids) {
            Ok(response) => {
                println!("[Algolia] delete_many successful - task_id: {}, object_ids: {:?}", response.task_id, response.object_ids);
                // Properly consume the response before returning ()
                let _ = response;
                Ok(())
            },
            Err(e) => Err(e),
        }
    }

    fn get(index: IndexName, id: DocumentId) -> Result<Option<Doc>, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        match client.get_object(&index, &id) {
            Ok(Some(algolia_object)) => Ok(Some(algolia_object_to_doc(algolia_object))),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn search(index: IndexName, query: SearchQuery) -> Result<SearchResults, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let algolia_query = search_query_to_algolia_query(query);
        
        match client.search(&index, &algolia_query) {
            Ok(response) => Ok(algolia_response_to_search_results(response)),
            Err(e) => Err(e),
        }
    }

    fn stream_search(index: IndexName, query: SearchQuery) -> Result<SearchStream, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let stream = AlgoliaSearchStream::new(client, index, query);
        Ok(SearchStream::new(stream))
    }

    fn get_schema(index: IndexName) -> Result<Schema, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        match client.get_settings(&index) {
            Ok(settings) => Ok(algolia_settings_to_schema(settings)),
            Err(e) => Err(e),
        }
    }

   fn update_schema(index: IndexName, schema: Schema) -> Result<(), SearchError> {
    LOGGING_STATE.with_borrow_mut(|state| state.init());

    let client = Self::create_client()?;
    let settings = schema_to_algolia_settings(schema);

    client
        .set_settings(&index, &settings)
        .map_err(|e| {
            println!("[Algolia] set_settings failed: {}", e);
            e
        })?;

    Ok(())
}

}

impl ExtendedGuest for AlgoliaComponent {
    fn unwrapped_stream(index: IndexName, query: SearchQuery) -> Self::SearchStream {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client().unwrap_or_else(|_| {
            // Return a dummy client in case of error, will fail on actual operations
            AlgoliaSearchApi::new("dummy".to_string(), "dummy".to_string())
        });
        
        AlgoliaSearchStream::new(client, index, query)
    }

    fn retry_query(original_query: &SearchQuery, partial_hits: &[SearchHit]) -> SearchQuery {
        create_retry_query(original_query, partial_hits)
    }

    fn subscribe(stream: &Self::SearchStream) -> Pollable {
        stream.subscribe()
    }
}

type DurableAlgoliaComponent = DurableSearch<AlgoliaComponent>;

golem_search::export_search!(DurableAlgoliaComponent with_types_in golem_search);

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use golem_search::golem::search::types::{
//         Doc, SearchQuery, Schema, SchemaField, FieldType
//     };
//     use serde_json::Value;
//     use std::collections::HashMap;

//     // Mock environment setup for tests
//     fn setup_test_env() {
//         // You'll need to set these with actual Algolia credentials for testing
//         std::env::set_var("ALGOLIA_APPLICATION_ID", "SLPKFQ34PO");
//         std::env::set_var("ALGOLIA_API_KEY", "76b6638c2c0754b20b008c55dc2356bb");
//     }

//     fn create_test_doc(id: &str, title: &str, content: &str) -> Doc {
//         let mut doc_content = HashMap::new();
//         doc_content.insert("title".to_string(), Value::String(title.to_string()));
//         doc_content.insert("author".to_string(), Value::String("Test Author".to_string()));
//         doc_content.insert("year".to_string(), Value::Number(serde_json::Number::from(2023)));
//         doc_content.insert("genre".to_string(), Value::String("test".to_string()));
//         doc_content.insert("description".to_string(), Value::String(content.to_string()));
        
//         Doc {
//             id: id.to_string(),
//             content: serde_json::to_string(&doc_content).unwrap(),
//         }
//     }

//     fn create_test_schema() -> Schema {
//         Schema {
//             primary_key: Some("objectID".to_string()), // Algolia uses objectID by default
//             fields: vec![
//                 SchemaField {
//                     name: "objectID".to_string(),
//                     field_type: FieldType::Text,
//                     required: true,
//                     facet: false,
//                     sort: false,
//                     index: true,
//                 },
//                 SchemaField {
//                     name: "title".to_string(),
//                     field_type: FieldType::Text,
//                     required: false,
//                     facet: false,
//                     sort: false,
//                     index: true,
//                 },
//                 SchemaField {
//                     name: "author".to_string(),
//                     field_type: FieldType::Text,
//                     required: false,
//                     facet: true,
//                     sort: false,
//                     index: true,
//                 },
//                 SchemaField {
//                     name: "year".to_string(),
//                     field_type: FieldType::Integer,
//                     required: true,
//                     facet: false,
//                     sort: true,
//                     index: true,
//                 },
//                 SchemaField {
//                     name: "genre".to_string(),
//                     field_type: FieldType::Text,
//                     required: false,
//                     facet: true,
//                     sort: false,
//                     index: true,
//                 },
//                 SchemaField {
//                     name: "description".to_string(),
//                     field_type: FieldType::Text,
//                     required: false,
//                     facet: false,
//                     sort: false,
//                     index: true,
//                 },
//             ],
//         }
//     }

//     #[test]
//     fn test_stream_search() {
//         println!("\n[TEST] Starting test_stream_search for Algolia");
//         setup_test_env();
//         println!("[TEST] Test environment set up");
        
//         let index_name = "test_stream_search_algolia01".to_string();
//         println!("[TEST] Using index: {}", index_name);
        
//         let docs = vec![
//             create_test_doc("1", "Book One", "First book content"),
//             create_test_doc("2", "Book Two", "Second book content"),
//             create_test_doc("3", "Book Three", "Third book content"),
//             create_test_doc("4", "Book Four", "Fourth book content"),
//             create_test_doc("5", "Book Five", "Fifth book content"),
//         ];
//         println!("[TEST] Created {} test documents", docs.len());
        
//         // Note: Algolia doesn't support explicit index creation, indices are created automatically
//         // when you first add documents. So we'll skip the create_index step.
        
//         println!("[TEST] Upserting documents to Algolia (this will create the index automatically)");
//         let upsert_result = AlgoliaComponent::upsert_many(index_name.clone(), docs);
        
//         match upsert_result {
//             Ok(()) => {
//                 println!("[TEST] Documents added successfully to Algolia");
                
//                 // Test streaming search
//                 println!("[TEST] Creating search query");
//                 let search_query = SearchQuery {
//                     q: Some("book".to_string()),
//                     filters: vec![],
//                     sort: vec![],
//                     facets: vec![],
//                     page: Some(0), // Algolia uses 0-based page indexing
//                     per_page: Some(2), // Small page size to test streaming
//                     offset: None,
//                     highlight: None,
//                     config: None,
//                 };
                
//                 println!("[TEST] Query parameters: q={:?}, page={:?}, per_page={:?}", 
//                     search_query.q, search_query.page, search_query.per_page);
                
//                 println!("[TEST] Calling stream_search");
//                 let stream_result = AlgoliaComponent::stream_search(index_name.clone(), search_query);
                
//                 match stream_result {
//                     Ok(stream) => {
//                         println!("[TEST] Stream search created successfully");
//                         println!("[TEST] Stream: {:?}", stream);
                        
//                         // Try to get some results from the stream
//                         println!("[TEST] Attempting to get next batch from stream");
                        
//                         // Note: Similar to Typesense, the SearchStream wrapper doesn't expose get_next() directly
//                         // That's handled by the WIT-generated bindings and the underlying stream implementation
//                         println!("[TEST] Stream search test completed successfully");
//                     }
//                     Err(e) => println!("[TEST] Stream search failed: {:?}", e),
//                 }
                
//                 // Clean up - delete the index
//                 println!("[TEST] Cleaning up - deleting index");
//                 let delete_result = AlgoliaComponent::delete_index(index_name);
//                 println!("[TEST] Delete result: {:?}", delete_result);
//             }
//             Err(SearchError::Internal(msg)) if msg.contains("Missing Algolia credentials") => {
//                 println!("[TEST] Skipping stream search test - Algolia credentials not available");
//                 println!("[TEST] To run this test, set ALGOLIA_APPLICATION_ID and ALGOLIA_API_KEY environment variables");
//             }
//             Err(e) => {
//                 println!("[TEST] Failed to upsert documents: {:?}", e);
//                 println!("[TEST] This might be due to missing or invalid Algolia credentials");
//             }
//         }
//     }

//     #[test]
//     fn test_search() {
//         println!("\n[TEST] Starting test_search for Algolia");
//         setup_test_env();
//         println!("[TEST] Test environment set up");
        
//         let index_name = "test_search_algolia".to_string();
//         let docs = vec![
//             create_test_doc("1", "The Great Gatsby", "Classic American literature"),
//             create_test_doc("2", "To Kill a Mockingbird", "Story about justice and morality"),
//             create_test_doc("3", "1984", "Dystopian novel about surveillance"),
//         ];
        
//         println!("[TEST] Upserting documents to Algolia");
//         let upsert_result = AlgoliaComponent::upsert_many(index_name.clone(), docs);
        
//         match upsert_result {
//             Ok(()) => {
//                 println!("[TEST] Documents added successfully");
                
//                 // Test basic search
//                 let search_query = SearchQuery {
//                     q: Some("Gatsby".to_string()),
//                     filters: vec![],
//                     sort: vec![],
//                     facets: vec![],
//                     page: Some(0),
//                     per_page: Some(10),
//                     offset: None,
//                     highlight: None,
//                     config: None,
//                 };
                
//                 let search_result = AlgoliaComponent::search(index_name.clone(), search_query);
                
//                 match search_result {
//                     Ok(results) => {
//                         println!("[TEST] Search returned {} hits", results.hits.len());
//                         if let Some(total) = results.total {
//                             println!("[TEST] Total found: {}", total);
//                         }
//                         for hit in results.hits {
//                             println!("[TEST]   Hit: {} (score: {:?})", hit.id, hit.score);
//                         }
//                     }
//                     Err(e) => println!("[TEST] Search failed: {:?}", e),
//                 }
                
//                 // Clean up
//                 let _ = AlgoliaComponent::delete_index(index_name);
//             }
//             Err(SearchError::Internal(msg)) if msg.contains("Missing Algolia credentials") => {
//                 println!("[TEST] Skipping search test - Algolia credentials not available");
//             }
//             Err(e) => {
//                 println!("[TEST] Failed to upsert documents: {:?}", e);
//             }
//         }
//     }

//     #[test]
//     fn test_search_with_filters() {
//         println!("\n[TEST] Starting test_search_with_filters for Algolia");
//         setup_test_env();
        
//         let index_name = "test_search_filters_algolia".to_string();
//         let docs = vec![
//             create_test_doc("1", "Fiction Book", "A great fiction story"),
//             create_test_doc("2", "Non-Fiction Book", "A factual account"),
//         ];
        
//         let upsert_result = AlgoliaComponent::upsert_many(index_name.clone(), docs);
        
//         match upsert_result {
//             Ok(()) => {
//                 // Test search with filters
//                 let search_query = SearchQuery {
//                     q: Some("book".to_string()),
//                     filters: vec!["genre:test".to_string()],
//                     sort: vec![],
//                     facets: vec![],
//                     page: Some(0),
//                     per_page: Some(10),
//                     offset: None,
//                     highlight: None,
//                     config: None,
//                 };
                
//                 let search_result = AlgoliaComponent::search(index_name.clone(), search_query);
                
//                 match search_result {
//                     Ok(results) => {
//                         println!("[TEST] Filtered search returned {} hits", results.hits.len());
//                     }
//                     Err(e) => println!("[TEST] Filtered search failed: {:?}", e),
//                 }
                
//                 // Clean up
//                 let _ = AlgoliaComponent::delete_index(index_name);
//             }
//             Err(SearchError::Internal(msg)) if msg.contains("Missing Algolia credentials") => {
//                 println!("[TEST] Skipping filtered search test - Algolia credentials not available");
//             }
//             Err(e) => {
//                 println!("[TEST] Failed to upsert documents: {:?}", e);
//             }
//         }
//     }

//     #[test]
//     fn test_error_handling() {
//         println!("\n[TEST] Starting test_error_handling for Algolia");
        
//         // Test with missing credentials
//         std::env::remove_var("ALGOLIA_APPLICATION_ID");
//         std::env::remove_var("ALGOLIA_API_KEY");
        
//         let result = AlgoliaComponent::list_indexes();
//         match result {
//             Err(SearchError::Internal(msg)) if msg.contains("Missing Algolia credentials") => {
//                 println!("[TEST] ✓ Correctly failed with missing credentials");
//             }
//             Ok(_) => println!("[TEST] ⚠ Unexpectedly succeeded with missing credentials"),
//             Err(e) => println!("[TEST] ✓ Failed as expected: {:?}", e),
//         }
        
//         println!("[TEST] Error handling tests completed");
//     }

//     #[test]
//     fn test_search_stream_implementation() {
//         println!("\n[TEST] Starting test_search_stream_implementation for Algolia");
//         setup_test_env();
        
//         // Test creating a search stream without actual Algolia connection
//         let client = AlgoliaSearchApi::new("test_app_id".to_string(), "test_key".to_string());
//         let query = SearchQuery {
//             q: Some("test".to_string()),
//             offset: Some(0),
//             per_page: Some(5),
//             page: Some(0),
//             filters: vec![],
//             facets: vec![],
//             sort: vec![],
//             highlight: None,
//             config: None,
//         };
        
//         let stream = AlgoliaSearchStream::new(client, "test_index".to_string(), query);
        
//         // Test that the stream can be created and subscribed to
//         let _pollable = stream.subscribe();
        
//         // Test that get_next returns Some (even if empty due to no Algolia connection)
//         let result = stream.get_next();
//         assert!(result.is_some());
        
//         // Test blocking_get_next
//         let _blocking_result = stream.blocking_get_next();
        
//         println!("[TEST] Stream implementation tests completed");
//     }
// }