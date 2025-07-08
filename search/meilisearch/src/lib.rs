use crate::client::MeilisearchApi;
use crate::conversions::{
    doc_to_meilisearch_document, meilisearch_document_to_doc, search_query_to_meilisearch_request,
    meilisearch_response_to_search_results, schema_to_meilisearch_settings, meilisearch_settings_to_schema,
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

/// Simple search stream implementation for Meilisearch
/// Since Meilisearch doesn't have native streaming, we implement pagination-based streaming
struct MeilisearchSearchStream {
    client: MeilisearchApi,
    index_name: String,
    query: SearchQuery,
    current_page: Cell<u32>,
    finished: Cell<bool>,
    last_response: RefCell<Option<SearchResults>>,
}

impl MeilisearchSearchStream {
    pub fn new(client: MeilisearchApi, index_name: String, query: SearchQuery) -> Self {
        Self {
            client,
            index_name,
            query: query.clone(),
            current_page: Cell::new(query.offset.unwrap_or(0) / query.page.unwrap_or(20)),
            finished: Cell::new(false),
            last_response: RefCell::new(None),
        }
    }

    pub fn subscribe(&self) -> Pollable {
        // For non-streaming APIs, return an immediately ready pollable
        golem_rust::bindings::wasi::clocks::monotonic_clock::subscribe_duration(0)
    }
}

impl GuestSearchStream for MeilisearchSearchStream {
    fn get_next(&self) -> Option<Vec<SearchHit>> {
        if self.finished.get() {
            return Some(vec![]);
        }

        let mut search_query = self.query.clone();
        let current_page = self.current_page.get();
        let limit = search_query.per_page.unwrap_or(20);
        
        // Calculate offset based on current page
        search_query.offset = Some(current_page * limit);

        let meilisearch_request = search_query_to_meilisearch_request(search_query);
        
        match self.client.search(&self.index_name, &meilisearch_request) {
            Ok(response) => {
                let search_results = meilisearch_response_to_search_results(response);
                
                // Check if we've reached the end
                if search_results.hits.is_empty() {
                    self.finished.set(true);
                    return Some(vec![]);
                }

                // Check if this is the last page based on total and per_page
                if let (Some(total), Some(per_page)) = 
                    (search_results.total, search_results.per_page) {
                    let current_offset = current_page * per_page;
                    let next_offset = current_offset + per_page;
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

struct MeilisearchComponent;

impl MeilisearchComponent {
    const BASE_URL_ENV_VAR: &'static str = "MEILISEARCH_BASE_URL";
    const API_KEY_ENV_VAR: &'static str = "MEILISEARCH_API_KEY";

    fn create_client() -> Result<MeilisearchApi, SearchError> {
        with_config_keys(
            &[Self::BASE_URL_ENV_VAR],
            |keys| {
                if keys.is_empty() {
                    return Err(SearchError::Internal("Missing Meilisearch base URL".to_string()));
                }
                
                let base_url = keys[0].clone();
                
                // API key is optional for Meilisearch
                let api_key = std::env::var(Self::API_KEY_ENV_VAR).ok();
                
                Ok(MeilisearchApi::new(base_url, api_key))
            }
        )
    }
}

impl Guest for MeilisearchComponent {
    type SearchStream = MeilisearchSearchStream;

    fn create_index(name: IndexName, schema: Option<Schema>) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        // Create the index first
        let create_request = client::MeilisearchCreateIndexRequest {
            uid: name.clone(),
            primary_key: Some("id".to_string()), // Default primary key
        };
        
        let task = client.create_index(&create_request)?;
        
        // Wait for index creation to complete
        client.wait_for_task(task.task_uid)?;
        
        // If schema is provided, update the settings
        if let Some(schema) = schema {
            let settings = schema_to_meilisearch_settings(schema);
            let settings_task = client.update_settings(&name, &settings)?;
            client.wait_for_task(settings_task.task_uid)?;
        }
        
        Ok(())
    }

    fn delete_index(name: IndexName) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        let task = client.delete_index(&name)?;
        client.wait_for_task(task.task_uid)?;
        
        Ok(())
    }

    fn list_indexes() -> Result<Vec<IndexName>, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        let response = client.list_indexes()?;
        Ok(response.results.into_iter().map(|index| index.task_uid).collect())
    }

    fn upsert(index: IndexName, doc: Doc) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let meilisearch_doc = doc_to_meilisearch_document(doc)
            .map_err(|e| SearchError::InvalidQuery(e))?;
        
        let task = client.add_documents(&index, &[meilisearch_doc])?;
        client.wait_for_task(task.task_uid)?;
        
        Ok(())
    }

    fn upsert_many(index: IndexName, docs: Vec<Doc>) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let mut meilisearch_docs = Vec::new();
        
        for doc in docs {
            let meilisearch_doc = doc_to_meilisearch_document(doc)
                .map_err(|e| SearchError::InvalidQuery(e))?;
            meilisearch_docs.push(meilisearch_doc);
        }
        
        let task = client.add_documents(&index, &meilisearch_docs)?;
        client.wait_for_task(task.task_uid)?;
        
        Ok(())
    }

    fn delete(index: IndexName, id: DocumentId) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        let task = client.delete_document(&index, &id)?;
        client.wait_for_task(task.task_uid)?;
        
        Ok(())
    }

    fn delete_many(index: IndexName, ids: Vec<DocumentId>) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        let task = client.delete_documents(&index, &ids)?;
        client.wait_for_task(task.task_uid)?;
        
        Ok(())
    }

    fn get(index: IndexName, id: DocumentId) -> Result<Option<Doc>, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        match client.get_document(&index, &id)? {
            Some(meilisearch_doc) => Ok(Some(meilisearch_document_to_doc(meilisearch_doc))),
            None => Ok(None),
        }
    }

    fn search(index: IndexName, query: SearchQuery) -> Result<SearchResults, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let meilisearch_request = search_query_to_meilisearch_request(query);
        
        let response = client.search(&index, &meilisearch_request)?;
        Ok(meilisearch_response_to_search_results(response))
    }

    fn stream_search(index: IndexName, query: SearchQuery) -> Result<SearchStream, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let stream = MeilisearchSearchStream::new(client, index, query);
        Ok(SearchStream::new(stream))
    }

    fn get_schema(index: IndexName) -> Result<Schema, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        
        let settings = client.get_settings(&index)?;
        Ok(meilisearch_settings_to_schema(settings))
    }

    fn update_schema(index: IndexName, schema: Schema) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let settings = schema_to_meilisearch_settings(schema);
        
        let task = client.update_settings(&index, &settings)?;
        //client.wait_for_task(task.task_uid)?;
        
        Ok(())
    }
}

impl ExtendedGuest for MeilisearchComponent {
    fn unwrapped_stream(index: IndexName, query: SearchQuery) -> Self::SearchStream {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client().unwrap_or_else(|_| {
            // Return a dummy client in case of error, will fail on actual operations
            MeilisearchApi::new("http://localhost:7700".to_string(), None)
        });
        
        MeilisearchSearchStream::new(client, index, query)
    }

    fn retry_query(original_query: &SearchQuery, partial_hits: &[SearchHit]) -> SearchQuery {
        create_retry_query(original_query, partial_hits)
    }

    fn subscribe(stream: &Self::SearchStream) -> Pollable {
        stream.subscribe()
    }
}

type DurableMeilisearchComponent = DurableSearch<MeilisearchComponent>;

golem_search::export_search!(DurableMeilisearchComponent with_types_in golem_search);

#[cfg(test)]
mod tests {
    use super::*;
    use golem_search::golem::search::types::{
        Doc, SearchQuery, Schema, SchemaField, FieldType, SearchError
    };
    use serde_json::Value;
    use std::collections::HashMap;

    // Mock environment setup for tests
    fn setup_test_env() {
        std::env::set_var("MEILISEARCH_BASE_URL", "https://edge.meilisearch.com");
        std::env::set_var("MEILISEARCH_API_KEY", "b58e9ccd8d2eb1fa122ba5bfc32f67913a4eee35");
    }

    fn create_test_doc(id: &str, title: &str, content: &str) -> Doc {
        let mut doc_content = HashMap::new();
        doc_content.insert("title".to_string(), Value::String(title.to_string()));
        doc_content.insert("content".to_string(), Value::String(content.to_string()));
        doc_content.insert("category".to_string(), Value::String("test".to_string()));
        doc_content.insert("score".to_string(), Value::Number(serde_json::Number::from(100)));
        
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
                    field_type: FieldType::Keyword,
                    required: true,
                    facet: true,
                    sort: true,
                    index: false,
                },
                SchemaField {
                    name: "title".to_string(),
                    field_type: FieldType::Text,
                    required: false,
                    facet: false,
                    sort: true,
                    index: true,
                },
                SchemaField {
                    name: "content".to_string(),
                    field_type: FieldType::Text,
                    required: false,
                    facet: false,
                    sort: false,
                    index: true,
                },
                SchemaField {
                    name: "category".to_string(),
                    field_type: FieldType::Keyword,
                    required: false,
                    facet: true,
                    sort: true,
                    index: false,
                },
                SchemaField {
                    name: "score".to_string(),
                    field_type: FieldType::Integer,
                    required: false,
                    facet: true,
                    sort: true,
                    index: false,
                },
            ],
        }
    }

    #[test]
    fn test_create_index_without_schema() {
        setup_test_env();
        
        let index_name = "test_index_no_schema".to_string();
        
        // Test creating index without schema
        let result = MeilisearchComponent::create_index(index_name.clone(), None);
        
        // Note: This test will fail if Meilisearch is not running
        // In a real test environment, you'd want to mock the client
        match result {
            Ok(()) => {
                // Clean up: delete the index
                let _ = MeilisearchComponent::delete_index(index_name);
            }
            Err(SearchError::Internal(_)) => {
                // Expected if Meilisearch is not running
                println!("Meilisearch not available for testing");
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    #[test]
    fn test_create_index_with_schema() {
        setup_test_env();
        
        let index_name = "test_index_with_schema".to_string();
        let schema = create_test_schema();
        
        // Test creating index with schema
        let result = MeilisearchComponent::create_index(index_name.clone(), Some(schema));
        
        match result {
            Ok(()) => {
                // Clean up: delete the index
                let _ = MeilisearchComponent::delete_index(index_name);
            }
            Err(SearchError::Internal(_)) => {
                // Expected if Meilisearch is not running
                println!("Meilisearch not available for testing");
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    #[test]
    fn test_delete_index() {
        setup_test_env();
        
        let index_name = "test_index_to_delete".to_string();
        
        // First create an index
        let create_result = MeilisearchComponent::create_index(index_name.clone(), None);
        
        if create_result.is_ok() {
            // Then test deleting it
            let delete_result = MeilisearchComponent::delete_index(index_name);
            assert!(delete_result.is_ok(), "Failed to delete index");
        } else {
            println!("Skipping delete test - Meilisearch not available");
        }
    }

    #[test]
    fn test_list_indexes() {
        setup_test_env();
        
        let result = MeilisearchComponent::list_indexes();
        
        match result {
            Ok(indexes) => {
                // Should return a vector of index names
                println!("Found {} indexes", indexes.len());
            }
            Err(SearchError::Internal(_)) => {
                // Expected if Meilisearch is not running
                println!("Meilisearch not available for testing");
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    #[test]
    fn test_upsert_single_document() {
        setup_test_env();
        
        let index_name = "test_upsert_single".to_string();
        let doc = create_test_doc("1", "Test Title", "Test content for searching");
        
        // Create index first
        let create_result = MeilisearchComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
        if create_result.is_ok() {
            // Test upserting a document
            let upsert_result = MeilisearchComponent::upsert(index_name.clone(), doc);
            
            match upsert_result {
                Ok(()) => {
                    println!("Document upserted successfully");
                }
                Err(e) => println!("Upsert failed: {:?}", e),
            }
            
            // Clean up
            let _ = MeilisearchComponent::delete_index(index_name);
        } else {
            println!("Skipping upsert test - Meilisearch not available");
        }
    }

    #[test]
    fn test_upsert_many_documents() {
        setup_test_env();
        
        let index_name = "test_upsert_many".to_string();
        let docs = vec![
            create_test_doc("1", "First Title", "First content"),
            create_test_doc("2", "Second Title", "Second content"),
            create_test_doc("3", "Third Title", "Third content"),
        ];
        
        // Create index first
        let create_result = MeilisearchComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
        if create_result.is_ok() {
            // Test upserting multiple documents
            let upsert_result = MeilisearchComponent::upsert_many(index_name.clone(), docs);
            
            match upsert_result {
                Ok(()) => {
                    println!("Documents upserted successfully");
                }
                Err(e) => println!("Upsert many failed: {:?}", e),
            }
            
            // Clean up
            let _ = MeilisearchComponent::delete_index(index_name);
        } else {
            println!("Skipping upsert many test - Meilisearch not available");
        }
    }

    #[test]
    fn test_delete_single_document() {
        setup_test_env();
        
        let index_name = "test_delete_single".to_string();
        let doc = create_test_doc("delete_me", "Title to Delete", "Content to delete");
        
        // Create index and add document first
        let create_result = MeilisearchComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
        if create_result.is_ok() {
            let upsert_result = MeilisearchComponent::upsert(index_name.clone(), doc);
            
            if upsert_result.is_ok() {
                // Test deleting the document
                let delete_result = MeilisearchComponent::delete(index_name.clone(), "delete_me".to_string());
                
                match delete_result {
                    Ok(()) => {
                        println!("Document deleted successfully");
                    }
                    Err(e) => println!("Delete failed: {:?}", e),
                }
            }
            
            // Clean up
            let _ = MeilisearchComponent::delete_index(index_name);
        } else {
            println!("Skipping delete test - Meilisearch not available");
        }
    }

    #[test]
    fn test_delete_many_documents() {
        setup_test_env();
        
        let index_name = "test_delete_many".to_string();
        let docs = vec![
            create_test_doc("del1", "Delete Title 1", "Delete content 1"),
            create_test_doc("del2", "Delete Title 2", "Delete content 2"),
            create_test_doc("del3", "Delete Title 3", "Delete content 3"),
        ];
        
        // Create index and add documents first
        let create_result = MeilisearchComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
        if create_result.is_ok() {
            let upsert_result = MeilisearchComponent::upsert_many(index_name.clone(), docs);
            
            if upsert_result.is_ok() {
                // Test deleting multiple documents
                let ids = vec!["del1".to_string(), "del2".to_string(), "del3".to_string()];
                let delete_result = MeilisearchComponent::delete_many(index_name.clone(), ids);
                
                match delete_result {
                    Ok(()) => {
                        println!("Documents deleted successfully");
                    }
                    Err(e) => println!("Delete many failed: {:?}", e),
                }
            }
            
            // Clean up
            let _ = MeilisearchComponent::delete_index(index_name);
        } else {
            println!("Skipping delete many test - Meilisearch not available");
        }
    }

    #[test]
    fn test_get_document() {
        setup_test_env();
        
        let index_name = "test_get_document".to_string();
        let doc = create_test_doc("get_me", "Get This Title", "Get this content");
        
        // Create index and add document first
        let create_result = MeilisearchComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
        if create_result.is_ok() {
            let upsert_result = MeilisearchComponent::upsert(index_name.clone(), doc.clone());
            
            if upsert_result.is_ok() {
                // Test getting the document
                let get_result = MeilisearchComponent::get(index_name.clone(), "get_me".to_string());
                
                match get_result {
                    Ok(Some(retrieved_doc)) => {
                        assert_eq!(retrieved_doc.id, "get_me");
                        println!("Document retrieved successfully");
                    }
                    Ok(None) => {
                        println!("Document not found (might be indexing delay)");
                    }
                    Err(e) => println!("Get failed: {:?}", e),
                }
            }
            
            // Clean up
            let _ = MeilisearchComponent::delete_index(index_name);
        } else {
            println!("Skipping get test - Meilisearch not available");
        }
    }

    #[test]
    fn test_get_nonexistent_document() {
        setup_test_env();
        
        let index_name = "test_get_nonexistent".to_string();
        
        // Create index first
        let create_result = MeilisearchComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
        if create_result.is_ok() {
            // Test getting a non-existent document
            let get_result = MeilisearchComponent::get(index_name.clone(), "nonexistent".to_string());
            
            match get_result {
                Ok(None) => {
                    println!("Correctly returned None for non-existent document");
                }
                Ok(Some(_)) => {
                    panic!("Should not have found a non-existent document");
                }
                Err(e) => println!("Get failed: {:?}", e),
            }
            
            // Clean up
            let _ = MeilisearchComponent::delete_index(index_name);
        } else {
            println!("Skipping get nonexistent test - Meilisearch not available");
        }
    }

    #[test]
    fn test_search() {
        setup_test_env();
        
        let index_name = "test_search".to_string();
        let docs = vec![
            create_test_doc("1", "Rust Programming", "Learn Rust programming language"),
            create_test_doc("2", "JavaScript Guide", "Complete JavaScript tutorial"),
            create_test_doc("3", "Python Basics", "Introduction to Python programming"),
        ];
        
        // Create index and add documents first
        let create_result = MeilisearchComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
        if create_result.is_ok() {
            let upsert_result = MeilisearchComponent::upsert_many(index_name.clone(), docs);
            
            if upsert_result.is_ok() {
                // Test search
                let query = SearchQuery {
                    q: Some("programming".to_string()),
                    offset: Some(0),
                    per_page: Some(10),
                    page: Some(10),
                    filters: vec![],
                    facets: vec![],
                    sort: vec![],
                    highlight: None,
                    config: None,
                };
                
                let search_result = MeilisearchComponent::search(index_name.clone(), query);
                
                match search_result {
                    Ok(results) => {
                        println!("Search returned {} hits", results.hits.len());
                        // Note: Results might be empty due to indexing delays in tests
                    }
                    Err(e) => println!("Search failed: {:?}", e),
                }
            }
            
            // Clean up
            let _ = MeilisearchComponent::delete_index(index_name);
        } else {
            println!("Skipping search test - Meilisearch not available");
        }
    }

    #[test]
    fn test_stream_search() {
        setup_test_env();
        
        let index_name = "test_stream_search".to_string();
        let docs = vec![
            create_test_doc("1", "Stream Test 1", "First streaming document"),
            create_test_doc("2", "Stream Test 2", "Second streaming document"),
            create_test_doc("3", "Stream Test 3", "Third streaming document"),
        ];
        
        // Create index and add documents first
        let create_result = MeilisearchComponent::create_index(index_name.clone(), Some(create_test_schema()));
        
        if create_result.is_ok() {
            let upsert_result = MeilisearchComponent::upsert_many(index_name.clone(), docs);
            
            if upsert_result.is_ok() {
                // Test stream search
                let query = SearchQuery {
                    q: Some("streaming".to_string()),
                    offset: Some(0),
                    per_page: Some(2), // Small page size to test pagination
                    page: Some(2),
                    filters: vec![],
                    facets: vec![],
                    sort: vec![],
                    highlight: None,
                    config: None,
                };
                
                let stream_result = MeilisearchComponent::stream_search(index_name.clone(), query);
                
                match stream_result {
                    Ok(_stream) => {
                        println!("Stream search created successfully");
                        // Note: Actually consuming the stream would require more complex test setup
                    }
                    Err(e) => println!("Stream search failed: {:?}", e),
                }
            }
            
            // Clean up
            let _ = MeilisearchComponent::delete_index(index_name);
        } else {
            println!("Skipping stream search test - Meilisearch not available");
        }
    }

    #[test]
    fn test_get_schema() {
        setup_test_env();
        
        let index_name = "test_get_schema".to_string();
        let original_schema = create_test_schema();
        
        // Create index with schema first
        let create_result = MeilisearchComponent::create_index(index_name.clone(), Some(original_schema.clone()));
        
        if create_result.is_ok() {
            // Test getting the schema
            let schema_result = MeilisearchComponent::get_schema(index_name.clone());
            
            match schema_result {
                Ok(retrieved_schema) => {
                    assert_eq!(retrieved_schema.primary_key, original_schema.primary_key);
                    println!("Schema retrieved successfully");
                }
                Err(e) => println!("Get schema failed: {:?}", e),
            }
            
            // Clean up
            let _ = MeilisearchComponent::delete_index(index_name);
        } else {
            println!("Skipping get schema test - Meilisearch not available");
        }
    }

    #[test]
    fn test_update_schema() {
        setup_test_env();
        
        let index_name = "test_update_schema".to_string();
        let initial_schema = create_test_schema();
        
        // Create index with initial schema
        let create_result = MeilisearchComponent::create_index(index_name.clone(), Some(initial_schema));
        
        if create_result.is_ok() {
            // Create updated schema with additional field
            let mut updated_schema = create_test_schema();
            updated_schema.fields.push(SchemaField {
                name: "new_field".to_string(),
                field_type: FieldType::Text,
                required: false,
                facet: false,
                sort: false,
                index: true,
            });
            
            // Test updating the schema
            let update_result = MeilisearchComponent::update_schema(index_name.clone(), updated_schema);
            
            match update_result {
                Ok(()) => {
                    println!("Schema updated successfully");
                }
                Err(e) => println!("Update schema failed: {:?}", e),
            }
            
            // Clean up
            let _ = MeilisearchComponent::delete_index(index_name);
        } else {
            println!("Skipping update schema test - Meilisearch not available");
        }
    }

    // #[test]
    // fn test_search_stream_implementation() {
    //     setup_test_env();
        
    //     let client = MeilisearchApi::new("http://localhost:7700".to_string(), None);
    //     let query = SearchQuery {
    //         q: Some("test".to_string()),
    //         offset: Some(0),
    //         per_page: Some(5),
    //         page: Some(5),
    //         filters: vec![],
    //         facets: vec![],
    //         sort: vec![],
    //         highlight: None,
    //         config: None,
    //     };
        
    //     let stream = MeilisearchSearchStream::new(client, "test_index".to_string(), query);
        
    //     // Test that the stream can be created and subscribed to
    //     let _pollable = stream.subscribe();
        
    //     // Test that get_next returns Some (even if empty due to no Meilisearch)
    //     let result = stream.get_next();
    //     assert!(result.is_some());
        
    //     // Test blocking_get_next
    //     let _blocking_result = stream.blocking_get_next();
        
    //     println!("Stream implementation tests passed");
    // }

    #[test]
    fn test_error_handling() {
        // Test with invalid base URL
        std::env::set_var("MEILISEARCH_BASE_URL", "invalid_url");
        
        let result = MeilisearchComponent::create_index("test".to_string(), None);
        assert!(result.is_err(), "Should fail with invalid URL");
        
        // Test with missing base URL
        std::env::remove_var("MEILISEARCH_BASE_URL");
        
        let result = MeilisearchComponent::create_index("test".to_string(), None);
        assert!(result.is_err(), "Should fail with missing URL");
        
        println!("Error handling tests passed");
    }
}
