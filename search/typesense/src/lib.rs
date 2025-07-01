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
        let page = query.page.unwrap_or(1);
        Self {
            client,
            index_name,
            query,
            current_page: Cell::new(page),
            finished: Cell::new(false),
            last_response: RefCell::new(None),
        }
    }

    fn get_next(&self) -> Option<Vec<SearchHit>> {
        if self.finished.get() {
            return None;
        }

        // Prepare query for current page
        let mut typesense_query = search_query_to_typesense_query(self.query.clone());
        typesense_query.page = Some(self.current_page.get());
        
        // Set default page size if not specified
        if typesense_query.per_page.is_none() {
            typesense_query.per_page = Some(20);
        }

        match self.client.search(&self.index_name, &typesense_query) {
            Ok(response) => {
                let search_results = typesense_response_to_search_results(response);
                
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
                
                if hits.is_empty() {
                    None
                } else {
                    Some(hits)
                }
            }
            Err(_) => {
                self.finished.set(true);
                None
            }
        }
    }

    fn blocking_get_next(&self) -> Option<Vec<SearchHit>> {
        // For sync implementation, this is the same as get_next
        self.get_next()
    }

    fn subscribe(&self) -> Pollable {
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
        with_config_keys(
            &[Self::API_KEY_ENV_VAR, Self::BASE_URL_ENV_VAR],
            |keys| {
                if keys.len() != 2 {
                    return Err(SearchError::Internal("Missing Typesense credentials".to_string()));
                }
                
                let api_key = keys[0].clone();
                let base_url = keys[1].clone();
                
                Ok(TypesenseSearchApi::new(api_key, base_url))
            }
        )
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
        // We need to search for the specific document by ID
        let query = SearchQuery {
            q: Some(id.clone()),
            filters: vec![format!("id:={}", id)],
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
        let client = Self::create_client()?;
        let stream = TypesenseSearchStream::new(client, index, query);
        Ok(SearchStream::new(stream))
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

impl GuestSearchStream for TypesenseSearchStream {
    fn get_next(&self) -> Option<Vec<SearchHit>> {
        self.get_next()
    }

    fn blocking_get_next(&self) -> Vec<SearchHit> {
        self.blocking_get_next().unwrap_or_default()
    }
}

impl ExtendedGuest for TypesenseComponent {
    fn unwrapped_stream(index: IndexName, query: SearchQuery) -> Self::SearchStream {
        let client = Self::create_client().expect("Failed to create client");
        TypesenseSearchStream::new(client, index, query)
    }

    fn retry_query(original_query: &SearchQuery, partial_hits: &[SearchHit]) -> SearchQuery {
        let mut retry_query = original_query.clone();
        
        // If we have partial results, we might want to exclude already seen document IDs
        // or adjust pagination to continue from where we left off
        if !partial_hits.is_empty() {
            // Adjust offset to skip already received hits
            let current_offset = original_query.offset.unwrap_or(0);
            let received_count = partial_hits.len() as u32;
            retry_query.offset = Some(current_offset + received_count);
        }
        
        retry_query
    }

    fn subscribe(stream: &Self::SearchStream) -> Pollable {
        stream.subscribe()
    }
}

type DurableTypesenseComponent = DurableSearch<TypesenseComponent>;

golem_search::export_search!(DurableTypesenseComponent with_types_in golem_search);
