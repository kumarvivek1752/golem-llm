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
        

        search_query.offset = Some(current_page * limit);

        let opensearch_request = search_query_to_opensearch_request(search_query);
        
        match self.client.search(&self.index_name, &opensearch_request) {
            Ok(response) => {
                let search_results = opensearch_response_to_search_results(response);
                

                if search_results.hits.is_empty() {
                    self.finished.set(true);
                    return Some(vec![]);
                }


                if let Some(total) = search_results.total {
                    let current_offset = current_page * limit;
                    let next_offset = current_offset + limit;
                    if next_offset >= total {
                        self.finished.set(true);
                    }
                }


                if (search_results.hits.len() as u32) < limit {
                    self.finished.set(true);
                }


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
                

                let username = std::env::var(Self::USERNAME_ENV_VAR).ok();
                let password = std::env::var(Self::PASSWORD_ENV_VAR).ok();
                let api_key = std::env::var(Self::API_KEY_ENV_VAR).ok();
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

        let mut bulk_operations = Vec::new();
        for doc in docs {
            let opensearch_doc = doc_to_opensearch_document(doc)
                .map_err(|e| SearchError::InvalidQuery(e))?;
            
            let doc_id = opensearch_doc.get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            
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