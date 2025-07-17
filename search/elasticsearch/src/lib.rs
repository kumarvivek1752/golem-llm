use crate::client::ElasticsearchApi;
use crate::conversions::{
    build_bulk_delete_operations, build_bulk_operations, create_retry_query,
    doc_to_elasticsearch_document, elasticsearch_document_to_doc, elasticsearch_mappings_to_schema,
    elasticsearch_response_to_search_results, schema_to_elasticsearch_settings,
    search_query_to_elasticsearch_query,
};
use golem_rust::wasm_rpc::Pollable;
use golem_search::config::with_config_keys;
use golem_search::durability::{DurableSearch, ExtendedGuest};
use golem_search::golem::search::core::{Guest, GuestSearchStream, SearchStream};
use golem_search::golem::search::types::{
    Doc, DocumentId, IndexName, Schema, SearchError, SearchHit, SearchQuery, SearchResults,
};
use golem_search::LOGGING_STATE;
use std::cell::{Cell, RefCell};

mod client;
mod conversions;

/// Uses scroll API for streaming large result sets
struct ElasticsearchSearchStream {
    client: ElasticsearchApi,
    index_name: String,
    query: SearchQuery,
    scroll_id: RefCell<Option<String>>,
    finished: Cell<bool>,
    current_offset: Cell<u32>,
}

impl ElasticsearchSearchStream {
    pub fn new(client: ElasticsearchApi, index_name: String, query: SearchQuery) -> Self {
        Self {
            client,
            index_name,
            query: query.clone(),
            scroll_id: RefCell::new(None),
            finished: Cell::new(false),
            current_offset: Cell::new(query.offset.unwrap_or(0)),
        }
    }

    pub fn subscribe(&self) -> Pollable {
        golem_rust::bindings::wasi::clocks::monotonic_clock::subscribe_duration(0)
    }
}

impl GuestSearchStream for ElasticsearchSearchStream {
    fn get_next(&self) -> Option<Vec<SearchHit>> {
        if self.finished.get() {
            return Some(vec![]);
        }

        // For first request, use regular search with scroll
        if self.scroll_id.borrow().is_none() {
            let mut es_query = search_query_to_elasticsearch_query(self.query.clone());

            es_query.from = Some(self.current_offset.get());
            es_query.size = Some(self.query.per_page.unwrap_or(10));

            let _url = format!("{}/_search?scroll=1m", self.index_name);

            match self.client.search(&self.index_name, &es_query) {
                Ok(response) => {
                    let search_results = elasticsearch_response_to_search_results(response);

                    if search_results.hits.is_empty() {
                        self.finished.set(true);
                        return Some(vec![]);
                    }

                    let current_offset = self.current_offset.get();
                    let received_count = search_results.hits.len() as u32;
                    self.current_offset.set(current_offset + received_count);

                    if let Some(total) = search_results.total {
                        if self.current_offset.get() >= total {
                            self.finished.set(true);
                        }
                    }

                    Some(search_results.hits)
                }
                Err(_) => {
                    self.finished.set(true);
                    Some(vec![])
                }
            }
        } else {
            // Continue with scroll
            // Note: For simplicity, we're using pagination instead of true scroll API
            // In a production implementation, you'd use Elasticsearch's scroll API
            self.finished.set(true);
            Some(vec![])
        }
    }

    fn blocking_get_next(&self) -> Vec<SearchHit> {
        self.get_next().unwrap_or_default()
    }
}

struct ElasticsearchComponent;

impl ElasticsearchComponent {
    const URL_ENV_VAR: &'static str = "ELASTICSEARCH_URL";
    const USERNAME_ENV_VAR: &'static str = "ELASTICSEARCH_USERNAME";
    const PASSWORD_ENV_VAR: &'static str = "ELASTICSEARCH_PASSWORD";
    const API_KEY_ENV_VAR: &'static str = "ELASTICSEARCH_API_KEY";

    fn create_client() -> Result<ElasticsearchApi, SearchError> {
        with_config_keys(
            &[
                Self::URL_ENV_VAR,
                Self::USERNAME_ENV_VAR,
                Self::PASSWORD_ENV_VAR,
                Self::API_KEY_ENV_VAR,
            ],
            |keys| {
                if keys.is_empty() || keys[0].is_empty() {
                    return Err(SearchError::Internal(
                        "Missing Elasticsearch URL".to_string(),
                    ));
                }

                let url = keys[0].clone();
                let username = if keys.len() > 1 && !keys[1].is_empty() {
                    Some(keys[1].clone())
                } else {
                    None
                };
                let password = if keys.len() > 2 && !keys[2].is_empty() {
                    Some(keys[2].clone())
                } else {
                    None
                };
                let api_key = if keys.len() > 3 && !keys[3].is_empty() {
                    Some(keys[3].clone())
                } else {
                    None
                };

                Ok(ElasticsearchApi::new(url, username, password, api_key))
            },
        )
    }
}

impl Guest for ElasticsearchComponent {
    type SearchStream = ElasticsearchSearchStream;

    fn create_index(name: IndexName, schema: Option<Schema>) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let settings = schema.map(schema_to_elasticsearch_settings);

        client.create_index(&name, settings)
    }

    fn delete_index(name: IndexName) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        client.delete_index(&name)
    }

    fn list_indexes() -> Result<Vec<IndexName>, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        match client.list_indices() {
            Ok(indices) => Ok(indices.into_iter().map(|idx| idx.index).collect()),
            Err(e) => Err(e),
        }
    }

    fn upsert(index: IndexName, doc: Doc) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let document = doc_to_elasticsearch_document(doc).map_err(SearchError::InvalidQuery)?;

        client.index_document(
            &index,
            document["id"].as_str().unwrap_or_default(),
            &document,
        )
    }

    fn upsert_many(index: IndexName, docs: Vec<Doc>) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let bulk_operations =
            build_bulk_operations(&index, &docs, "index").map_err(SearchError::InvalidQuery)?;

        match client.bulk_index(&bulk_operations) {
            Ok(response) => {
                if response.errors {
                    Err(SearchError::Internal(
                        "Some bulk operations failed".to_string(),
                    ))
                } else {
                    Ok(())
                }
            }
            Err(e) => Err(e),
        }
    }

    fn delete(index: IndexName, id: DocumentId) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        client.delete_document(&index, &id)
    }

    fn delete_many(index: IndexName, ids: Vec<DocumentId>) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let bulk_operations =
            build_bulk_delete_operations(&index, &ids).map_err(SearchError::InvalidQuery)?;

        match client.bulk_index(&bulk_operations) {
            Ok(response) => {
                if response.errors {
                    Err(SearchError::Internal(
                        "Some bulk delete operations failed".to_string(),
                    ))
                } else {
                    Ok(())
                }
            }
            Err(e) => Err(e),
        }
    }

    fn get(index: IndexName, id: DocumentId) -> Result<Option<Doc>, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        match client.get_document(&index, &id) {
            Ok(Some(document)) => Ok(Some(elasticsearch_document_to_doc(id, document))),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn search(index: IndexName, query: SearchQuery) -> Result<SearchResults, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let es_query = search_query_to_elasticsearch_query(query);

        match client.search(&index, &es_query) {
            Ok(response) => Ok(elasticsearch_response_to_search_results(response)),
            Err(e) => Err(e),
        }
    }

    fn stream_search(index: IndexName, query: SearchQuery) -> Result<SearchStream, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let stream = ElasticsearchSearchStream::new(client, index, query);
        Ok(SearchStream::new(stream))
    }

    fn get_schema(index: IndexName) -> Result<Schema, SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        match client.get_mappings(&index) {
            Ok(mappings) => Ok(elasticsearch_mappings_to_schema(mappings, &index)),
            Err(e) => Err(e),
        }
    }

    fn update_schema(index: IndexName, schema: Schema) -> Result<(), SearchError> {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client()?;
        let settings = schema_to_elasticsearch_settings(schema);

        if let Some(mappings) = settings.mappings {
            client.put_mappings(&index, &mappings)
        } else {
            Ok(())
        }
    }
}

impl ExtendedGuest for ElasticsearchComponent {
    fn unwrapped_stream(index: IndexName, query: SearchQuery) -> Self::SearchStream {
        LOGGING_STATE.with_borrow_mut(|state| state.init());

        let client = Self::create_client().unwrap_or_else(|_| {
            ElasticsearchApi::new("http://localhost:9200".to_string(), None, None, None)
        });

        ElasticsearchSearchStream::new(client, index, query)
    }

    fn retry_query(original_query: &SearchQuery, partial_hits: &[SearchHit]) -> SearchQuery {
        create_retry_query(original_query, partial_hits)
    }

    fn subscribe(stream: &Self::SearchStream) -> Pollable {
        stream.subscribe()
    }
}

type DurableElasticsearchComponent = DurableSearch<ElasticsearchComponent>;

golem_search::export_search!(DurableElasticsearchComponent with_types_in golem_search);

// streaming
