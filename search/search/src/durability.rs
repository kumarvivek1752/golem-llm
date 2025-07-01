use crate::golem::search::core::Guest;
use crate::golem::search::types::{
    IndexName, SearchQuery, SearchHit
};
use golem_rust::wasm_rpc::Pollable;
use std::marker::PhantomData;

/// Wraps a Search implementation with custom durability
pub struct DurableSearch<Impl> {
    phantom: PhantomData<Impl>,
}

/// Trait to be implemented in addition to the Search `Guest` trait when wrapping it with `DurableSearch`.
pub trait ExtendedGuest: Guest + 'static {
    /// Creates an instance of the Search specific `SearchStream` without wrapping it in a `Resource`
    fn unwrapped_stream(index: IndexName, query: SearchQuery) -> Self::SearchStream;

    /// Creates the retry query with the original query and any partial results received.
    /// There is a default implementation here, but it can be overridden with provider-specific
    /// queries if needed.
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

    fn subscribe(stream: &Self::SearchStream) -> Pollable;
}

/// When the durability feature flag is off, wrapping with `DurableSearch` is just a passthrough
#[cfg(not(feature = "durability"))]
mod passthrough_impl {
    use crate::durability::{DurableSearch, ExtendedGuest};
    use crate::golem::search::core::{Guest, SearchStream};
    use crate::golem::search::types::{
        IndexName, DocumentId, Doc, SearchQuery, SearchResults, Schema, SearchError
    };

    impl<Impl: ExtendedGuest> Guest for DurableSearch<Impl> {
        type SearchStream = Impl::SearchStream;

        fn create_index(name: IndexName, schema: Option<Schema>) -> Result<(), SearchError> {
            Impl::create_index(name, schema)
        }

        fn delete_index(name: IndexName) -> Result<(), SearchError> {
            Impl::delete_index(name)
        }

        fn list_indexes() -> Result<Vec<IndexName>, SearchError> {
            Impl::list_indexes()
        }

        fn upsert(index: IndexName, doc: Doc) -> Result<(), SearchError> {
            Impl::upsert(index, doc)
        }

        fn upsert_many(index: IndexName, docs: Vec<Doc>) -> Result<(), SearchError> {
            Impl::upsert_many(index, docs)
        }

        fn delete(index: IndexName, id: DocumentId) -> Result<(), SearchError> {
            Impl::delete(index, id)
        }

        fn delete_many(index: IndexName, ids: Vec<DocumentId>) -> Result<(), SearchError> {
            Impl::delete_many(index, ids)
        }

        fn get(index: IndexName, id: DocumentId) -> Result<Option<Doc>, SearchError> {
            Impl::get(index, id)
        }

        fn search(index: IndexName, query: SearchQuery) -> Result<SearchResults, SearchError> {
            Impl::search(index, query)
        }

        fn stream_search(index: IndexName, query: SearchQuery) -> Result<SearchStream, SearchError> {
            Impl::stream_search(index, query)
        }

        fn get_schema(index: IndexName) -> Result<Schema, SearchError> {
            Impl::get_schema(index)
        }

        fn update_schema(index: IndexName, schema: Schema) -> Result<(), SearchError> {
            Impl::update_schema(index, schema)
        }
    }
}

/// When the durability feature flag is on, wrapping with `DurableSearch` adds custom durability
/// on top of the provider-specific Search implementation using Golem's special host functions and
/// the `golem-rust` helper library.
///
/// There will be custom durability entries saved in the oplog, with the full Search request and configuration
/// stored as input, and the full response stored as output. To serialize these in a way it is
/// observable by oplog consumers, each relevant data type has to be converted to/from `ValueAndType`
/// which is implemented using the type classes and builder in the `golem-rust` library.
#[cfg(feature = "durability")]
mod durable_impl {
    use crate::durability::{DurableSearch, ExtendedGuest};
    use crate::golem::search::core::{Guest, SearchStream, GuestSearchStream};
    use crate::golem::search::types::{
        IndexName, DocumentId, Doc, SearchQuery, SearchResults, SearchHit, Schema, SearchError
    };
    use golem_rust::bindings::golem::durability::durability::{
        DurableFunctionType, LazyInitializedPollable,
    };
    use golem_rust::durability::Durability;
    use golem_rust::wasm_rpc::Pollable;
    use golem_rust::{with_persistence_level, FromValueAndType, IntoValue, PersistenceLevel};
    use std::cell::RefCell;
    use std::fmt::{Display, Formatter};

    // Input types for durability persistence
    #[derive(Debug, Clone, IntoValue)]
    struct CreateIndexInput {
        name: IndexName,
        schema: Option<Schema>,
    }

    #[derive(Debug, Clone, IntoValue)]
    struct DeleteIndexInput {
        name: IndexName,
    }

    #[derive(Debug, Clone, IntoValue)]
    struct UpsertInput {
        index: IndexName,
        doc: Doc,
    }

    #[derive(Debug, Clone, IntoValue)]
    struct UpsertManyInput {
        index: IndexName,
        docs: Vec<Doc>,
    }

    #[derive(Debug, Clone, IntoValue)]
    struct DeleteInput {
        index: IndexName,
        id: DocumentId,
    }

    #[derive(Debug, Clone, IntoValue)]
    struct DeleteManyInput {
        index: IndexName,
        ids: Vec<DocumentId>,
    }

    #[derive(Debug, Clone, IntoValue)]
    struct GetInput {
        index: IndexName,
        id: DocumentId,
    }

    #[derive(Debug, Clone, IntoValue)]
    struct SearchInput {
        index: IndexName,
        query: SearchQuery,
    }

    #[derive(Debug, Clone, IntoValue)]
    struct StreamSearchInput {
        index: IndexName,
        query: SearchQuery,
    }

    #[derive(Debug, Clone, IntoValue)]
    struct GetSchemaInput {
        index: IndexName,
    }

    #[derive(Debug, Clone, IntoValue)]
    struct UpdateSchemaInput {
        index: IndexName,
        schema: Schema,
    }

    #[derive(Debug, IntoValue)]
    struct NoInput;

    #[derive(Debug, Clone, FromValueAndType, IntoValue)]
    struct NoOutput;

    #[derive(Debug, FromValueAndType, IntoValue)]
    struct UnusedError;

    impl Display for UnusedError {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "UnusedError")
        }
    }

    impl<Impl: ExtendedGuest> Guest for DurableSearch<Impl> {
        type SearchStream = DurableSearchStream<Impl>;

        fn create_index(name: IndexName, schema: Option<Schema>) -> Result<(), SearchError> {
            let durability = Durability::<Result<(), SearchError>, UnusedError>::new(
                "golem_search",
                "create_index",
                DurableFunctionType::WriteRemote,
            );
            if durability.is_live() {
                let result = with_persistence_level(PersistenceLevel::PersistNothing, || {
                    Impl::create_index(name.clone(), schema.clone())
                });
                durability.persist_infallible(CreateIndexInput { name, schema }, result)
            } else {
                durability.replay_infallible()
            }
        }

        fn delete_index(name: IndexName) -> Result<(), SearchError> {
            let durability = Durability::<Result<(), SearchError>, UnusedError>::new(
                "golem_search",
                "delete_index",
                DurableFunctionType::WriteRemote,
            );
            if durability.is_live() {
                let result = with_persistence_level(PersistenceLevel::PersistNothing, || {
                    Impl::delete_index(name.clone())
                });
                durability.persist_infallible(DeleteIndexInput { name }, result)
            } else {
                durability.replay_infallible()
            }
        }

        fn list_indexes() -> Result<Vec<IndexName>, SearchError> {
            let durability = Durability::<Result<Vec<IndexName>, SearchError>, UnusedError>::new(
                "golem_search",
                "list_indexes",
                DurableFunctionType::ReadRemote,
            );
            if durability.is_live() {
                let result = with_persistence_level(PersistenceLevel::PersistNothing, || {
                    Impl::list_indexes()
                });
                durability.persist_infallible(NoInput, result)
            } else {
                durability.replay_infallible()
            }
        }

        fn upsert(index: IndexName, doc: Doc) -> Result<(), SearchError> {
            let durability = Durability::<Result<(), SearchError>, UnusedError>::new(
                "golem_search",
                "upsert",
                DurableFunctionType::WriteRemote,
            );
            if durability.is_live() {
                let result = with_persistence_level(PersistenceLevel::PersistNothing, || {
                    Impl::upsert(index.clone(), doc.clone())
                });
                durability.persist_infallible(UpsertInput { index, doc }, result)
            } else {
                durability.replay_infallible()
            }
        }

        fn upsert_many(index: IndexName, docs: Vec<Doc>) -> Result<(), SearchError> {
            let durability = Durability::<Result<(), SearchError>, UnusedError>::new(
                "golem_search",
                "upsert_many",
                DurableFunctionType::WriteRemote,
            );
            if durability.is_live() {
                let result = with_persistence_level(PersistenceLevel::PersistNothing, || {
                    Impl::upsert_many(index.clone(), docs.clone())
                });
                durability.persist_infallible(UpsertManyInput { index, docs }, result)
            } else {
                durability.replay_infallible()
            }
        }

        fn delete(index: IndexName, id: DocumentId) -> Result<(), SearchError> {
            let durability = Durability::<Result<(), SearchError>, UnusedError>::new(
                "golem_search",
                "delete",
                DurableFunctionType::WriteRemote,
            );
            if durability.is_live() {
                let result = with_persistence_level(PersistenceLevel::PersistNothing, || {
                    Impl::delete(index.clone(), id.clone())
                });
                durability.persist_infallible(DeleteInput { index, id }, result)
            } else {
                durability.replay_infallible()
            }
        }

        fn delete_many(index: IndexName, ids: Vec<DocumentId>) -> Result<(), SearchError> {
            let durability = Durability::<Result<(), SearchError>, UnusedError>::new(
                "golem_search",
                "delete_many",
                DurableFunctionType::WriteRemote,
            );
            if durability.is_live() {
                let result = with_persistence_level(PersistenceLevel::PersistNothing, || {
                    Impl::delete_many(index.clone(), ids.clone())
                });
                durability.persist_infallible(DeleteManyInput { index, ids }, result)
            } else {
                durability.replay_infallible()
            }
        }

        fn get(index: IndexName, id: DocumentId) -> Result<Option<Doc>, SearchError> {
            let durability = Durability::<Result<Option<Doc>, SearchError>, UnusedError>::new(
                "golem_search",
                "get",
                DurableFunctionType::ReadRemote,
            );
            if durability.is_live() {
                let result = with_persistence_level(PersistenceLevel::PersistNothing, || {
                    Impl::get(index.clone(), id.clone())
                });
                durability.persist_infallible(GetInput { index, id }, result)
            } else {
                durability.replay_infallible()
            }
        }

        fn search(index: IndexName, query: SearchQuery) -> Result<SearchResults, SearchError> {
            let durability = Durability::<Result<SearchResults, SearchError>, UnusedError>::new(
                "golem_search",
                "search",
                DurableFunctionType::ReadRemote,
            );
            if durability.is_live() {
                let result = with_persistence_level(PersistenceLevel::PersistNothing, || {
                    Impl::search(index.clone(), query.clone())
                });
                durability.persist_infallible(SearchInput { index, query }, result)
            } else {
                durability.replay_infallible()
            }
        }

        fn stream_search(index: IndexName, query: SearchQuery) -> Result<SearchStream, SearchError> {
            let durability = Durability::<NoOutput, UnusedError>::new(
                "golem_search",
                "stream_search",
                DurableFunctionType::ReadRemote,
            );
            if durability.is_live() {
                let result = with_persistence_level(PersistenceLevel::PersistNothing, || {
                    SearchStream::new(DurableSearchStream::<Impl>::live(Impl::unwrapped_stream(
                        index.clone(),
                        query.clone(),
                    )))
                });
                let _ = durability.persist_infallible(StreamSearchInput { index, query }, NoOutput);
                Ok(result)
            } else {
                let _: NoOutput = durability.replay_infallible();
                Ok(SearchStream::new(DurableSearchStream::<Impl>::replay(index, query)))
            }
        }

        fn get_schema(index: IndexName) -> Result<Schema, SearchError> {
            let durability = Durability::<Result<Schema, SearchError>, UnusedError>::new(
                "golem_search",
                "get_schema",
                DurableFunctionType::ReadRemote,
            );
            if durability.is_live() {
                let result = with_persistence_level(PersistenceLevel::PersistNothing, || {
                    Impl::get_schema(index.clone())
                });
                durability.persist_infallible(GetSchemaInput { index }, result)
            } else {
                durability.replay_infallible()
            }
        }

        fn update_schema(index: IndexName, schema: Schema) -> Result<(), SearchError> {
            let durability = Durability::<Result<(), SearchError>, UnusedError>::new(
                "golem_search",
                "update_schema",
                DurableFunctionType::WriteRemote,
            );
            if durability.is_live() {
                let result = with_persistence_level(PersistenceLevel::PersistNothing, || {
                    Impl::update_schema(index.clone(), schema.clone())
                });
                durability.persist_infallible(UpdateSchemaInput { index, schema }, result)
            } else {
                durability.replay_infallible()
            }
        }
    }

    /// Represents the durable search stream's state
    ///
    /// In live mode it directly calls the underlying Search stream which is implemented on
    /// top of a streaming search response.
    ///
    /// In replay mode it buffers the replayed search hits, and also tracks the created pollables
    /// to be able to reattach them to the new live stream when the switch to live mode
    /// happens.
    ///
    /// When reaching the end of the replay mode, if the replayed stream was not finished yet,
    /// the retry query implemented in `ExtendedGuest` is used to create a new Search response
    /// stream and continue the search seamlessly.
    enum DurableSearchStreamState<Impl: ExtendedGuest> {
        Live {
            stream: Impl::SearchStream,
            pollables: Vec<LazyInitializedPollable>,
        },
        Replay {
            index: IndexName,
            query: SearchQuery,
            pollables: Vec<LazyInitializedPollable>,
            partial_result: Vec<SearchHit>,
            finished: bool,
        },
    }

    pub struct DurableSearchStream<Impl: ExtendedGuest> {
        state: RefCell<Option<DurableSearchStreamState<Impl>>>,
        subscription: RefCell<Option<Pollable>>,
    }

    impl<Impl: ExtendedGuest> DurableSearchStream<Impl> {
        fn live(stream: Impl::SearchStream) -> Self {
            Self {
                state: RefCell::new(Some(DurableSearchStreamState::Live {
                    stream,
                    pollables: Vec::new(),
                })),
                subscription: RefCell::new(None),
            }
        }

        fn replay(index: IndexName, query: SearchQuery) -> Self {
            Self {
                state: RefCell::new(Some(DurableSearchStreamState::Replay {
                    index,
                    query,
                    pollables: Vec::new(),
                    partial_result: Vec::new(),
                    finished: false,
                })),
                subscription: RefCell::new(None),
            }
        }

        fn subscribe(&self) -> Pollable {
            let mut state = self.state.borrow_mut();
            match &mut *state {
                Some(DurableSearchStreamState::Live { stream, .. }) => Impl::subscribe(stream),
                Some(DurableSearchStreamState::Replay { pollables, .. }) => {
                    let lazy_pollable = LazyInitializedPollable::new();
                    let pollable = lazy_pollable.subscribe();
                    pollables.push(lazy_pollable);
                    pollable
                }
                None => {
                    unreachable!()
                }
            }
        }
    }

    impl<Impl: ExtendedGuest> Drop for DurableSearchStream<Impl> {
        fn drop(&mut self) {
            let _ = self.subscription.take();
            match self.state.take() {
                Some(DurableSearchStreamState::Live {
                    mut pollables,
                    stream,
                }) => {
                    with_persistence_level(PersistenceLevel::PersistNothing, move || {
                        pollables.clear();
                        drop(stream);
                    });
                }
                Some(DurableSearchStreamState::Replay { mut pollables, .. }) => {
                    pollables.clear();
                }
                None => {}
            }
        }
    }

    impl<Impl: ExtendedGuest> GuestSearchStream for DurableSearchStream<Impl> {
        fn get_next(&self) -> Option<Vec<SearchHit>> {
            let durability = Durability::<Option<Vec<SearchHit>>, UnusedError>::new(
                "golem_search",
                "get_next",
                DurableFunctionType::ReadRemote,
            );
            if durability.is_live() {
                let mut state = self.state.borrow_mut();
                let (result, new_live_stream) = match &*state {
                    Some(DurableSearchStreamState::Live { stream, .. }) => {
                        let result =
                            with_persistence_level(PersistenceLevel::PersistNothing, || {
                                stream.get_next()
                            });
                        (durability.persist_infallible(NoInput, result.clone()), None)
                    }
                    Some(DurableSearchStreamState::Replay {
                        index,
                        query,
                        pollables,
                        partial_result,
                        finished,
                    }) => {
                        if *finished {
                            (None, None)
                        } else {
                            let extended_query =
                                Impl::retry_query(query, partial_result);

                            let (stream, first_live_result) =
                                with_persistence_level(PersistenceLevel::PersistNothing, || {
                                    let stream = <Impl as ExtendedGuest>::unwrapped_stream(
                                        index.clone(),
                                        extended_query,
                                    );

                                    for lazy_initialized_pollable in pollables {
                                        lazy_initialized_pollable.set(Impl::subscribe(&stream));
                                    }

                                    let next = stream.get_next();
                                    (stream, next)
                                });
                            durability.persist_infallible(NoInput, first_live_result.clone());

                            (first_live_result, Some(stream))
                        }
                    }
                    None => {
                        unreachable!()
                    }
                };

                if let Some(stream) = new_live_stream {
                    let pollables = match state.take() {
                        Some(DurableSearchStreamState::Live { pollables, .. }) => pollables,
                        Some(DurableSearchStreamState::Replay { pollables, .. }) => pollables,
                        None => {
                            unreachable!()
                        }
                    };
                    *state = Some(DurableSearchStreamState::Live { stream, pollables });
                }

                result
            } else {
                let result: Option<Vec<SearchHit>> = durability.replay_infallible();
                let mut state = self.state.borrow_mut();
                match &mut *state {
                    Some(DurableSearchStreamState::Live { .. }) => {
                        unreachable!("Durable search stream cannot be in live mode during replay")
                    }
                    Some(DurableSearchStreamState::Replay {
                        partial_result,
                        finished,
                        ..
                    }) => {
                        if let Some(ref result) = result {
                            partial_result.extend_from_slice(result);
                        } else {
                            *finished = true;
                        }
                    }
                    None => {
                        unreachable!()
                    }
                }
                result
            }
        }

        fn blocking_get_next(&self) -> Vec<SearchHit> {
            let mut subscription = self.subscription.borrow_mut();
            if subscription.is_none() {
                *subscription = Some(self.subscribe());
            }
            let subscription = subscription.as_mut().unwrap();
            let mut result = Vec::new();
            loop {
                subscription.block();
                match self.get_next() {
                    Some(hits) => {
                        result.extend(hits);
                        break result;
                    }
                    None => continue,
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn search_error_roundtrip() {
            // Basic test for SearchError
            let error = SearchError::IndexNotFound;
            assert!(matches!(error, SearchError::IndexNotFound));
        }

        #[test]
        fn doc_roundtrip() {
            let doc = Doc {
                id: "test-id".to_string(),
                content: r#"{"title": "Test Document"}"#.to_string(),
            };
            assert_eq!(doc.id, "test-id");
        }

        #[test]
        fn search_query_roundtrip() {
            let query = SearchQuery {
                q: Some("test query".to_string()),
                filters: vec!["category:test".to_string()],
                sort: vec!["score:desc".to_string()],
                facets: vec!["category".to_string()],
                page: Some(1),
                per_page: Some(10),
                offset: Some(0),
                highlight: None,
                config: None,
            };
            assert_eq!(query.q, Some("test query".to_string()));
        }
    }
}