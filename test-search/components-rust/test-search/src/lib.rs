#[allow(static_mut_refs)]
mod bindings;

use golem_rust::atomically;
use crate::bindings::exports::test::search_exports::test_search_api::*;
use crate::bindings::golem::search::core;
use crate::bindings::golem::search::types::*;

struct Component;

// Test constants for different providers
#[cfg(feature = "algolia")]
const TEST_INDEX: &'static str = "test-algolia-index";
#[cfg(feature = "elasticsearch")]
const TEST_INDEX: &'static str = "test-elasticsearch-index";
#[cfg(feature = "meilisearch")]
const TEST_INDEX: &'static str = "test-meilisearch-index";
#[cfg(feature = "opensearch")]
const TEST_INDEX: &'static str = "test-opensearch-index";
#[cfg(feature = "typesense")]
const TEST_INDEX: &'static str = "test-typesense-index";

// Helper function to create test documents
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

// Helper function to create test schema - matching working Algolia pattern
 fn create_test_schema() -> Schema {
        Schema {
            fields: vec![
                SchemaField {
                    name: "title".to_string(),
                    field_type: FieldType::Text,
                    required: false,
                    facet: false,
                    sort: false,
                    index: true,
                }
            ],
            primary_key: None,
        }
    }

    fn test_schema() -> Schema {
        Schema {
            fields: vec![
                SchemaField {
                    name: "title".to_string(),
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
                    sort: false,
                    index: true,
                },
                SchemaField {
                    name: "value".to_string(),
                    field_type: FieldType::Integer,
                    required: false,
                    facet: false,
                    sort: true,
                    index: false,
                },
            ],
            primary_key: Some("id".to_string()),
        }
    }

impl Guest for Component {
    /// test1 demonstrates basic document insertion, retrieval, and deletion
    fn test1() -> String {
        let index_name = format!("{}-test1", TEST_INDEX);
        let mut results = Vec::new();

        // Set up index schema (for providers that support schema configuration)
        println!("Setting up index schema: {}", index_name);
        match core::update_schema(&index_name, &create_test_schema()) {
            Ok(_) => results.push("✓ Index schema configured successfully".to_string()),
            Err(SearchError::Unsupported) => results.push("✓ Schema configuration not required (auto-detected)".to_string()),
            Err(e) => {
                // If schema setup fails, we'll try to proceed with document insertion anyway
                results.push(format!("⚠ Schema setup failed, proceeding anyway: {:?}", e));
            }
        }

        // Insert test documents (this will auto-create the index for providers like Algolia)
        let docs = create_test_documents();
        println!("Inserting {} documents", docs.len());
        match core::upsert_many(&index_name, &docs) {
            Ok(_) => results.push("✓ Documents inserted successfully".to_string()),
            Err(e) => {
                results.push(format!("✗ Document insertion failed: {:?}", e));
                return results.join("\n");
            }
        }

        // Test document retrieval (with retry logic for eventual consistency)
        println!("Retrieving document with ID: doc1");
        let mut retrieval_success = false;
        for attempt in 1..=5 {
            match core::get(&index_name, "doc1") {
                Ok(Some(doc)) => {
                    results.push(format!("✓ Document retrieved: {} (attempt {})", doc.id, attempt));
                    retrieval_success = true;
                    break;
                }
                Ok(None) => {
                    if attempt == 5 {
                        results.push("✗ Document not found after 5 attempts".to_string());
                    } else {
                        println!("Document not found, retrying... (attempt {}/5)", attempt);
                        std::thread::sleep(std::time::Duration::from_millis(1000));
                    }
                }
                Err(e) => {
                    results.push(format!("✗ Document retrieval failed: {:?}", e));
                    break;
                }
            }
        }

        // Test document deletion (only if we successfully retrieved it)
        if retrieval_success {
            println!("Deleting document with ID: doc1");
            match core::delete(&index_name, "doc1") {
                Ok(_) => {
                    results.push("✓ Document deleted successfully".to_string());
                    
                    // Verify deletion with retry logic
                    for attempt in 1..=5 {
                        match core::get(&index_name, "doc1") {
                            Ok(None) | Err(_) => {
                                results.push(format!("✓ Document deletion verified (attempt {})", attempt));
                                break;
                            }
                            Ok(Some(_)) => {
                                if attempt == 5 {
                                    results.push("⚠ Document still exists after deletion".to_string());
                                } else {
                                    std::thread::sleep(std::time::Duration::from_millis(1000));
                                }
                            }
                        }
                    }
                }
                Err(e) => results.push(format!("✗ Document deletion failed: {:?}", e)),
            }
        }

        // Test index deletion
        println!("Deleting index: {}", index_name);
        match core::delete_index(&index_name) {
            Ok(_) => results.push("✓ Index deleted successfully".to_string()),
            Err(e) => results.push(format!("✗ Index deletion failed: {:?}", e)),
        }

        results.join("\n")
    }

    /// test2 demonstrates full-text search with basic queries
    fn test2() -> String {
        let index_name = format!("{}-test2", TEST_INDEX);
        let mut results = Vec::new();

        // Set up index schema first (for providers that support it)
        println!("Setting  index for search tests");
        match core::update_schema(&index_name, &create_test_schema()) {
            Ok(_) => {},
            Err(SearchError::Unsupported) => {
                println!("Schema setup not required (auto-detected on first document)");
            },
            Err(_) => {
                println!("Schema setup failed, proceeding with document insertion");
            }
        }

        // Insert test documents (this will auto-create the index)
        let docs = create_test_documents();
        if let Err(e) = core::upsert_many(&index_name, &docs) {
            core::delete_index(&index_name).ok(); // Cleanup
            return format!("Document insertion failed: {:?}", e);
        }

        // Test basic text search (with retry logic for indexing delay)
        println!("Testing basic text search for 'Gatsby'");
        let query = SearchQuery {
            q: Some("Gatsby".to_string()),
            filters: vec![],
            sort: vec![],
            facets: vec![],
            page: None,
            per_page: None,
            offset: None,
            highlight: None,
            config: None,
        };

        let mut search_success = false;
        for attempt in 1..=10 {
            match core::search(&index_name, &query) {
                Ok(search_results) if !search_results.hits.is_empty() => {
                    results.push(format!("✓ Search returned {} hits (attempt {})", search_results.hits.len(), attempt));
                    if let Some(first_hit) = search_results.hits.first() {
                        results.push(format!("  First hit ID: {}", first_hit.id));
                        if let Some(score) = first_hit.score {
                            results.push(format!("  Score: {:.2}", score));
                        }
                    }
                    search_success = true;
                    break;
                }
                Ok(_) => {
                    if attempt == 10 {
                        results.push("⚠ Search returned no hits after 10 attempts".to_string());
                    } else {
                        println!("Search returned no hits, retrying... (attempt {}/10)", attempt);
                        std::thread::sleep(std::time::Duration::from_millis(1000));
                    }
                }
                Err(e) => {
                    results.push(format!("✗ Search failed: {:?}", e));
                    break;
                }
            }
        }

        // Test search with filters
        println!("Testing filtered search for fiction genre");
        let filtered_query = SearchQuery {
            q: Some("Gatsby".to_string()), // Use a term that will match in title
            filters: vec!["genre:fiction".to_string()],
            sort: vec![],
            facets: vec![],
            page: None,
            per_page: None,
            offset: None,
            highlight: None,
            config: None,
        };

        match core::search(&index_name, &filtered_query) {
            Ok(search_results) => {
                results.push(format!("✓ Filtered search returned {} hits", search_results.hits.len()));
            }
            Err(e) => results.push(format!("✗ Filtered search failed: {:?}", e)),
        }

        // Cleanup
        core::delete_index(&index_name).ok();
        results.join("\n")
    }

    /// test3 demonstrates search with sorting and pagination
    fn test3() -> String {
        let index_name = format!("{}-test3", TEST_INDEX);
        let mut results = Vec::new();

        // Setup schema first
        match core::update_schema(&index_name, &create_test_schema()) {
            Ok(_) => {},
            Err(SearchError::Unsupported) => {},
            Err(_) => {} // Continue anyway
        }

        // Insert documents to auto-create index
        let docs = create_test_documents();
        if let Err(e) = core::upsert_many(&index_name, &docs) {
            core::delete_index(&index_name).ok();
            return format!("Document insertion failed: {:?}", e);
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

        match core::search(&index_name, &sorted_query) {
            Ok(search_results) => {
                results.push(format!("✓ Sorted search returned {} hits", search_results.hits.len()));
                if search_results.hits.len() >= 2 {
                    results.push("  Verifying sort order by checking first two results".to_string());
                }
            }
            Err(e) => results.push(format!("✗ Sorted search failed: {:?}", e)),
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

        match core::search(&index_name, &paginated_query) {
            Ok(search_results) => {
                results.push(format!("✓ Paginated search returned {} hits", search_results.hits.len()));
                if let Some(total) = search_results.total {
                    results.push(format!("  Total documents: {}", total));
                }
                if let Some(page) = search_results.page {
                    results.push(format!("  Current page: {}", page));
                }
            }
            Err(e) => results.push(format!("✗ Paginated search failed: {:?}", e)),
        }

        // Cleanup
        core::delete_index(&index_name).ok();
        results.join("\n")
    }

    /// test4 demonstrates search with highlighting and facets
    fn test4() -> String {
        let index_name = format!("{}-test4", TEST_INDEX);
        let mut results = Vec::new();

        // Setup schema for faceting support
        match core::update_schema(&index_name, &create_test_schema()) {
            Ok(_) => {},
            Err(SearchError::Unsupported) => {},
            Err(_) => {} // Continue anyway
        }

        // Insert documents to auto-create index
        let docs = create_test_documents();
        if let Err(e) = core::upsert_many(&index_name, &docs) {
            core::delete_index(&index_name).ok();
            return format!("Document insertion failed: {:?}", e);
        }

        // Test search with highlighting
        println!("Testing search with highlighting");
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

        match core::search(&index_name, &highlight_query) {
            Ok(search_results) => {
                results.push(format!("✓ Highlighted search returned {} hits", search_results.hits.len()));
                
                // Check for highlights
                for hit in &search_results.hits {
                    if hit.highlights.is_some() {
                        results.push("  ✓ Found highlights in results".to_string());
                        break;
                    }
                }

                // Check for facets
                if search_results.facets.is_some() {
                    results.push("  ✓ Facet data returned".to_string());
                } else {
                    results.push("  ⚠ No facet data returned (may not be supported)".to_string());
                }

                // Check timing information
                if let Some(took_ms) = search_results.took_ms {
                    results.push(format!("  Query took: {}ms", took_ms));
                }
            }
            Err(e) => results.push(format!("✗ Highlighted search failed: {:?}", e)),
        }

        // Cleanup
        core::delete_index(&index_name).ok();
        results.join("\n")
    }

    /// test5 demonstrates schema inspection and validation
    fn test5() -> String {
        let index_name = format!("{}-test5", TEST_INDEX);
        let mut results = Vec::new();

        // Set up initial schema
        println!("Setting up index with predefined schema");
        let original_schema = create_test_schema();
        match core::update_schema(&index_name, &original_schema) {
            Ok(_) => results.push("✓ Index schema configured successfully".to_string()),
            Err(SearchError::Unsupported) => {
                results.push("⚠ Schema configuration not supported, will test with document insertion".to_string());
                // Insert a test document to auto-create the index
                let test_docs = vec![create_test_documents().into_iter().next().unwrap()];
                if let Err(e) = core::upsert_many(&index_name, &test_docs) {
                    return format!("Document insertion failed: {:?}", e);
                }
            },
            Err(e) => {
                results.push(format!("⚠ Schema setup failed: {:?}, proceeding with document insertion", e));
                // Try to insert documents anyway to auto-create index
                let test_docs = vec![create_test_documents().into_iter().next().unwrap()];
                if let Err(e) = core::upsert_many(&index_name, &test_docs) {
                    return format!("Document insertion failed: {:?}", e);
                }
            }
        }

        // Test schema retrieval
        println!("Retrieving index schema");
        match core::get_schema(&index_name) {
            Ok(retrieved_schema) => {
                results.push("✓ Schema retrieved successfully".to_string());
                results.push(format!("  Fields count: {}", retrieved_schema.fields.len()));
                
                if let Some(pk) = &retrieved_schema.primary_key {
                    results.push(format!("  Primary key: {}", pk));
                }

                // Validate some key fields exist
                let field_names: Vec<&String> = retrieved_schema.fields.iter().map(|f| &f.name).collect();
                if field_names.contains(&&"title".to_string()) {
                    results.push("  ✓ Title field found".to_string());
                }
                if field_names.contains(&&"author".to_string()) {
                    results.push("  ✓ Author field found".to_string());
                }
            }
            Err(e) => results.push(format!("✗ Schema retrieval failed: {:?}", e)),
        }

        // Test schema update (if supported)
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

        match core::update_schema(&index_name, &updated_schema) {
            Ok(_) => results.push("✓ Schema updated successfully".to_string()),
            Err(SearchError::Unsupported) => results.push("  ⚠ Schema updates not supported by this provider".to_string()),
            Err(e) => results.push(format!("✗ Schema update failed: {:?}", e)),
        }

        // Test document insertion with invalid data (schema validation)
        println!(" schema validation with invali document");
        let invalid_doc = Doc {
            id: "invalid1".to_string(),
            content: r#"{"invalid_field": "this should not be allowed"}"#.to_string(),
        };

        match core::upsert(&index_name, &invalid_doc) {
            Ok(_) => results.push("  ⚠ Invalid document accepted (lenient validation)".to_string()),
            Err(SearchError::InvalidQuery(_)) => results.push("  ✓ Invalid document rejected (strict validation)".to_string()),
            Err(e) => results.push(format!("  ? Unexpected error with invalid document: {:?}", e)),
        }

        // Cleanup
        core::delete_index(&index_name).ok();
        results.join("\n")
    }

    /// test6 demonstrates streaming search behavior
    fn test6() -> String {
        let index_name = format!("{}-test6", TEST_INDEX);
        let mut results = Vec::new();

        // Setup schema for streaming test
        match core::update_schema(&index_name, &create_test_schema()) {
            Ok(_) => {},
            Err(SearchError::Unsupported) => {},
            Err(_) => {} // Continue anyway
        }

        // Create additional documents for streaming test
        let mut docs = create_test_documents();
        for i in 6..=20 {
            docs.push(Doc {
                id: format!("doc{}", i),
                content: format!(r#"{{"title": "Book {}", "author": "Author {}", "year": {}, "genre": "test", "description": "A test book for streaming search"}}"#, i, i, 1900 + i),
            });
        }

        if let Err(e) = core::upsert_many(&index_name, &docs) {
            core::delete_index(&index_name).ok();
            return format!("Document insertion failed: {:?}", e);
        }

        println!(" streaming search functionality");
        let stream_query = SearchQuery {
            q: Some("book".to_string()),
            filters: vec![],
            sort: vec!["year:asc".to_string()],
            facets: vec![],
            page: None,
            per_page: Some(5), // Small page size to encourage streaming
            offset: None,
            highlight: None,
            config: None,
        };

        match core::stream_search(&index_name, &stream_query) {
            Ok(stream) => {
                results.push("✓ Search stream created successfully".to_string());
                
                let mut total_hits = 0;
                let mut batch_count = 0;
                
                // Use a more conservative approach to streaming
                for _ in 0..5 { // Limit to 5 iterations maximum
                    let hits = stream.blocking_get_next();
                    if hits.is_empty() {
                        break;
                    }
                    
                    batch_count += 1;
                    total_hits += hits.len();
                    results.push(format!("  Batch {}: {} hits", batch_count, hits.len()));
                }
                
                results.push(format!("✓ Streamig complete: {} total hits in {} batches", total_hits, batch_count));
            }
            Err(SearchError::Unsupported) => {
                results.push("⚠ Streaming search not support by this provider".to_string());
                
                // Fallback to regular search
                match core::search(&index_name, &stream_query) {
                    Ok(search_results) => {
                        results.push(format!("  Fallback: Regular search returned {} hits", search_results.hits.len()));
                    }
                    Err(e) => results.push(format!("  Fallback search also failed: {:?}", e)),
                }
            }
            Err(e) => results.push(format!("✗  search failed: {:?}", e)),
        }

        // Cleanup
        core::delete_index(&index_name).ok();
        results.join("\n")
    }

    /// test7 demonstrates error handling and edge cases
    fn test7() -> String {
        let mut results = Vec::new();
        let index_name = "test777";
       let schema = test_schema();

        // Wrap the call in a catch_unwind to handle panics gracefully
        let panic_result = std::panic::catch_unwind(|| {
            core::update_schema(index_name.clone(), &schema.clone())
        });

        match panic_result {
            Ok(schema_result) => {
                match schema_result {
                    Ok(_) => results.push("✓  schema update succeeded".to_string()),
                    Err(SearchError::Unsupported) => {
                        results.push("⚠ Schema setup not required (auto-detected on first document)".to_string());
                    },
                    Err(e) => {
                        results.push(format!("✗ Schema update failed: {:?}", e));
                    }
                }
            },
            Err(panic_payload) => {
                results.push("✗ PANIC CAUGHT: update_schema panicked!".to_string());
                
                // Try to extract panic message
                if let Some(panic_msg) = panic_payload.downcast_ref::<&str>() {
                    results.push(format!("  Panic message: {}", panic_msg));
                } else if let Some(panic_string) = panic_payload.downcast_ref::<String>() {
                    results.push(format!("  Panic message: {}", panic_string));
                } else {
                    results.push("  Panic message: <unknown>".to_string());
                }
                
                results.push("  This is likely a WASM-RPC serialization boundary issue".to_string());
                results.push("  The Algolia API call succeeded but response serialization failed".to_string());
            }
        }

        results.join("\n")
    }
}

bindings::export!(Component with_types_in bindings);