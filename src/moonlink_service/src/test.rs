use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use bytes::Bytes;
use moonlink::row::{moonlink_row_to_proto, MoonlinkRow, RowValue};
use more_asserts as ma;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use serial_test::serial;
use tokio::net::TcpStream;

use crate::rest_api::{
    CreateTableResponse, FileUploadResponse, GetTableSchemaResponse, HealthResponse,
    IngestResponse, ListTablesResponse,
};
use crate::start_with_config;
use crate::test_guard::TestGuard;
use crate::test_utils::*;
use moonlink::decode_serialized_read_state_for_testing;
use moonlink_backend::table_status::TableStatus;
use moonlink_rpc::{load_files, scan_table_begin, scan_table_end};

/// Util function to get table creation payload.
fn get_create_table_payload(database: &str, table: &str) -> serde_json::Value {
    let create_table_payload = json!({
        "database": database,
        "table": table,
        "schema": [
            {"name": "id", "data_type": "int32", "nullable": false},
            {"name": "name", "data_type": "string", "nullable": false},
            {"name": "email", "data_type": "string", "nullable": true},
            {"name": "age", "data_type": "int32", "nullable": true}
        ],
        "table_config": {
            "mooncake": {
                "append_only": true,
                "row_identity": "None"
            }
        }
    });
    create_table_payload
}

/// Optional nested schema payload for testing nested struct/list without adding a new test.
fn get_create_table_payload_nested(database: &str, table: &str) -> serde_json::Value {
    json!({
        "database": database,
        "table": table,
        "schema": [
            {"name": "id", "data_type": "int32", "nullable": false},
            {"name": "user", "data_type": "struct", "nullable": true, "fields": [
                {"name": "name", "data_type": "string", "nullable": true},
                {"name": "age", "data_type": "int32", "nullable": true},
                {"name": "emails", "data_type": "list", "nullable": true, "item": {"name": "email", "data_type": "string", "nullable": true}},
                {"name": "location", "data_type": "struct", "nullable": true, "fields": [
                    {"name": "lat", "data_type": "float64", "nullable": true},
                    {"name": "lon", "data_type": "float64", "nullable": true}
                ]}
            ]},
            {"name": "events", "data_type": "list", "nullable": true, "item": {"name": "ts", "data_type": "int64", "nullable": true}}
        ],
        "table_config": {"mooncake": {"append_only": true, "row_identity": "None"}}
    })
}

/// Util function to get table drop payload.
fn get_drop_table_payload(database: &str, table: &str) -> serde_json::Value {
    let drop_table_payload = json!({
        "database": database,
        "table": table
    });
    drop_table_payload
}

/// Util function to get table optimize payload.
fn get_optimize_table_payload(database: &str, table: &str, mode: &str) -> serde_json::Value {
    let optimize_table_payload = json!({
        "database": database,
        "table": table,
        "mode": mode
    });
    optimize_table_payload
}

/// Util function to get create snapshot payload.
fn get_create_snapshot_payload(database: &str, table: &str, lsn: u64) -> serde_json::Value {
    let snapshot_creation_payload = json!({
        "database": database,
        "table": table,
        "lsn": lsn
    });
    snapshot_creation_payload
}

/// Util function to get table flush payload.
fn get_flush_table_payload(database: &str, table: &str, lsn: u64) -> serde_json::Value {
    let flush_table_payload = json!({
        "database": database,
        "table": table,
        "lsn": lsn
    });
    flush_table_payload
}

/// Util function to create table via REST API.
async fn create_table(client: &reqwest::Client, database: &str, table: &str, nested: bool) {
    // REST API doesn't allow duplicate source table name.
    let crafted_src_table_name = format!("{database}.{table}");

    // Use nested schema when explicitly requested to keep tests lightweight and explicit.
    let payload = if nested {
        get_create_table_payload_nested(database, table)
    } else {
        get_create_table_payload(database, table)
    };
    let response = client
        .post(format!("{REST_ADDR}/tables/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
}

/// Util function to drop table via REST API.
async fn drop_table(client: &reqwest::Client, database: &str, table: &str) {
    let payload = get_drop_table_payload(database, table);
    let crafted_src_table_name = format!("{database}.{table}");
    let response = client
        .delete(format!("{REST_ADDR}/tables/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
}

async fn list_tables(client: &reqwest::Client) -> Vec<TableStatus> {
    let response = client
        .get(format!("{REST_ADDR}/tables"))
        .header("content-type", "application/json")
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
    let response: ListTablesResponse = response.json().await.unwrap();
    response.tables
}

/// Util function to load all record batches inside of the given [`path`].
async fn read_all_batches(url: &str) -> Vec<RecordBatch> {
    let resp = reqwest::get(url).await.unwrap();
    assert!(resp.status().is_success(), "Response status is {resp:?}");
    let data: Bytes = resp.bytes().await.unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .build()
        .unwrap();

    reader.into_iter().map(|b| b.unwrap()).collect()
}

#[tokio::test]
#[serial]
async fn test_health_check_endpoint() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{REST_ADDR}/health"))
        .header("content-type", "application/json")
        .send()
        .await
        .unwrap();

    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
    let response: HealthResponse = response.json().await.unwrap();
    assert_eq!(response.service, "moonlink-rest-api");
    assert_eq!(response.status, "healthy");
    ma::assert_gt!(response.timestamp, 0);
}

#[tokio::test]
#[serial]
async fn test_schema() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table.
    let client = reqwest::Client::new();
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let payload = json!({
        "database": DATABASE,
        "table": TABLE,
        "schema": [
            {"name": "int32", "data_type": "int32", "nullable": false},
            {"name": "int64", "data_type": "int64", "nullable": false},
            {"name": "string", "data_type": "string", "nullable": false},
            {"name": "text", "data_type": "text", "nullable": false},
            {"name": "bool", "data_type": "bool", "nullable": false},
            {"name": "boolean", "data_type": "boolean", "nullable": false},
            {"name": "float32", "data_type": "float32", "nullable": false},
            {"name": "float64", "data_type": "float64", "nullable": false},
            {"name": "date32", "data_type": "date32", "nullable": false},
            {"name": "decimal(10)", "data_type": "decimal(10,2)", "nullable": false}, // only precision value
            {"name": "decimal(10,2)", "data_type": "decimal(10,2)", "nullable": false}, // lowercase
            {"name": "Decimal(10,2)", "data_type": "decimal(10,2)", "nullable": false}, // uppercase
            {"name": "Decimal(10,0)", "data_type": "decimal(10,0)", "nullable": false}, // scale = 0
            {"name": "Decimal(2,-3)", "data_type": "decimal(2,-3)", "nullable": false}, // negative scale value
            {"name": "props", "data_type": "struct", "nullable": true, "fields": [
                {"name": "score", "data_type": "float64", "nullable": true},
                {"name": "labels", "data_type": "list", "nullable": true, "item": {"name": "label", "data_type": "string", "nullable": true}},
                {"name": "coords", "data_type": "struct", "nullable": true, "fields": [
                    {"name": "x", "data_type": "int32", "nullable": true},
                    {"name": "y", "data_type": "int32", "nullable": true}
                ]}
            ]},
            {"name": "history", "data_type": "list", "nullable": true, "item": {"name": "ts", "data_type": "int64", "nullable": true}}
        ],
        "table_config": {
            "mooncake": {
                "append_only": true
            }
        }
    });
    let response = client
        .post(format!("{REST_ADDR}/tables/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
    let response: CreateTableResponse = response.json().await.unwrap();
    assert_eq!(response.database, DATABASE);
    assert_eq!(response.table, crafted_src_table_name);
    assert_eq!(response.lsn, 1);
}

/// Util function to optimize table via REST API.
async fn optimize_table(client: &reqwest::Client, database: &str, table: &str, mode: &str) {
    let payload = get_optimize_table_payload(database, table, mode);
    let crafted_src_table_name = format!("{database}.{table}");
    let response = client
        .post(format!(
            "{REST_ADDR}/tables/{crafted_src_table_name}/optimize"
        ))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
}

/// Util function to test optimize table
async fn run_optimize_table_test(mode: &str) {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // Ingest some data.
    let insert_payload = json!({
        "operation": "insert",
        "request_mode": "async",
        "data": {
            "id": 1,
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "age": 30
        }
    });
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let response = client
        .post(format!("{REST_ADDR}/ingest/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&insert_payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );

    // test for optimize table on mode 'full'
    optimize_table(&client, DATABASE, TABLE, mode).await;

    // Scan table and get data file and puffin files back.
    let mut moonlink_stream = TcpStream::connect(MOONLINK_ADDR).await.unwrap();
    let bytes = scan_table_begin(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
        /*lsn=*/ 1,
    )
    .await
    .unwrap();
    let (data_file_paths, puffin_file_paths, puffin_deletion, positional_deletion) =
        decode_serialized_read_state_for_testing(bytes);
    assert_eq!(data_file_paths.len(), 1);
    let record_batches = read_all_batches(&data_file_paths[0]).await;
    let expected_arrow_batch = create_test_arrow_batch();
    assert_eq!(record_batches, vec![expected_arrow_batch]);

    assert!(puffin_file_paths.is_empty());
    assert!(puffin_deletion.is_empty());
    assert!(positional_deletion.is_empty());

    scan_table_end(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
    )
    .await
    .unwrap();
}

#[tokio::test]
#[serial]
async fn test_optimize_table_on_full_mode() {
    run_optimize_table_test("full").await;
}

#[tokio::test]
#[serial]
async fn test_optimize_table_on_index_mode() {
    run_optimize_table_test("index").await;
}

#[tokio::test]
#[serial]
async fn test_optimize_table_on_data_mode() {
    run_optimize_table_test("data").await;
}

/// Util function to sync flush via REST API.
async fn flush_table(client: &reqwest::Client, database: &str, table: &str, lsn: u64) {
    let payload = get_flush_table_payload(database, table, lsn);
    let crafted_src_table_name = format!("{database}.{table}");
    let response = client
        .post(format!("{REST_ADDR}/tables/{crafted_src_table_name}/flush"))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
}

/// Util function to create snapshot via REST API.
async fn create_snapshot(client: &reqwest::Client, database: &str, table: &str, lsn: u64) {
    let payload = get_create_snapshot_payload(database, table, lsn);
    let crafted_src_table_name = format!("{database}.{table}");
    let response = client
        .post(format!(
            "{REST_ADDR}/tables/{crafted_src_table_name}/snapshot"
        ))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
}

#[tokio::test]
#[serial]
async fn test_table_schema_fetch() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // Get schema back.
    let payload = json!({
        "database": DATABASE,
        "table:": TABLE
    });
    let response = client
        .get(format!("{REST_ADDR}/schema/{DATABASE}/{TABLE}"))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
    let response: GetTableSchemaResponse = response.json().await.unwrap();

    // Validate schema.
    let serialized_schema = response.serialized_schema;
    let reader = StreamReader::try_new(serialized_schema.as_slice(), /*projection=*/ None).unwrap();
    let schema = reader.schema();
    assert_eq!(schema, create_test_arrow_schema());
}

/// Test Create Snapshot
#[tokio::test]
#[serial]
async fn test_create_snapshot() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // Ingest some data.
    let insert_payload = json!({
        "operation": "insert",
        "request_mode": "sync",
        "data": {
            "id": 1,
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "age": 30
        }
    });
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let response = client
        .post(format!("{REST_ADDR}/ingest/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&insert_payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
    let response: IngestResponse = response.json().await.unwrap();
    let lsn = response.lsn.unwrap();

    // After all changes reflected at mooncake snapshot, flush table and trigger an iceberg snapshot.
    flush_table(&client, DATABASE, TABLE, lsn).await;
    create_snapshot(&client, DATABASE, TABLE, lsn).await;

    let mut moonlink_stream = TcpStream::connect(MOONLINK_ADDR).await.unwrap();
    let bytes = scan_table_begin(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
        /*lsn=*/ lsn,
    )
    .await
    .unwrap();
    let (data_file_paths, puffin_file_paths, puffin_deletion, positional_deletion) =
        decode_serialized_read_state_for_testing(bytes);
    assert_eq!(data_file_paths.len(), 1);
    let record_batches = read_all_batches(&data_file_paths[0]).await;
    let expected_arrow_batch = create_test_arrow_batch();
    assert_eq!(record_batches, vec![expected_arrow_batch]);

    assert!(puffin_file_paths.is_empty());
    assert!(puffin_deletion.is_empty());
    assert!(positional_deletion.is_empty());

    scan_table_end(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
    )
    .await
    .unwrap();
}

/// Test basic table creation, insertion and query.
#[tokio::test]
#[serial]
async fn test_moonlink_standalone_data_ingestion() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table (nested schema).
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ true).await;

    // Ingest nested data.
    let insert_payload = create_test_arrow_insert_payload_nested();
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let response = client
        .post(format!("{REST_ADDR}/ingest/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&insert_payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );

    // Scan table and get data file and puffin files back.
    let mut moonlink_stream = TcpStream::connect(MOONLINK_ADDR).await.unwrap();
    let bytes = scan_table_begin(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
        /*lsn=*/ 1,
    )
    .await
    .unwrap();
    let (data_file_paths, puffin_file_paths, puffin_deletion, positional_deletion) =
        decode_serialized_read_state_for_testing(bytes);
    assert_eq!(data_file_paths.len(), 1);
    let record_batches = read_all_batches(&data_file_paths[0]).await;
    let expected_arrow_batch = create_test_arrow_batch_nested();
    assert_eq!(record_batches, vec![expected_arrow_batch]);

    assert!(puffin_file_paths.is_empty());
    assert!(puffin_deletion.is_empty());
    assert!(positional_deletion.is_empty());

    scan_table_end(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
    )
    .await
    .unwrap();
}

/// Test basic table creation, file upload and query.
#[tokio::test]
#[serial]
async fn test_moonlink_standalone_file_upload() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // Upload a file.
    let parquet_file = generate_parquet_file(&get_moonlink_backend_dir()).await;
    let file_upload_payload = json!({
        "operation": "upload",
        "request_mode": "sync",
        "files": [parquet_file],
        "storage_config": {
            "fs": {
                "root_directory": get_moonlink_backend_dir(),
                "atomic_write_dir": get_moonlink_backend_dir()
            }
        }
    });
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let response = client
        .post(format!("{REST_ADDR}/upload/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&file_upload_payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
    let response: FileUploadResponse = response.json().await.unwrap();
    let lsn = response.lsn.unwrap();

    // Scan table and get data file and puffin files back.
    let mut moonlink_stream = TcpStream::connect(MOONLINK_ADDR).await.unwrap();
    let bytes = scan_table_begin(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
        lsn,
    )
    .await
    .unwrap();
    let (data_file_paths, puffin_file_paths, puffin_deletion, positional_deletion) =
        decode_serialized_read_state_for_testing(bytes);
    assert_eq!(data_file_paths.len(), 1);
    let record_batches = read_all_batches(&data_file_paths[0]).await;
    let expected_arrow_batch = create_test_arrow_batch();
    assert_eq!(record_batches, vec![expected_arrow_batch]);

    assert!(puffin_file_paths.is_empty());
    assert!(puffin_deletion.is_empty());
    assert!(positional_deletion.is_empty());

    scan_table_end(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
    )
    .await
    .unwrap();
}

#[tokio::test]
#[serial]
async fn test_moonlink_standalone_protobuf_ingestion() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // Build protobuf payload for a single row.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("Alice Johnson".as_bytes().to_vec()),
        RowValue::ByteArray("alice@example.com".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    let proto_row = moonlink_row_to_proto(row);
    let mut buf = Vec::new();
    prost::Message::encode(&proto_row, &mut buf).unwrap();

    let insert_payload = json!({
        "operation": "insert",
        "request_mode": "sync",
        "data": buf
    });
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let response = client
        .post(format!("{REST_ADDR}/ingestpb/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&insert_payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
    let response: IngestResponse = response.json().await.unwrap();
    let lsn = response.lsn.unwrap();

    // Scan table and get data file and puffin files back.
    let mut moonlink_stream = TcpStream::connect(MOONLINK_ADDR).await.unwrap();
    let bytes = scan_table_begin(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
        lsn,
    )
    .await
    .unwrap();
    let (data_file_paths, puffin_file_paths, puffin_deletion, positional_deletion) =
        decode_serialized_read_state_for_testing(bytes);
    assert_eq!(data_file_paths.len(), 1);
    let record_batches = read_all_batches(&data_file_paths[0]).await;
    let expected_arrow_batch = create_test_arrow_batch();
    assert_eq!(record_batches, vec![expected_arrow_batch]);

    assert!(puffin_file_paths.is_empty());
    assert!(puffin_deletion.is_empty());
    assert!(positional_deletion.is_empty());

    scan_table_end(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
    )
    .await
    .unwrap();
}

/// Test basic table creation, file insert and query.
#[tokio::test]
#[serial]
async fn test_moonlink_standalone_file_insert() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // Upload a file.
    let parquet_file = generate_parquet_file(&get_moonlink_backend_dir()).await;
    let file_upload_payload = json!({
        "operation": "insert",
        "files": [parquet_file],
        "request_mode": "async",
        "storage_config": {
            "fs": {
                "root_directory": get_moonlink_backend_dir(),
                "atomic_write_dir": get_moonlink_backend_dir()
            }
        }
    });
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let response = client
        .post(format!("{REST_ADDR}/upload/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&file_upload_payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );

    // Scan table and get data file and puffin files back.
    let mut moonlink_stream = TcpStream::connect(MOONLINK_ADDR).await.unwrap();
    let bytes = scan_table_begin(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
        // Four events generated: one append and one commit, with commit LSN 2.
        /*lsn=*/
        2,
    )
    .await
    .unwrap();
    let (data_file_paths, puffin_file_paths, puffin_deletion, positional_deletion) =
        decode_serialized_read_state_for_testing(bytes);
    assert_eq!(data_file_paths.len(), 1);
    let record_batches = read_all_batches(&data_file_paths[0]).await;
    let expected_arrow_batch = create_test_arrow_batch();
    assert_eq!(record_batches, vec![expected_arrow_batch]);

    assert!(puffin_file_paths.is_empty());
    assert!(puffin_deletion.is_empty());
    assert!(positional_deletion.is_empty());

    scan_table_end(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
    )
    .await
    .unwrap();
}

/// Testing scenario: two tables with the same name, but under different databases are created.
#[tokio::test]
#[serial]
async fn test_multiple_tables_creation() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create the first test table.
    let client: reqwest::Client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // Create the second test table.
    create_table(&client, "second-database", TABLE, /*nested=*/ false).await;
}

#[tokio::test]
#[serial]
async fn test_drop_and_recreate_table() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // List table before drop.
    let list_results = list_tables(&client).await;
    assert_eq!(list_results.len(), 1);

    // Drop test table.
    drop_table(&client, DATABASE, TABLE).await;

    // List table after drop.
    let list_results = list_tables(&client).await;
    assert_eq!(list_results.len(), 0);

    // Recreate the same table.
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // List table after recreation.
    let list_results = list_tables(&client).await;
    assert_eq!(list_results.len(), 1);
}

/// Testing scenario: create multiple tables, and check list table result.
#[tokio::test]
#[serial]
async fn test_list_tables() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create two test tables.
    let client: reqwest::Client = reqwest::Client::new();
    create_table(&client, "database-1", "table-1", /*nested=*/ false).await;
    create_table(&client, "database-2", "table-2", /*nested=*/ false).await;

    // List test tables.
    let mut list_results = list_tables(&client).await;
    assert_eq!(list_results.len(), 2);
    list_results.sort_by(|a, b| {
        (
            &a.database,
            &a.table,
            a.commit_lsn,
            a.flush_lsn,
            &a.iceberg_warehouse_location,
        )
            .cmp(&(
                &b.database,
                &b.table,
                b.commit_lsn,
                b.flush_lsn,
                &b.iceberg_warehouse_location,
            ))
    });

    // Validate list results.
    assert_eq!(list_results[0].database, "database-1");
    assert_eq!(list_results[0].table, "table-1");

    assert_eq!(list_results[1].database, "database-2");
    assert_eq!(list_results[1].table, "table-2");
}

/// Dummy testing for bulk ingest files into mooncake table.
#[tokio::test]
#[serial]
async fn test_bulk_ingest_files() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    let client: reqwest::Client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // A dummy stub-level interface testing.
    let mut moonlink_stream = TcpStream::connect(MOONLINK_ADDR).await.unwrap();
    load_files(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
        /*files=*/ vec![],
    )
    .await
    .unwrap();
}

/// ==========================
/// Failure tests
/// ==========================
///
/// Error case: invalid operation name.
#[tokio::test]
#[serial]
async fn test_invalid_operation() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // Test invalid operation to upload a file.
    let file_upload_payload = json!({
        "operation": "invalid_upload_operation",
        "files": ["parquet_file"],
        "storage_config": {
            "fs": {
                "root_directory": get_moonlink_backend_dir(),
                "atomic_write_dir": get_moonlink_backend_dir()
            }
        }
    });
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let response = client
        .post(format!("{REST_ADDR}/upload/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&file_upload_payload)
        .send()
        .await
        .unwrap();
    assert!(!response.status().is_success());

    // Test invalid operation to ingest data.
    let insert_payload = json!({
        "operation": "invalid_ingest_operation",
        "data": {
            "id": 1,
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "age": 30
        }
    });
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let response = client
        .post(format!("{REST_ADDR}/ingest/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&insert_payload)
        .send()
        .await
        .unwrap();
    assert!(!response.status().is_success());
}

/// Error case: non-existent source table.
#[tokio::test]
#[serial]
async fn test_non_existent_table() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create the test table.
    let client: reqwest::Client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // Test invalid operation to upload a file.
    let file_upload_payload = json!({
        "operation": "upload",
        "request_mode": "async",
        "files": ["parquet_file"],
        "storage_config": {
            "fs": {
                "root_directory": get_moonlink_backend_dir(),
                "atomic_write_dir": get_moonlink_backend_dir()
            }
        }
    });
    let _response = client
        .post(format!("{REST_ADDR}/upload/non_existent_source_table"))
        .header("content-type", "application/json")
        .json(&file_upload_payload)
        .send()
        .await
        .unwrap();
    // Make sure service doesn't crash.

    // Test invalid operation to ingest data.
    let insert_payload = json!({
        "operation": "invalid_ingest_operation",
        "data": {
            "id": 1,
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "age": 30
        }
    });
    let _response = client
        .post(format!("{REST_ADDR}/ingest/non_existent_source_table"))
        .header("content-type", "application/json")
        .json(&insert_payload)
        .send()
        .await
        .unwrap();
    // Make sure service doesn't crash.
}

/// Error case: create a mooncake table with an invalid table config.
#[tokio::test]
#[serial]
async fn test_create_table_with_invalid_config() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    let client: reqwest::Client = reqwest::Client::new();
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");

    // ==================
    // Invalid config 1
    // ==================
    //
    // Create an invalid config payload.
    let payload = json!({
        "database": DATABASE,
        "table": TABLE,
        "schema": [
            {"name": "id", "data_type": "int32", "nullable": false}
        ],
        "table_config": {
            "mooncake": {
                "append_only": false,
                "row_identity": "None"
            }
        }
    });
    let response = client
        .post(format!("{REST_ADDR}/tables/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(!response.status().is_success());

    // ==================
    // Invalid config 2
    // ==================
    //
    // Create an invalid config payload.
    let payload = json!({
        "database": DATABASE,
        "table": TABLE,
        "schema": [
            {"name": "id", "data_type": "int32", "nullable": false}
        ],
        "table_config": {
            "mooncake": {
                "append_only": true,
                "row_identity": "FullRow"
            }
        }
    });
    let response = client
        .post(format!("{REST_ADDR}/tables/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(!response.status().is_success());
}

#[tokio::test]
#[serial]
async fn test_schema_invalid_struct_missing_fields() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    let client = reqwest::Client::new();
    let crafted_src_table_name = format!("{DATABASE}.invalid-struct-table");
    let payload = json!({
        "database": DATABASE,
        "table": "invalid-struct-table",
        "schema": [
            {"name": "id", "data_type": "int32", "nullable": false},
            {"name": "bad_struct", "data_type": "struct", "nullable": true}
        ],
        "table_config": {"mooncake": {"append_only": true}}
    });

    let response = client
        .post(format!("{REST_ADDR}/tables/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(
        !response.status().is_success(),
        "Response status is {response:?}"
    );
}

#[tokio::test]
#[serial]
async fn test_schema_invalid_list_missing_item() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    let client = reqwest::Client::new();
    let crafted_src_table_name = format!("{DATABASE}.invalid-list-table");
    let payload = json!({
        "database": DATABASE,
        "table": "invalid-list-table",
        "schema": [
            {"name": "id", "data_type": "int32", "nullable": false},
            {"name": "bad_list", "data_type": "list", "nullable": true}
        ],
        "table_config": {"mooncake": {"append_only": true}}
    });

    let response = client
        .post(format!("{REST_ADDR}/tables/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(!response.status().is_success());
}

#[tokio::test]
#[serial]
async fn test_kafka_avro_stress_ingest() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table with Avro schema directly
    let client = reqwest::Client::new();
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let avro_schema_json = r#"{
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "age", "type": "int"}
        ]
    }"#;


    println!("avro_schema_json: {}", avro_schema_json);

    // Create table with Avro schema
    let create_table_payload = serde_json::json!({
        "database": DATABASE,
        "table": TABLE,
        "avro_schema": avro_schema_json,
        "table_config": {
            "mooncake": {
                "append_only": true,
                "row_identity": "None"
            }
        }
    });
    let resp = client
        .post(format!("{REST_ADDR}/tables/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&create_table_payload)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "failed to create table with Avro schema: {resp:?}"
    );

    // Build Avro Schema locally for encoding single-datum payloads
    let avro_schema = apache_avro::Schema::parse_str(avro_schema_json).unwrap();

    // Stress parameters
    #[cfg(debug_assertions)]
    let total_rows: usize = 100_000;
    #[cfg(not(debug_assertions))]
    let total_rows: usize = 1_000_000;
    let workers: usize = 50;
    let per_worker: usize = total_rows / workers;

    let ingestion_start = std::time::Instant::now();
    let progress_counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    // Spawn concurrent ingestion tasks
    // Spawn progress monitor
    let progress_monitor = progress_counter.clone();
    let total_rows_clone = total_rows;
    let monitor_handle = tokio::spawn(async move {
        let mut last_count = 0;
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            let current_count = progress_monitor.load(std::sync::atomic::Ordering::SeqCst);
            if current_count >= total_rows_clone {
                break;
            }
            let rate = (current_count - last_count) as f64 / 5.0;
            println!(
                "Progress: {}/{} rows ({:.1}%) - Rate: {:.0} rows/sec",
                current_count,
                total_rows_clone,
                current_count as f64 / total_rows_clone as f64 * 100.0,
                rate
            );
            last_count = current_count;
        }
    });
    let mut handles = Vec::with_capacity(workers);
    for w in 0..workers {
        let client_cloned = client.clone();
        let avro_schema_cloned = avro_schema.clone();
        let table_path = crafted_src_table_name.clone();
        let start_id = (w * per_worker) as i32;
        let progress_counter_clone = progress_counter.clone();
        handles.push(tokio::spawn(async move {
            let mut local_max_lsn: u64 = 0;
            for i in 0..per_worker {
                if i % 1000 == 0 {
                    println!("Worker {w}: processed {i}/{per_worker} rows");
                }
                let id = start_id + (i as i32);
                let name = format!("user_{id}");
                let email = format!("user_{id}@example.com");
                let age = 20 + (id % 30);
                let record = apache_avro::types::Value::Record(vec![
                    ("id".to_string(), apache_avro::types::Value::Int(id)),
                    ("name".to_string(), apache_avro::types::Value::String(name)),
                    (
                        "email".to_string(),
                        apache_avro::types::Value::String(email),
                    ),
                    ("age".to_string(), apache_avro::types::Value::Int(age)),
                ]);
                let bytes = apache_avro::to_avro_datum(&avro_schema_cloned, record).unwrap();
                let resp = client_cloned
                    .post(format!("{REST_ADDR}/kafka/{table_path}/ingest"))
                    .header("content-type", "application/octet-stream")
                    .body(bytes)
                    .send()
                    .await
                    .unwrap();
                assert!(resp.status().is_success(), "ingest failed: {resp:?}");
                let ingest: IngestResponse = resp.json().await.unwrap();
                if let Some(lsn) = ingest.lsn {
                    local_max_lsn = local_max_lsn.max(lsn);
                }
                let current_progress =
                    progress_counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if current_progress % 10000 == 0 {
                    println!(
                        "Overall progress: {}/{} rows ({:.1}%)",
                        current_progress + 1,
                        total_rows,
                        (current_progress + 1) as f64 / total_rows as f64 * 100.0
                    );
                }
            }
            local_max_lsn
        }));
    }
    // Wait for progress monitor to finish
    monitor_handle.await.unwrap();

    // Collect max LSN from all workers
    let ingestion_duration = ingestion_start.elapsed();
    println!(
        "Ingested {} rows in {:?} ({:.2} rows/sec)",
        total_rows,
        ingestion_duration,
        total_rows as f64 / ingestion_duration.as_secs_f64()
    );
    let mut max_lsn: u64 = 0;
    for h in handles {
        let worker_max = h.await.unwrap();
        max_lsn = max_lsn.max(worker_max);
    }
    // Ensure data is flushed and visible up to max_lsn
    flush_table(&client, DATABASE, TABLE, max_lsn).await;

    // Scan table and verify row count
    let mut moonlink_stream = TcpStream::connect(MOONLINK_ADDR).await.unwrap();
    let bytes = scan_table_begin(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
        /*lsn=*/ max_lsn,
    )
    .await
    .unwrap();
    let (data_file_paths, _puffin_file_paths, puffin_deletion, positional_deletion) =
        decode_serialized_read_state_for_testing(bytes);

    // Count rows across all data files
    let verification_start = std::time::Instant::now();
    let mut total_rows_read = 0usize;
    for path in data_file_paths {
        let batches = read_all_batches(&path).await;
        for b in batches {
            total_rows_read += b.num_rows();
        }
    }

    assert_eq!(total_rows_read, total_rows);
    let verification_duration = verification_start.elapsed();
    println!(
        "Verified {} rows in {:?} ({:.2} rows/sec)",
        total_rows_read,
        verification_duration,
        total_rows_read as f64 / verification_duration.as_secs_f64()
    );
    assert!(puffin_deletion.is_empty());
    assert!(positional_deletion.is_empty());

    scan_table_end(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
    )
    .await
    .unwrap();
}
