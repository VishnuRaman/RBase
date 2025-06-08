// use std::path::PathBuf;
// use std::sync::Arc;
//
// use actix_web::{
//     web, App, HttpResponse, HttpServer, Responder,
//     middleware::Logger,
//     error::{ErrorBadRequest, ErrorInternalServerError, ErrorNotFound},
// };
// use serde::{Deserialize, Serialize};
// use serde_json::json;
//
// use crate::pool::{ConnectionPool, Connection};
// use crate::batch::{Batch, AsyncBatchExt};
// use crate::filter::{Filter, FilterSet};
// use crate::aggregation::{AggregationType, AggregationSet};
//
// /// Configuration for the REST server
// #[derive(Clone)]
// pub struct RestConfig {
//     /// The base directory for tables
//     pub base_dir: PathBuf,
//     /// The host to bind to
//     pub host: String,
//     /// The port to bind to
//     pub port: u16,
//     /// The number of connections in the pool
//     pub pool_size: usize,
// }
//
// impl Default for RestConfig {
//     fn default() -> Self {
//         Self {
//             base_dir: PathBuf::from("./data"),
//             host: "127.0.0.1".to_string(),
//             port: 8080,
//             pool_size: 10,
//         }
//     }
// }
//
// /// Application state shared across all routes
// pub struct AppState {
//     /// The connection pool
//     pub pool: ConnectionPool,
// }
//
// /// Request body for creating a column family
// #[derive(Deserialize)]
// struct CreateCfRequest {
//     /// The name of the column family
//     name: String,
// }
//
// /// Request body for put operation
// #[derive(Deserialize)]
// struct PutRequest {
//     /// The row key
//     row: String,
//     /// The column name
//     column: String,
//     /// The value to put
//     value: String,
// }
//
// /// Request body for delete operation
// #[derive(Deserialize)]
// struct DeleteRequest {
//     /// The row key
//     row: String,
//     /// The column name
//     column: String,
//     /// Optional TTL in milliseconds
//     ttl_ms: Option<u64>,
// }
//
// /// Request body for batch operations
// #[derive(Deserialize)]
// struct BatchRequest {
//     /// The operations to perform
//     operations: Vec<BatchOperation>,
// }
//
// /// A single operation in a batch request
// #[derive(Deserialize)]
// #[serde(tag = "type", content = "data")]
// enum BatchOperation {
//     /// Put operation
//     Put(PutRequest),
//     /// Delete operation
//     Delete(DeleteRequest),
// }
//
// /// Request body for get operation
// #[derive(Deserialize)]
// struct GetRequest {
//     /// The row key
//     row: String,
//     /// The column name
//     column: String,
//     /// Optional maximum number of versions to return
//     max_versions: Option<usize>,
// }
//
// /// Request body for scan operation
// #[derive(Deserialize)]
// struct ScanRequest {
//     /// The row key
//     row: String,
//     /// Optional maximum number of versions per column
//     max_versions_per_column: Option<usize>,
// }
//
// /// Request body for filter operation
// #[derive(Deserialize)]
// struct FilterRequest {
//     /// The row key
//     row: String,
//     /// The filter set
//     filter_set: FilterSetRequest,
// }
//
// /// Filter set for filter requests
// #[derive(Deserialize, Clone)]
// struct FilterSetRequest {
//     /// Column filters
//     column_filters: Vec<ColumnFilterRequest>,
//     /// Optional timestamp range
//     timestamp_range: Option<(Option<u64>, Option<u64>)>,
//     /// Optional maximum number of versions
//     max_versions: Option<usize>,
// }
//
// /// Column filter for filter requests
// #[derive(Deserialize, Clone)]
// struct ColumnFilterRequest {
//     /// The column name
//     column: String,
//     /// The filter to apply
//     filter: Filter,
// }
//
// /// Request body for aggregation operation
// #[derive(Deserialize)]
// struct AggregationRequest {
//     /// The row key
//     row: String,
//     /// Optional filter set
//     filter_set: Option<FilterSetRequest>,
//     /// The aggregation set
//     aggregation_set: AggregationSetRequest,
// }
//
// /// Aggregation set for aggregation requests
// #[derive(Deserialize, Clone)]
// struct AggregationSetRequest {
//     /// The aggregations to perform
//     aggregations: Vec<AggregationItemRequest>,
// }
//
// /// Aggregation item for aggregation requests
// #[derive(Deserialize, Clone)]
// struct AggregationItemRequest {
//     /// The column name
//     column: String,
//     /// The aggregation type
//     aggregation_type: String,
// }
//
// /// Convert a filter set request to a filter set
// fn convert_filter_set(filter_set_req: FilterSetRequest) -> FilterSet {
//     let mut filter_set = FilterSet::new();
//
//     for column_filter in filter_set_req.column_filters {
//         filter_set.add_column_filter(
//             column_filter.column.into_bytes(),
//             column_filter.filter,
//         );
//     }
//
//     if let Some((min, max)) = filter_set_req.timestamp_range {
//         filter_set.with_timestamp_range(min, max);
//     }
//
//     if let Some(max_versions) = filter_set_req.max_versions {
//         filter_set.with_max_versions(max_versions);
//     }
//
//     filter_set
// }
//
// /// Convert an aggregation type string to an aggregation type
// fn convert_aggregation_type(agg_type: &str) -> Result<AggregationType, actix_web::Error> {
//     match agg_type {
//         "count" => Ok(AggregationType::Count),
//         "sum" => Ok(AggregationType::Sum),
//         "average" => Ok(AggregationType::Average),
//         "min" => Ok(AggregationType::Min),
//         "max" => Ok(AggregationType::Max),
//         _ => Err(ErrorBadRequest(format!("Invalid aggregation type: {}", agg_type))),
//     }
// }
//
// /// Convert an aggregation set request to an aggregation set
// fn convert_aggregation_set(agg_set_req: AggregationSetRequest) -> Result<AggregationSet, actix_web::Error> {
//     let mut agg_set = AggregationSet::new();
//
//     for agg in agg_set_req.aggregations {
//         let agg_type = convert_aggregation_type(&agg.aggregation_type)?;
//         agg_set.add_aggregation(agg.column.into_bytes(), agg_type);
//     }
//
//     Ok(agg_set)
// }
//
// /// Health check endpoint
// async fn health_check() -> impl Responder {
//     HttpResponse::Ok().json(json!({ "status": "ok" }))
// }
//
// /// Create a column family
// async fn create_cf(
//     state: web::Data<AppState>,
//     path: web::Path<String>,
//     req: web::Json<CreateCfRequest>,
// ) -> Result<impl Responder, actix_web::Error> {
//     let table_name = path.into_inner();
//     let conn = state.pool.get().await.map_err(|e| {
//         ErrorInternalServerError(format!("Failed to get connection from pool: {}", e))
//     })?;
//
//     conn.table.create_cf(&req.name).await.map_err(|e| {
//         ErrorInternalServerError(format!("Failed to create column family: {}", e))
//     })?;
//
//     Ok(HttpResponse::Created().json(json!({
//         "status": "created",
//         "table": table_name,
//         "column_family": req.name
//     })))
// }
//
// /// Put a value
// async fn put(
//     state: web::Data<AppState>,
//     path: web::Path<(String, String)>,
//     req: web::Json<PutRequest>,
// ) -> Result<impl Responder, actix_web::Error> {
//     let (table_name, cf_name) = path.into_inner();
//     let conn = state.pool.get().await.map_err(|e| {
//         ErrorInternalServerError(format!("Failed to get connection from pool: {}", e))
//     })?;
//
//     let cf = conn.table.cf(&cf_name).await.ok_or_else(|| {
//         ErrorNotFound(format!("Column family not found: {}", cf_name))
//     })?;
//
//     cf.put(
//         req.row.clone().into_bytes(),
//         req.column.clone().into_bytes(),
//         req.value.clone().into_bytes(),
//     ).await.map_err(|e| {
//         ErrorInternalServerError(format!("Failed to put value: {}", e))
//     })?;
//
//     Ok(HttpResponse::Ok().json(json!({
//         "status": "ok",
//         "table": table_name,
//         "column_family": cf_name,
//         "row": req.row,
//         "column": req.column
//     })))
// }
//
// /// Delete a value
// async fn delete(
//     state: web::Data<AppState>,
//     path: web::Path<(String, String)>,
//     req: web::Json<DeleteRequest>,
// ) -> Result<impl Responder, actix_web::Error> {
//     let (table_name, cf_name) = path.into_inner();
//     let conn = state.pool.get().await.map_err(|e| {
//         ErrorInternalServerError(format!("Failed to get connection from pool: {}", e))
//     })?;
//
//     let cf = conn.table.cf(&cf_name).await.ok_or_else(|| {
//         ErrorNotFound(format!("Column family not found: {}", cf_name))
//     })?;
//
//     if let Some(ttl_ms) = req.ttl_ms {
//         cf.delete_with_ttl(
//             req.row.clone().into_bytes(),
//             req.column.clone().into_bytes(),
//             Some(ttl_ms),
//         ).await.map_err(|e| {
//             ErrorInternalServerError(format!("Failed to delete value: {}", e))
//         })?;
//     } else {
//         cf.delete(
//             req.row.clone().into_bytes(),
//             req.column.clone().into_bytes(),
//         ).await.map_err(|e| {
//             ErrorInternalServerError(format!("Failed to delete value: {}", e))
//         })?;
//     }
//
//     Ok(HttpResponse::Ok().json(json!({
//         "status": "ok",
//         "table": table_name,
//         "column_family": cf_name,
//         "row": req.row,
//         "column": req.column
//     })))
// }
//
// /// Execute a batch of operations
// async fn batch(
//     state: web::Data<AppState>,
//     path: web::Path<(String, String)>,
//     req: web::Json<BatchRequest>,
// ) -> Result<impl Responder, actix_web::Error> {
//     let (table_name, cf_name) = path.into_inner();
//     let conn = state.pool.get().await.map_err(|e| {
//         ErrorInternalServerError(format!("Failed to get connection from pool: {}", e))
//     })?;
//
//     let cf = conn.table.cf(&cf_name).await.ok_or_else(|| {
//         ErrorNotFound(format!("Column family not found: {}", cf_name))
//     })?;
//
//     let mut batch = Batch::new();
//
//     for op in &req.operations {
//         match op {
//             BatchOperation::Put(put_req) => {
//                 batch.put(
//                     put_req.row.clone().into_bytes(),
//                     put_req.column.clone().into_bytes(),
//                     put_req.value.clone().into_bytes(),
//                 );
//             },
//             BatchOperation::Delete(delete_req) => {
//                 if let Some(ttl_ms) = delete_req.ttl_ms {
//                     batch.delete_with_ttl(
//                         delete_req.row.clone().into_bytes(),
//                         delete_req.column.clone().into_bytes(),
//                         Some(ttl_ms),
//                     );
//                 } else {
//                     batch.delete(
//                         delete_req.row.clone().into_bytes(),
//                         delete_req.column.clone().into_bytes(),
//                     );
//                 }
//             },
//         }
//     }
//
//     cf.execute_batch(&batch).await.map_err(|e| {
//         ErrorInternalServerError(format!("Failed to execute batch: {}", e))
//     })?;
//
//     Ok(HttpResponse::Ok().json(json!({
//         "status": "ok",
//         "table": table_name,
//         "column_family": cf_name,
//         "operations_count": req.operations.len()
//     })))
// }
//
// /// Get a value
// async fn get(
//     state: web::Data<AppState>,
//     path: web::Path<(String, String)>,
//     req: web::Json<GetRequest>,
// ) -> Result<impl Responder, actix_web::Error> {
//     let (table_name, cf_name) = path.into_inner();
//     let conn = state.pool.get().await.map_err(|e| {
//         ErrorInternalServerError(format!("Failed to get connection from pool: {}", e))
//     })?;
//
//     let cf = conn.table.cf(&cf_name).await.ok_or_else(|| {
//         ErrorNotFound(format!("Column family not found: {}", cf_name))
//     })?;
//
//     if let Some(max_versions) = req.max_versions {
//         let versions = cf.get_versions(
//             req.row.as_bytes(),
//             req.column.as_bytes(),
//             max_versions,
//         ).await.map_err(|e| {
//             ErrorInternalServerError(format!("Failed to get versions: {}", e))
//         })?;
//
//         let result: Vec<_> = versions.into_iter()
//             .map(|(ts, value)| {
//                 json!({
//                     "timestamp": ts,
//                     "value": String::from_utf8_lossy(&value).to_string()
//                 })
//             })
//             .collect();
//
//         Ok(HttpResponse::Ok().json(result))
//     } else {
//         let value = cf.get(
//             req.row.as_bytes(),
//             req.column.as_bytes(),
//         ).await.map_err(|e| {
//             ErrorInternalServerError(format!("Failed to get value: {}", e))
//         })?;
//
//         match value {
//             Some(v) => Ok(HttpResponse::Ok().json(json!({
//                 "value": String::from_utf8_lossy(&v).to_string()
//             }))),
//             None => Ok(HttpResponse::NotFound().json(json!({
//                 "status": "not_found",
//                 "table": table_name,
//                 "column_family": cf_name,
//                 "row": req.row,
//                 "column": req.column
//             }))),
//         }
//     }
// }
//
// /// Scan a row
// async fn scan(
//     state: web::Data<AppState>,
//     path: web::Path<(String, String)>,
//     req: web::Json<ScanRequest>,
// ) -> Result<impl Responder, actix_web::Error> {
//     let (table_name, cf_name) = path.into_inner();
//     let conn = state.pool.get().await.map_err(|e| {
//         ErrorInternalServerError(format!("Failed to get connection from pool: {}", e))
//     })?;
//
//     let cf = conn.table.cf(&cf_name).await.ok_or_else(|| {
//         ErrorNotFound(format!("Column family not found: {}", cf_name))
//     })?;
//
//     let max_versions = req.max_versions_per_column.unwrap_or(1);
//     let result = cf.scan_row_versions(
//         req.row.as_bytes(),
//         max_versions,
//     ).await.map_err(|e| {
//         ErrorInternalServerError(format!("Failed to scan row: {}", e))
//     })?;
//
//     let mut response = serde_json::Map::new();
//
//     for (column, versions) in result {
//         let column_str = String::from_utf8_lossy(&column).to_string();
//         let versions_json: Vec<_> = versions.into_iter()
//             .map(|(ts, value)| {
//                 json!({
//                     "timestamp": ts,
//                     "value": String::from_utf8_lossy(&value).to_string()
//                 })
//             })
//             .collect();
//
//         response.insert(column_str, json!(versions_json));
//     }
//
//     Ok(HttpResponse::Ok().json(response))
// }
//
// /// Filter a row
// async fn filter(
//     state: web::Data<AppState>,
//     path: web::Path<(String, String)>,
//     req: web::Json<FilterRequest>,
// ) -> Result<impl Responder, actix_web::Error> {
//     let (table_name, cf_name) = path.into_inner();
//     let conn = state.pool.get().await.map_err(|e| {
//         ErrorInternalServerError(format!("Failed to get connection from pool: {}", e))
//     })?;
//
//     let cf = conn.table.cf(&cf_name).await.ok_or_else(|| {
//         ErrorNotFound(format!("Column family not found: {}", cf_name))
//     })?;
//
//     let filter_set = convert_filter_set(req.filter_set.clone());
//     let result = cf.scan_row_with_filter(
//         req.row.as_bytes(),
//         &filter_set,
//     ).await.map_err(|e| {
//         ErrorInternalServerError(format!("Failed to filter row: {}", e))
//     })?;
//
//     let mut response = serde_json::Map::new();
//
//     for (column, versions) in result {
//         let column_str = String::from_utf8_lossy(&column).to_string();
//         let versions_json: Vec<_> = versions.into_iter()
//             .map(|(ts, value)| {
//                 json!({
//                     "timestamp": ts,
//                     "value": String::from_utf8_lossy(&value).to_string()
//                 })
//             })
//             .collect();
//
//         response.insert(column_str, json!(versions_json));
//     }
//
//     Ok(HttpResponse::Ok().json(response))
// }
//
// /// Aggregate a row
// async fn aggregate(
//     state: web::Data<AppState>,
//     path: web::Path<(String, String)>,
//     req: web::Json<AggregationRequest>,
// ) -> Result<impl Responder, actix_web::Error> {
//     let (table_name, cf_name) = path.into_inner();
//     let conn = state.pool.get().await.map_err(|e| {
//         ErrorInternalServerError(format!("Failed to get connection from pool: {}", e))
//     })?;
//
//     let cf = conn.table.cf(&cf_name).await.ok_or_else(|| {
//         ErrorNotFound(format!("Column family not found: {}", cf_name))
//     })?;
//
//     let filter_set = req.filter_set.as_ref().map(|fs| convert_filter_set(fs.clone()));
//     let aggregation_set = convert_aggregation_set(req.aggregation_set.clone())?;
//
//     let result = cf.aggregate(
//         req.row.as_bytes(),
//         filter_set.as_ref(),
//         &aggregation_set,
//     ).await.map_err(|e| {
//         ErrorInternalServerError(format!("Failed to aggregate row: {}", e))
//     })?;
//
//     let mut response = serde_json::Map::new();
//
//     response.extend(result.iter().map(|(column, agg_result)| {
//         let column_str = String::from_utf8_lossy(column).to_string();
//         (column_str, json!(agg_result.to_string()))
//     }));
//
//
//
//     Ok(HttpResponse::Ok().json(response))
// }
//
// /// Flush a column family
// async fn flush(
//     state: web::Data<AppState>,
//     path: web::Path<(String, String)>,
// ) -> Result<impl Responder, actix_web::Error> {
//     let (table_name, cf_name) = path.into_inner();
//     let conn = state.pool.get().await.map_err(|e| {
//         ErrorInternalServerError(format!("Failed to get connection from pool: {}", e))
//     })?;
//
//     let cf = conn.table.cf(&cf_name).await.ok_or_else(|| {
//         ErrorNotFound(format!("Column family not found: {}", cf_name))
//     })?;
//
//     cf.flush().await.map_err(|e| {
//         ErrorInternalServerError(format!("Failed to flush column family: {}", e))
//     })?;
//
//     Ok(HttpResponse::Ok().json(json!({
//         "status": "ok",
//         "table": table_name,
//         "column_family": cf_name
//     })))
// }
//
// /// Compact a column family
// async fn compact(
//     state: web::Data<AppState>,
//     path: web::Path<(String, String)>,
// ) -> Result<impl Responder, actix_web::Error> {
//     let (table_name, cf_name) = path.into_inner();
//     let conn = state.pool.get().await.map_err(|e| {
//         ErrorInternalServerError(format!("Failed to get connection from pool: {}", e))
//     })?;
//
//     let cf = conn.table.cf(&cf_name).await.ok_or_else(|| {
//         ErrorNotFound(format!("Column family not found: {}", cf_name))
//     })?;
//
//     cf.compact().await.map_err(|e| {
//         ErrorInternalServerError(format!("Failed to compact column family: {}", e))
//     })?;
//
//     Ok(HttpResponse::Ok().json(json!({
//         "status": "ok",
//         "table": table_name,
//         "column_family": cf_name
//     })))
// }
//
// /// Start the REST server
// pub async fn start_server(config: RestConfig) -> std::io::Result<()> {
//     let pool = ConnectionPool::new(&config.base_dir, config.pool_size);
//     let app_state = web::Data::new(AppState { pool });
//
//     println!("Starting RedBase REST server on {}:{}", config.host, config.port);
//
//     HttpServer::new(move || {
//         App::new()
//             .app_data(app_state.clone())
//             .wrap(Logger::default())
//             .route("/health", web::get().to(health_check))
//             .route("/tables/{table}/cf", web::post().to(create_cf))
//             .route("/tables/{table}/cf/{cf}/put", web::post().to(put))
//             .route("/tables/{table}/cf/{cf}/delete", web::post().to(delete))
//             .route("/tables/{table}/cf/{cf}/batch", web::post().to(batch))
//             .route("/tables/{table}/cf/{cf}/get", web::post().to(get))
//             .route("/tables/{table}/cf/{cf}/scan", web::post().to(scan))
//             .route("/tables/{table}/cf/{cf}/filter", web::post().to(filter))
//             .route("/tables/{table}/cf/{cf}/aggregate", web::post().to(aggregate))
//             .route("/tables/{table}/cf/{cf}/flush", web::post().to(flush))
//             .route("/tables/{table}/cf/{cf}/compact", web::post().to(compact))
//     })
//     .bind(format!("{}:{}", config.host, config.port))?
//     .run()
//     .await
// }
