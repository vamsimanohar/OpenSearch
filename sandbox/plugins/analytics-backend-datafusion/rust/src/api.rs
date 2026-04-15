/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Bridge-agnostic API layer.
//!
//! All functions in this module use plain Rust types — no JNI, no FFI-specific
//! types. Both the current JNI bridge (`lib.rs`) and a future FFM/C bridge can
//! call these functions directly.
//!
//! # Pointer contract
//!
//! Functions that accept `i64` pointer arguments require non-zero, valid pointers
//! to the corresponding Rust type. The caller (bridge layer) is responsible for
//! null-checking before calling. Functions that return `i64` return heap-allocated
//! pointers via `Box::into_raw`; the caller owns the pointer and must call the
//! corresponding close function exactly once.
//!
//! # Thread safety
//!
//! - `init_runtime_manager` and `shutdown_runtime_manager` must be called from a
//!   single thread (node startup/shutdown).
//! - `create_global_runtime` / `close_global_runtime` are not thread-safe for the
//!   same pointer.
//! - `execute_query`: async. Safe to call concurrently with different shard/runtime pointers.
//!   The bridge layer wraps with `block_on` or `spawn`.
//! - `stream_next`: async. The bridge layer wraps with `block_on` or `spawn`.
//! - `stream_get_schema`, `stream_close` must NOT be called
//!   concurrently on the same stream pointer.
//!
//! # FFM bridge example
//!
//! When migrating from JNI to JDK FFM (Foreign Function & Memory API), create an
//! `ffi_bridge.rs` that exports `extern "C"` functions calling this API. The JNI
//! bridge (`lib.rs`) and FFM bridge are interchangeable — only the type conversion
//! layer differs.
//!
//! ```rust,ignore
//! // ffi_bridge.rs — extern "C" bridge for JDK FFM (replaces lib.rs JNI bridge)
//! //
//! // Java side uses java.lang.foreign.Linker to call these functions directly.
//! // Strings are passed as (pointer, length) pairs. Byte arrays likewise.
//! // No JNIEnv, no JString, no GlobalRef — pure C ABI.
//!
//! use crate::api;
//! use crate::runtime_manager::RuntimeManager;
//! use std::sync::{Arc, OnceLock};
//!
//! static RUNTIME_MANAGER: OnceLock<Arc<RuntimeManager>> = OnceLock::new();
//!
//! /// Initialize the Tokio runtime manager.
//! /// Java: MethodHandle = linker.downcallHandle(lib.find("df_init"), FunctionDescriptor.ofVoid(JAVA_INT));
//! #[no_mangle]
//! pub extern "C" fn df_init(cpu_threads: i32) {
//!     RUNTIME_MANAGER.get_or_init(|| Arc::new(RuntimeManager::new(cpu_threads as usize)));
//! }
//!
//! /// Create a global DataFusion runtime. Returns pointer as i64, or 0 on error.
//! /// Java: MethodHandle = linker.downcallHandle(lib.find("df_create_runtime"),
//! ///     FunctionDescriptor.of(JAVA_LONG, JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG));
//! #[no_mangle]
//! pub extern "C" fn df_create_runtime(
//!     memory_limit: i64,
//!     spill_dir_ptr: *const u8,
//!     spill_dir_len: i64,
//!     spill_limit: i64,
//! ) -> i64 {
//!     let spill_dir = unsafe {
//!         std::str::from_utf8_unchecked(
//!             std::slice::from_raw_parts(spill_dir_ptr, spill_dir_len as usize)
//!         )
//!     };
//!     api::create_global_runtime(memory_limit, spill_dir, spill_limit).unwrap_or(0)
//! }
//!
//! /// Execute a query. Returns stream pointer as i64, or 0 on error.
//! /// Error message written to (err_buf_ptr, err_buf_len), actual length returned via err_len_out.
//! /// Java: MethodHandle = linker.downcallHandle(lib.find("df_execute_query"),
//! ///     FunctionDescriptor.of(JAVA_LONG, JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG));
//! #[no_mangle]
//! pub extern "C" fn df_execute_query(
//!     shard_view_ptr: i64,
//!     table_name_ptr: *const u8,
//!     table_name_len: i64,
//!     plan_ptr: *const u8,
//!     plan_len: i64,
//!     runtime_ptr: i64,
//! ) -> i64 {
//!     let manager = RUNTIME_MANAGER.get().expect("not initialized");
//!     let table_name = unsafe {
//!         std::str::from_utf8_unchecked(
//!             std::slice::from_raw_parts(table_name_ptr, table_name_len as usize)
//!         )
//!     };
//!     let plan_bytes = unsafe {
//!         std::slice::from_raw_parts(plan_ptr, plan_len as usize)
//!     };
//!     manager.io_runtime.block_on(unsafe {
//!         api::execute_query(shard_view_ptr, table_name, plan_bytes, runtime_ptr, manager)
//!     }).unwrap_or(0)
//! }
//!
//! /// Get next batch. Returns FFI_ArrowArray pointer, 0 for end-of-stream, -1 on error.
//! #[no_mangle]
//! pub extern "C" fn df_stream_next(stream_ptr: i64) -> i64 {
//!     let manager = RUNTIME_MANAGER.get().expect("not initialized");
//!     manager.io_runtime.block_on(unsafe { api::stream_next(stream_ptr) }).unwrap_or(-1)
//! }
//!
//! /// Close a stream. Safe with 0.
//! #[no_mangle]
//! pub extern "C" fn df_stream_close(stream_ptr: i64) {
//!     unsafe { api::stream_close(stream_ptr) };
//! }
//!
//! // Java side (JDK 22+):
//! //
//! //   try (Arena arena = Arena.ofConfined()) {
//! //       SymbolLookup lib = SymbolLookup.libraryLookup("libopensearch_datafusion.so", arena);
//! //       Linker linker = Linker.nativeLinker();
//! //
//! //       var init = linker.downcallHandle(
//! //           lib.find("df_init").get(),
//! //           FunctionDescriptor.ofVoid(ValueLayout.JAVA_INT)
//! //       );
//! //       init.invoke(Runtime.getRuntime().availableProcessors());
//! //
//! //       var createRuntime = linker.downcallHandle(
//! //           lib.find("df_create_runtime").get(),
//! //           FunctionDescriptor.of(JAVA_LONG, JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG)
//! //       );
//! //       MemorySegment spillDir = arena.allocateFrom("/tmp/spill");
//! //       long runtimePtr = (long) createRuntime.invoke(512_000_000L, spillDir, spillDir.byteSize(), 256_000_000L);
//! //
//! //       var executeQuery = linker.downcallHandle(
//! //           lib.find("df_execute_query").get(),
//! //           FunctionDescriptor.of(JAVA_LONG, JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG)
//! //       );
//! //       MemorySegment tableName = arena.allocateFrom("my_table");
//! //       MemorySegment plan = arena.allocateFrom(MemoryLayout.sequenceLayout(planBytes.length, JAVA_BYTE), planBytes);
//! //       long streamPtr = (long) executeQuery.invoke(shardViewPtr, tableName, tableName.byteSize(), plan, plan.byteSize(), runtimePtr);
//! //   }
//! ```

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::ipc::writer::StreamWriter as IpcStreamWriter;
use arrow::ipc::reader::StreamReader as IpcStreamReader;
use arrow_array::{Array, RecordBatch, StructArray};
use arrow_array::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{FairSpillPool, GreedyMemoryPool, TrackConsumersPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::execution::RecordBatchStream;
use datafusion::prelude::SessionConfig;
use futures::TryStreamExt;
use log::{info, error};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;

use crate::cross_rt_stream::CrossRtStream;
use crate::runtime_manager::RuntimeManager;

// mimalloc C API — linked via the global allocator set in the native lib crate.
extern "C" {
    /// Force mimalloc to return unused pages to the OS.
    /// `force = true` aggressively decommits all freed pages.
    fn mi_collect(force: bool);
}

/// Forces mimalloc to decommit freed pages back to the OS.
///
/// Called after each query completes to prevent RSS accumulation across queries.
/// Without this, mimalloc retains freed pages in thread-local heaps for reuse,
/// causing the process RSS to grow monotonically until the OS OOM killer triggers.
fn mimalloc_purge() {
    unsafe { mi_collect(true) };
}

/// Result container for Arrow IPC bytes returned to Java.
/// Java accesses the data via `ipc_result_data_ptr` and `ipc_result_data_len`,
/// then frees via `free_ipc_result`.
pub struct IpcResult {
    pub data: Vec<u8>,
}

/// Returns a pointer to the IPC data buffer.
///
/// # Safety
/// `result_ptr` must be a valid pointer returned by `execute_iceberg_query_to_ipc`.
pub unsafe fn ipc_result_data_ptr(result_ptr: i64) -> i64 {
    let result = &*(result_ptr as *const IpcResult);
    result.data.as_ptr() as i64
}

/// Returns the length of the IPC data buffer.
///
/// # Safety
/// `result_ptr` must be a valid pointer returned by `execute_iceberg_query_to_ipc`.
pub unsafe fn ipc_result_data_len(result_ptr: i64) -> i64 {
    let result = &*(result_ptr as *const IpcResult);
    result.data.len() as i64
}

/// Frees an IPC result. Safe to call with 0 (no-op).
///
/// # Safety
/// `result_ptr` must be 0 or a valid pointer returned by `execute_iceberg_query_to_ipc`.
pub unsafe fn free_ipc_result(result_ptr: i64) {
    if result_ptr != 0 {
        let _ = Box::from_raw(result_ptr as *mut IpcResult);
    }
}

/// Serializes a list of RecordBatches to Arrow IPC stream format.
fn batches_to_ipc(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>, DataFusionError> {
    let mut buf = Vec::new();
    {
        let mut writer = IpcStreamWriter::try_new(&mut buf, schema.as_ref())
            .map_err(|e| DataFusionError::Execution(format!("IPC writer creation failed: {}", e)))?;
        for batch in batches {
            writer.write(batch)
                .map_err(|e| DataFusionError::Execution(format!("IPC write failed: {}", e)))?;
        }
        writer.finish()
            .map_err(|e| DataFusionError::Execution(format!("IPC finish failed: {}", e)))?;
    }
    Ok(buf)
}

/// Deserializes Arrow IPC stream bytes to RecordBatches.
fn ipc_to_batches(ipc_bytes: &[u8]) -> Result<(SchemaRef, Vec<RecordBatch>), DataFusionError> {
    let cursor = std::io::Cursor::new(ipc_bytes);
    let reader = IpcStreamReader::try_new(cursor, None)
        .map_err(|e| DataFusionError::Execution(format!("IPC reader creation failed: {}", e)))?;
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| DataFusionError::Execution(format!("IPC read failed: {}", e)))?;
    Ok((schema, batches))
}

/// Build ObjectMeta for each file using the given object store.
pub async fn create_object_metas(
    store: &dyn object_store::ObjectStore,
    base_path: &str,
    filenames: Vec<String>,
) -> Result<Vec<object_store::ObjectMeta>, DataFusionError> {
    let mut metas = Vec::with_capacity(filenames.len());
    for filename in filenames {
        let full_path = if filename.starts_with('/') || filename.contains(base_path) {
            filename
        } else {
            format!("{}/{}", base_path.trim_end_matches('/'), filename)
        };
        let path = object_store::path::Path::from(full_path.as_str());
        let meta = store.head(&path).await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to get object meta for {}: {}", full_path, e))
        })?;
        metas.push(meta);
    }
    Ok(metas)
}

/// Opaque runtime handle returned to the caller.
/// Contains the DataFusion RuntimeEnv (memory pool, disk spill, cache).
pub struct DataFusionRuntime {
    pub runtime_env: datafusion::execution::runtime_env::RuntimeEnv,
}

/// Opaque shard view handle returned to the caller.
pub(crate) struct ShardView {
    pub table_path: ListingTableUrl,
    pub object_metas: Arc<Vec<object_store::ObjectMeta>>,
}

/// Creates a DataFusion global runtime with the given resource limits.
///
/// Returns a heap-allocated pointer (as i64) to `DataFusionRuntime`.
/// Caller must call `close_global_runtime` exactly once to free it.
pub fn create_global_runtime(
    memory_pool_limit: i64,
    spill_dir: &str,
    spill_limit: i64,
) -> Result<i64, DataFusionError> {
    let disk_manager = DiskManagerBuilder::default()
        .with_max_temp_directory_size(spill_limit as u64)
        .with_mode(DiskManagerMode::Directories(vec![PathBuf::from(spill_dir)]));

    // Memory pool selection via sign convention:
    //   limit > 0  → GreedyMemoryPool(limit)  — capped, first-come-first-served
    //   limit == 0  → GreedyMemoryPool(MAX)    — unlimited greedy
    //   limit < 0   → FairSpillPool(abs)       — capped, fair sharing across operators
    let memory_pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> = if memory_pool_limit < 0 {
        let pool_size = memory_pool_limit.unsigned_abs() as usize;
        eprintln!("[POOL] FairSpillPool: {} GB", pool_size / (1024 * 1024 * 1024));
        Arc::new(TrackConsumersPool::new(
            FairSpillPool::new(pool_size),
            NonZeroUsize::new(5).unwrap(),
        ))
    } else {
        let pool_size = if memory_pool_limit == 0 { usize::MAX } else { memory_pool_limit as usize };
        eprintln!("[POOL] GreedyMemoryPool: {} GB", if pool_size == usize::MAX { 0 } else { pool_size / (1024 * 1024 * 1024) });
        Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(pool_size),
            NonZeroUsize::new(5).unwrap(),
        ))
    };

    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(memory_pool)
        .with_disk_manager_builder(disk_manager)
        .build()?;

    let runtime = DataFusionRuntime { runtime_env };
    Ok(Box::into_raw(Box::new(runtime)) as i64)
}

/// Closes a DataFusion global runtime. Safe to call with 0 (no-op).
///
/// # Safety
/// `ptr` must be 0 or a valid pointer returned by `create_global_runtime`.
pub unsafe fn close_global_runtime(ptr: i64) {
    if ptr != 0 {
        let _ = Box::from_raw(ptr as *mut DataFusionRuntime);
    }
}

/// Creates a native reader (ShardView) for the given path and files.
///
/// Returns a heap-allocated pointer (as i64) to `ShardView`.
/// Caller must call `close_reader` exactly once to free it.
pub fn create_reader(
    table_path: &str,
    mut filenames: Vec<String>,
    tokio_rt_manager: &RuntimeManager,
) -> Result<i64, DataFusionError> {
    filenames.sort();

    let table_url = ListingTableUrl::parse(table_path)
        .map_err(|e| DataFusionError::Execution(format!("Invalid table path: {}", e)))?;

    // TODO: use global runtime's object store instead of building a throwaway RuntimeEnv
    let default_rt = RuntimeEnvBuilder::new().build()?;
    let store = default_rt.object_store(&table_url)?;

    let object_metas = tokio_rt_manager.io_runtime.block_on(
        create_object_metas(store.as_ref(), table_path, filenames),
    )?;

    let shard_view = ShardView {
        table_path: table_url,
        object_metas: Arc::new(object_metas),
    };
    Ok(Box::into_raw(Box::new(shard_view)) as i64)
}

/// Closes a native reader. Safe to call with 0 (no-op).
///
/// # Safety
/// `ptr` must be 0 or a valid pointer returned by `create_reader`.
pub unsafe fn close_reader(ptr: i64) {
    if ptr != 0 {
        let _ = Box::from_raw(ptr as *mut ShardView);
    }
}

/// Executes a query. Returns a heap-allocated pointer (as i64) to the result stream.
/// Caller must call `stream_close` exactly once to free it.
///
/// This is an async function — the bridge layer decides how to run it
/// (`block_on` for synchronous JNI, `spawn` for async delivery).
///
/// # Safety
/// `shard_view_ptr` and `runtime_ptr` must be valid, non-zero pointers.
pub async unsafe fn execute_query(
    shard_view_ptr: i64,
    table_name: &str,
    plan_bytes: &[u8],
    runtime_ptr: i64,
    manager: &RuntimeManager,
) -> Result<i64, DataFusionError> {
    let shard_view = &*(shard_view_ptr as *const ShardView);
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);

    let table_path = shard_view.table_path.clone();
    let object_metas = shard_view.object_metas.clone();
    let cpu_executor = manager.cpu_executor();

    let result = crate::query_executor::execute_query(
        table_path,
        object_metas,
        table_name.to_string(),
        plan_bytes.to_vec(),
        runtime,
        cpu_executor,
    )
    .await?;

    Ok(result)
}

/// Executes a SQL query against Parquet files (S3 or local) via DataFusion.
///
/// For S3 paths: builds an S3 object store from provided credentials.
/// For file:// paths: uses the local filesystem directly.
///
/// Returns a heap-allocated pointer (as i64) to the result stream.
/// Caller must call `stream_close` exactly once to free it.
pub async fn execute_iceberg_query(
    s3_region: &str,
    s3_bucket: Option<&str>,
    s3_access_key_id: Option<&str>,
    s3_secret_access_key: Option<&str>,
    s3_session_token: Option<&str>,
    s3_endpoint: Option<&str>,
    file_paths: Vec<String>,
    file_sizes: Vec<i64>,
    table_name: &str,
    sql_query: &str,
    runtime: &DataFusionRuntime,
    cpu_executor: crate::executor::DedicatedExecutor,
    io_handle: tokio::runtime::Handle,
) -> Result<i64, DataFusionError> {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
    use datafusion::execution::cache::cache_manager::CacheManagerConfig;
    use datafusion::prelude::SessionContext;

    use std::time::Instant;
    let t_start = Instant::now();

    // Detect local file mode: file paths start with file://
    let is_local = file_paths.first().map_or(false, |p| p.starts_with("file://"));

    info!(
        "execute_iceberg_query: region={}, bucket={:?}, files={}, local={}, table={}, sql={}",
        s3_region, s3_bucket, file_paths.len(), is_local, table_name, sql_query
    );

    // Build per-query RuntimeEnv sharing global memory pool
    let runtime_env = RuntimeEnvBuilder::from_runtime_env(&runtime.runtime_env)
        .with_cache_manager(
            CacheManagerConfig::default()
                .with_file_metadata_cache(Some(
                    runtime.runtime_env.cache_manager.get_file_metadata_cache(),
                ))
                .with_files_statistics_cache(
                    runtime.runtime_env.cache_manager.get_file_statistic_cache(),
                ),
        )
        .build()
        .map_err(|e| {
            error!("Failed to build runtime env: {}", e);
            e
        })?;

    // Store reference for schema inference — either S3 or local filesystem
    let store: Arc<dyn ObjectStore>;
    let file_urls: Vec<String>;

    if is_local {
        // Local filesystem — use object_store's LocalFileSystem
        let local_store = Arc::new(object_store::local::LocalFileSystem::new());
        store = local_store;

        // file:// URLs work directly as ListingTable paths
        file_urls = file_paths.clone();

        // Register local filesystem for file:// scheme
        let local_url = url::Url::parse("file:///").map_err(|e| {
            DataFusionError::Execution(format!("Invalid file URL: {}", e))
        })?;
        runtime_env.register_object_store(&local_url, store.clone());
        eprintln!("[PERF] Using local filesystem for {} files", file_paths.len());
    } else {
        // S3 mode — build object store with credentials
        let bucket = s3_bucket.unwrap_or("unknown");
        let mut builder = AmazonS3Builder::new()
            .with_region(s3_region)
            .with_bucket_name(bucket);

        if let Some(key) = s3_access_key_id {
            builder = builder.with_access_key_id(key);
        }
        if let Some(secret) = s3_secret_access_key {
            builder = builder.with_secret_access_key(secret);
        }
        if let Some(token) = s3_session_token {
            builder = builder.with_token(token);
        }
        if let Some(endpoint) = s3_endpoint {
            builder = builder.with_endpoint(endpoint);
        }

        let s3_store = Arc::new(builder.build().map_err(|e| {
            DataFusionError::Execution(format!("Failed to build S3 object store: {}", e))
        })?);
        store = s3_store.clone();

        // Register CrossRuntimeObjectStore — delegates S3 I/O to IO runtime
        let store_url = url::Url::parse(&format!("s3://{}/", bucket)).map_err(|e| {
            DataFusionError::Execution(format!("Invalid S3 URL: {}", e))
        })?;
        let cross_rt_store = Arc::new(
            crate::cross_rt_object_store::CrossRuntimeObjectStore::new(s3_store.clone(), io_handle)
        );
        runtime_env.register_object_store(&store_url, cross_rt_store);

        // Build file URLs from object metas
        let bucket_prefix = format!("s3://{}/", bucket);
        file_urls = file_paths.iter().map(|path| {
            if path.starts_with("s3://") {
                path.clone()
            } else {
                format!("{}{}", bucket_prefix, path)
            }
        }).collect();
        eprintln!("[PERF] Using S3 object store for {} files", file_paths.len());
    }

    // Build synthetic ObjectMeta from Iceberg manifest metadata (avoids HEAD calls).
    let object_metas: Vec<object_store::ObjectMeta> = file_paths.iter().zip(file_sizes.iter()).map(|(path, &size)| {
        // Strip scheme prefix to get the object store key
        let key = if let Some(stripped) = path.strip_prefix("file://") {
            stripped.to_string()
        } else if let Some(bucket) = s3_bucket {
            let bucket_prefix = format!("s3://{}/", bucket);
            if let Some(stripped) = path.strip_prefix(&bucket_prefix) {
                stripped.to_string()
            } else if let Some(stripped) = path.strip_prefix("s3://") {
                if let Some(after_bucket) = stripped.find('/') {
                    stripped[after_bucket + 1..].to_string()
                } else {
                    path.clone()
                }
            } else {
                path.clone()
            }
        } else {
            path.clone()
        };
        object_store::ObjectMeta {
            location: object_store::path::Path::from(key.as_str()),
            size: size as u64,
            last_modified: Default::default(),
            e_tag: None,
            version: None,
        }
    }).collect();
    eprintln!("[PERF] Built {} synthetic ObjectMeta (no HEAD calls): {}ms", object_metas.len(), t_start.elapsed().as_millis());

    // Build session — limit partitions to file count to avoid empty partition tasks
    let num_files = object_metas.len();
    let mut config = SessionConfig::new();
    let half_cpus = (num_cpus::get() / 2).max(1);
    config.options_mut().execution.target_partitions = num_files.min(half_cpus).max(1);
    config.options_mut().execution.batch_size = 8192;

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        .build();

    let ctx = SessionContext::new_with_state(state);

    // Read schema directly from the first Parquet file using its synthetic ObjectMeta.
    let file_format = Arc::new(ParquetFormat::new());

    use datafusion::datasource::file_format::FileFormat;
    let t_schema = Instant::now();
    let resolved_schema = file_format.infer_schema(
        &ctx.state(),
        &store,
        &[object_metas[0].clone()],
    ).await?;
    eprintln!("[PERF] Schema inference: {}ms ({} fields)", t_schema.elapsed().as_millis(), resolved_schema.fields().len());

    // Register each Parquet file individually via ListingTable
    let listing_options = ListingOptions::new(file_format)
        .with_file_extension(".parquet")
        .with_collect_stat(true);

    let table_config = ListingTableConfig::new_with_multi_paths(
        file_urls.iter()
            .map(|u| ListingTableUrl::parse(u))
            .collect::<Result<Vec<_>, _>>()?
    )
        .with_listing_options(listing_options)
        .with_schema(resolved_schema);

    let t_register = Instant::now();
    let provider = Arc::new(ListingTable::try_new(table_config)?);
    ctx.register_table(table_name, provider)?;
    eprintln!("[PERF] Table registration: {}ms", t_register.elapsed().as_millis());

    // Reset S3 I/O counters for this query
    crate::cross_rt_object_store::reset_s3_counters();

    // Plan and create a streaming execution.
    let t_sql = Instant::now();
    let dataframe = ctx.sql(sql_query).await?;
    let plan = dataframe.create_physical_plan().await?;
    eprintln!("[PERF] SQL planning + physical plan: {}ms", t_sql.elapsed().as_millis());
    eprintln!("[PERF] Physical plan:\n{}", datafusion::physical_plan::displayable(plan.as_ref()).indent(true));

    let memory_pool = runtime.runtime_env.memory_pool.clone();
    let pool_reserved = memory_pool.reserved();
    eprintln!("[PERF] Memory pool before execution: {} MB reserved", pool_reserved / (1024 * 1024));

    let stream = datafusion::physical_plan::execute_stream(plan, ctx.task_ctx())?;
    eprintln!("[PERF] execute_iceberg_query setup: {}ms", t_start.elapsed().as_millis());

    // CrossRtStream: CPU executor pulls batches
    let cross_rt_stream = CrossRtStream::new_with_df_error_stream(stream, cpu_executor);
    let wrapped = MemoryTrackingStream {
        inner: RecordBatchStreamAdapter::new(cross_rt_stream.schema(), cross_rt_stream),
        memory_pool,
        peak_memory: std::sync::atomic::AtomicUsize::new(pool_reserved),
    };

    Ok(Box::into_raw(Box::new(wrapped)) as i64)
}

/// Wraps a RecordBatchStream with memory pool tracking for PERF logging.
pub struct MemoryTrackingStream {
    pub inner: RecordBatchStreamAdapter<CrossRtStream>,
    pub memory_pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
    pub peak_memory: std::sync::atomic::AtomicUsize,
}

/// Returns the Arrow schema for the given stream as a heap-allocated FFI_ArrowSchema pointer.
///
/// # Safety
/// `stream_ptr` must be a valid, non-zero pointer to a MemoryTrackingStream.
pub unsafe fn stream_get_schema(stream_ptr: i64) -> Result<i64, DataFusionError> {
    let stream = &*(stream_ptr as *const MemoryTrackingStream);
    let schema = stream.inner.schema();
    let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref())
        .map_err(|e| DataFusionError::Execution(format!("Schema conversion failed: {}", e)))?;
    Ok(Box::into_raw(Box::new(ffi_schema)) as i64)
}

/// Loads the next record batch from the stream.
///
/// Returns a heap-allocated FFI_ArrowArray pointer (as i64), or 0 if end-of-stream.
///
/// This is an async function — the bridge layer decides how to run it.
///
/// # Safety
/// `stream_ptr` must be a valid, non-zero pointer. Must not be called concurrently
/// on the same stream.
pub async unsafe fn stream_next(
    stream_ptr: i64,
) -> Result<i64, DataFusionError> {
    let tracking = &mut *(stream_ptr as *mut MemoryTrackingStream);

    let t = std::time::Instant::now();
    let result = tracking.inner.try_next().await?;

    // Track peak memory
    let current = tracking.memory_pool.reserved();
    tracking.peak_memory.fetch_max(current, std::sync::atomic::Ordering::Relaxed);

    match result {
        Some(batch) => {
            let rows = batch.num_rows();
            let cols = batch.num_columns();
            eprintln!(
                "[PERF] stream_next: {}ms, batch={}rows x {}cols, mem={} MB",
                t.elapsed().as_millis(), rows, cols, current / (1024 * 1024)
            );
            let struct_array: StructArray = batch.into();
            let array_data = struct_array.into_data();
            let ffi_array = FFI_ArrowArray::new(&array_data);
            Ok(Box::into_raw(Box::new(ffi_array)) as i64)
        }
        None => {
            let peak = tracking.peak_memory.load(std::sync::atomic::Ordering::Relaxed);
            eprintln!(
                "[PERF] stream_next: {}ms, end-of-stream. Memory: current={} MB, peak={} MB",
                t.elapsed().as_millis(), current / (1024 * 1024), peak / (1024 * 1024)
            );
            crate::cross_rt_object_store::dump_s3_counters();
            Ok(0)
        }
    }
}

/// Closes a result stream. Safe to call with 0 (no-op).
/// After dropping the stream, forces mimalloc to return freed pages to the OS
/// so that accumulated native memory from previous queries doesn't cause OOM.
///
/// # Safety
/// `stream_ptr` must be 0 or a valid pointer returned by `execute_query`.
pub unsafe fn stream_close(stream_ptr: i64) {
    if stream_ptr != 0 {
        let _ = Box::from_raw(stream_ptr as *mut MemoryTrackingStream);
    }
    // Force mimalloc to decommit freed pages back to the OS.
    // Without this, mimalloc holds freed virtual memory pages in thread-local
    // heaps for reuse, causing RSS to accumulate across queries until OOM.
    mimalloc_purge();
}

/// Executes a SQL query against Parquet files and returns results as Arrow IPC bytes.
///
/// Same setup as `execute_iceberg_query` but collects all result batches and
/// serializes them to Arrow IPC stream format. The returned `IpcResult` contains
/// the complete IPC byte stream (schema + all batches).
///
/// This is used by distributed workers to produce Arrow IPC bytes for efficient
/// network transport — Java passes the bytes through without deserialization.
pub async fn execute_iceberg_query_to_ipc(
    s3_region: &str,
    s3_bucket: Option<&str>,
    s3_access_key_id: Option<&str>,
    s3_secret_access_key: Option<&str>,
    s3_session_token: Option<&str>,
    s3_endpoint: Option<&str>,
    file_paths: Vec<String>,
    file_sizes: Vec<i64>,
    table_name: &str,
    sql_query: &str,
    runtime: &DataFusionRuntime,
    cpu_executor: crate::executor::DedicatedExecutor,
    io_handle: tokio::runtime::Handle,
) -> Result<Vec<u8>, DataFusionError> {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
    use datafusion::execution::cache::cache_manager::CacheManagerConfig;
    use datafusion::prelude::SessionContext;

    use std::time::Instant;
    let t_start = Instant::now();

    let is_local = file_paths.first().map_or(false, |p| p.starts_with("file://"));

    info!(
        "execute_iceberg_query_to_ipc: region={}, bucket={:?}, files={}, local={}, table={}, sql={}",
        s3_region, s3_bucket, file_paths.len(), is_local, table_name, sql_query
    );

    let runtime_env = RuntimeEnvBuilder::from_runtime_env(&runtime.runtime_env)
        .with_cache_manager(
            CacheManagerConfig::default()
                .with_file_metadata_cache(Some(
                    runtime.runtime_env.cache_manager.get_file_metadata_cache(),
                ))
                .with_files_statistics_cache(
                    runtime.runtime_env.cache_manager.get_file_statistic_cache(),
                ),
        )
        .build()
        .map_err(|e| {
            error!("Failed to build runtime env: {}", e);
            e
        })?;

    let store: Arc<dyn ObjectStore>;
    let file_urls: Vec<String>;

    if is_local {
        let local_store = Arc::new(object_store::local::LocalFileSystem::new());
        store = local_store;
        file_urls = file_paths.clone();
        let local_url = url::Url::parse("file:///").map_err(|e| {
            DataFusionError::Execution(format!("Invalid file URL: {}", e))
        })?;
        runtime_env.register_object_store(&local_url, store.clone());
    } else {
        let bucket = s3_bucket.unwrap_or("unknown");
        let mut builder = AmazonS3Builder::new()
            .with_region(s3_region)
            .with_bucket_name(bucket);

        if let Some(key) = s3_access_key_id {
            builder = builder.with_access_key_id(key);
        }
        if let Some(secret) = s3_secret_access_key {
            builder = builder.with_secret_access_key(secret);
        }
        if let Some(token) = s3_session_token {
            builder = builder.with_token(token);
        }
        if let Some(endpoint) = s3_endpoint {
            builder = builder.with_endpoint(endpoint);
        }

        let s3_store = Arc::new(builder.build().map_err(|e| {
            DataFusionError::Execution(format!("Failed to build S3 object store: {}", e))
        })?);
        store = s3_store.clone();

        let store_url = url::Url::parse(&format!("s3://{}/", bucket)).map_err(|e| {
            DataFusionError::Execution(format!("Invalid S3 URL: {}", e))
        })?;
        let cross_rt_store = Arc::new(
            crate::cross_rt_object_store::CrossRuntimeObjectStore::new(s3_store.clone(), io_handle)
        );
        runtime_env.register_object_store(&store_url, cross_rt_store);

        let bucket_prefix = format!("s3://{}/", bucket);
        file_urls = file_paths.iter().map(|path| {
            if path.starts_with("s3://") {
                path.clone()
            } else {
                format!("{}{}", bucket_prefix, path)
            }
        }).collect();
    }

    let object_metas: Vec<object_store::ObjectMeta> = file_paths.iter().zip(file_sizes.iter()).map(|(path, &size)| {
        let key = if let Some(stripped) = path.strip_prefix("file://") {
            stripped.to_string()
        } else if let Some(bucket) = s3_bucket {
            let bucket_prefix = format!("s3://{}/", bucket);
            if let Some(stripped) = path.strip_prefix(&bucket_prefix) {
                stripped.to_string()
            } else if let Some(stripped) = path.strip_prefix("s3://") {
                if let Some(after_bucket) = stripped.find('/') {
                    stripped[after_bucket + 1..].to_string()
                } else {
                    path.clone()
                }
            } else {
                path.clone()
            }
        } else {
            path.clone()
        };
        object_store::ObjectMeta {
            location: object_store::path::Path::from(key.as_str()),
            size: size as u64,
            last_modified: Default::default(),
            e_tag: None,
            version: None,
        }
    }).collect();

    let num_files = object_metas.len();
    let mut config = SessionConfig::new();
    let half_cpus = (num_cpus::get() / 2).max(1);
    config.options_mut().execution.target_partitions = num_files.min(half_cpus).max(1);
    config.options_mut().execution.batch_size = 8192;

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        .build();

    let ctx = SessionContext::new_with_state(state);

    let file_format = Arc::new(ParquetFormat::new());
    use datafusion::datasource::file_format::FileFormat;
    let resolved_schema = file_format.infer_schema(
        &ctx.state(),
        &store,
        &[object_metas[0].clone()],
    ).await?;

    let listing_options = ListingOptions::new(file_format)
        .with_file_extension(".parquet")
        .with_collect_stat(true);

    let table_config = ListingTableConfig::new_with_multi_paths(
        file_urls.iter()
            .map(|u| ListingTableUrl::parse(u))
            .collect::<Result<Vec<_>, _>>()?
    )
        .with_listing_options(listing_options)
        .with_schema(resolved_schema);

    let provider = Arc::new(ListingTable::try_new(table_config)?);
    ctx.register_table(table_name, provider)?;

    crate::cross_rt_object_store::reset_s3_counters();

    let dataframe = ctx.sql(sql_query).await?;
    let plan = dataframe.create_physical_plan().await?;

    let stream = datafusion::physical_plan::execute_stream(plan, ctx.task_ctx())?;
    let schema = stream.schema();

    // Collect all batches via CrossRtStream (CPU executor pulls batches)
    let cross_rt_stream = CrossRtStream::new_with_df_error_stream(stream, cpu_executor);
    let batches: Vec<RecordBatch> = cross_rt_stream.try_collect().await?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    eprintln!(
        "[PERF] execute_iceberg_query_to_ipc: {}ms, {} batches, {} rows",
        t_start.elapsed().as_millis(), batches.len(), total_rows
    );

    crate::cross_rt_object_store::dump_s3_counters();

    // Serialize to Arrow IPC stream format
    let ipc_bytes = batches_to_ipc(&schema, &batches)?;
    eprintln!("[PERF] Arrow IPC serialization: {} bytes", ipc_bytes.len());

    // Drop intermediate data before purging
    drop(batches);

    // Force mimalloc to return freed pages to the OS after query execution.
    // Without this, worker RSS accumulates across queries causing OOM.
    mimalloc_purge();

    Ok(ipc_bytes)
}

/// A PartitionStream backed by in-memory RecordBatches.
/// Used to create StreamingTable from deserialized Arrow IPC data.
#[derive(Debug)]
struct MemoryPartitionStream {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl datafusion::physical_plan::streaming::PartitionStream for MemoryPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(
        &self,
        _ctx: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::execution::SendableRecordBatchStream {
        let schema = self.schema.clone();
        let batches = self.batches.clone();
        let stream = futures::stream::iter(batches.into_iter().map(Ok));
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}

/// Executes SQL against Arrow IPC input data using DataFusion StreamingTable.
///
/// Used by the coordinator to merge worker results: takes Arrow IPC bytes from
/// multiple workers, registers them as a single StreamingTable named "input",
/// runs the merge SQL, and returns the result as Arrow IPC bytes.
///
/// This is the foundation for Phase 2.2 (coordinator StreamingTable merge) and
/// will later be used by Stage 1 workers for shuffle consumption.
pub async fn execute_from_ipc(
    ipc_batches: Vec<&[u8]>,
    sql: &str,
    runtime: &DataFusionRuntime,
    cpu_executor: crate::executor::DedicatedExecutor,
) -> Result<Vec<u8>, DataFusionError> {
    use datafusion::catalog::streaming::StreamingTable;
    use datafusion::prelude::SessionContext;

    use std::time::Instant;
    let t_start = Instant::now();

    // Deserialize all worker IPC bytes into RecordBatches
    let mut all_batches = Vec::new();
    let mut schema: Option<SchemaRef> = None;
    for (i, ipc_bytes) in ipc_batches.iter().enumerate() {
        let (batch_schema, batches) = ipc_to_batches(ipc_bytes)?;
        if schema.is_none() {
            schema = Some(batch_schema);
        }
        eprintln!("[PERF] execute_from_ipc: worker {} → {} batches", i, batches.len());
        all_batches.extend(batches);
    }

    let schema = schema.ok_or_else(|| {
        DataFusionError::Execution("No Arrow IPC data from workers".to_string())
    })?;

    info!(
        "execute_from_ipc: {} workers, {} total batches, sql={}",
        ipc_batches.len(), all_batches.len(), sql
    );

    // Create a StreamingTable from the collected batches via PartitionStream
    let partition = Arc::new(MemoryPartitionStream {
        schema: schema.clone(),
        batches: all_batches,
    }) as Arc<dyn datafusion::physical_plan::streaming::PartitionStream>;

    let streaming_table = StreamingTable::try_new(
        schema.clone(),
        vec![partition],
    )?;

    // Build session and register the streaming table as "input"
    let runtime_env = RuntimeEnvBuilder::from_runtime_env(&runtime.runtime_env)
        .build()
        .map_err(|e| {
            error!("Failed to build runtime env for merge: {}", e);
            e
        })?;

    let config = SessionConfig::new();
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        .build();

    let ctx = SessionContext::new_with_state(state);
    ctx.register_table("input", Arc::new(streaming_table))?;

    // Execute the merge SQL
    let dataframe = ctx.sql(sql).await?;
    let plan = dataframe.create_physical_plan().await?;
    let stream = datafusion::physical_plan::execute_stream(plan, ctx.task_ctx())?;
    let result_schema = stream.schema();

    // Collect results via CrossRtStream
    let cross_rt_stream = CrossRtStream::new_with_df_error_stream(stream, cpu_executor);
    let result_batches: Vec<RecordBatch> = cross_rt_stream.try_collect().await?;

    let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
    eprintln!(
        "[PERF] execute_from_ipc: {}ms, {} result rows",
        t_start.elapsed().as_millis(), total_rows
    );

    // Serialize result to Arrow IPC
    let ipc = batches_to_ipc(&result_schema, &result_batches)?;

    // Drop intermediate data before purging
    drop(result_batches);

    // Force mimalloc to return freed pages to the OS after merge.
    mimalloc_purge();

    Ok(ipc)
}

/// Converts SQL to Substrait plan bytes (test only).
///
/// # Safety
/// `shard_view_ptr` and `runtime_ptr` must be valid, non-zero pointers.
pub unsafe fn sql_to_substrait(
    shard_view_ptr: i64,
    table_name: &str,
    sql: &str,
    runtime_ptr: i64,
    manager: &RuntimeManager,
) -> Result<Vec<u8>, DataFusionError> {
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::execution::cache::{CacheAccessor, DefaultListFilesCache};
    use datafusion::execution::cache::cache_manager::CacheManagerConfig;
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use prost::Message;

    let shard_view = &*(shard_view_ptr as *const ShardView);
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let table_path = shard_view.table_path.clone();
    let object_metas = shard_view.object_metas.clone();
    let table_name = table_name.to_string();

    manager.io_runtime.block_on(async {
        let list_file_cache = Arc::new(DefaultListFilesCache::default());
        list_file_cache.put(
            &datafusion::execution::cache::TableScopedPath {
                table: None,
                path: table_path.prefix().clone(),
            },
            object_metas,
        );
        let runtime_env = RuntimeEnvBuilder::from_runtime_env(&runtime.runtime_env)
            .with_cache_manager(
                CacheManagerConfig::default()
                    .with_list_files_cache(Some(list_file_cache))
                    .with_file_metadata_cache(Some(
                        runtime.runtime_env.cache_manager.get_file_metadata_cache(),
                    ))
                    .with_files_statistics_cache(
                        runtime.runtime_env.cache_manager.get_file_statistic_cache(),
                    ),
            )
            .build()?;

        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_runtime_env(Arc::from(runtime_env))
            .with_default_features()
            .build();
        let ctx = datafusion::prelude::SessionContext::new_with_state(state);

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::new()))
            .with_file_extension(".parquet")
            .with_collect_stat(true);
        let schema = listing_options.infer_schema(&ctx.state(), &table_path).await?;
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(schema);
        ctx.register_table(&table_name, Arc::new(ListingTable::try_new(config)?))?;

        let plan = ctx.sql(sql).await?.logical_plan().clone();
        let substrait = to_substrait_plan(&plan, &ctx.state())?;
        let mut buf = Vec::new();
        substrait.encode(&mut buf)
            .map_err(|e| DataFusionError::Execution(format!("Substrait encode failed: {}", e)))?;
        Ok(buf)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Int64Array, StringArray};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn test_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_ipc_roundtrip_single_batch() {
        let schema = test_schema();
        let batch = test_batch(&[1, 2, 3], &["alice", "bob", "charlie"]);

        let ipc_bytes = batches_to_ipc(&schema, &[batch.clone()]).unwrap();
        let (decoded_schema, decoded_batches) = ipc_to_batches(&ipc_bytes).unwrap();

        assert_eq!(decoded_schema, schema);
        assert_eq!(decoded_batches.len(), 1);
        assert_eq!(decoded_batches[0].num_rows(), 3);
        assert_eq!(decoded_batches[0], batch);
    }

    #[test]
    fn test_ipc_roundtrip_multiple_batches() {
        let schema = test_schema();
        let batch1 = test_batch(&[1, 2], &["a", "b"]);
        let batch2 = test_batch(&[3, 4, 5], &["c", "d", "e"]);

        let ipc_bytes = batches_to_ipc(&schema, &[batch1.clone(), batch2.clone()]).unwrap();
        let (_, decoded_batches) = ipc_to_batches(&ipc_bytes).unwrap();

        assert_eq!(decoded_batches.len(), 2);
        assert_eq!(decoded_batches[0], batch1);
        assert_eq!(decoded_batches[1], batch2);
    }

    #[test]
    fn test_ipc_roundtrip_empty_batch() {
        let schema = test_schema();
        let batch = RecordBatch::new_empty(schema.clone());

        let ipc_bytes = batches_to_ipc(&schema, &[batch]).unwrap();
        let (decoded_schema, decoded_batches) = ipc_to_batches(&ipc_bytes).unwrap();

        assert_eq!(decoded_schema, schema);
        assert_eq!(decoded_batches.len(), 1);
        assert_eq!(decoded_batches[0].num_rows(), 0);
    }

    #[test]
    fn test_ipc_roundtrip_no_batches() {
        let schema = test_schema();

        let ipc_bytes = batches_to_ipc(&schema, &[]).unwrap();
        let (decoded_schema, decoded_batches) = ipc_to_batches(&ipc_bytes).unwrap();

        assert_eq!(decoded_schema, schema);
        assert_eq!(decoded_batches.len(), 0);
    }

    #[test]
    fn test_ipc_schema_preserved() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Int64, true),
            Field::new("col_b", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        )
        .unwrap();

        let ipc_bytes = batches_to_ipc(&schema, &[batch]).unwrap();
        let (decoded_schema, _) = ipc_to_batches(&ipc_bytes).unwrap();

        assert_eq!(decoded_schema.fields().len(), 2);
        assert_eq!(decoded_schema.field(0).name(), "col_a");
        assert!(decoded_schema.field(0).is_nullable());
        assert_eq!(decoded_schema.field(1).name(), "col_b");
        assert!(!decoded_schema.field(1).is_nullable());
    }

    fn create_test_runtime() -> DataFusionRuntime {
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        let runtime_env = RuntimeEnvBuilder::new()
            .build()
            .unwrap();
        DataFusionRuntime {
            runtime_env: runtime_env,
        }
    }

    fn create_test_executor() -> crate::executor::DedicatedExecutor {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(2).enable_all();
        crate::executor::DedicatedExecutor::new("test-cpu", builder)
    }

    #[tokio::test]
    async fn test_execute_from_ipc_concat() {
        let runtime = create_test_runtime();
        let executor = create_test_executor();

        let schema = test_schema();
        let batch1 = test_batch(&[1, 2], &["a", "b"]);
        let batch2 = test_batch(&[3, 4], &["c", "d"]);

        let ipc1 = batches_to_ipc(&schema, &[batch1]).unwrap();
        let ipc2 = batches_to_ipc(&schema, &[batch2]).unwrap();

        let result_bytes = execute_from_ipc(
            vec![ipc1.as_slice(), ipc2.as_slice()],
            "SELECT * FROM input",
            &runtime,
            executor.clone(),
        )
        .await
        .unwrap();

        let (_, result_batches) = ipc_to_batches(&result_bytes).unwrap();
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);
        executor.join_blocking();
    }

    #[tokio::test]
    async fn test_execute_from_ipc_aggregation() {
        let runtime = create_test_runtime();
        let executor = create_test_executor();

        let agg_schema = Arc::new(Schema::new(vec![
            Field::new("cnt", DataType::Int64, false),
            Field::new("total", DataType::Int64, false),
        ]));
        let batch1 = RecordBatch::try_new(
            agg_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Int64Array::from(vec![5000])),
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            agg_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![200])),
                Arc::new(Int64Array::from(vec![3000])),
            ],
        )
        .unwrap();

        let ipc1 = batches_to_ipc(&agg_schema, &[batch1]).unwrap();
        let ipc2 = batches_to_ipc(&agg_schema, &[batch2]).unwrap();

        let result_bytes = execute_from_ipc(
            vec![ipc1.as_slice(), ipc2.as_slice()],
            "SELECT SUM(cnt) AS cnt, SUM(total) AS total FROM input",
            &runtime,
            executor.clone(),
        )
        .await
        .unwrap();

        let (_, result_batches) = ipc_to_batches(&result_bytes).unwrap();
        assert_eq!(result_batches.len(), 1);
        assert_eq!(result_batches[0].num_rows(), 1);

        let cnt_col = result_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let total_col = result_batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(cnt_col.value(0), 300);
        assert_eq!(total_col.value(0), 8000);
        executor.join_blocking();
    }

    #[tokio::test]
    async fn test_execute_from_ipc_topk() {
        let runtime = create_test_runtime();
        let executor = create_test_executor();

        let schema = test_schema();
        let batch1 = test_batch(&[10, 30], &["x", "z"]);
        let batch2 = test_batch(&[20, 40], &["y", "w"]);

        let ipc1 = batches_to_ipc(&schema, &[batch1]).unwrap();
        let ipc2 = batches_to_ipc(&schema, &[batch2]).unwrap();

        let result_bytes = execute_from_ipc(
            vec![ipc1.as_slice(), ipc2.as_slice()],
            "SELECT * FROM input ORDER BY id DESC LIMIT 2",
            &runtime,
            executor.clone(),
        )
        .await
        .unwrap();

        let (_, result_batches) = ipc_to_batches(&result_bytes).unwrap();
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        let id_col = result_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 40);
        assert_eq!(id_col.value(1), 30);
        executor.join_blocking();
    }
}
