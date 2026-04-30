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

use std::io::Cursor;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::ipc::reader::StreamReader;
use arrow_array::{Array, StructArray};
use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::RecordBatch;
use arrow_schema::ffi::FFI_ArrowSchema;
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
use crate::local_executor::LocalSession;
use crate::partition_stream::PartitionStreamSender;
use crate::query_memory_pool_tracker::QueryTrackingContext;
use crate::runtime_manager::RuntimeManager;

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
/// `context_id` enables per-query memory tracking: when non-zero, a
/// [`crate::query_memory_pool_tracker::QueryTrackingContext`] is created and its
/// [`crate::query_memory_pool_tracker::QueryMemoryPool`] is installed on the per-query
/// `RuntimeEnv`. A value of 0 disables tracking.
///
/// # Safety
/// `shard_view_ptr` and `runtime_ptr` must be valid, non-zero pointers.
pub async unsafe fn execute_query(
    shard_view_ptr: i64,
    table_name: &str,
    plan_bytes: &[u8],
    runtime_ptr: i64,
    manager: &RuntimeManager,
    context_id: i64,
) -> Result<i64, DataFusionError> {
    let shard_view = &*(shard_view_ptr as *const ShardView);
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);

    let table_path = shard_view.table_path.clone();
    let object_metas = shard_view.object_metas.clone();
    let cpu_executor = manager.cpu_executor();

    // Wire up per-query tracking if the caller supplied a non-zero context_id.
    let tracking_ctx = crate::query_memory_pool_tracker::QueryTrackingContext::new(
        context_id,
        runtime.runtime_env.memory_pool.clone(),
    );
    let query_memory_pool = tracking_ctx
        .memory_pool()
        .map(|p| p as Arc<dyn datafusion::execution::memory_pool::MemoryPool>);
    // Keep the tracking context alive for the duration of the query planning.
    // Once the returned stream is dropped, `tracking_ctx` drops with it (it is
    // captured by the closures in `query_executor::execute_query` via the pool
    // cloned into the `RuntimeEnv`). We explicitly drop it here to mark
    // completion immediately — the registry retains a snapshot for JNI readers.
    let _ = &tracking_ctx;

    let result = crate::query_executor::execute_query(
        table_path,
        object_metas,
        table_name.to_string(),
        plan_bytes.to_vec(),
        runtime,
        cpu_executor,
        query_memory_pool,
    )
    .await?;

    // Dropping the context marks it completed; metrics remain in the registry.
    drop(tracking_ctx);
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
    config.options_mut().execution.target_partitions = num_files.min(16).max(1);
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
///
/// # Safety
/// `stream_ptr` must be 0 or a valid pointer returned by `execute_query`.
pub unsafe fn stream_close(stream_ptr: i64) {
    if stream_ptr != 0 {
        let _ = Box::from_raw(stream_ptr as *mut MemoryTrackingStream);
    }
}

/// Logical table name under which IPC-backed batches are registered for
/// [`execute_from_ipc`]. The caller's SQL must reference this name.
pub const EXCHANGE_INPUT_TABLE: &str = "__exchange_input__";

/// Executes a SQL query against an in-memory table built from Arrow IPC stream bytes.
///
/// `ipc_bytes` must be a valid Arrow IPC stream (one or more record batches prefixed by a
/// schema header) — exactly what `arrow_ipc::writer::StreamWriter` produces. An empty
/// stream (schema only, no batches) is not an error: the table is registered with the
/// schema and zero partitions of data.
///
/// The in-memory table is registered under [`EXCHANGE_INPUT_TABLE`] in a fresh
/// `SessionContext` bound to the supplied `runtime` (sharing its memory pool and caches).
/// The stream is returned in the same boxed [`MemoryTrackingStream`] form as
/// [`execute_iceberg_query`], so the caller closes it via `stream_close`.
pub async fn execute_from_ipc(
    ipc_bytes: Vec<u8>,
    sql: &str,
    runtime: &DataFusionRuntime,
    cpu_executor: crate::executor::DedicatedExecutor,
) -> Result<i64, DataFusionError> {
    use datafusion::arrow::ipc::reader::StreamReader;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::MemTable;
    use datafusion::execution::cache::cache_manager::CacheManagerConfig;
    use datafusion::prelude::SessionContext;

    // Decode the IPC stream into batches. The reader validates the header and
    // yields the schema before any batches; an empty stream is legal and just
    // produces zero batches.
    let mut reader = StreamReader::try_new(std::io::Cursor::new(ipc_bytes), None)
        .map_err(|e| DataFusionError::Execution(format!("Invalid Arrow IPC stream: {}", e)))?;
    let schema = reader.schema();
    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch_result in reader.by_ref() {
        let batch = batch_result.map_err(|e| {
            DataFusionError::Execution(format!("Failed to read IPC batch: {}", e))
        })?;
        batches.push(batch);
    }

    info!(
        "execute_from_ipc: schema fields={}, batches={}, sql={}",
        schema.fields().len(),
        batches.len(),
        sql
    );

    // Share the global memory pool / caches, same pattern as execute_iceberg_query.
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

    let mut config = SessionConfig::new();
    // A single partition is sufficient for coordinator-side merges over already
    // materialized IPC batches; increasing partitions here just adds empty splits.
    config.options_mut().execution.target_partitions = 1;
    config.options_mut().execution.batch_size = 8192;

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);

    // MemTable::try_new requires at least one partition; use a single partition
    // containing all batches (or no batches for schema-only input).
    let mem_table = MemTable::try_new(schema.clone(), vec![batches]).map_err(|e| {
        DataFusionError::Execution(format!("Failed to build MemTable from IPC batches: {}", e))
    })?;
    ctx.register_table(EXCHANGE_INPUT_TABLE, Arc::new(mem_table))?;

    // Plan + stream, exactly like execute_iceberg_query.
    let dataframe = ctx.sql(sql).await?;
    let stream = dataframe.execute_stream().await?;

    let memory_pool = runtime.runtime_env.memory_pool.clone();
    let pool_reserved = memory_pool.reserved();

    let cross_rt_stream = CrossRtStream::new_with_df_error_stream(stream, cpu_executor);
    let wrapped = MemoryTrackingStream {
        inner: RecordBatchStreamAdapter::new(cross_rt_stream.schema(), cross_rt_stream),
        memory_pool,
        peak_memory: std::sync::atomic::AtomicUsize::new(pool_reserved),
    };

    Ok(Box::into_raw(Box::new(wrapped)) as i64)
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

// ---------------------------------------------------------------------------
// Coordinator-reduce local execution API
//
// Mirrors the shard-scan path: a `LocalSession` pointer is created once per
// reduce stage, streaming inputs are registered under synthetic names, a
// Substrait plan is executed against those inputs, and the output stream is
// drained via the existing `stream_next` / `stream_close` exports (because
// `execute_local_plan` hands back a `QueryStreamHandle` of the same shape
// `execute_query` returns).
// ---------------------------------------------------------------------------

/// Creates a `LocalSession` bound to the given runtime's [`RuntimeEnv`]
/// (memory pool, disk manager, and caches are shared).
///
/// Returns a heap-allocated pointer (as i64) to `LocalSession`. Caller must
/// call `close_local_session` exactly once to free it.
///
/// # Safety
/// `runtime_ptr` must be a valid, non-zero pointer returned by
/// `create_global_runtime`.
pub unsafe fn create_local_session(runtime_ptr: i64) -> Result<i64, DataFusionError> {
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let session = LocalSession::new(&runtime.runtime_env);
    Ok(Box::into_raw(Box::new(session)) as i64)
}

/// Closes a `LocalSession`. Safe to call with 0 (no-op).
///
/// # Safety
/// `ptr` must be 0 or a valid pointer returned by `create_local_session`.
pub unsafe fn close_local_session(ptr: i64) {
    if ptr != 0 {
        let _ = Box::from_raw(ptr as *mut LocalSession);
    }
}

/// Registers a streaming input on the session under `input_id`, using the
/// Arrow schema decoded from the IPC stream bytes.
///
/// The IPC bytes are expected to be a single schema message produced by
/// Arrow's streaming IPC writer (e.g. Java's `MessageSerializer.serializeMetadata`
/// or an `ArrowStreamWriter` flush of just the schema). Only the schema is
/// read — any payload in the buffer is ignored.
///
/// Returns a heap-allocated pointer (as i64) to a [`PartitionStreamSender`].
/// Caller must call `sender_close` exactly once to free it (closing the
/// sender signals EOF to the receiver side, so the native execute driver
/// naturally completes).
///
/// # Safety
/// `session_ptr` must be a valid, non-zero pointer returned by
/// `create_local_session`.
pub unsafe fn register_partition_stream(
    session_ptr: i64,
    input_id: &str,
    schema_ipc: &[u8],
) -> Result<i64, DataFusionError> {
    let session = &mut *(session_ptr as *mut LocalSession);
    let mut cursor = Cursor::new(schema_ipc);
    let reader = StreamReader::try_new(&mut cursor, None).map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to decode Arrow IPC schema for '{}': {}",
            input_id, e
        ))
    })?;
    let schema = reader.schema();
    let sender = session.register_partition(input_id, schema)?;
    Ok(Box::into_raw(Box::new(sender)) as i64)
}

/// Executes a Substrait plan against a `LocalSession` and returns a
/// `QueryStreamHandle` pointer whose output can be drained via the existing
/// `stream_next` / `stream_close` exports.
///
/// The returned stream wraps the DataFusion output in the same
/// `CrossRtStream` + `RecordBatchStreamAdapter` shape as `execute_query`,
/// so the session produces batches on the CPU executor while `stream_next`
/// consumes them on the I/O runtime.
///
/// This is an async function — the bridge layer decides how to run it
/// (`block_on` for synchronous FFM entry, `spawn` for async delivery).
///
/// # Safety
/// `session_ptr` must be a valid, non-zero pointer returned by
/// `create_local_session`.
pub async unsafe fn execute_local_plan(
    session_ptr: i64,
    substrait_bytes: &[u8],
    manager: &RuntimeManager,
    context_id: i64,
) -> Result<i64, DataFusionError> {
    let session = &*(session_ptr as *const LocalSession);

    // Per-query memory tracking — wraps the session's global pool. A
    // `context_id` of 0 disables tracking (pool is not consulted).
    let query_context = QueryTrackingContext::new(context_id, session.memory_pool());

    let df_stream = session.execute_substrait(substrait_bytes).await?;

    // Wrap the output in the same CrossRtStream + RecordBatchStreamAdapter
    // shape as `execute_query`, so existing `stream_next` / `stream_close`
    // drain this handle unchanged.
    let cross_rt_stream =
        CrossRtStream::new_with_df_error_stream(df_stream, manager.cpu_executor());
    let wrapped = RecordBatchStreamAdapter::new(cross_rt_stream.schema(), cross_rt_stream);

    let handle = QueryStreamHandle::new(wrapped, query_context);
    Ok(Box::into_raw(Box::new(handle)) as i64)
}

/// Imports an Arrow C Data batch and pushes it through the partition
/// stream's mpsc. The Rust side takes ownership of the
/// `FFI_ArrowArray` / `FFI_ArrowSchema` structs on success — the Java side
/// must not release them after a successful send. On error ownership is
/// released back to Rust's drop impls (the imported structs go out of scope
/// without being forgotten).
///
/// The `io_handle` is the Tokio handle used to drive the blocking send;
/// typically the `io_runtime` handle from the global `RuntimeManager`.
///
/// # Safety
/// - `sender_ptr` must be a valid, non-zero pointer returned by
///   `register_partition_stream`.
/// - `array_ptr` must point to a populated `FFI_ArrowArray` struct owned by
///   the caller; ownership transfers to Rust on success.
/// - `schema_ptr` must point to a populated `FFI_ArrowSchema` struct owned
///   by the caller; ownership transfers to Rust on success.
pub unsafe fn sender_send(
    sender_ptr: i64,
    array_ptr: i64,
    schema_ptr: i64,
    io_handle: &tokio::runtime::Handle,
) -> Result<(), DataFusionError> {
    let sender = &*(sender_ptr as *const PartitionStreamSender);

    // Take ownership of the Java-allocated FFI structs. `from_raw` reads
    // the struct contents into Rust-owned values; the original memory is
    // now Rust's responsibility to drop.
    let ffi_array = FFI_ArrowArray::from_raw(array_ptr as *mut FFI_ArrowArray);
    let ffi_schema = FFI_ArrowSchema::from_raw(schema_ptr as *mut FFI_ArrowSchema);

    // `from_ffi` takes the array by value (consumes it) and the schema by
    // reference (it is still dropped when `ffi_schema` goes out of scope).
    let array_data = arrow_array::ffi::from_ffi(ffi_array, &ffi_schema).map_err(|e| {
        DataFusionError::Execution(format!("Failed to import Arrow C Data array: {}", e))
    })?;

    let struct_array = StructArray::from(array_data);
    let batch = RecordBatch::from(struct_array);

    sender.send_blocking(Ok(batch), io_handle)
}

/// Closes a partition stream sender. Dropping the sender closes the mpsc,
/// which the receiver side (DataFusion's streaming table) interprets as
/// end-of-input.
///
/// Safe to call with 0 (no-op).
///
/// # Safety
/// `sender_ptr` must be 0 or a valid pointer returned by
/// `register_partition_stream`.
pub unsafe fn sender_close(sender_ptr: i64) {
    if sender_ptr != 0 {
        let _ = Box::from_raw(sender_ptr as *mut PartitionStreamSender);
    }
}

/// Imports a batch of Arrow C Data structures into a [`Vec<RecordBatch>`] and
/// registers them as an in-memory table on the given session under `input_id`.
///
/// The Java side has accumulated all shard responses, exported each
/// `VectorSchemaRoot` to a paired `FFI_ArrowArray` / `FFI_ArrowSchema`, and
/// passed the raw pointers as two parallel slices. Rust takes ownership of
/// the FFI structs on success.
///
/// On error ownership is released back to Rust's drop impls (the imported
/// structs go out of scope without being forgotten).
///
/// # Safety
/// - `session_ptr` must be a valid, non-zero pointer returned by
///   `create_local_session`.
/// - `array_ptrs` and `schema_ptrs` must point to populated FFI structs owned
///   by the caller; ownership transfers to Rust on success.
pub unsafe fn register_memtable(
    session_ptr: i64,
    input_id: &str,
    schema_ipc: &[u8],
    array_ptrs: &[i64],
    schema_ptrs: &[i64],
) -> Result<(), DataFusionError> {
    if array_ptrs.len() != schema_ptrs.len() {
        return Err(DataFusionError::Execution(format!(
            "register_memtable: array_ptrs.len()={} != schema_ptrs.len()={}",
            array_ptrs.len(),
            schema_ptrs.len()
        )));
    }
    let session = &mut *(session_ptr as *mut LocalSession);

    let mut cursor = Cursor::new(schema_ipc);
    let reader = StreamReader::try_new(&mut cursor, None).map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to decode Arrow IPC schema for '{}': {}",
            input_id, e
        ))
    })?;
    let table_schema = reader.schema();

    // The IPC schema is what the substrait plan was compiled against — same as the streaming
    // sink registers. The exported VSRs may arrive with batch-level schemas that differ in
    // nullability/metadata/field-naming details; the streaming sink tolerates this because
    // DataFusion's streaming source addresses columns by index. `MemTable::try_new` instead
    // checks each batch's schema against the table schema. To stay compatible with both
    // shapes, rebuild each imported batch with `table_schema` — the column data is reused
    // verbatim, but the schema header is the planner's.
    let mut batches = Vec::with_capacity(array_ptrs.len());
    for (&array_ptr, &schema_ptr) in array_ptrs.iter().zip(schema_ptrs.iter()) {
        let ffi_array = FFI_ArrowArray::from_raw(array_ptr as *mut FFI_ArrowArray);
        let ffi_schema = FFI_ArrowSchema::from_raw(schema_ptr as *mut FFI_ArrowSchema);
        let array_data = arrow_array::ffi::from_ffi(ffi_array, &ffi_schema).map_err(|e| {
            DataFusionError::Execution(format!("Failed to import Arrow C Data array: {}", e))
        })?;
        let struct_array = StructArray::from(array_data);
        let raw = RecordBatch::from(struct_array);
        let aligned = RecordBatch::try_new(Arc::clone(&table_schema), raw.columns().to_vec()).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to align imported batch to registered schema for '{}': {}",
                input_id, e
            ))
        })?;
        batches.push(aligned);
    }

    session.register_memtable(input_id, table_schema, batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::ipc::writer::StreamWriter;

    use crate::executor::DedicatedExecutor;

    // ---------------------------------------------------------------------
    // Fixtures
    // ---------------------------------------------------------------------

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, false)]))
    }

    fn batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    /// Serialize batches into an Arrow IPC stream byte buffer.
    fn batches_to_ipc(schema: &Arc<Schema>, batches: &[RecordBatch]) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref()).unwrap();
            for b in batches {
                writer.write(b).unwrap();
            }
            writer.finish().unwrap();
        }
        buf
    }

    /// Small self-contained runtime for tests — isolated from the global
    /// `TOKIO_RUNTIME_MANAGER` held by `ffm.rs` (so tests can run in parallel).
    struct TestHarness {
        runtime_ptr: i64,
        tokio_rt: tokio::runtime::Runtime,
        cpu_executor: DedicatedExecutor,
    }

    impl TestHarness {
        fn new() -> Self {
            let tokio_rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();

            let mut cpu_builder = tokio::runtime::Builder::new_multi_thread();
            cpu_builder.worker_threads(1).enable_all();
            let cpu_executor = DedicatedExecutor::new("test-ipc-cpu", cpu_builder);

            // Small pool is plenty for these tests. 64 MiB.
            let runtime_ptr =
                create_global_runtime(64 * 1024 * 1024, "/tmp", 64 * 1024 * 1024).unwrap();

            Self { runtime_ptr, tokio_rt, cpu_executor }
        }

        fn runtime(&self) -> &DataFusionRuntime {
            unsafe { &*(self.runtime_ptr as *const DataFusionRuntime) }
        }

        /// Drain a `MemoryTrackingStream` pointer and return the total row count.
        fn drain_rows(&self, stream_ptr: i64) -> usize {
            assert!(stream_ptr > 0, "expected positive stream pointer, got {}", stream_ptr);
            let tracking = unsafe { &mut *(stream_ptr as *mut MemoryTrackingStream) };
            let mut rows = 0;
            self.tokio_rt.block_on(async {
                while let Some(batch) = tracking.inner.try_next().await.unwrap() {
                    rows += batch.num_rows();
                }
            });
            unsafe { stream_close(stream_ptr) };
            rows
        }
    }

    impl Drop for TestHarness {
        fn drop(&mut self) {
            self.cpu_executor.shutdown();
            unsafe { close_global_runtime(self.runtime_ptr) };
        }
    }

    // ---------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------

    #[test]
    fn test_execute_from_ipc_select_all() {
        let harness = TestHarness::new();
        let schema = test_schema();
        let batches = vec![batch(&[1, 2, 3]), batch(&[4, 5])];
        let ipc = batches_to_ipc(&schema, &batches);

        let ptr = harness
            .tokio_rt
            .block_on(execute_from_ipc(
                ipc,
                "SELECT * FROM __exchange_input__",
                harness.runtime(),
                harness.cpu_executor.clone(),
            ))
            .unwrap();

        let rows = harness.drain_rows(ptr);
        assert_eq!(rows, 5);
    }

    #[test]
    fn test_execute_from_ipc_empty_stream_returns_zero_rows() {
        let harness = TestHarness::new();
        let schema = test_schema();
        // Writer must be finished even without any batches so the IPC stream is valid.
        let ipc = batches_to_ipc(&schema, &[]);

        // Sanity check: the IPC bytes decode to zero batches but a valid schema.
        {
            let reader =
                datafusion::arrow::ipc::reader::StreamReader::try_new(
                    std::io::Cursor::new(&ipc),
                    None,
                )
                .unwrap();
            assert_eq!(reader.schema().fields().len(), 1);
        }

        let ptr = harness
            .tokio_rt
            .block_on(execute_from_ipc(
                ipc,
                "SELECT * FROM __exchange_input__",
                harness.runtime(),
                harness.cpu_executor.clone(),
            ))
            .unwrap();

        let rows = harness.drain_rows(ptr);
        assert_eq!(rows, 0);
    }

    #[test]
    fn test_execute_from_ipc_count_star_aggregation() {
        let harness = TestHarness::new();
        let schema = test_schema();
        let batches = vec![batch(&[10, 20, 30]), batch(&[40, 50, 60, 70])];
        let ipc = batches_to_ipc(&schema, &batches);

        let ptr = harness
            .tokio_rt
            .block_on(execute_from_ipc(
                ipc,
                "SELECT COUNT(*) AS c FROM __exchange_input__",
                harness.runtime(),
                harness.cpu_executor.clone(),
            ))
            .unwrap();

        // Aggregation returns a single row containing the count.
        assert!(ptr > 0);
        let tracking = unsafe { &mut *(ptr as *mut MemoryTrackingStream) };
        let count_batch = harness
            .tokio_rt
            .block_on(async { tracking.inner.try_next().await.unwrap() })
            .unwrap();
        assert_eq!(count_batch.num_rows(), 1);
        let col = count_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 7);
        // Drain any trailer and close.
        harness
            .tokio_rt
            .block_on(async { while tracking.inner.try_next().await.unwrap().is_some() {} });
        unsafe { stream_close(ptr) };
    }

    #[test]
    fn test_execute_from_ipc_invalid_bytes_returns_error() {
        let harness = TestHarness::new();
        // Random garbage with no IPC header.
        let bad_ipc: Vec<u8> = vec![0u8, 1, 2, 3, 4, 5, 6, 7];

        let err = harness
            .tokio_rt
            .block_on(execute_from_ipc(
                bad_ipc,
                "SELECT * FROM __exchange_input__",
                harness.runtime(),
                harness.cpu_executor.clone(),
            ))
            .unwrap_err();
        assert!(
            err.to_string().contains("Invalid Arrow IPC stream")
                || err.to_string().contains("IPC"),
            "unexpected error message: {}",
            err
        );
    }

    #[test]
    fn test_execute_from_ipc_registers_table_under_expected_name() {
        let harness = TestHarness::new();
        let schema = test_schema();
        let ipc = batches_to_ipc(&schema, &[batch(&[1])]);

        // Query referencing the unique sentinel name — confirms the constant is the
        // one the SQL must use.
        let ptr = harness
            .tokio_rt
            .block_on(execute_from_ipc(
                ipc,
                &format!("SELECT val FROM {}", EXCHANGE_INPUT_TABLE),
                harness.runtime(),
                harness.cpu_executor.clone(),
            ))
            .unwrap();

        let rows = harness.drain_rows(ptr);
        assert_eq!(rows, 1);
        assert_eq!(EXCHANGE_INPUT_TABLE, "__exchange_input__");
    }
}
