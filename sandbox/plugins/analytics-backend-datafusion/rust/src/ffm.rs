/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! FFM bridge for DataFusion.

use std::slice;
use std::str;
use std::sync::Arc;

use native_bridge_common::ffm_safe;
use parking_lot::RwLock;

use crate::api;
use crate::runtime_manager::RuntimeManager;

static TOKIO_RUNTIME_MANAGER: RwLock<Option<Arc<RuntimeManager>>> = RwLock::new(None);

unsafe fn str_from_raw<'a>(ptr: *const u8, len: i64) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err("null string pointer".to_string());
    }
    if len < 0 {
        return Err(format!("negative string length: {}", len));
    }
    let bytes = slice::from_raw_parts(ptr, len as usize);
    str::from_utf8(bytes).map_err(|e| format!("invalid UTF-8: {}", e))
}

fn get_rt_manager() -> Result<Arc<RuntimeManager>, String> {
    TOKIO_RUNTIME_MANAGER
        .read()
        .clone()
        .ok_or_else(|| "Runtime manager not initialized".to_string())
}

#[no_mangle]
pub extern "C" fn df_init_runtime_manager(cpu_threads: i32) {
    let mut guard = TOKIO_RUNTIME_MANAGER.write();
    *guard = Some(Arc::new(RuntimeManager::new(cpu_threads as usize)));
}

#[no_mangle]
pub extern "C" fn df_shutdown_runtime_manager() {
    let mgr = TOKIO_RUNTIME_MANAGER.write().take();
    if let Some(mgr) = mgr {
        mgr.shutdown();
    }
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_create_global_runtime(
    memory_pool_limit: i64,
    spill_dir_ptr: *const u8,
    spill_dir_len: i64,
    spill_limit: i64,
) -> i64 {
    let spill_dir = str_from_raw(spill_dir_ptr, spill_dir_len).map_err(|e| format!("df_create_global_runtime: {}", e))?;
    api::create_global_runtime(memory_pool_limit, spill_dir, spill_limit)
        .map_err(|e| e.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn df_close_global_runtime(ptr: i64) {
    api::close_global_runtime(ptr);
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_create_reader(
    table_path_ptr: *const u8,
    table_path_len: i64,
    files_ptr: *const *const u8,
    files_len_ptr: *const i64,
    files_count: i64,
) -> i64 {
    let table_path = str_from_raw(table_path_ptr, table_path_len).map_err(|e| format!("df_create_reader: {}", e))?;
    let mut filenames = Vec::with_capacity(files_count as usize);
    for i in 0..files_count as usize {
        let ptr = *files_ptr.add(i);
        let len = *files_len_ptr.add(i);
        filenames.push(str_from_raw(ptr, len).map_err(|e| format!("df_create_reader: {}", e))?.to_string());
    }
    let mgr = get_rt_manager()?;
    api::create_reader(table_path, filenames, &mgr).map_err(|e| e.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn df_close_reader(ptr: i64) {
    api::close_reader(ptr);
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_execute_query(
    shard_view_ptr: i64,
    table_name_ptr: *const u8,
    table_name_len: i64,
    plan_ptr: *const u8,
    plan_len: i64,
    runtime_ptr: i64,
) -> i64 {
    let mgr = get_rt_manager()?;
    let table_name = str_from_raw(table_name_ptr, table_name_len).map_err(|e| format!("df_execute_query: {}", e))?;
    let plan_bytes = slice::from_raw_parts(plan_ptr, plan_len as usize);
    mgr.io_runtime
        .block_on(api::execute_query(shard_view_ptr, table_name, plan_bytes, runtime_ptr, &mgr))
        .map_err(|e| e.to_string())
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_stream_get_schema(stream_ptr: i64) -> i64 {
    api::stream_get_schema(stream_ptr).map_err(|e| e.to_string())
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_stream_next(stream_ptr: i64) -> i64 {
    let mgr = get_rt_manager()?;
    mgr.io_runtime
        .block_on(api::stream_next(stream_ptr))
        .map_err(|e| e.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn df_stream_close(stream_ptr: i64) {
    api::stream_close(stream_ptr);
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_execute_iceberg_query(
    s3_region_ptr: *const u8,
    s3_region_len: i64,
    s3_bucket_ptr: *const u8,
    s3_bucket_len: i64,
    s3_access_key_ptr: *const u8,
    s3_access_key_len: i64,
    s3_secret_key_ptr: *const u8,
    s3_secret_key_len: i64,
    s3_session_token_ptr: *const u8,
    s3_session_token_len: i64,
    s3_endpoint_ptr: *const u8,
    s3_endpoint_len: i64,
    file_paths_ptr: *const *const u8,
    file_paths_lens: *const i64,
    file_sizes_ptr: *const i64,
    files_count: i64,
    table_name_ptr: *const u8,
    table_name_len: i64,
    sql_query_ptr: *const u8,
    sql_query_len: i64,
    runtime_ptr: i64,
) -> i64 {
    let mgr = get_rt_manager()?;

    let s3_region = str_from_raw(s3_region_ptr, s3_region_len)
        .map_err(|e| format!("df_execute_iceberg_query: s3_region: {}", e))?;

    // Optional strings: treat null or zero-length as None
    let s3_bucket = if s3_bucket_ptr.is_null() || s3_bucket_len <= 0 {
        None
    } else {
        Some(str_from_raw(s3_bucket_ptr, s3_bucket_len)
            .map_err(|e| format!("df_execute_iceberg_query: s3_bucket: {}", e))?)
    };
    let s3_access_key = if s3_access_key_ptr.is_null() || s3_access_key_len <= 0 {
        None
    } else {
        Some(str_from_raw(s3_access_key_ptr, s3_access_key_len)
            .map_err(|e| format!("df_execute_iceberg_query: s3_access_key: {}", e))?)
    };
    let s3_secret_key = if s3_secret_key_ptr.is_null() || s3_secret_key_len <= 0 {
        None
    } else {
        Some(str_from_raw(s3_secret_key_ptr, s3_secret_key_len)
            .map_err(|e| format!("df_execute_iceberg_query: s3_secret_key: {}", e))?)
    };
    let s3_session_token = if s3_session_token_ptr.is_null() || s3_session_token_len <= 0 {
        None
    } else {
        Some(str_from_raw(s3_session_token_ptr, s3_session_token_len)
            .map_err(|e| format!("df_execute_iceberg_query: s3_session_token: {}", e))?)
    };
    let s3_endpoint = if s3_endpoint_ptr.is_null() || s3_endpoint_len <= 0 {
        None
    } else {
        Some(str_from_raw(s3_endpoint_ptr, s3_endpoint_len)
            .map_err(|e| format!("df_execute_iceberg_query: s3_endpoint: {}", e))?)
    };

    let table_name = str_from_raw(table_name_ptr, table_name_len)
        .map_err(|e| format!("df_execute_iceberg_query: table_name: {}", e))?;
    let sql_query = str_from_raw(sql_query_ptr, sql_query_len)
        .map_err(|e| format!("df_execute_iceberg_query: sql_query: {}", e))?;

    // Parse file paths and sizes from parallel arrays
    let count = files_count as usize;
    let mut file_paths = Vec::with_capacity(count);
    for i in 0..count {
        let ptr = *file_paths_ptr.add(i);
        let len = *file_paths_lens.add(i);
        file_paths.push(
            str_from_raw(ptr, len)
                .map_err(|e| format!("df_execute_iceberg_query: file_path[{}]: {}", i, e))?
                .to_string(),
        );
    }
    let file_sizes: Vec<i64> = slice::from_raw_parts(file_sizes_ptr, count).to_vec();

    let runtime = &*(runtime_ptr as *const api::DataFusionRuntime);

    mgr.io_runtime
        .block_on(api::execute_iceberg_query(
            s3_region,
            s3_bucket,
            s3_access_key,
            s3_secret_key,
            s3_session_token,
            s3_endpoint,
            file_paths,
            file_sizes,
            table_name,
            sql_query,
            runtime,
            mgr.cpu_executor(),
            mgr.io_runtime.handle().clone(),
        ))
        .map_err(|e| e.to_string())
}

/// Executes a SQL query and returns results as Arrow IPC bytes (boxed IpcResult).
/// Returns a pointer to IpcResult; use df_ipc_result_data_ptr/len to access data,
/// and df_free_ipc_result to free.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_execute_iceberg_query_to_ipc(
    s3_region_ptr: *const u8,
    s3_region_len: i64,
    s3_bucket_ptr: *const u8,
    s3_bucket_len: i64,
    s3_access_key_ptr: *const u8,
    s3_access_key_len: i64,
    s3_secret_key_ptr: *const u8,
    s3_secret_key_len: i64,
    s3_session_token_ptr: *const u8,
    s3_session_token_len: i64,
    s3_endpoint_ptr: *const u8,
    s3_endpoint_len: i64,
    file_paths_ptr: *const *const u8,
    file_paths_lens: *const i64,
    file_sizes_ptr: *const i64,
    files_count: i64,
    table_name_ptr: *const u8,
    table_name_len: i64,
    sql_query_ptr: *const u8,
    sql_query_len: i64,
    runtime_ptr: i64,
) -> i64 {
    let mgr = get_rt_manager()?;

    let s3_region = str_from_raw(s3_region_ptr, s3_region_len)
        .map_err(|e| format!("df_execute_iceberg_query_to_ipc: s3_region: {}", e))?;
    let s3_bucket = if s3_bucket_ptr.is_null() || s3_bucket_len <= 0 { None }
        else { Some(str_from_raw(s3_bucket_ptr, s3_bucket_len).map_err(|e| format!("s3_bucket: {}", e))?) };
    let s3_access_key = if s3_access_key_ptr.is_null() || s3_access_key_len <= 0 { None }
        else { Some(str_from_raw(s3_access_key_ptr, s3_access_key_len).map_err(|e| format!("s3_access_key: {}", e))?) };
    let s3_secret_key = if s3_secret_key_ptr.is_null() || s3_secret_key_len <= 0 { None }
        else { Some(str_from_raw(s3_secret_key_ptr, s3_secret_key_len).map_err(|e| format!("s3_secret_key: {}", e))?) };
    let s3_session_token = if s3_session_token_ptr.is_null() || s3_session_token_len <= 0 { None }
        else { Some(str_from_raw(s3_session_token_ptr, s3_session_token_len).map_err(|e| format!("s3_session_token: {}", e))?) };
    let s3_endpoint = if s3_endpoint_ptr.is_null() || s3_endpoint_len <= 0 { None }
        else { Some(str_from_raw(s3_endpoint_ptr, s3_endpoint_len).map_err(|e| format!("s3_endpoint: {}", e))?) };

    let table_name = str_from_raw(table_name_ptr, table_name_len)
        .map_err(|e| format!("table_name: {}", e))?;
    let sql_query = str_from_raw(sql_query_ptr, sql_query_len)
        .map_err(|e| format!("sql_query: {}", e))?;

    let count = files_count as usize;
    let mut file_paths = Vec::with_capacity(count);
    for i in 0..count {
        let ptr = *file_paths_ptr.add(i);
        let len = *file_paths_lens.add(i);
        file_paths.push(str_from_raw(ptr, len).map_err(|e| format!("file_path[{}]: {}", i, e))?.to_string());
    }
    let file_sizes: Vec<i64> = slice::from_raw_parts(file_sizes_ptr, count).to_vec();

    let runtime = &*(runtime_ptr as *const api::DataFusionRuntime);

    let ipc_bytes = mgr.io_runtime
        .block_on(api::execute_iceberg_query_to_ipc(
            s3_region, s3_bucket, s3_access_key, s3_secret_key,
            s3_session_token, s3_endpoint, file_paths, file_sizes,
            table_name, sql_query, runtime,
            mgr.cpu_executor(), mgr.io_runtime.handle().clone(),
        ))
        .map_err(|e| e.to_string())?;

    Ok(Box::into_raw(Box::new(api::IpcResult { data: ipc_bytes })) as i64)
}

/// Returns a pointer to the IPC data bytes inside an IpcResult.
#[no_mangle]
pub unsafe extern "C" fn df_ipc_result_data_ptr(result_ptr: i64) -> i64 {
    api::ipc_result_data_ptr(result_ptr)
}

/// Returns the length of the IPC data bytes inside an IpcResult.
#[no_mangle]
pub unsafe extern "C" fn df_ipc_result_data_len(result_ptr: i64) -> i64 {
    api::ipc_result_data_len(result_ptr)
}

/// Frees an IpcResult. Safe to call with 0.
#[no_mangle]
pub unsafe extern "C" fn df_free_ipc_result(result_ptr: i64) {
    api::free_ipc_result(result_ptr);
}

/// Executes SQL against Arrow IPC input data using DataFusion StreamingTable.
/// Takes multiple IPC byte buffers (one per worker), runs merge SQL, returns IpcResult.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_execute_from_ipc(
    ipc_ptrs: *const *const u8,
    ipc_lens: *const i64,
    ipc_count: i64,
    sql_ptr: *const u8,
    sql_len: i64,
    runtime_ptr: i64,
) -> i64 {
    let mgr = get_rt_manager()?;

    let sql = str_from_raw(sql_ptr, sql_len)
        .map_err(|e| format!("df_execute_from_ipc: sql: {}", e))?;

    let count = ipc_count as usize;
    let mut ipc_slices: Vec<&[u8]> = Vec::with_capacity(count);
    for i in 0..count {
        let ptr = *ipc_ptrs.add(i);
        let len = *ipc_lens.add(i) as usize;
        ipc_slices.push(slice::from_raw_parts(ptr, len));
    }

    let runtime = &*(runtime_ptr as *const api::DataFusionRuntime);

    let result_bytes = mgr.io_runtime
        .block_on(api::execute_from_ipc(
            ipc_slices, sql, runtime, mgr.cpu_executor(),
        ))
        .map_err(|e| e.to_string())?;

    Ok(Box::into_raw(Box::new(api::IpcResult { data: result_bytes })) as i64)
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_sql_to_substrait(
    shard_view_ptr: i64,
    table_name_ptr: *const u8,
    table_name_len: i64,
    sql_ptr: *const u8,
    sql_len: i64,
    runtime_ptr: i64,
    out_ptr: *mut u8,
    out_cap: i64,
    out_len: *mut i64,
) -> i64 {
    let mgr = get_rt_manager()?;
    let table_name = str_from_raw(table_name_ptr, table_name_len).map_err(|e| format!("df_sql_to_substrait: table_name: {}", e))?;
    let sql = str_from_raw(sql_ptr, sql_len).map_err(|e| format!("df_sql_to_substrait: sql: {}", e))?;
    let bytes = api::sql_to_substrait(shard_view_ptr, table_name, sql, runtime_ptr, &mgr)
        .map_err(|e| e.to_string())?;
    if bytes.len() > out_cap as usize {
        return Err(format!(
            "substrait plan size {} exceeds buffer capacity {}",
            bytes.len(),
            out_cap
        ));
    }
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, bytes.len());
    if !out_len.is_null() {
        *out_len = bytes.len() as i64;
    }
    Ok(0)
}
