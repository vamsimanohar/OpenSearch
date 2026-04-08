/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;

use datafusion::{
    common::DataFusionError,
    datasource::file_format::parquet::ParquetFormat,
    datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    execution::context::SessionContext,
    execution::runtime_env::RuntimeEnvBuilder,
    execution::SessionStateBuilder,
    physical_plan::execute_stream,
    prelude::*,
};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use jni::sys::jlong;
use log::{debug, error, info};
use prost::Message;
use substrait::proto::Plan;

use crate::api::DataFusionRuntime;
use crate::cross_rt_stream::CrossRtStream;
use crate::executor::DedicatedExecutor;
use crate::s3_store::{register_s3_store, S3Config};

/// Execute a Substrait plan against S3-backed Parquet files via DataFusion.
///
/// This is the Iceberg variant of `query_executor::execute_query`. Instead of
/// receiving a pre-built `ShardView` with cached object metas, it takes an
/// `S3Config` plus a list of S3 file paths and registers them as a
/// `ListingTable` after wiring the S3 object store into the session.
///
/// When `global_runtime` is provided, the session shares the global memory pool
/// and disk manager (enabling spill-to-disk for large aggregations).
pub async fn execute_iceberg_query(
    s3_config: S3Config,
    file_paths: Vec<String>,
    table_name: String,
    plan_bytes: Vec<u8>,
    cpu_executor: DedicatedExecutor,
    global_runtime: Option<&DataFusionRuntime>,
) -> Result<jlong, DataFusionError> {
    info!("[DataFusion-Rust] execute_iceberg_query: table={}, files={}, plan_bytes={}, bucket={}, has_global_runtime={}",
        table_name, file_paths.len(), plan_bytes.len(), s3_config.bucket, global_runtime.is_some());

    if file_paths.is_empty() {
        return Err(DataFusionError::Plan("No file paths provided for Iceberg query".to_string()));
    }

    // Build a RuntimeEnv — share the global memory pool + disk manager if available,
    // otherwise create a standalone one (for testing / fallback).
    let runtime_env = match global_runtime {
        Some(rt) => {
            let builder = RuntimeEnvBuilder::from_runtime_env(&rt.runtime_env);
            builder.build().map_err(|e| {
                error!("Failed to build runtime env from global runtime: {}", e);
                e
            })?
        }
        None => {
            RuntimeEnvBuilder::new().build().map_err(|e| {
                error!("Failed to build runtime env: {}", e);
                e
            })?
        }
    };

    // Only register S3 store when we actually have an S3 bucket.
    // For local file:// paths (e.g., Hadoop catalog testing), DataFusion
    // handles them natively via LocalFileSystem — no registration needed.
    if !s3_config.bucket.is_empty() {
        register_s3_store(&runtime_env, &s3_config)?;
    }

    // Build a fresh session state per query.
    // Use available CPU cores for target_partitions for better parallelism.
    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = false;
    config.options_mut().execution.target_partitions = num_cpus;
    config.options_mut().execution.batch_size = 8192;

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        .build();

    let ctx = SessionContext::new_with_state(state);

    // Parse file paths into ListingTableUrls
    let table_paths: Vec<ListingTableUrl> = file_paths
        .iter()
        .map(|p| ListingTableUrl::parse(p))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            error!("Failed to parse file paths: {}", e);
            e
        })?;

    // Register Parquet files as a table via ListingTable
    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet")
        .with_collect_stat(true);

    // Use the first path for schema inference
    let resolved_schema = listing_options
        .infer_schema(&ctx.state(), &table_paths[0])
        .await
        .map_err(|e| {
            error!("Failed to infer schema: {}", e);
            e
        })?;

    let table_config = ListingTableConfig::new_with_multi_paths(table_paths)
        .with_listing_options(listing_options)
        .with_schema(resolved_schema);

    let provider = Arc::new(ListingTable::try_new(table_config).map_err(|e| {
        error!("Failed to create listing table: {}", e);
        e
    })?);

    ctx.register_table(&table_name, provider).map_err(|e| {
        error!("Failed to register table: {}", e);
        e
    })?;

    // Decode substrait -> logical plan -> physical plan -> stream
    let substrait_plan = Plan::decode(plan_bytes.as_slice()).map_err(|e| {
        DataFusionError::Execution(format!("Failed to decode Substrait: {}", e))
    })?;
    debug!("[DataFusion-Rust] Substrait plan decoded: {} relations",
        substrait_plan.relations.len());

    let logical_plan = from_substrait_plan(&ctx.state(), &substrait_plan).await?;
    info!("[DataFusion-Rust] Logical plan:");
    for line in format!("{}", logical_plan.display_indent()).lines() {
        info!("[DataFusion-Rust]   {}", line);
    }
    let dataframe = ctx.execute_logical_plan(logical_plan).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    info!("[DataFusion-Rust] Physical plan:");
    for line in format!("{}", datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)).lines() {
        info!("[DataFusion-Rust]   {}", line);
    }

    let df_stream = execute_stream(physical_plan, ctx.task_ctx()).map_err(|e| {
        error!("Failed to create execution stream: {}", e);
        e
    })?;

    // Wrap in CrossRtStream — CPU work runs on DedicatedExecutor
    let cross_rt_stream = CrossRtStream::new_with_df_error_stream(df_stream, cpu_executor);
    let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        cross_rt_stream.schema(),
        cross_rt_stream,
    );

    Ok(Box::into_raw(Box::new(wrapped)) as jlong)
}
