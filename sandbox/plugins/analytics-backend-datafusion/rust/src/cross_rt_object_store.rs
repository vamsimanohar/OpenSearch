/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! An `ObjectStore` wrapper that delegates all async I/O to the IO runtime.
//!
//! When DataFusion runs queries on the CPU executor, `ParquetExec` calls
//! `ObjectStore::get_range()` etc. Those calls need the tokio IO reactor
//! (TCP sockets, TLS) which lives on the IO runtime. This wrapper spawns
//! every async operation on the IO runtime via its stored handle,
//! so the CPU executor can `await` the results without deadlocking.

use std::fmt::{self, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use tokio::runtime::Handle;

/// Global counters for S3 I/O tracking (reset per query via reset_s3_counters)
static S3_GET_RANGE_CALLS: AtomicU64 = AtomicU64::new(0);
static S3_GET_RANGE_BYTES: AtomicU64 = AtomicU64::new(0);
static S3_GET_RANGE_MS: AtomicU64 = AtomicU64::new(0);
static S3_GET_RANGES_CALLS: AtomicU64 = AtomicU64::new(0);
static S3_GET_RANGES_BYTES: AtomicU64 = AtomicU64::new(0);
static S3_GET_RANGES_MS: AtomicU64 = AtomicU64::new(0);
static S3_GET_OPTS_CALLS: AtomicU64 = AtomicU64::new(0);
static S3_GET_OPTS_MS: AtomicU64 = AtomicU64::new(0);

pub fn reset_s3_counters() {
    S3_GET_RANGE_CALLS.store(0, Ordering::Relaxed);
    S3_GET_RANGE_BYTES.store(0, Ordering::Relaxed);
    S3_GET_RANGE_MS.store(0, Ordering::Relaxed);
    S3_GET_RANGES_CALLS.store(0, Ordering::Relaxed);
    S3_GET_RANGES_BYTES.store(0, Ordering::Relaxed);
    S3_GET_RANGES_MS.store(0, Ordering::Relaxed);
    S3_GET_OPTS_CALLS.store(0, Ordering::Relaxed);
    S3_GET_OPTS_MS.store(0, Ordering::Relaxed);
}

pub fn dump_s3_counters() {
    let range_calls = S3_GET_RANGE_CALLS.load(Ordering::Relaxed);
    let range_bytes = S3_GET_RANGE_BYTES.load(Ordering::Relaxed);
    let range_ms = S3_GET_RANGE_MS.load(Ordering::Relaxed);
    let ranges_calls = S3_GET_RANGES_CALLS.load(Ordering::Relaxed);
    let ranges_bytes = S3_GET_RANGES_BYTES.load(Ordering::Relaxed);
    let ranges_ms = S3_GET_RANGES_MS.load(Ordering::Relaxed);
    let opts_calls = S3_GET_OPTS_CALLS.load(Ordering::Relaxed);
    let opts_ms = S3_GET_OPTS_MS.load(Ordering::Relaxed);
    eprintln!(
        "[PERF] S3 I/O summary: get_range={{calls={}, bytes={:.1}MB, time={}ms}} get_ranges={{calls={}, bytes={:.1}MB, time={}ms}} get_opts={{calls={}, time={}ms}}",
        range_calls, range_bytes as f64 / 1_048_576.0, range_ms,
        ranges_calls, ranges_bytes as f64 / 1_048_576.0, ranges_ms,
        opts_calls, opts_ms,
    );
}

/// Wraps an `ObjectStore` so that every async call is dispatched to the IO runtime.
/// Stores the IO runtime handle directly rather than relying on thread-locals,
/// so it works from both IO runtime threads and CPU executor threads.
#[derive(Debug)]
pub struct CrossRuntimeObjectStore {
    inner: Arc<dyn ObjectStore>,
    io_handle: Handle,
}

impl CrossRuntimeObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, io_handle: Handle) -> Self {
        Self { inner, io_handle }
    }

    /// Spawn a future on the IO runtime and await the result.
    async fn spawn_on_io<F, T>(&self, f: F) -> T
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let join_handle = self.io_handle.spawn(f);
        join_handle.await.expect("IO runtime task panicked")
    }
}

impl Display for CrossRuntimeObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "CrossRuntime({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CrossRuntimeObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let store = Arc::clone(&self.inner);
        let location = location.clone();
        self.spawn_on_io(async move { store.put_opts(&location, payload, opts).await }).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let store = Arc::clone(&self.inner);
        let location = location.clone();
        self.spawn_on_io(async move { store.put_multipart_opts(&location, opts).await }).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let store = Arc::clone(&self.inner);
        let location = location.clone();
        let t = Instant::now();
        let result = self.spawn_on_io(async move { store.get_opts(&location, options).await }).await;
        let elapsed = t.elapsed().as_millis() as u64;
        S3_GET_OPTS_CALLS.fetch_add(1, Ordering::Relaxed);
        S3_GET_OPTS_MS.fetch_add(elapsed, Ordering::Relaxed);
        result
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        let store = Arc::clone(&self.inner);
        let loc = location.clone();
        let range_size = range.end - range.start;
        let t = Instant::now();
        let result = self.spawn_on_io(async move { store.get_range(&loc, range).await }).await;
        let elapsed = t.elapsed().as_millis() as u64;
        S3_GET_RANGE_CALLS.fetch_add(1, Ordering::Relaxed);
        S3_GET_RANGE_BYTES.fetch_add(range_size, Ordering::Relaxed);
        S3_GET_RANGE_MS.fetch_add(elapsed, Ordering::Relaxed);
        if elapsed > 500 {
            eprintln!(
                "[PERF] S3 get_range: {}ms, {:.1}MB, file={}",
                elapsed, range_size as f64 / 1_048_576.0, location
            );
        }
        result
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let store = Arc::clone(&self.inner);
        let loc = location.clone();
        let total_bytes: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        let num_ranges = ranges.len();
        let ranges = ranges.to_vec();
        let t = Instant::now();
        let result = self.spawn_on_io(async move { store.get_ranges(&loc, &ranges).await }).await;
        let elapsed = t.elapsed().as_millis() as u64;
        S3_GET_RANGES_CALLS.fetch_add(1, Ordering::Relaxed);
        S3_GET_RANGES_BYTES.fetch_add(total_bytes, Ordering::Relaxed);
        S3_GET_RANGES_MS.fetch_add(elapsed, Ordering::Relaxed);
        if elapsed > 500 {
            eprintln!(
                "[PERF] S3 get_ranges: {}ms, {} ranges, {:.1}MB, file={}",
                elapsed, num_ranges, total_bytes as f64 / 1_048_576.0, location
            );
        }
        result
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let store = Arc::clone(&self.inner);
        let location = location.clone();
        self.spawn_on_io(async move { store.head(&location).await }).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let store = Arc::clone(&self.inner);
        let location = location.clone();
        self.spawn_on_io(async move { store.delete(&location).await }).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let store = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        self.spawn_on_io(async move { store.list_with_delimiter(prefix.as_ref()).await }).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let store = Arc::clone(&self.inner);
        let from = from.clone();
        let to = to.clone();
        self.spawn_on_io(async move { store.copy(&from, &to).await }).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let store = Arc::clone(&self.inner);
        let from = from.clone();
        let to = to.clone();
        self.spawn_on_io(async move { store.copy_if_not_exists(&from, &to).await }).await
    }
}
