/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Rust→Java logging via FFM callback.
//!
//! Java registers a function pointer at startup via `native_logger_init`.
//! Rust calls that pointer to log. No JNI.
//!
//! Also implements the `log` crate's `Log` trait so that libraries like
//! DataFusion that use `log::info!`/`log::debug!` automatically route
//! through the same FFM bridge to Java Log4j.

use std::sync::atomic::{AtomicPtr, Ordering};

/// Callback signature: `void log(int level, const char* msg, long msg_len)`
type LogCallback = unsafe extern "C" fn(i32, *const u8, i64);

static LOG_CALLBACK: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum LogLevel {
    Debug = 0,
    Info = 1,
    Error = 2,
}

/// Called by Java at startup to register the log callback.
#[no_mangle]
pub unsafe extern "C" fn native_logger_init(callback: LogCallback) {
    LOG_CALLBACK.store(callback as *mut (), Ordering::Release);

    // Register the log crate adapter so log::info!/log::debug! from
    // DataFusion and other libraries route through this bridge.
    let _ = log::set_logger(&FFM_LOGGER);
    log::set_max_level(log::LevelFilter::Debug);

    log(LogLevel::Info, "Native logger initialized successfully");
}

pub fn log(level: LogLevel, message: &str) {
    let ptr = LOG_CALLBACK.load(Ordering::Acquire);
    if ptr.is_null() {
        eprintln!("[RUST_LOG_FALLBACK] {:?}: {}", level, message);
        return;
    }
    let callback: LogCallback = unsafe { std::mem::transmute(ptr) };
    unsafe { callback(level as i32, message.as_ptr(), message.len() as i64) };
}

// ── log crate adapter ───────────────────────────────────────────────────────

/// Bridges the standard `log` crate to our FFM callback.
struct FfmLogger;

static FFM_LOGGER: FfmLogger = FfmLogger;

impl log::Log for FfmLogger {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        !LOG_CALLBACK.load(Ordering::Acquire).is_null()
    }

    fn log(&self, record: &log::Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let level = match record.level() {
            log::Level::Error | log::Level::Warn => LogLevel::Error,
            log::Level::Info => LogLevel::Info,
            log::Level::Debug | log::Level::Trace => LogLevel::Debug,
        };
        let message = format!("[{}] {}", record.target(), record.args());
        self::log(level, &message);
    }

    fn flush(&self) {}
}

// ── macros ──────────────────────────────────────────────────────────────────

#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        $crate::logger::log($crate::logger::LogLevel::Debug, &format!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => {
        $crate::logger::log($crate::logger::LogLevel::Info, &format!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => {
        $crate::logger::log($crate::logger::LogLevel::Error, &format!($($arg)*))
    };
}
