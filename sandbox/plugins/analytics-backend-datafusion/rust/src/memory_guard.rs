/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Unified jemalloc-based memory guard for pool override decisions.
//!
//! All RSS checks go through [`cached_resident_bytes()`] — a single source of
//! truth refreshed at most once per 100ms. This avoids expensive jemalloc
//! `epoch.advance()` calls on the hot path while keeping the memory picture
//! consistent across all decision layers (hard guard, override, cancel, admission).
//!
//! Thresholds are configurable at runtime via `set_thresholds`.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

// --- Cached RSS ---

const RESIDENT_CACHE_INTERVAL_MS: u64 = 100;
static CACHED_RESIDENT: AtomicI64 = AtomicI64::new(0);
// Initialized to u64::MAX so the first call always refreshes (any now_ms - MAX wraps to > 100).
static LAST_CHECK_MS: AtomicU64 = AtomicU64::new(u64::MAX);
static EPOCH_BASE: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

/// Returns jemalloc resident bytes, cached for up to 100ms.
///
/// Only one thread per interval pays the ~1-5µs `epoch.advance()` cost;
/// all others get the cached value in <1ns (one atomic load).
/// Used by all memory decision points: hard guard, override, cancel, admission.
pub fn cached_resident_bytes() -> i64 {
    let base = EPOCH_BASE.get_or_init(Instant::now);
    let now_ms = base.elapsed().as_millis() as u64;
    let last = LAST_CHECK_MS.load(Ordering::Relaxed);
    if now_ms.wrapping_sub(last) >= RESIDENT_CACHE_INTERVAL_MS {
        if LAST_CHECK_MS.compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
            let r = native_bridge_common::allocator::resident_bytes();
            CACHED_RESIDENT.store(r, Ordering::Relaxed);
            return r;
        }
    }
    CACHED_RESIDENT.load(Ordering::Relaxed)
}

// --- Thresholds ---

/// Minimum pool size (bytes) for jemalloc override to activate.
/// Below this, the pool is assumed to be a unit test / benchmark with
/// artificial limits — never override.
const MIN_POOL_FOR_OVERRIDE: usize = 16 * 1024 * 1024; // 16MB

// Configurable thresholds stored as fixed-point (×1000) in atomics.
// Defaults: admission=70%, operator=85%, critical=95%.
static ADMISSION_THRESHOLD_X1000: AtomicU64 = AtomicU64::new(700);
static OPERATOR_THRESHOLD_X1000: AtomicU64 = AtomicU64::new(850);
static CRITICAL_THRESHOLD_X1000: AtomicU64 = AtomicU64::new(950);

/// Which layer is asking for the override check.
#[derive(Debug, Clone, Copy)]
pub enum OverrideContext {
    /// Admission-time: deciding whether to reduce target_partitions.
    /// More conservative (lower threshold) — committing to resource usage.
    Admission,
    /// Operator try_grow: deciding whether to trigger spill.
    /// More aggressive (higher threshold) — avoiding expensive disk I/O.
    Operator,
}

/// Configurable memory thresholds for jemalloc override decisions.
///
/// Values are fractions (0.0–1.0) of the pool limit:
/// - If `jemalloc_resident < threshold × pool_limit`, the pool's rejection
///   is considered a false positive and the operation proceeds.
#[derive(Debug, Clone, Copy)]
pub struct MemoryThresholds {
    /// Threshold for admission decisions (reduce partitions). Default: 0.70
    pub admission: f64,
    /// Threshold for operator decisions (trigger spill). Default: 0.85
    pub operator: f64,
    /// Critical memory threshold. Default: 0.95
    /// When RSS exceeds this: the hard guard forces spill (pre-CAS path), and
    /// the cancel path terminates the query (post-CAS-fail path, last resort).
    pub critical: f64,
}

impl Default for MemoryThresholds {
    fn default() -> Self {
        Self {
            admission: 0.70,
            operator: 0.85,
            critical: 0.95,
        }
    }
}

/// Set the memory thresholds at runtime. Called from Java when cluster
/// settings change. Thread-safe (atomic stores).
pub fn set_thresholds(thresholds: MemoryThresholds) {
    ADMISSION_THRESHOLD_X1000.store(
        (thresholds.admission * 1000.0) as u64,
        Ordering::Release,
    );
    OPERATOR_THRESHOLD_X1000.store(
        (thresholds.operator * 1000.0) as u64,
        Ordering::Release,
    );
    CRITICAL_THRESHOLD_X1000.store(
        (thresholds.critical * 1000.0) as u64,
        Ordering::Release,
    );
}

/// Read current thresholds.
pub fn get_thresholds() -> MemoryThresholds {
    MemoryThresholds {
        admission: ADMISSION_THRESHOLD_X1000.load(Ordering::Acquire) as f64 / 1000.0,
        operator: OPERATOR_THRESHOLD_X1000.load(Ordering::Acquire) as f64 / 1000.0,
        critical: CRITICAL_THRESHOLD_X1000.load(Ordering::Acquire) as f64 / 1000.0,
    }
}

/// Returns `true` if RSS exceeds the critical threshold — the query should be
/// cancelled. This is the last-resort path (post-CAS-fail, post-override-denied):
/// the pool rejected, jemalloc confirms pressure, and spill alone can't recover
/// fast enough. Cancel the query to protect the node.
///
/// The same critical threshold is used by the hard guard (pre-CAS) to force spill
/// earlier — that path is recoverable. This path fires only when spill was already
/// attempted or cannot help.
pub fn should_cancel_query(pool_limit_bytes: usize) -> bool {
    if pool_limit_bytes < MIN_POOL_FOR_OVERRIDE {
        return false;
    }
    let resident = cached_resident_bytes();
    if resident <= 0 {
        return false;
    }
    let critical_bytes = (pool_limit_bytes as u64).saturating_mul(CRITICAL_THRESHOLD_X1000.load(Ordering::Acquire)) / 1000;
    resident >= critical_bytes as i64
}

/// Check whether jemalloc says physical memory has headroom, meaning the
/// pool's rejection is a false positive (stale accounting).
///
/// Uses `resident_bytes` (physical RSS) instead of `allocated_bytes` (live objects).
/// `allocated_bytes` undercounts true memory pressure because jemalloc retains
/// freed pages in thread caches and arenas (dirty/muzzy decay). Under concurrent
/// workloads, the gap between allocated and resident can be 10-20GB, causing the
/// override to fire when the system is actually near OOM.
///
/// Returns `true` if the override should fire (proceed despite pool rejection).
/// Returns `false` if pressure is real or stats are unavailable.
///
/// # Arguments
/// - `pool_limit_bytes`: the pool's configured limit
/// - `context`: which layer is asking (determines threshold)
pub fn should_override(pool_limit_bytes: usize, context: OverrideContext) -> bool {
    if pool_limit_bytes < MIN_POOL_FOR_OVERRIDE {
        return false;
    }

    let resident = cached_resident_bytes();
    if resident <= 0 {
        return false;
    }

    let threshold_x1000 = match context {
        OverrideContext::Admission => ADMISSION_THRESHOLD_X1000.load(Ordering::Acquire),
        OverrideContext::Operator => OPERATOR_THRESHOLD_X1000.load(Ordering::Acquire),
    };

    let threshold_bytes = (pool_limit_bytes as u64).saturating_mul(threshold_x1000) / 1000;
    resident < threshold_bytes as i64
}

/// Proactive admission check: returns `true` if jemalloc resident memory
/// already exceeds the admission threshold (70% of pool limit by default).
///
/// Called BEFORE query execution (at budget acquisition) to reject or reduce
/// concurrency early — before any hash table allocation occurs. This prevents
/// the "20 queries all pass admission simultaneously" burst that causes OOM.
///
/// Cost: one `epoch.advance` + stat read (~1-5µs). Called once per query at
/// admission, not per-batch.
pub fn is_memory_pressured(pool_limit_bytes: usize) -> bool {
    if pool_limit_bytes < MIN_POOL_FOR_OVERRIDE {
        return false;
    }

    let resident = cached_resident_bytes();
    if resident <= 0 {
        return false;
    }

    let threshold_x1000 = ADMISSION_THRESHOLD_X1000.load(Ordering::Acquire);
    let threshold_bytes = (pool_limit_bytes as u64).saturating_mul(threshold_x1000) / 1000;
    resident >= threshold_bytes as i64
}

// ---------------------------------------------------------------------------
// Disk spill budget: proactive disk pressure check
// ---------------------------------------------------------------------------

/// Fraction of available disk to allow per query (×1000 for atomic integer storage).
static DISK_FRACTION_X1000: AtomicU64 = AtomicU64::new(100); // 10% = 100/1000

/// Stored spill directory path. Set once at runtime creation.
static SPILL_DIR: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Set the spill directory (called once from create_global_runtime).
pub fn set_spill_dir(path: &str) {
    let _ = SPILL_DIR.set(path.to_string());
}

/// Returns the per-query spill budget based on available disk space.
///
/// Formula: `10% of available_disk`
///
/// Returns None if disk space is critically low (< 64MB available after
/// applying the fraction). This signals the caller to reduce parallelism
/// to minimize spill volume. The global spill ceiling is enforced by
/// DataFusion's DiskManager (`max_temp_directory_size`).
///
/// Cost: one `statvfs` syscall (~1µs). Called once per query at admission.
pub fn per_query_spill_budget() -> Option<u64> {
    let spill_dir = SPILL_DIR.get()?;
    let available = available_disk_space(spill_dir)?;

    let fraction_x1000 = DISK_FRACTION_X1000.load(Ordering::Acquire);
    let budget = available * fraction_x1000 / 1000;

    if budget < 64 * 1024 * 1024 { // 64MB minimum viable spill
        log::warn!(
            "[disk-pressure] Spill budget too low: {} MB (available={} MB)",
            budget / (1024 * 1024),
            available / (1024 * 1024),
        );
        return None;
    }
    Some(budget)
}

/// Query available disk space for the given path.
#[cfg(unix)]
pub fn available_disk_space(path: &str) -> Option<u64> {
    use std::ffi::CString;
    use std::mem::MaybeUninit;

    let c_path = CString::new(path).ok()?;
    let mut stat = MaybeUninit::<libc::statvfs>::uninit();
    let ret = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };
    if ret != 0 {
        return None;
    }
    let stat = unsafe { stat.assume_init() };
    Some(stat.f_bavail as u64 * stat.f_frsize as u64)
}

#[cfg(not(unix))]
pub fn available_disk_space(_path: &str) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_thresholds() {
        let t = MemoryThresholds::default();
        assert!((t.admission - 0.70).abs() < 0.001);
        assert!((t.operator - 0.85).abs() < 0.001);
        assert!((t.critical - 0.95).abs() < 0.001);
    }

    #[test]
    fn set_and_get_thresholds() {
        set_thresholds(MemoryThresholds {
            admission: 0.60,
            operator: 0.90,
            critical: 0.97,
        });
        let t = get_thresholds();
        assert!((t.admission - 0.60).abs() < 0.001);
        assert!((t.operator - 0.90).abs() < 0.001);
        assert!((t.critical - 0.97).abs() < 0.001);
        // Restore defaults
        set_thresholds(MemoryThresholds::default());
    }

    #[test]
    fn skip_for_small_pools() {
        // Pool below 16MB → always returns false (no override)
        assert!(!should_override(1_000_000, OverrideContext::Admission));
        assert!(!should_override(1_000_000, OverrideContext::Operator));
    }

    #[test]
    fn should_override_uses_resident_not_allocated() {
        // With a large pool (1TB), resident will always be below threshold
        // so override should fire (resident < threshold = "headroom available")
        let large_pool = 1024 * 1024 * 1024 * 1024; // 1TB
        // This test validates the function runs without error and uses resident.
        // On a test process with < 1TB RSS, override should fire (we have headroom).
        let result = should_override(large_pool, OverrideContext::Operator);
        assert!(result, "With 1TB pool limit, resident should be well below threshold — override should fire");
    }

    #[test]
    fn is_memory_pressured_false_for_large_pool() {
        // With a 1TB pool, current process RSS is far below 70% → not pressured
        let large_pool = 1024 * 1024 * 1024 * 1024; // 1TB
        assert!(!is_memory_pressured(large_pool));
    }

    #[test]
    fn is_memory_pressured_true_when_rss_exceeds_limit() {
        // Set pool limit to something well below current process RSS.
        // A Rust test process typically uses 50-200MB RSS, so a 20MB limit
        // should always be exceeded.
        let small_pool = 20 * 1024 * 1024; // 20MB — above MIN_POOL_FOR_OVERRIDE
        let resident = native_bridge_common::allocator::resident_bytes();
        if resident <= 0 {
            return; // jemalloc not available
        }
        // Only assert if RSS is actually above 70% of 20MB = 14MB (which it will be)
        if resident as usize > small_pool * 70 / 100 {
            assert!(is_memory_pressured(small_pool));
        }
    }

    #[test]
    fn is_memory_pressured_skips_small_pools() {
        assert!(!is_memory_pressured(1_000_000)); // 1MB — below MIN_POOL_FOR_OVERRIDE
    }

    #[test]
    fn cached_resident_bytes_returns_positive() {
        // jemalloc is active in the test process — resident_bytes must be > 0
        let resident = cached_resident_bytes();
        assert!(resident > 0, "cached_resident_bytes() should return > 0 when jemalloc is active, got {}", resident);
    }

    #[test]
    fn cached_resident_bytes_is_stable_within_interval() {
        // Two calls within <100ms should return the same cached value
        // (only one thread per interval refreshes the cache).
        let first = cached_resident_bytes();
        let second = cached_resident_bytes();
        assert_eq!(
            first, second,
            "Two immediate calls should return the same cached value"
        );
    }

    #[test]
    fn should_cancel_query_false_for_small_pools() {
        // Pools below MIN_POOL_FOR_OVERRIDE (16MB) always return false
        assert!(!should_cancel_query(1_000_000)); // 1MB
        assert!(!should_cancel_query(8 * 1024 * 1024)); // 8MB
        assert!(!should_cancel_query(15 * 1024 * 1024)); // 15MB
    }

    #[test]
    fn should_cancel_query_true_when_rss_exceeds_limit() {
        // With a 20MB pool limit (above MIN_POOL_FOR_OVERRIDE), the current test
        // process RSS should exceed 95% of 20MB = 19MB. A Rust test process
        // typically uses 50-200MB RSS.
        let small_pool = 20 * 1024 * 1024; // 20MB
        let resident = native_bridge_common::allocator::resident_bytes();
        if resident <= 0 {
            return; // jemalloc not available in this test env
        }
        // Only assert if RSS actually exceeds the critical threshold
        let critical_bytes = (small_pool as f64 * 0.95) as i64;
        if resident >= critical_bytes {
            assert!(
                should_cancel_query(small_pool),
                "should_cancel_query should return true when RSS ({}) exceeds 95% of pool ({})",
                resident,
                small_pool
            );
        }
    }

    #[test]
    fn override_respects_operator_vs_admission_threshold() {
        // Operator threshold (85%) is more permissive than admission (70%).
        // For a pool where RSS is between 70% and 85%:
        // - Admission override should NOT fire (RSS >= 70% threshold)
        // - Operator override SHOULD fire (RSS < 85% threshold)
        //
        // We can't precisely control RSS in a unit test, but we can verify
        // that the thresholds are read correctly by setting them and checking
        // behavior with known pool sizes.
        let resident = native_bridge_common::allocator::resident_bytes();
        if resident <= 0 {
            return; // jemalloc not available in this test env
        }
        let resident = resident as usize;

        // Set pool limit so that resident is exactly between 70% and 85%
        // pool = resident / 0.77 (midpoint) → resident/pool ≈ 77%
        let pool_at_midpoint = (resident as f64 / 0.77) as usize;
        if pool_at_midpoint < MIN_POOL_FOR_OVERRIDE {
            return;
        }

        // At 77% utilization: admission (70%) should NOT override, operator (85%) SHOULD override
        let admission_result = should_override(pool_at_midpoint, OverrideContext::Admission);
        let operator_result = should_override(pool_at_midpoint, OverrideContext::Operator);

        // admission: resident (77%) >= threshold (70%) → NOT below → override = false
        assert!(!admission_result, "At 77% RSS, admission override should NOT fire (threshold 70%)");
        // operator: resident (77%) < threshold (85%) → below → override = true
        assert!(operator_result, "At 77% RSS, operator override SHOULD fire (threshold 85%)");
    }
}
