#[cfg(not(target_arch = "wasm32"))]
use std::sync::OnceLock;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;
#[cfg(target_arch = "wasm32")]
use web_sys::js_sys::Date;

/// Represents a monotonic timestamp in seconds that works across native and WASM.
pub type TimePoint = f64;

#[cfg(not(target_arch = "wasm32"))]
fn monotonic_anchor() -> &'static Instant {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now)
}

/// Returns a monotonically increasing timestamp (seconds) suitable for measuring durations.
pub fn now_seconds() -> TimePoint {
    #[cfg(target_arch = "wasm32")]
    {
        Date::now() / 1000.0
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        monotonic_anchor().elapsed().as_secs_f64()
    }
}

/// Returns the elapsed seconds since `start`.
pub fn elapsed_since(start: TimePoint) -> f64 {
    (now_seconds() - start).max(0.0)
}
