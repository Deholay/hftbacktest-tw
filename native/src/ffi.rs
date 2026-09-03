use std::slice;

use crate::engine::SlimEngine;
use crate::types::{AssetConfig, BboRow, BboView, OrderView};

#[unsafe(no_mangle)]
pub extern "C" fn hbt_slim_version() -> u32 {
    1
}

#[unsafe(no_mangle)]
/// Creates an engine from two borrowed BBO arrays.
///
/// # Safety
/// Each non-null row pointer must reference `len` initialized, readable `BboRow` values for the
/// duration of this call.
pub unsafe extern "C" fn hbt_slim_create(
    rows0: *const BboRow,
    len0: usize,
    adjustment0: i64,
    feed0: i64,
    entry0: i64,
    response0: i64,
    tick0: f64,
    rows1: *const BboRow,
    len1: usize,
    adjustment1: i64,
    feed1: i64,
    entry1: i64,
    response1: i64,
    tick1: f64,
) -> *mut SlimEngine {
    if (rows0.is_null() && len0 != 0) || (rows1.is_null() && len1 != 0) {
        return std::ptr::null_mut();
    }
    let left = if len0 == 0 {
        Vec::new()
    } else {
        unsafe { slice::from_raw_parts(rows0, len0) }.to_vec()
    };
    let right = if len1 == 0 {
        Vec::new()
    } else {
        unsafe { slice::from_raw_parts(rows1, len1) }.to_vec()
    };
    let engine = SlimEngine::new(
        [left, right],
        [
            AssetConfig {
                local_adjustment_ns: adjustment0,
                feed_offset_ns: feed0,
                entry_latency_ns: entry0,
                response_latency_ns: response0,
                tick_size: tick0,
            },
            AssetConfig {
                local_adjustment_ns: adjustment1,
                feed_offset_ns: feed1,
                entry_latency_ns: entry1,
                response_latency_ns: response1,
                tick_size: tick1,
            },
        ],
    );
    Box::into_raw(Box::new(engine))
}

#[unsafe(no_mangle)]
/// Releases an engine returned by [`hbt_slim_create`].
///
/// # Safety
/// `engine` must be null or a live pointer returned by [`hbt_slim_create`] that has not been freed.
pub unsafe extern "C" fn hbt_slim_free(engine: *mut SlimEngine) {
    if !engine.is_null() {
        drop(unsafe { Box::from_raw(engine) });
    }
}

#[unsafe(no_mangle)]
/// Returns the engine clock.
///
/// # Safety
/// `engine` must be null or point to a live [`SlimEngine`].
pub unsafe extern "C" fn hbt_slim_current_timestamp(engine: *const SlimEngine) -> i64 {
    unsafe { engine.as_ref() }.map_or(0, SlimEngine::current_timestamp)
}

#[unsafe(no_mangle)]
/// Advances the engine clock by `duration_ns`.
///
/// # Safety
/// `engine` must point to a live, exclusively borrowed [`SlimEngine`].
pub unsafe extern "C" fn hbt_slim_elapse(engine: *mut SlimEngine, duration_ns: i64) -> i32 {
    let Some(engine) = (unsafe { engine.as_mut() }) else {
        return -1;
    };
    engine.elapse(duration_ns)
}

#[unsafe(no_mangle)]
/// Copies the selected asset's local BBO view into `output`.
///
/// # Safety
/// `engine` must point to a live engine and `output` must be null or writable for one `BboView`.
pub unsafe extern "C" fn hbt_slim_depth(
    engine: *const SlimEngine,
    asset_no: usize,
    output: *mut BboView,
) -> i32 {
    let (Some(engine), Some(output)) = (unsafe { engine.as_ref() }, unsafe { output.as_mut() })
    else {
        return -1;
    };
    let Some(view) = engine.depth(asset_no) else {
        return -2;
    };
    *output = view;
    0
}

#[unsafe(no_mangle)]
/// Copies the latest feed-latency sample into the provided output pointers.
///
/// # Safety
/// `engine` must point to a live engine; non-null output pointers must be writable `i64` values.
pub unsafe extern "C" fn hbt_slim_feed_latency(
    engine: *const SlimEngine,
    asset_no: usize,
    exch: *mut i64,
    local: *mut i64,
) -> i32 {
    let Some(engine) = (unsafe { engine.as_ref() }) else {
        return -1;
    };
    let Some(value) = engine.feed_latency(asset_no) else {
        return 1;
    };
    if let Some(out) = unsafe { exch.as_mut() } {
        *out = value.0;
    }
    if let Some(out) = unsafe { local.as_mut() } {
        *out = value.1;
    }
    0
}

#[unsafe(no_mangle)]
/// Copies the latest order-latency sample into the provided output pointers.
///
/// # Safety
/// `engine` must point to a live engine; non-null output pointers must be writable `i64` values.
pub unsafe extern "C" fn hbt_slim_order_latency(
    engine: *const SlimEngine,
    asset_no: usize,
    req: *mut i64,
    exch: *mut i64,
    resp: *mut i64,
) -> i32 {
    let Some(engine) = (unsafe { engine.as_ref() }) else {
        return -1;
    };
    let Some(value) = engine.order_latency(asset_no) else {
        return 1;
    };
    if let Some(out) = unsafe { req.as_mut() } {
        *out = value.0;
    }
    if let Some(out) = unsafe { exch.as_mut() } {
        *out = value.1;
    }
    if let Some(out) = unsafe { resp.as_mut() } {
        *out = value.2;
    }
    0
}

#[unsafe(no_mangle)]
/// Submits an immediate order.
///
/// # Safety
/// `engine` must point to a live, exclusively borrowed [`SlimEngine`].
pub unsafe extern "C" fn hbt_slim_submit(
    engine: *mut SlimEngine,
    asset_no: usize,
    order_id: u64,
    side: i32,
    price: f64,
    qty: f64,
    tif: i32,
) -> i32 {
    let Some(engine) = (unsafe { engine.as_mut() }) else {
        return -1;
    };
    engine.submit(asset_no, order_id, side, price, qty, tif)
}

#[unsafe(no_mangle)]
/// Processes events until an order response or timeout.
///
/// # Safety
/// `engine` must point to a live, exclusively borrowed [`SlimEngine`].
pub unsafe extern "C" fn hbt_slim_wait_order_response(
    engine: *mut SlimEngine,
    asset_no: usize,
    order_id: u64,
    timeout_ns: i64,
) -> i32 {
    let Some(engine) = (unsafe { engine.as_mut() }) else {
        return -1;
    };
    engine.wait_response(asset_no, order_id, timeout_ns)
}

#[unsafe(no_mangle)]
/// Copies a visible order response into `output`.
///
/// # Safety
/// `engine` must point to a live engine and `output` must be null or writable for one `OrderView`.
pub unsafe extern "C" fn hbt_slim_order(
    engine: *const SlimEngine,
    asset_no: usize,
    order_id: u64,
    output: *mut OrderView,
) -> i32 {
    let (Some(engine), Some(output)) = (unsafe { engine.as_ref() }, unsafe { output.as_mut() })
    else {
        return -1;
    };
    let Some(order) = engine.order(asset_no, order_id) else {
        return 1;
    };
    if order.response_visible == 0 {
        return 1;
    }
    *output = order;
    0
}
