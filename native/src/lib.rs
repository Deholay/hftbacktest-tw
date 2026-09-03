//! Domain-neutral deterministic BBO scheduler and immediate-order matcher.

mod book;
mod engine;
mod ffi;
mod matcher;
mod scheduler;
mod types;

pub use engine::SlimEngine;
pub use ffi::{
    hbt_slim_create, hbt_slim_current_timestamp, hbt_slim_depth, hbt_slim_elapse,
    hbt_slim_feed_latency, hbt_slim_free, hbt_slim_order, hbt_slim_order_latency, hbt_slim_submit,
    hbt_slim_version, hbt_slim_wait_order_response,
};
pub use types::{
    BboRow, BboView, OrderView, STATUS_EXPIRED, STATUS_FILLED, STATUS_NEW, TIF_FOK, TIF_IOC,
};
