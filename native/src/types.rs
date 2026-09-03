pub const STATUS_NEW: i32 = 1;
pub const STATUS_EXPIRED: i32 = 2;
pub const STATUS_FILLED: i32 = 3;
pub const TIF_FOK: i32 = 2;
pub const TIF_IOC: i32 = 3;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BboRow {
    pub source_seq: u64,
    pub exch_ts: i64,
    pub local_ts_raw: i64,
    pub bid_px: f64,
    pub ask_px: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
    pub last_px: f64,
    pub total_volume: i64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BboView {
    pub bid_px: f64,
    pub ask_px: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
    pub exch_ts: i64,
    pub local_ts: i64,
    pub valid: i32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct OrderView {
    pub order_id: u64,
    pub asset_no: u32,
    pub side: i32,
    pub tif: i32,
    pub status: i32,
    pub requested_price: f64,
    pub requested_qty: f64,
    pub exec_price: f64,
    pub exec_qty: f64,
    pub leaves_qty: f64,
    pub req_local_ts: i64,
    pub exch_ts: i64,
    pub resp_local_ts: i64,
    pub response_visible: i32,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AssetConfig {
    pub(crate) local_adjustment_ns: i64,
    pub(crate) feed_offset_ns: i64,
    pub(crate) entry_latency_ns: i64,
    pub(crate) response_latency_ns: i64,
    pub(crate) tick_size: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{align_of, offset_of, size_of};

    // The ctypes consumer and release artifact support Linux x86-64. These
    // assertions deliberately freeze that supported C ABI rather than making
    // undocumented claims about other architecture-specific C layouts.
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    #[test]
    fn c_abi_layout_matches_the_python_binding() {
        assert_eq!((size_of::<BboRow>(), align_of::<BboRow>()), (72, 8));
        assert_eq!(offset_of!(BboRow, source_seq), 0);
        assert_eq!(offset_of!(BboRow, exch_ts), 8);
        assert_eq!(offset_of!(BboRow, local_ts_raw), 16);
        assert_eq!(offset_of!(BboRow, bid_px), 24);
        assert_eq!(offset_of!(BboRow, ask_px), 32);
        assert_eq!(offset_of!(BboRow, bid_qty), 40);
        assert_eq!(offset_of!(BboRow, ask_qty), 48);
        assert_eq!(offset_of!(BboRow, last_px), 56);
        assert_eq!(offset_of!(BboRow, total_volume), 64);

        assert_eq!((size_of::<BboView>(), align_of::<BboView>()), (56, 8));
        assert_eq!(offset_of!(BboView, bid_px), 0);
        assert_eq!(offset_of!(BboView, ask_px), 8);
        assert_eq!(offset_of!(BboView, bid_qty), 16);
        assert_eq!(offset_of!(BboView, ask_qty), 24);
        assert_eq!(offset_of!(BboView, exch_ts), 32);
        assert_eq!(offset_of!(BboView, local_ts), 40);
        assert_eq!(offset_of!(BboView, valid), 48);

        assert_eq!((size_of::<OrderView>(), align_of::<OrderView>()), (96, 8));
        assert_eq!(offset_of!(OrderView, order_id), 0);
        assert_eq!(offset_of!(OrderView, asset_no), 8);
        assert_eq!(offset_of!(OrderView, side), 12);
        assert_eq!(offset_of!(OrderView, tif), 16);
        assert_eq!(offset_of!(OrderView, status), 20);
        assert_eq!(offset_of!(OrderView, requested_price), 24);
        assert_eq!(offset_of!(OrderView, requested_qty), 32);
        assert_eq!(offset_of!(OrderView, exec_price), 40);
        assert_eq!(offset_of!(OrderView, exec_qty), 48);
        assert_eq!(offset_of!(OrderView, leaves_qty), 56);
        assert_eq!(offset_of!(OrderView, req_local_ts), 64);
        assert_eq!(offset_of!(OrderView, exch_ts), 72);
        assert_eq!(offset_of!(OrderView, resp_local_ts), 80);
        assert_eq!(offset_of!(OrderView, response_visible), 88);
    }
}
