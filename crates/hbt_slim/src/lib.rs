//! Domain-neutral deterministic BBO scheduler and immediate-order matcher.

use std::collections::HashMap;
use std::slice;

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
struct AssetConfig {
    local_adjustment_ns: i64,
    feed_offset_ns: i64,
    entry_latency_ns: i64,
    response_latency_ns: i64,
    tick_size: f64,
}

#[derive(Clone, Debug)]
struct AssetState {
    rows: Vec<BboRow>,
    local_order: Vec<usize>,
    exch_order: Vec<usize>,
    local_cursor: usize,
    exch_cursor: usize,
    local_depth: DepthState,
    exch_depth: DepthState,
    local_view: BboView,
    exch_view: BboView,
    feed_latency: Option<(i64, i64)>,
    order_latency: Option<(i64, i64, i64)>,
    config: AssetConfig,
}

impl AssetState {
    fn new(rows: Vec<BboRow>, config: AssetConfig) -> Self {
        let mut local_order: Vec<usize> = (0..rows.len()).collect();
        local_order.sort_by_key(|&index| {
            let row = rows[index];
            (
                row.local_ts_raw
                    .saturating_add(config.local_adjustment_ns)
                    .saturating_add(config.feed_offset_ns),
                row.source_seq,
            )
        });
        let mut exch_order: Vec<usize> = (0..rows.len()).collect();
        exch_order.sort_by_key(|&index| (rows[index].exch_ts, rows[index].source_seq));
        Self {
            rows,
            local_order,
            exch_order,
            local_cursor: 0,
            exch_cursor: 0,
            local_depth: DepthState::default(),
            exch_depth: DepthState::default(),
            local_view: BboView::default(),
            exch_view: BboView::default(),
            feed_latency: None,
            order_latency: None,
            config,
        }
    }

    fn local_ts(&self, row: BboRow) -> i64 {
        row.local_ts_raw
            .saturating_add(self.config.local_adjustment_ns)
            .saturating_add(self.config.feed_offset_ns)
    }
}

#[derive(Clone, Debug, Default)]
struct DepthState {
    bids: HashMap<i64, f64>,
    asks: HashMap<i64, f64>,
    best_bid: Option<i64>,
    best_ask: Option<i64>,
    low_bid: Option<i64>,
    high_ask: Option<i64>,
}

impl DepthState {
    fn depth_below(&self, start: i64) -> Option<i64> {
        let low = self.low_bid?;
        self.bids
            .keys()
            .copied()
            .filter(|tick| *tick >= low && *tick < start)
            .max()
    }

    fn depth_above(&self, start: i64) -> Option<i64> {
        let high = self.high_ask?;
        self.asks
            .keys()
            .copied()
            .filter(|tick| *tick > start && *tick <= high)
            .min()
    }

    fn clear_bid(&mut self, clear_tick: i64) {
        if let Some(best_tick) = self.best_bid {
            self.bids
                .retain(|tick, _| *tick < clear_tick || *tick > best_tick);
        }
        // HashMapMarketDepth calls depth_below(clear_upto - 1), whose
        // upper bound is exclusive. Preserve that one-tick gap exactly.
        self.best_bid = self.depth_below(clear_tick.saturating_sub(1));
        if self.best_bid.is_none() {
            self.low_bid = None;
        }
    }

    fn clear_ask(&mut self, clear_tick: i64) {
        if let Some(best_tick) = self.best_ask {
            self.asks
                .retain(|tick, _| *tick < best_tick || *tick > clear_tick);
        }
        // HashMapMarketDepth calls depth_above(clear_upto + 1), whose
        // lower bound is exclusive. Preserve that one-tick gap exactly.
        self.best_ask = self.depth_above(clear_tick.saturating_add(1));
        if self.best_ask.is_none() {
            self.high_ask = None;
        }
    }

    fn update_bid(&mut self, tick: i64, qty: f64) {
        if qty > 0.0 {
            self.bids.insert(tick, qty);
        } else {
            self.bids.remove(&tick);
        }
        if qty <= 0.0 {
            if self.best_bid == Some(tick) {
                self.best_bid = self.depth_below(tick);
                if self.best_bid.is_none() {
                    self.low_bid = None;
                }
            }
            return;
        }
        if self.best_bid.is_none_or(|best| tick > best) {
            self.best_bid = Some(tick);
            if self.best_ask.is_some_and(|ask| tick >= ask) {
                self.best_ask = self.depth_above(tick);
            }
        }
        self.low_bid = Some(self.low_bid.map_or(tick, |low| low.min(tick)));
    }

    fn update_ask(&mut self, tick: i64, qty: f64) {
        if qty > 0.0 {
            self.asks.insert(tick, qty);
        } else {
            self.asks.remove(&tick);
        }
        if qty <= 0.0 {
            if self.best_ask == Some(tick) {
                self.best_ask = self.depth_above(tick);
                if self.best_ask.is_none() {
                    self.high_ask = None;
                }
            }
            return;
        }
        if self.best_ask.is_none_or(|best| tick < best) {
            self.best_ask = Some(tick);
            if self.best_bid.is_some_and(|bid| bid >= tick) {
                self.best_bid = self.depth_below(tick);
            }
        }
        self.high_ask = Some(self.high_ask.map_or(tick, |high| high.max(tick)));
    }

    fn apply_row(&mut self, row: BboRow, timestamp: i64, tick_size: f64) -> BboView {
        let price_tick = |price: f64| {
            (tick_size.is_finite() && tick_size > 0.0 && price.is_finite() && price > 0.0)
                .then(|| (price / tick_size).round() as i64)
        };
        if let Some(bid_tick) = price_tick(row.bid_px) {
            self.clear_bid(bid_tick);
            self.update_bid(bid_tick, row.bid_qty);
        }
        if let Some(ask_tick) = price_tick(row.ask_px) {
            self.clear_ask(ask_tick);
            self.update_ask(ask_tick, row.ask_qty);
        }
        let (bid_px, bid_qty) = self.best_bid.map_or((f64::NAN, 0.0), |tick| {
            (
                tick as f64 * tick_size,
                *self.bids.get(&tick).unwrap_or(&0.0),
            )
        });
        let (ask_px, ask_qty) = self.best_ask.map_or((f64::NAN, 0.0), |tick| {
            (
                tick as f64 * tick_size,
                *self.asks.get(&tick).unwrap_or(&0.0),
            )
        });
        BboView {
            bid_px,
            ask_px,
            bid_qty,
            ask_qty,
            exch_ts: row.exch_ts,
            local_ts: timestamp,
            valid: i32::from(self.best_bid.is_some() && self.best_ask.is_some()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingKind {
    Response,
    Request,
}

#[derive(Clone, Copy, Debug)]
struct PendingEvent {
    ts: i64,
    asset_no: usize,
    order_id: u64,
    kind: PendingKind,
    serial: u64,
}

#[derive(Clone, Copy, Debug)]
enum EventSource {
    LocalData { asset: usize, row_index: usize },
    Pending { index: usize },
    ExchData { asset: usize, row_index: usize },
}

#[derive(Clone, Copy, Debug)]
struct NextEvent {
    key: (i64, usize, u8, u64),
    source: EventSource,
}

#[derive(Debug)]
pub struct SlimEngine {
    assets: [AssetState; 2],
    orders: HashMap<(usize, u64), OrderView>,
    pending: Vec<PendingEvent>,
    next_serial: u64,
    current_ts: i64,
}

impl SlimEngine {
    fn new(rows: [Vec<BboRow>; 2], configs: [AssetConfig; 2]) -> Self {
        let assets = [
            AssetState::new(rows[0].clone(), configs[0]),
            AssetState::new(rows[1].clone(), configs[1]),
        ];
        let first_ts = assets
            .iter()
            .flat_map(|asset| {
                asset
                    .rows
                    .iter()
                    .flat_map(|row| [row.exch_ts, asset.local_ts(*row)])
            })
            .min()
            .unwrap_or(0);
        Self {
            assets,
            orders: HashMap::new(),
            pending: Vec::new(),
            next_serial: 0,
            current_ts: first_ts,
        }
    }

    fn next_event(&self) -> Option<NextEvent> {
        let mut best: Option<NextEvent> = None;
        let mut consider = |candidate: NextEvent| {
            if best.is_none_or(|current| candidate.key < current.key) {
                best = Some(candidate);
            }
        };
        for asset_no in 0..2 {
            let asset = &self.assets[asset_no];
            if let Some(&row_index) = asset.local_order.get(asset.local_cursor) {
                let row = asset.rows[row_index];
                consider(NextEvent {
                    key: (asset.local_ts(row), asset_no, 0, row.source_seq),
                    source: EventSource::LocalData {
                        asset: asset_no,
                        row_index,
                    },
                });
            }
            if let Some(&row_index) = asset.exch_order.get(asset.exch_cursor) {
                let row = asset.rows[row_index];
                consider(NextEvent {
                    key: (row.exch_ts, asset_no, 2, row.source_seq),
                    source: EventSource::ExchData {
                        asset: asset_no,
                        row_index,
                    },
                });
            }
        }
        for (index, pending) in self.pending.iter().enumerate() {
            let priority = if pending.kind == PendingKind::Response {
                1
            } else {
                3
            };
            consider(NextEvent {
                key: (pending.ts, pending.asset_no, priority, pending.serial),
                source: EventSource::Pending { index },
            });
        }
        best
    }

    fn process_event(&mut self, event: NextEvent) -> Option<(usize, u64)> {
        self.current_ts = event.key.0;
        match event.source {
            EventSource::LocalData { asset, row_index } => {
                let state = &mut self.assets[asset];
                let row = state.rows[row_index];
                let local_ts = row
                    .local_ts_raw
                    .saturating_add(state.config.local_adjustment_ns)
                    .saturating_add(state.config.feed_offset_ns);
                state.local_view =
                    state
                        .local_depth
                        .apply_row(row, local_ts, state.config.tick_size);
                state.feed_latency = Some((row.exch_ts, local_ts));
                state.local_cursor += 1;
                None
            }
            EventSource::ExchData { asset, row_index } => {
                let state = &mut self.assets[asset];
                let row = state.rows[row_index];
                state.exch_view =
                    state
                        .exch_depth
                        .apply_row(row, row.exch_ts, state.config.tick_size);
                state.exch_cursor += 1;
                None
            }
            EventSource::Pending { index } => {
                let pending = self.pending.swap_remove(index);
                match pending.kind {
                    PendingKind::Request => {
                        self.process_order_request(pending.asset_no, pending.order_id, pending.ts);
                        None
                    }
                    PendingKind::Response => {
                        if let Some(order) =
                            self.orders.get_mut(&(pending.asset_no, pending.order_id))
                        {
                            order.resp_local_ts = pending.ts;
                            order.response_visible = 1;
                            self.assets[pending.asset_no].order_latency =
                                Some((order.req_local_ts, order.exch_ts, order.resp_local_ts));
                        }
                        Some((pending.asset_no, pending.order_id))
                    }
                }
            }
        }
    }

    fn process_order_request(&mut self, asset_no: usize, order_id: u64, ts: i64) {
        let view = self.assets[asset_no].exch_view;
        let response_latency = self.assets[asset_no].config.response_latency_ns;
        if let Some(order) = self.orders.get_mut(&(asset_no, order_id)) {
            order.exch_ts = ts;
            let crossing_price = if order.side > 0 {
                view.ask_px
            } else {
                view.bid_px
            };
            let crosses = view.valid != 0
                && crossing_price.is_finite()
                && crossing_price > 0.0
                && if order.side > 0 {
                    order.requested_price >= crossing_price
                } else {
                    order.requested_price <= crossing_price
                };
            if crosses {
                order.status = STATUS_FILLED;
                order.exec_price = crossing_price;
                order.exec_qty = order.requested_qty;
                order.leaves_qty = 0.0;
            } else {
                order.status = STATUS_EXPIRED;
                order.leaves_qty = 0.0;
            }
            self.next_serial += 1;
            self.pending.push(PendingEvent {
                ts: ts.saturating_add(response_latency),
                asset_no,
                order_id,
                kind: PendingKind::Response,
                serial: self.next_serial,
            });
        }
    }

    fn process_through(&mut self, target: i64, stop_response: Option<(usize, u64)>) -> bool {
        while let Some(event) = self.next_event() {
            if event.key.0 > target {
                break;
            }
            let response = self.process_event(event);
            if stop_response.is_some() && response == stop_response {
                return true;
            }
        }
        self.current_ts = target;
        false
    }

    fn submit(
        &mut self,
        asset_no: usize,
        order_id: u64,
        side: i32,
        price: f64,
        qty: f64,
        tif: i32,
    ) -> i32 {
        if asset_no >= 2 || !matches!(tif, TIF_FOK | TIF_IOC) || !matches!(side, -1 | 1) {
            return -1;
        }
        if self.orders.contains_key(&(asset_no, order_id)) {
            return -2;
        }
        let request_ts = self.current_ts;
        self.orders.insert(
            (asset_no, order_id),
            OrderView {
                order_id,
                asset_no: asset_no as u32,
                side,
                tif,
                status: STATUS_NEW,
                requested_price: price,
                requested_qty: qty,
                exec_price: 0.0,
                exec_qty: 0.0,
                leaves_qty: qty,
                req_local_ts: request_ts,
                exch_ts: 0,
                resp_local_ts: 0,
                response_visible: 0,
            },
        );
        self.next_serial += 1;
        self.pending.push(PendingEvent {
            ts: request_ts.saturating_add(self.assets[asset_no].config.entry_latency_ns),
            asset_no,
            order_id,
            kind: PendingKind::Request,
            serial: self.next_serial,
        });
        0
    }

    fn wait_response(&mut self, asset_no: usize, order_id: u64, timeout_ns: i64) -> i32 {
        if self
            .orders
            .get(&(asset_no, order_id))
            .is_some_and(|order| order.response_visible != 0)
        {
            return 0;
        }
        let deadline = self.current_ts.saturating_add(timeout_ns.max(0));
        if !self.process_through(deadline, Some((asset_no, order_id))) {
            return 1;
        }
        // HftBacktest's goto() narrows its target to the requested response
        // timestamp and then drains every other event at that same timestamp
        // before returning to the strategy.
        let response_ts = self.current_ts;
        self.process_through(response_ts, None);
        0
    }

    fn latest_pending_timestamp(&self) -> Option<i64> {
        self.assets
            .iter()
            .flat_map(|asset| {
                let local = (asset.local_cursor < asset.local_order.len())
                    .then(|| asset.local_ts(asset.rows[*asset.local_order.last().unwrap()]));
                let exchange = (asset.exch_cursor < asset.exch_order.len())
                    .then(|| asset.rows[*asset.exch_order.last().unwrap()].exch_ts);
                local.into_iter().chain(exchange)
            })
            .chain(self.pending.iter().map(|event| event.ts))
            .max()
    }
}

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
    unsafe { engine.as_ref() }.map_or(0, |value| value.current_ts)
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
    let target = engine.current_ts.saturating_add(duration_ns.max(0));
    if engine
        .latest_pending_timestamp()
        .is_none_or(|latest| target > latest)
    {
        return 1;
    }
    engine.process_through(target, None);
    0
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
    let Some(asset) = engine.assets.get(asset_no) else {
        return -2;
    };
    *output = asset.local_view;
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
    let Some(value) = engine
        .assets
        .get(asset_no)
        .and_then(|asset| asset.feed_latency)
    else {
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
    let Some(value) = engine
        .assets
        .get(asset_no)
        .and_then(|asset| asset.order_latency)
    else {
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
    let Some(order) = engine.orders.get(&(asset_no, order_id)) else {
        return 1;
    };
    if order.response_visible == 0 {
        return 1;
    }
    *output = *order;
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(seq: u64, exch: i64, local: i64, bid: f64, ask: f64, qty: f64) -> BboRow {
        BboRow {
            source_seq: seq,
            exch_ts: exch,
            local_ts_raw: local,
            bid_px: bid,
            ask_px: ask,
            bid_qty: qty,
            ask_qty: qty,
            last_px: bid,
            total_volume: 0,
        }
    }

    fn engine(rows: Vec<BboRow>, entry: i64, response: i64) -> SlimEngine {
        SlimEngine::new(
            [rows, vec![]],
            [
                AssetConfig {
                    local_adjustment_ns: 0,
                    feed_offset_ns: 0,
                    entry_latency_ns: entry,
                    response_latency_ns: response,
                    tick_size: 1.0,
                },
                AssetConfig {
                    local_adjustment_ns: 0,
                    feed_offset_ns: 0,
                    entry_latency_ns: 0,
                    response_latency_ns: 0,
                    tick_size: 1.0,
                },
            ],
        )
    }

    #[test]
    fn fok_ioc_crossing_and_no_partial_fill() {
        for tif in [TIF_FOK, TIF_IOC] {
            let mut buy = engine(vec![row(0, 100, 100, 99.0, 101.0, 1.0)], 0, 5);
            buy.process_through(100, None);
            assert_eq!(buy.submit(0, 1, 1, 101.0, 10.0, tif), 0);
            assert_eq!(buy.wait_response(0, 1, 10), 0);
            let order = buy.orders[&(0, 1)];
            assert_eq!(order.status, STATUS_FILLED);
            assert_eq!(order.exec_price, 101.0);
            assert_eq!(order.exec_qty, 10.0);

            let mut sell = engine(vec![row(0, 100, 100, 99.0, 101.0, 1.0)], 0, 0);
            sell.process_through(100, None);
            sell.submit(0, 2, -1, 100.0, 10.0, tif);
            sell.wait_response(0, 2, 0);
            assert_eq!(sell.orders[&(0, 2)].status, STATUS_EXPIRED);
        }
    }

    #[test]
    fn exchange_data_precedes_order_at_equal_timestamp() {
        let mut value = engine(
            vec![
                row(0, 100, 110, 99.0, 101.0, 1.0),
                row(1, 105, 115, 100.0, 102.0, 1.0),
            ],
            5,
            0,
        );
        value.process_through(100, None);
        value.submit(0, 1, 1, 101.0, 1.0, TIF_FOK);
        value.wait_response(0, 1, 10);
        assert_eq!(value.orders[&(0, 1)].status, STATUS_EXPIRED);
    }

    #[test]
    fn independent_feed_correction_and_order_latency_are_audited() {
        let config = AssetConfig {
            local_adjustment_ns: 20,
            feed_offset_ns: 7,
            entry_latency_ns: 3,
            response_latency_ns: 4,
            tick_size: 1.0,
        };
        let mut value = SlimEngine::new(
            [vec![row(0, 100, 80, 99.0, 101.0, 1.0)], vec![]],
            [config, config],
        );
        value.process_through(107, None);
        assert_eq!(value.assets[0].feed_latency, Some((100, 107)));
        value.submit(0, 1, 1, 101.0, 1.0, TIF_IOC);
        value.wait_response(0, 1, 10);
        assert_eq!(value.assets[0].order_latency, Some((107, 110, 114)));
    }

    #[test]
    fn locked_snapshot_respects_reference_clear_bounds() {
        let mut value = engine(
            vec![
                row(0, 100, 100, 49.0, 50.0, 20.0),
                row(1, 101, 101, 50.0, 50.0, 81.0),
            ],
            0,
            0,
        );
        value.process_through(101, None);
        assert_eq!(value.assets[0].local_view.valid, 0);
        assert!(value.assets[0].local_view.bid_px.is_nan());
        assert_eq!(value.assets[0].local_view.ask_px, 50.0);
        assert_eq!(value.assets[0].local_view.ask_qty, 81.0);
    }

    #[test]
    fn wait_response_drains_other_assets_at_the_same_timestamp() {
        let config = AssetConfig {
            local_adjustment_ns: 0,
            feed_offset_ns: 0,
            entry_latency_ns: 0,
            response_latency_ns: 5,
            tick_size: 1.0,
        };
        let mut value = SlimEngine::new(
            [
                vec![row(0, 100, 100, 99.0, 101.0, 1.0)],
                vec![row(0, 105, 105, 199.0, 201.0, 1.0)],
            ],
            [config, config],
        );
        value.process_through(100, None);
        value.submit(0, 1, 1, 101.0, 1.0, TIF_FOK);
        assert_eq!(value.wait_response(0, 1, 10), 0);
        assert_eq!(value.current_ts, 105);
        assert_eq!(value.assets[1].feed_latency, Some((105, 105)));
    }
}
