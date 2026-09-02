use std::collections::HashMap;

use crate::book::DepthState;
use crate::matcher::match_immediate;
use crate::scheduler::{EventSource, NextEvent, PendingEvent, PendingKind, consider_next_event};
use crate::types::{AssetConfig, BboRow, BboView, OrderView, STATUS_NEW, TIF_FOK, TIF_IOC};

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

#[derive(Debug)]
pub struct SlimEngine {
    assets: [AssetState; 2],
    orders: HashMap<(usize, u64), OrderView>,
    pending: Vec<PendingEvent>,
    next_serial: u64,
    current_ts: i64,
}

impl SlimEngine {
    pub(crate) fn new(rows: [Vec<BboRow>; 2], configs: [AssetConfig; 2]) -> Self {
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
        let mut best = None;
        for asset_no in 0..2 {
            let asset = &self.assets[asset_no];
            if let Some(&row_index) = asset.local_order.get(asset.local_cursor) {
                let row = asset.rows[row_index];
                consider_next_event(
                    &mut best,
                    NextEvent {
                        key: (asset.local_ts(row), asset_no, 0, row.source_seq),
                        source: EventSource::LocalData {
                            asset: asset_no,
                            row_index,
                        },
                    },
                );
            }
            if let Some(&row_index) = asset.exch_order.get(asset.exch_cursor) {
                let row = asset.rows[row_index];
                consider_next_event(
                    &mut best,
                    NextEvent {
                        key: (row.exch_ts, asset_no, 2, row.source_seq),
                        source: EventSource::ExchData {
                            asset: asset_no,
                            row_index,
                        },
                    },
                );
            }
        }
        for (index, pending) in self.pending.iter().enumerate() {
            consider_next_event(
                &mut best,
                NextEvent {
                    key: (
                        pending.ts,
                        pending.asset_no,
                        pending.kind.event_kind_priority(),
                        pending.serial,
                    ),
                    source: EventSource::Pending { index },
                },
            );
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
            let decision = match_immediate(
                order.side,
                order.requested_price,
                order.requested_qty,
                order.tif,
                view,
            );
            order.status = decision.status;
            order.exec_price = decision.exec_price;
            order.exec_qty = decision.exec_qty;
            order.leaves_qty = decision.leaves_qty;
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

    pub(crate) fn submit(
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

    pub(crate) fn wait_response(&mut self, asset_no: usize, order_id: u64, timeout_ns: i64) -> i32 {
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

    pub(crate) fn current_timestamp(&self) -> i64 {
        self.current_ts
    }

    pub(crate) fn elapse(&mut self, duration_ns: i64) -> i32 {
        let target = self.current_ts.saturating_add(duration_ns.max(0));
        if self
            .latest_pending_timestamp()
            .is_none_or(|latest| target > latest)
        {
            return 1;
        }
        self.process_through(target, None);
        0
    }

    pub(crate) fn depth(&self, asset_no: usize) -> Option<BboView> {
        self.assets.get(asset_no).map(|asset| asset.local_view)
    }

    pub(crate) fn feed_latency(&self, asset_no: usize) -> Option<(i64, i64)> {
        self.assets
            .get(asset_no)
            .and_then(|asset| asset.feed_latency)
    }

    pub(crate) fn order_latency(&self, asset_no: usize) -> Option<(i64, i64, i64)> {
        self.assets
            .get(asset_no)
            .and_then(|asset| asset.order_latency)
    }

    pub(crate) fn order(&self, asset_no: usize, order_id: u64) -> Option<OrderView> {
        self.orders.get(&(asset_no, order_id)).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{STATUS_EXPIRED, STATUS_FILLED};

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
