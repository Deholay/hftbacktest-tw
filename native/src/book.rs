use std::collections::HashMap;

use crate::types::{BboRow, BboView};

#[derive(Clone, Debug, Default)]
pub(crate) struct DepthState {
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

    pub(crate) fn apply_row(&mut self, row: BboRow, timestamp: i64, tick_size: f64) -> BboView {
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

    #[test]
    fn locked_snapshot_respects_reference_clear_bounds() {
        let mut depth = DepthState::default();
        depth.apply_row(row(0, 100, 100, 49.0, 50.0, 20.0), 100, 1.0);
        let view = depth.apply_row(row(1, 101, 101, 50.0, 50.0, 81.0), 101, 1.0);
        assert_eq!(view.valid, 0);
        assert!(view.bid_px.is_nan());
        assert_eq!(view.ask_px, 50.0);
        assert_eq!(view.ask_qty, 81.0);
    }
}
