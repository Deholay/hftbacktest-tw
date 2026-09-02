use crate::types::{BboView, STATUS_EXPIRED, STATUS_FILLED};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct MatchDecision {
    pub(crate) status: i32,
    pub(crate) exec_price: f64,
    pub(crate) exec_qty: f64,
    pub(crate) leaves_qty: f64,
}

pub(crate) fn match_immediate(
    side: i32,
    requested_price: f64,
    requested_qty: f64,
    _tif: i32,
    exchange_view: BboView,
) -> MatchDecision {
    let crossing_price = if side > 0 {
        exchange_view.ask_px
    } else {
        exchange_view.bid_px
    };
    let crosses = exchange_view.valid != 0
        && crossing_price.is_finite()
        && crossing_price > 0.0
        && if side > 0 {
            requested_price >= crossing_price
        } else {
            requested_price <= crossing_price
        };
    if crosses {
        MatchDecision {
            status: STATUS_FILLED,
            exec_price: crossing_price,
            exec_qty: requested_qty,
            leaves_qty: 0.0,
        }
    } else {
        MatchDecision {
            status: STATUS_EXPIRED,
            exec_price: 0.0,
            exec_qty: 0.0,
            leaves_qty: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{TIF_FOK, TIF_IOC};

    fn view(bid_px: f64, ask_px: f64, bid_qty: f64, ask_qty: f64) -> BboView {
        BboView {
            bid_px,
            ask_px,
            bid_qty,
            ask_qty,
            exch_ts: 100,
            local_ts: 100,
            valid: 1,
        }
    }

    #[test]
    fn crossing_fok_and_ioc_fill_for_buy_and_sell() {
        for tif in [TIF_FOK, TIF_IOC] {
            let buy = match_immediate(1, 102.0, 10.0, tif, view(99.0, 101.0, 1.0, 1.0));
            assert_eq!(buy.status, STATUS_FILLED);
            assert_eq!(buy.exec_price, 101.0);
            assert_eq!(buy.exec_qty, 10.0);
            assert_eq!(buy.leaves_qty, 0.0);

            let sell = match_immediate(-1, 98.0, 10.0, tif, view(99.0, 101.0, 1.0, 1.0));
            assert_eq!(sell.status, STATUS_FILLED);
            assert_eq!(sell.exec_price, 99.0);
            assert_eq!(sell.exec_qty, 10.0);
            assert_eq!(sell.leaves_qty, 0.0);
        }
    }

    #[test]
    fn non_crossing_fok_and_ioc_expire_for_buy_and_sell() {
        for tif in [TIF_FOK, TIF_IOC] {
            let buy = match_immediate(1, 100.0, 10.0, tif, view(99.0, 101.0, 1.0, 1.0));
            assert_eq!(buy.status, STATUS_EXPIRED);
            assert_eq!(buy.exec_qty, 0.0);
            assert_eq!(buy.leaves_qty, 0.0);

            let sell = match_immediate(-1, 100.0, 10.0, tif, view(99.0, 101.0, 1.0, 1.0));
            assert_eq!(sell.status, STATUS_EXPIRED);
            assert_eq!(sell.exec_qty, 0.0);
            assert_eq!(sell.leaves_qty, 0.0);
        }
    }

    #[test]
    fn requested_quantity_is_not_limited_by_displayed_bbo_for_buy_or_sell() {
        let buy = match_immediate(1, 101.0, 50.0, TIF_FOK, view(99.0, 101.0, 0.25, 0.5));
        let sell = match_immediate(-1, 99.0, 60.0, TIF_IOC, view(99.0, 101.0, 0.25, 0.5));
        assert_eq!(buy.exec_qty, 50.0);
        assert_eq!(sell.exec_qty, 60.0);
    }

    #[test]
    fn invalid_or_unavailable_exchange_bbo_expires_buy_and_sell() {
        let unavailable = BboView::default();
        assert_eq!(
            match_immediate(1, f64::MAX, 1.0, TIF_FOK, unavailable).status,
            STATUS_EXPIRED
        );
        assert_eq!(
            match_immediate(-1, 0.0, 1.0, TIF_IOC, unavailable).status,
            STATUS_EXPIRED
        );

        for invalid in [
            view(99.0, f64::NAN, 1.0, 1.0),
            view(f64::NAN, 101.0, 1.0, 1.0),
            view(0.0, 101.0, 1.0, 1.0),
            view(99.0, 0.0, 1.0, 1.0),
        ] {
            if !invalid.ask_px.is_finite() || invalid.ask_px <= 0.0 {
                assert_eq!(
                    match_immediate(1, f64::MAX, 1.0, TIF_FOK, invalid).status,
                    STATUS_EXPIRED
                );
            }
            if !invalid.bid_px.is_finite() || invalid.bid_px <= 0.0 {
                assert_eq!(
                    match_immediate(-1, 0.0, 1.0, TIF_IOC, invalid).status,
                    STATUS_EXPIRED
                );
            }
        }
    }

    #[test]
    fn exact_boundary_price_crosses_for_buy_and_sell() {
        let exchange = view(99.0, 101.0, 1.0, 1.0);
        assert_eq!(
            match_immediate(1, 101.0, 1.0, TIF_FOK, exchange).status,
            STATUS_FILLED
        );
        assert_eq!(
            match_immediate(-1, 99.0, 1.0, TIF_IOC, exchange).status,
            STATUS_FILLED
        );
    }
}
