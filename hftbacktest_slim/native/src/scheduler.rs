#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingKind {
    Response,
    Request,
}

impl PendingKind {
    pub(crate) const fn event_kind_priority(self) -> u8 {
        match self {
            Self::Response => 1,
            Self::Request => 3,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PendingEvent {
    pub(crate) ts: i64,
    pub(crate) asset_no: usize,
    pub(crate) order_id: u64,
    pub(crate) kind: PendingKind,
    pub(crate) serial: u64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum EventSource {
    LocalData { asset: usize, row_index: usize },
    Pending { index: usize },
    ExchData { asset: usize, row_index: usize },
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct NextEvent {
    pub(crate) key: (i64, usize, u8, u64),
    pub(crate) source: EventSource,
}

pub(crate) fn consider_next_event(best: &mut Option<NextEvent>, candidate: NextEvent) {
    if best.is_none_or(|current| candidate.key < current.key) {
        *best = Some(candidate);
    }
}

#[cfg(test)]
fn select_next_event(candidates: impl IntoIterator<Item = NextEvent>) -> Option<NextEvent> {
    let mut best = None;
    for candidate in candidates {
        consider_next_event(&mut best, candidate);
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_key_preserves_timestamp_asset_kind_and_serial_priority() {
        let events = [
            NextEvent {
                key: (100, 1, 0, 0),
                source: EventSource::LocalData {
                    asset: 1,
                    row_index: 0,
                },
            },
            NextEvent {
                key: (100, 0, PendingKind::Request.event_kind_priority(), 1),
                source: EventSource::Pending { index: 1 },
            },
            NextEvent {
                key: (100, 0, 2, 0),
                source: EventSource::ExchData {
                    asset: 0,
                    row_index: 0,
                },
            },
            NextEvent {
                key: (100, 0, PendingKind::Response.event_kind_priority(), 2),
                source: EventSource::Pending { index: 0 },
            },
        ];
        let selected = select_next_event(events).expect("event");
        assert_eq!(selected.key, (100, 0, 1, 2));
    }

    #[test]
    fn exact_duplicate_keys_keep_the_first_candidate() {
        let key = (100, 0, 0, 1);
        let selected = select_next_event([
            NextEvent {
                key,
                source: EventSource::LocalData {
                    asset: 0,
                    row_index: 7,
                },
            },
            NextEvent {
                key,
                source: EventSource::LocalData {
                    asset: 0,
                    row_index: 8,
                },
            },
        ])
        .expect("event");
        assert!(matches!(
            selected.source,
            EventSource::LocalData { row_index: 7, .. }
        ));
    }
}
