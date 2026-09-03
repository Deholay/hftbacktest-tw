# Slim two-asset strategy example

`CrossedMarketProbe` is a deliberately small, independent consumer of the
neutral `hftbacktest_slim` public API. Its strategy owns a fixed decision clock,
compares two local BBO views, submits a crossing FOK limit order, inspects the
neutral `OrderView`, and closes the engine through its context lifecycle.

It is not a futures/spot pricing, carry, capital, or reporting implementation.
The synthetic test in `tests/test_slim_two_asset_strategy.py` is the durable
extension-boundary example.
