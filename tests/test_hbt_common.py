from __future__ import annotations

import types
import unittest
from unittest.mock import patch

from scripts.hbt_common import HBT_TIME_IN_FORCE_SEMANTICS, hbt_time_in_force


class HbtTimeInForceTest(unittest.TestCase):
    def test_rod_and_gtc_map_to_explicit_gtc(self) -> None:
        package = types.SimpleNamespace(__name__="fake_hbt", GTC=10)
        self.assertEqual(hbt_time_in_force(package, "ROD"), 10)
        self.assertEqual(hbt_time_in_force(package, "gtc"), 10)

    def test_fok_and_ioc_are_loaded_from_order_module(self) -> None:
        package = types.SimpleNamespace(__name__="fake_hbt", GTC=0)
        order = types.SimpleNamespace(FOK=2, IOC=3)
        with patch("scripts.hbt_common.importlib.import_module", return_value=order) as imported:
            self.assertEqual(hbt_time_in_force(package, "FOK"), 2)
            self.assertEqual(hbt_time_in_force(package, "IOC"), 3)
        imported.assert_called_with("fake_hbt.order")

    def test_unknown_value_never_falls_back_to_gtc(self) -> None:
        package = types.SimpleNamespace(__name__="fake_hbt", GTC=0)
        with self.assertRaisesRegex(ValueError, "unknown HftBacktest time_in_force"):
            hbt_time_in_force(package, "DAY")

    def test_missing_supported_constant_fails_closed(self) -> None:
        package = types.SimpleNamespace(__name__="fake_hbt", GTC=0)
        with patch(
            "scripts.hbt_common.importlib.import_module",
            return_value=types.SimpleNamespace(),
        ):
            with self.assertRaisesRegex(RuntimeError, "does not expose required time_in_force IOC"):
                hbt_time_in_force(package, "IOC")

    def test_semantic_identity_is_versioned(self) -> None:
        self.assertEqual(HBT_TIME_IN_FORCE_SEMANTICS, "strict-v1")


if __name__ == "__main__":
    unittest.main()
