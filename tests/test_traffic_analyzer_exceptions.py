import unittest
from traffic_analyzer import TrafficAnalyzer


class FaultySelect:
    def select(self, *args, **kwargs):
        raise RuntimeError("boom-select")


class FaultyWithColumns:
    def with_columns(self, *args, **kwargs):
        raise RuntimeError("boom-with_columns")


class FaultySort:
    def sort(self, *args, **kwargs):
        raise RuntimeError("boom-sort")


class TestTrafficAnalyzerExceptions(unittest.TestCase):
    def test_defaults_when_no_data(self):
        ta = TrafficAnalyzer()
        self.assertEqual(ta.total(), 0)
        self.assertEqual(ta.per_day(), [])
        self.assertEqual(ta.top_k(3), [])
        self.assertEqual(ta.min_window_sum(), {})

    def test_add_paths_handles_bad_path(self):
        ta = TrafficAnalyzer()
        try:
            # Intentionally pass a bad path type to trigger internal exception
            ta.add_paths([None])  # type: ignore[arg-type]
        except Exception as e:  # pragma: no cover (should not raise)
            self.fail(f"add_paths raised unexpectedly: {e}")
        # Should still be able to call methods and get safe defaults
        self.assertEqual(ta.total(), 0)

    def test_total_handles_internal_exception(self):
        ta = TrafficAnalyzer()
        ta.lf = FaultySelect()  # type: ignore[assignment]
        self.assertEqual(ta.total(), 0)

    def test_per_day_handles_internal_exception(self):
        ta = TrafficAnalyzer()
        ta.lf = FaultyWithColumns()  # type: ignore[assignment]
        self.assertEqual(ta.per_day(), [])

    def test_top_k_handles_internal_exception(self):
        ta = TrafficAnalyzer()
        ta.lf = FaultySort()  # type: ignore[assignment]
        self.assertEqual(ta.top_k(3), [])

    def test_min_window_sum_handles_internal_exception(self):
        ta = TrafficAnalyzer()
        ta.lf = FaultySort()  # type: ignore[assignment]
        self.assertEqual(ta.min_window_sum(), {})


if __name__ == "__main__":
    unittest.main()


