import os
import tempfile
import unittest

from traffic_analyzer import TrafficAnalyzer


SAMPLE = """\
2021-12-01T05:00:00 5
2021-12-01T05:30:00 12
2021-12-01T06:00:00 14
2021-12-01T06:30:00 15
2021-12-01T07:00:00 25
2021-12-01T07:30:00 46
2021-12-01T08:00:00 42
2021-12-01T15:00:00 9
2021-12-01T15:30:00 11
2021-12-01T23:30:00 0
2021-12-05T09:30:00 18
2021-12-05T10:30:00 15
2021-12-05T11:30:00 7
2021-12-05T12:30:00 6
2021-12-05T13:30:00 9
2021-12-05T14:30:00 11
2021-12-05T15:30:00 15
2021-12-08T18:00:00 33
2021-12-08T19:00:00 28
2021-12-08T20:00:00 25
2021-12-08T21:00:00 21
2021-12-08T22:00:00 16
2021-12-08T23:00:00 11
2021-12-09T00:00:00 4
"""


class TestCore(unittest.TestCase):
    def setUp(self):
        fd, path = tempfile.mkstemp(text=True)
        os.close(fd)
        with open(path, "w", encoding="utf-8") as f:
            f.write(SAMPLE)
        self.temp_path = path
        self.ta = TrafficAnalyzer()
        self.ta.add_paths([self.temp_path])

    def tearDown(self):
        try:
            os.remove(self.temp_path)
        except FileNotFoundError:
            pass

    def test_total(self):
        self.assertEqual(self.ta.total(), 398)

    def test_per_day(self):
        self.assertEqual(
            self.ta.per_day(),
            [
                {"date": "2021-12-01", "total_cars": 179},
                {"date": "2021-12-05", "total_cars": 81},
                {"date": "2021-12-08", "total_cars": 134},
                {"date": "2021-12-09", "total_cars": 4},
            ],
        )

    def test_top3(self):
        self.assertEqual(
            self.ta.top_k(3),
            [
                {"datetime": "2021-12-01T07:30:00", "count": 46},
                {"datetime": "2021-12-01T08:00:00", "count": 42},
                {"datetime": "2021-12-08T18:00:00", "count": 33},
            ],
        )

    def test_min_window_sum(self):
        result = self.ta.min_window_sum()
        self.assertEqual(result["total_cars"], 31)
        self.assertEqual(
            result["datetime_range"],
            [
                "2021-12-01T05:00:00",
                "2021-12-01T05:30:00",
                "2021-12-01T06:00:00",
            ],
        )


if __name__ == "__main__":
    unittest.main()


