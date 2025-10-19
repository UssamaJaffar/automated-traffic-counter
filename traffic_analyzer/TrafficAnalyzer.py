import logging
from datetime import timedelta
from typing import List, Optional

import polars as pl

logger = logging.getLogger(__name__)


class TrafficAnalyzer:

    """Analyze half-hour traffic logs using a Polars LazyFrame pipeline.

    Overview
    - This class avoids loading everything into memory up-front. Instead, it
      builds a lazy query from one or more CSV sources (added via `add_paths`).
      Each public method composes transformations and collects only the minimal

    Expected schema per row
    - dt: pl.Datetime (the half-hour timestamp)
    - count: pl.Int64 (number of cars in that half-hour)

    Error handling
    - All public methods catch exceptions, log them, and return a safe default:
      total() -> 0, per_day()/top_k() -> [], min_window_sum() -> {}.
    """

    def __init__(self) -> None:
        """Create an empty analyzer with no data yet."""

        self.lf: Optional[pl.LazyFrame] = None

    def add_paths(self, paths: List[str]) -> None:

        """Add CSV file paths lazily (no immediate read).

        Each path is added as a scan_csv LazyFrame and concatenated to the
        pipeline. Downstream queries will filter/transform/collect as needed.
        """
        
        try:
            for p in paths:
                scan = pl.scan_csv(
                    p,
                    has_header=False,
                    separator=" ",
                    new_columns=["dt", "count"],
                    ignore_errors=True,
                    schema_overrides={"dt": pl.Datetime, "count": pl.Int64},
                )
        
                if self.lf is None:
                    self.lf = scan
        
                else:
                    self.lf = pl.concat([self.lf, scan], how="vertical_relaxed")
        
        except Exception as exc:
            logger.exception("failed to add paths: %s", exc)


    def total(self) -> int:

        """Return the total number of cars across the dataset.

        Safe default: 0 when no data or on error.
        """

        try:
            if self.lf is None:
                logger.warning("no data loaded: returning total=0")
                return 0
            
            result = self.lf.select(pl.col("count").sum()).collect()
            return int(result.item() or 0)

        except Exception as exc:
            logger.exception("failed to compute total: %s", exc)
            return 0

    def per_day(self) -> List[dict]:

        """Return per-day totals as a list of dicts: {"date", "total_cars"}.

        Safe default: [] when no data or on error.
        """
        try:

            if self.lf is None:
                logger.warning("no data loaded: returning empty per_day")
                return []
                
            result = (
                self.lf
                    .with_columns(pl.col("dt").dt.strftime("%Y-%m-%d").alias("date"))
                    .group_by("date")
                    .agg(pl.col("count").sum().alias("total_cars"))
                    .sort("date")
                    .select(["date", "total_cars"])  # project minimal columns
                    .collect()
            )
            
            return result.to_dicts()
        
        except Exception as exc:
            logger.exception("failed to compute per_day: %s", exc)
            return []

    def top_k(self, k: int = 3) -> List[dict]:

        """Return the top-k half-hour windows by car count.

        Output shape: list of dicts with keys {"datetime", "count"}.
        Sorted by count desc, then datetime asc. Safe default: [].
        """
        
        try:
        
            if self.lf is None:
                logger.warning("no data loaded: returning empty top_k")
                return []
        
            out = (
                self.lf
                    .sort(by=["count"], descending=[True])
                    .with_columns(pl.col("dt").dt.strftime("%Y-%m-%dT%H:%M:%S").alias("datetime"))
                    .select(["datetime", "count"])
                    .head(k)
                    .collect()
            )
        
            return out.to_dicts()
        
        except Exception as exc:
            logger.exception("failed to compute top_k: %s", exc)
            return []
        
    def min_window_sum(self) -> dict:
        
        """Return the 1.5-hour contiguous window with the least cars.

        Contiguity definition: three rows where dt increments exactly by
        30 minutes each step. Output dict has:
          - "datetime_range": [ts1, ts2, ts3]
          - "total_cars": integer sum over those three rows
        Safe default: {} when no data or on error.
        """
        
        try:
            if self.lf is None:
                logger.warning("no data loaded: returning empty min_window_sum")
                return {}
            result = (
                self.lf
                .sort("dt")
                .with_columns([
                    pl.col("dt").shift(-1).alias("dt1"),
                    pl.col("dt").shift(-2).alias("dt2"),
                    pl.col("count").shift(-1).alias("count1"),
                    pl.col("count").shift(-2).alias("count2"),
                ])
                # Keep only strictly contiguous half-hours: +30m then +30m
                .filter(
                    ((pl.col("dt1") - pl.col("dt")) == timedelta(minutes=30)) &
                    ((pl.col("dt2") - pl.col("dt1")) == timedelta(minutes=30))
                )
                # Sum the three consecutive counts
                .with_columns(
                    (pl.col("count") + pl.col("count1") + pl.col("count2")).alias("sum_count")
                )
                .select(["dt", "dt1", "dt2", "sum_count"])
                .sort("sum_count")
                .limit(1)
                .collect()
            )

            rows = result.to_dicts()
            formatted_result = {}

            if rows:
                row = rows[0]
                # Keep datetimes in natural [dt, dt1, dt2] order for readability
                formatted_result = {
                    "datetime_range": [datetime.isoformat() for key, datetime in row.items() if 'dt' in key],
                    "total_cars": row["sum_count"],
                }

            return formatted_result
            
        except Exception as exc:
            logger.exception("failed to compute min_window_sum: %s", exc)
            return {}