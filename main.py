import logging
import time
import json
from typing import List

from traffic_analyzer import TrafficAnalyzer

def main(file_paths: List[str]) -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    log = logging.getLogger("traffic")
    t0 = time.perf_counter()

    # Start banner
    log.info("")
    log.info("=== Traffic Analysis Start ===")
    log.info("Paths: %s", file_paths)
    log.info("")

    ta = TrafficAnalyzer()
    ta.add_paths(file_paths)

    total = ta.total()
    days = ta.per_day()
    top3 = ta.top_k(k=3)
    min_window = ta.min_window_sum()

    # Pretty summary
    log.info("--- Summary --------------------------------------------------")
    log.info("Total cars: %s", total)
    log.info("")
    log.info("Per-day totals:")
    log.info("\n%s", json.dumps(days, indent=2, ensure_ascii=False))

    log.info("\n----------------------------------------------------------")
    
    log.info("Top 3 half-hours:")
    log.info("\n%s", json.dumps(top3, indent=2, ensure_ascii=False))
    
    log.info("\n----------------------------------------------------------")
    log.info("Least 1.5h window:")
    log.info("\n%s", json.dumps(min_window, indent=2, ensure_ascii=False))


    # End banner
    dt_ms = int((time.perf_counter() - t0) * 1000)
    log.info("")
    log.info("=== Traffic Analysis Done (%sms) ===", dt_ms)
    log.info("")



if __name__ == "__main__":
    default_paths = ["./traffic_logs/traffic.logs"]
    main(default_paths)

