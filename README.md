# Traffic Analyzer

Analyze half-hour traffic logs and report:
- Total cars across the dataset
- Per-day totals: `YYYY-MM-DD -> total`
- Top 3 half-hours with most cars
- Least-cars 1.5-hour contiguous window (3 strictly consecutive half-hours)

## Why this approach

- Polars with LazyFrames: We use Polars to define queries lazily (scan_csv) and collect only minimal results. This keeps memory usage low and scales well while remaining concise and readable.
- Correctness for contiguity: The least-cars 1.5-hour window enforces true 30-minute increments between adjacent rows before summing, ensuring correctness beyond mere adjacency.
- Simple, robust API: The analyzer returns plain Python dicts/lists and guards every public method with try/except to return safe defaults and log errors without crashing.
- Clean console output: The main script prints a spaced, human-friendly summary; no custom logging dependencies required.
- Multi-file ready: The analyzer supports adding multiple files via `add_paths([...])`. Because we use a single LazyFrame pipeline that unions file scans, the design scales naturally to more inputs without code churn.

## Extensibility & architecture

- Data ingestion is abstracted as a lazy pipeline; adding more sources is just another `scan_csv` concatenation.
- Each metric is an isolated query method; adding a new metric is as simple as writing a new method that composes lazy operations and collects the minimal result.
- Return types are plain Python structures (lists/dicts), making it easy to integrate with CLIs, web APIs, or further processing.

## Requirements

- Python 3.10+
- Dependencies listed in `requirements.txt` (currently: `polars==1.34.0`)

Install:
```bash
pip install -r requirements.txt
```

## Project layout

- `traffic_analyzer/TrafficAnalyzer.py`: Core analyzer (Polars LazyFrame pipeline)
- `main.py`: Runs the analysis and prints a clear summary to the console
- `tests/`: Unit tests for both normal analytics and exception handling
- `Makefile`: Convenience commands

## Quick start

```bash
make install
make run
```

By default, `main.py` reads `traffic.logs` in the project root. Adjust the `default_paths` in `main.py` if needed.

## Output format

The console output is spaced and easy to scan:
```
=== Traffic Analysis Start ===
Paths: ['traffic.logs']

--- Summary --------------------------------------------------
Total cars: 398

Per-day totals:
  2021-12-01  179
  2021-12-05  81
  2021-12-08  134
  2021-12-09  4

Top 3 half-hours:
  2021-12-01T07:30:00  46
  2021-12-01T08:00:00  42
  2021-12-08T18:00:00  33

Least 1.5h window:
  Sum: 31
  2021-12-01T05:00:00
  2021-12-01T05:30:00
  2021-12-01T06:00:00

=== Traffic Analysis Done (XXms) ===
```

## Makefile commands

- `make install`: Install dependencies from `requirements.txt`.
- `make run`: Run the main application (`main.py`).
- `make test-analyzer`: Run only the analyzer tests (`tests/test_traffic_analyzer.py`).
- `make test-all`: Run both analyzer and exception tests.

## Testing

Run the full suite:
```bash
make test-all
```

Run only analyzer tests:
```bash
make test-analyzer
```

## Notes

- Errors never crash the app: methods log and return safe defaults (0/[]/{}) so you can still get a summary.
- The 1.5-hour window requires strict 30-minute contiguity; data gaps are ignored for that query.