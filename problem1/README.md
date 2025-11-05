# Problem 1 — Metro Transit (PostgreSQL)

## Schema Decisions
- Surrogate keys (`line_id`, `stop_id`) avoid ambiguity from duplicate names (the dataset includes a duplicate "5th / Colorado").
- Constraints:
  - `vehicle_type` limited to `rail|bus` via CHECK.
  - Positive `sequence` and nonnegative `time_offset_minutes`.
  - No negative passenger counts.
  - `UNIQUE(line_id, sequence)` guarantees one stop per position on a line.

## Loader Notes
- Stops are de‑duplicated by `stop_name` on ingest; when duplicates exist, the first occurrence is kept.
- Load order: `lines → stops → line_stops → trips → stop_events`.

## Hardest Query
- Q9 (Trips with 3+ delayed stops) due to grouping across filtered rows.

## Foreign Keys Prevent
- Inserting `stop_events` that reference nonexistent `trips` or `stops`.
- Inserting a `trip` that references a nonexistent line.

## Why Relational Fits
- Strong integrity constraints and natural joins across trips, stops, and lines.
