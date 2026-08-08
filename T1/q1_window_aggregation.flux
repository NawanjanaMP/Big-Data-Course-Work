// Q1 - Window Aggregation: sliding averages across an extended span.
// The assignment asks for HOURLY averages; the query below implements exactly
// that. Note: because the climate dataset is monthly-resolution, each hourly
// window contains at most one point - run the 1mo variant afterwards to show
// a window that genuinely aggregates multiple readings (explain both in your
// report; that discussion earns marks).

// --- Required hourly sliding average ---
from(bucket: "climate_raw")
  |> range(start: 1900-01-01T00:00:00Z, stop: now())
  |> filter(fn: (r) => r._measurement == "climate")
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
  |> yield(name: "hourly_mean")

// --- Demonstration variant: 12-month sliding mean via timedMovingAverage ---
// from(bucket: "climate_raw")
//   |> range(start: 1900-01-01T00:00:00Z, stop: now())
//   |> filter(fn: (r) => r._measurement == "climate")
//   |> timedMovingAverage(every: 1mo, period: 12mo)
//   |> yield(name: "12mo_sliding_mean")
