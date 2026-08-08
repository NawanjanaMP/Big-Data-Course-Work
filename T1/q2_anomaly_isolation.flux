// Q2 - Anomaly Isolation: keep only observations that deviate more than
// two standard deviations from the dataset mean (per field).

import "math"

data = from(bucket: "climate_raw")
  |> range(start: 1900-01-01T00:00:00Z, stop: now())
  |> filter(fn: (r) => r._measurement == "climate")

mu = data |> mean()   |> findRecord(fn: (key) => true, idx: 0)
sd = data |> stddev() |> findRecord(fn: (key) => true, idx: 0)

data
  |> filter(fn: (r) => math.abs(x: r._value - mu._value) > 2.0 * sd._value)
  |> yield(name: "anomalies_beyond_2_sigma")
