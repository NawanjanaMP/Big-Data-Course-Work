// Q3 - Downsampling into an auxiliary bucket with a 30-day retention rule.
//
// STEP A (run once in a shell, creates the target bucket with 30d retention):
//   docker exec influxdb-task1 influx bucket create \
//       --name climate_downsampled --org coventry --retention 30d \
//       --token coventry-bigdata-supersecret-token
//
// STEP B - this file: a CONTINUOUS InfluxDB task that summarises the raw
// series into the auxiliary bucket every hour. Paste it into
// UI -> Tasks -> Create Task (the `option task` header defines the schedule),
// or install from the shell:
//   docker exec -i influxdb-task1 influx task create \
//       --org coventry --token coventry-bigdata-supersecret-token \
//       -f /dev/stdin < queries/q3_downsampling_task.flux

option task = {name: "downsample_climate", every: 1h}

from(bucket: "climate_raw")
  |> range(start: -task.every)
  |> filter(fn: (r) => r._measurement == "climate")
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
  |> set(key: "rollup", value: "hourly_mean")
  |> to(bucket: "climate_downsampled", org: "coventry")

// STEP C (one-off backfill so the historical data actually appears in the
// downsampled bucket - run this in Data Explorer / CLI once):
//
// from(bucket: "climate_raw")
//   |> range(start: 1900-01-01T00:00:00Z, stop: 2100-01-01T00:00:00Z)
//   |> filter(fn: (r) => r._measurement == "climate")
//   |> aggregateWindow(every: 1mo, fn: mean, createEmpty: false)
//   |> to(bucket: "climate_downsampled", org: "coventry")
