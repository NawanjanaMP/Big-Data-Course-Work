from(bucket: "climate_downsampled")
  |> range(start: 1900-01-01T00:00:00Z, stop: 2100-01-01T00:00:00Z)
  |> limit(n: 10)
