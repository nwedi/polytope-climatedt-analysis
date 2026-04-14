# fdb_xarray

A thin wrapper that exposes Destination Earth Climate-DT FDB data (served through
`z3fdb`) as lazy `xarray.Dataset` objects. One function call replaces the
manual MARS-request assembly and integer-indexed zarr access.

## Prerequisites

- `z3fdb`, `zarr` (>= 3), `xarray`, `dask`, `pandas`, `numpy`
- A reachable FDB endpoint described in `./config.yaml`
- `destine_portfolio.py` (ships alongside this module) for variable metadata

## The main function

```python
from fdb_xarray import open_climate_dt

ds = open_climate_dt(
    portfolio,           # "CLMN" (monthly) or "CLTE" (hourly atmos / daily ocean)
    levtype,             # "sfc" | "pl" | "hl" | "sol" | "o2d" | "o3d"
    model,               # "IFS-FESOM" | "ICON" | (NEMO when available)
    experiment,          # "hist" | "SSP3-7.0" | "cont" | ...
    years=None,          # range(y0, y1+1) — open a full-year span
    start_date=None,     # alternative to years: "YYYY-MM-DD"
    end_date=None,       # alternative to years: "YYYY-MM-DD"
    resolution="standard",   # "standard" (nside=128) or "high" (nside=1024)
    variables=None,      # subset of the levtype's variables; default = all
    config_path="./config.yaml",
)
```

Returns an `xarray.Dataset` whose dimensions are:
- `(time, scenario, cell)` for 2D levtypes (`sfc`, `o2d`)
- `(time, scenario, level, cell)` for 3D levtypes (`pl`, `hl`, `sol`, `o3d`)

Variables carry `long_name` and `units`; the dataset carries `grid`, `nside`,
and the original `mars_request` as attributes. All arrays are dask-backed, so
opening is cheap; data is only fetched when you `.compute()`.

## Examples

### 1. Hourly surface — 2 m temperature snapshot

```python
ds = open_climate_dt(
    portfolio="CLTE", levtype="sfc",
    model="IFS-FESOM", experiment="hist",
    years=range(1990, 2015),
)

field = ds["2t"].sel(time="2014-01-01T12:00", scenario="hist").values
```

### 2. Hourly surface — annual mean via dask

```python
hist_mean = (
    ds["avg_tprate"]
    .sel(time=slice("2014-01-01", "2014-12-31"), scenario="hist")
    .mean("time")
    .compute()
    .values
)
```

The actual download happens only at `.compute()`; dask streams one hourly
chunk at a time.

### 3. Scenario projection (SSP3-7.0)

```python
ds_scen = open_climate_dt(
    portfolio="CLTE", levtype="sfc",
    model="IFS-FESOM", experiment="SSP3-7.0",
    years=range(2015, 2050),
)
t2_2049 = (
    ds_scen["2t"]
    .sel(time=slice("2049-01-01", "2049-12-31"), scenario="SSP3-7.0")
    .mean("time").compute()
)
```

### 4. 3D atmosphere (pressure levels)

```python
ds_pl = open_climate_dt(
    portfolio="CLTE", levtype="pl",
    model="IFS-FESOM", experiment="hist",
    years=range(2014, 2015),
    variables=["t", "z"],        # subset the 9-variable portfolio
)

t500 = ds_pl["t"].sel(time="2014-01-01T12:00", scenario="hist", level=500)
profile = ds_pl["t"].sel(time="2014-07-01T12:00", scenario="hist").isel(cell=0).compute()
```

### 5. 3D ocean (daily, 75 levels)

```python
ds_o3d = open_climate_dt(
    portfolio="CLTE", levtype="o3d",
    model="IFS-FESOM", experiment="hist",
    years=range(2014, 2015),
    variables=["avg_thetao"],
)
surface_sst = ds_o3d["avg_thetao"].sel(time="2014-07-01", level=1, scenario="hist").compute()
```

### 6. High resolution — short window with explicit dates

For high-res hourly, use `start_date`/`end_date` instead of `years` to scope
the request to a month or a few days.

```python
ds_hi = open_climate_dt(
    portfolio="CLTE", levtype="sfc",
    model="IFS-FESOM", experiment="hist",
    start_date="2014-01-01", end_date="2014-01-31",
    resolution="high",
    variables=["2t"],
)
# nside=1024, 12_582_912 cells, 744 hourly steps

monthly_mean = ds_hi["2t"].sel(scenario="hist").mean("time").compute()
daily_means  = ds_hi["2t"].sel(scenario="hist").resample(time="1D").mean()
```

### 7. Monthly ocean fields (CLMN)

Pre-aggregated monthly means are available for ocean variables in standard and
high resolution.

```python
ds_m = open_climate_dt(
    portfolio="CLMN", levtype="o2d",
    model="IFS-FESOM", experiment="hist",
    years=range(2014, 2015),
    resolution="high",
    variables=["avg_tos"],
)
# dims: time=12, cell=12_582_912
jul_sst = ds_m["avg_tos"].sel(time="2014-07-01", scenario="hist").compute()
```

## What's in the dataset

```python
ds = open_climate_dt(portfolio="CLTE", levtype="sfc",
                     model="IFS-FESOM", experiment="hist",
                     years=range(1990, 2015))
print(ds)                # dims, coords, variables, attrs
print(list(ds.data_vars))
print(ds["2t"].attrs)    # {'long_name': '2 metre temperature', 'units': 'K'}
print(ds.attrs)          # portfolio, levtype, model, experiment, grid, nside, mars_request
```

The `cell` coordinate is a HEALPix nest index (`ds.attrs["grid"] == "healpix-nest"`,
`ds.attrs["nside"]` is 128 for standard and 1024 for high). Plot via `healpy`:

```python
import healpy as hp
hp.mollview(field, nest=True, flip="geo", cmap="RdYlBu_r")
```

## Availability matrix (`experiment="hist"`, current config)

| portfolio | levtype | res      | IFS-FESOM | ICON | IFS-NEMO |
|-----------|---------|----------|:---------:|:----:|:--------:|
| CLMN      | sfc     | any      | · | · | · |
| CLMN      | pl      | standard | + | + | · |
| CLMN      | pl      | high     | · | · | · |
| CLMN      | o2d     | standard | + | + | · |
| CLMN      | o2d     | high     | + | + | · |
| CLMN      | o3d     | standard | + | + | · |
| CLMN      | o3d     | high     | · | · | · |
| CLTE      | any     | any      | + | + | · |

- **CLTE** is fully populated for IFS-FESOM and ICON.
- **Monthly atmospheric surface fields are not in CLMN.** To get them, open
  `CLTE` hourly over the target window and aggregate (see example 6).
- **IFS-NEMO is not reachable** through this FDB endpoint. Use polytope if
  you need it.

## Companion scripts

- `05_xarray_wrapper_demo.py` — basic wrapper usage, numerical parity check.
- `06_timing_compare.py` — xarray path vs. raw dask path; ~8 % overhead on
  a 2014 annual-mean run.
- `07_open_easy.py` — `open_climate_dt` for sfc and pl.
- `08_projections_and_3d.py` — SSP3-7.0 projection, 3D atmosphere, 3D ocean.
- `09_high_res_monthly.py` — high-res CLTE with monthly/daily aggregation.

## Notes

- The wrapper translates `freq` from the portfolio into a pandas offset:
  `"h" → "1h"`, `"D" → "1D"` (daily data also uses `time=0000` in MARS with
  the merged `(date, time)` axis), `"MS" → monthly starts`.
- `experiment="hist"` maps to `activity=baseline`; everything else maps to
  `activity=projections`.
- The `scenario` dimension always has size 1 in practice (single experiment
  per open); it exists to preserve the activity/experiment axis.
- Opening is metadata-only. Even selecting multiple years is cheap; the
  first real I/O is the first `.compute()` (or `.values`) call.
