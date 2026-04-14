import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr
import zarr

from z3fdb import SimpleStoreBuilder, AxisDefinition, Chunking, ExtractorType


_FREQ_TO_PANDAS = {"h": "1h", "D": "1D", "MS": "MS"}

_PORTFOLIO_REGISTRY = None


def _load_portfolio_registry():
    global _PORTFOLIO_REGISTRY
    if _PORTFOLIO_REGISTRY is None:
        from destine_portfolio import (
            PORTFOLIO_GEN2_CLMN,
            PORTFOLIO_GEN2_CLTE,
            PORTFOLIO_GEN2_STORYLINE,
        )
        _PORTFOLIO_REGISTRY = {
            "CLMN": PORTFOLIO_GEN2_CLMN,
            "CLTE": PORTFOLIO_GEN2_CLTE,
            "STORYLINE": PORTFOLIO_GEN2_STORYLINE,
        }
    return _PORTFOLIO_REGISTRY


def _activity_for(experiment):
    return "baseline" if experiment == "hist" else "projections"


def _build_mars_and_axes(
    portfolio_name, portfolio_entry, levtype, model, experiment,
    years, resolution, variables,
    start_date=None, end_date=None,
):
    freq = portfolio_entry["freq"]
    levels = portfolio_entry["levels"]
    stream = {"CLMN": "clmn", "CLTE": "clte", "STORYLINE": "clte"}[portfolio_name.upper()]
    activity = _activity_for(experiment)
    vars_list = list(variables) if variables is not None else list(portfolio_entry["variables"])
    param_part = "type=fc,levtype=" + levtype + ",param=" + "/".join(vars_list)

    base = (
        f"class=d1,dataset=climate-dt,activity={activity},experiment={experiment},"
        f"generation=2,realization=1,expver=0001,stream={stream},"
        f"model={model},resolution={resolution},"
    )

    def _mars_date(s):  # "2014-01-15" or "20140115" -> "20140115"
        return str(s).replace("-", "")[:8]

    if start_date is not None or end_date is not None:
        if freq not in ("h", "D"):
            raise ValueError("start_date/end_date only supported for freq h or D")
        if start_date is None or end_date is None:
            raise ValueError("provide both start_date and end_date")
        d0 = _mars_date(start_date)
        d1 = _mars_date(end_date)
        iso_start = f"{d0[:4]}-{d0[4:6]}-{d0[6:8]}"
    else:
        ys = list(years)
        d0 = f"{ys[0]}0101"
        d1 = f"{ys[-1]}1231"
        iso_start = f"{ys[0]}-01-01"

    if freq == "h":
        time_part = f"date={d0}/to/{d1}/by/1,time=0000/to/2300/by/1,"
        time_axes = [AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE)]
        pd_freq = "1h"
        start = iso_start
    elif freq == "D":
        # Daily data still uses a time=0000 key with merged (date,time) axis.
        time_part = f"date={d0}/to/{d1}/by/1,time=0000,"
        time_axes = [AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE)]
        pd_freq = "1D"
        start = iso_start
    elif freq == "MS":
        years = list(years)
        y0, y1 = years[0], years[-1]
        year_str = "year=" + "/".join(str(y) for y in years) + ","
        month_str = "month=1/2/3/4/5/6/7/8/9/10/11/12,"
        time_part = year_str + month_str
        time_axes = [AxisDefinition(["year", "month"], Chunking.SINGLE_VALUE)]
        pd_freq = "MS"
        start = f"{y0}-01-01"
    else:
        raise ValueError(f"Unknown freq {freq!r} in portfolio entry")

    level_part = ""
    level_axes = []
    if levels is not None:
        level_part = "levelist=" + "/".join(str(l) for l in levels) + ","
        level_axes = [AxisDefinition(["levelist"], Chunking.SINGLE_VALUE)]

    mars = base + time_part + level_part + param_part

    axes = (
        time_axes
        + [AxisDefinition(["param"], Chunking.SINGLE_VALUE)]
        + level_axes
        + [AxisDefinition(["activity", "experiment"], Chunking.SINGLE_VALUE)]
    )
    return mars, axes, pd_freq, start, vars_list, levels


def to_dataset(
    zarr_array,
    keys,
    start_date,
    freq="1h",
    scenario_labels=None,
    portfolio_levtype=None,
    levels=None,
):
    """Wrap a z3fdb zarr array as an xarray.Dataset with one variable per param.

    Zarr shape is (time, param, scenario, cell) or, when levels is given,
    (time, param, level, scenario, cell). Each param slice becomes its own
    DataArray; dask keeps everything lazy.
    """
    has_level = levels is not None
    if has_level:
        nt, nparam, nlev, nscen, ncell = zarr_array.shape
        if len(levels) != nlev:
            raise ValueError(
                f"levels has {len(levels)} entries, level axis has {nlev}"
            )
    else:
        nt, nparam, nscen, ncell = zarr_array.shape

    if len(keys) != nparam:
        raise ValueError(f"keys has {len(keys)} entries, param axis has {nparam}")

    darr = da.from_zarr(zarr_array)
    time = pd.date_range(start=str(start_date), periods=nt, freq=freq)
    cell = np.arange(ncell, dtype=np.int64)
    if scenario_labels is None:
        scenario_labels = [f"s{i}" for i in range(nscen)]

    var_meta = (portfolio_levtype or {}).get("variables", {})

    data_vars = {}
    for i, name in enumerate(keys):
        meta = var_meta.get(name, {})
        attrs = {k: meta[k] for k in ("long_name", "units") if k in meta}
        if has_level:
            # (time, level, scenario, cell) -> (time, scenario, level, cell)
            arr = darr[:, i, :, :, :].transpose(0, 2, 1, 3)
            dims = ("time", "scenario", "level", "cell")
            coords = {
                "time": time,
                "scenario": list(scenario_labels),
                "level": list(levels),
                "cell": cell,
            }
        else:
            arr = darr[:, i, :, :]
            dims = ("time", "scenario", "cell")
            coords = {
                "time": time,
                "scenario": list(scenario_labels),
                "cell": cell,
            }
        data_vars[name] = xr.DataArray(
            arr, dims=dims, coords=coords, name=name, attrs=attrs,
        )

    ds = xr.Dataset(data_vars)
    ds.attrs["grid"] = "healpix-nest"
    ds.attrs["nside"] = int(np.sqrt(ncell / 12))
    return ds


def open_climate_dt(
    portfolio,
    levtype,
    model,
    experiment,
    years=None,
    resolution="standard",
    variables=None,
    config_path="./config.yaml",
    start_date=None,
    end_date=None,
):
    """Open a Climate DT FDB selection as an xarray.Dataset.

    Reads metadata from destine_portfolio (freq, levels, variable units/long_name)
    and constructs the MARS request, axis definitions and time coordinate
    accordingly. Returns a lazy (dask-backed) xr.Dataset.
    """
    registry = _load_portfolio_registry()
    pname = portfolio.upper()
    if pname not in registry:
        raise ValueError(f"Unknown portfolio {portfolio!r}; known: {list(registry)}")
    entry = registry[pname][levtype]

    if years is None and (start_date is None or end_date is None):
        raise ValueError("provide either years=range(...) or start_date+end_date")
    mars, axes, pd_freq, start, keys, levels = _build_mars_and_axes(
        pname, entry, levtype, model, experiment, years, resolution, variables,
        start_date=start_date, end_date=end_date,
    )

    builder = SimpleStoreBuilder(config_path)
    builder.add_part(mars, axes, ExtractorType.GRIB)
    store = builder.build()
    zarr_arr = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

    ds = to_dataset(
        zarr_arr,
        keys=keys,
        start_date=start,
        freq=pd_freq,
        scenario_labels=[experiment],
        portfolio_levtype=entry,
        levels=levels,
    )
    ds.attrs.update({
        "portfolio": pname,
        "levtype": levtype,
        "model": model,
        "experiment": experiment,
        "resolution": resolution,
        "mars_request": mars,
    })
    return ds
