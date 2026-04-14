import sys
import matplotlib.pyplot as plt
import healpy as hp

from fdb_xarray import open_climate_dt

# --- Case 1: surface hourly (CLTE/sfc) -----------------------------------
ds = open_climate_dt(
    portfolio="CLTE",
    levtype="sfc",
    model="IFS-FESOM",
    experiment="hist",
    years=range(1990, 2015),
)
print(ds)
print()
print("variables:", list(ds.data_vars))
print("2t attrs:", ds["2t"].attrs)
print("dataset attrs:", {k: ds.attrs[k] for k in ("portfolio", "levtype", "model", "experiment", "grid", "nside")})

# Sanity: same 2014 annual-mean avg_tprate as 04/05 (expected mean 3.45761e-05)
hist_mean = (
    ds["avg_tprate"]
    .sel(time=slice("2014-01-01T00:00", "2014-12-31T23:00"), scenario="hist")
    .mean("time")
    .compute()
    .values
)
print("avg_tprate 2014 mean:", float(hist_mean.mean()))

hp.mollview(
    hist_mean,
    title="IFS-FESOM — avg total precipitation rate — 2014 (via open_climate_dt)",
    unit=ds["avg_tprate"].attrs.get("units", "m"),
    cmap="Blues", min=0, max=0.0001, nest=True, flip="geo",
)
plt.savefig("avg-precip-fesom-easy.png", dpi=150, bbox_inches="tight")
plt.close()

# --- Case 2: pressure levels (CLTE/pl) to validate the level dim ----------
ds_pl = open_climate_dt(
    portfolio="CLTE",
    levtype="pl",
    model="IFS-FESOM",
    experiment="hist",
    years=range(2014, 2015),   # just one year — 3D field is heavy
    variables=["t"],           # single variable to keep it light
)
print()
print(ds_pl)
print("pl levels:", ds_pl["level"].values)

# Single snapshot at 500 hPa
t500 = (
    ds_pl["t"]
    .sel(time="2014-01-01T12:00", scenario="hist", level=500)
    .values
)
print("t @ 500 hPa shape:", t500.shape, "mean:", float(t500.mean()))

hp.mollview(
    t500,
    title="IFS-FESOM — t @ 500 hPa — 2014-01-01 12:00",
    unit=ds_pl["t"].attrs.get("units", "K"),
    cmap="RdYlBu_r", nest=True, flip="geo",
)
plt.savefig("t500-fesom-easy.png", dpi=150, bbox_inches="tight")
plt.close()

sys.exit(0)
