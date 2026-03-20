"""
DestinE Climate DT Generation 2 data portfolio.

Catalogue of all variables available from the Climate DT
Generation 2 monthly (clmn) stream, organised by levtype.

Based on https://confluence.ecmwf.int/display/DDCZ/DestinE+ClimateDT+Phase+2+clmn+Parameters

Usage:
    from destine_portfolio import PORTFOLIO
    lt = PORTFOLIO["sfc"]          # dict with levtype, levels, variables
    lt["levtype"]                 # "sfc"
    lt["variables"]["avg_2t"]     # {"dims": D_SFC, "long_name": ..., "units": ...}
"""

# ── Dimension tuples ────────────────────────────────────────────────
D_SFC = ("model", "time", "cell")
D_LEV = ("model", "time", "level", "cell")
D_O2D = ("model", "time", "cell")

# ── Surface (sfc) ──────────────────────────────────────────────────
SFC_VARIABLES = {
    # Single-level / surface atmosphere (instant params in clte)
    "avg_tclw":     {"dims": D_SFC, "long_name": "Time-mean total column liquid water",             "units": "kg m**-2"},
    "avg_tciw":     {"dims": D_SFC, "long_name": "Time-mean total column cloud ice water",          "units": "kg m**-2"},
    "avg_sp":       {"dims": D_SFC, "long_name": "Time-mean surface pressure",                      "units": "Pa"},
    "avg_tcw":      {"dims": D_SFC, "long_name": "Time-mean total column water",                    "units": "kg m**-2"},
    "avg_tcwv":     {"dims": D_SFC, "long_name": "Time-mean total column water vapour",             "units": "kg m**-2"},
    "avg_sd":       {"dims": D_SFC, "long_name": "Time-mean snow depth water equivalent",           "units": "kg m**-2"},
    "avg_msl":      {"dims": D_SFC, "long_name": "Time-mean mean sea level pressure",               "units": "Pa"},
    "avg_tcc":      {"dims": D_SFC, "long_name": "Time-mean total cloud cover",                     "units": "%"},
    "avg_10u":      {"dims": D_SFC, "long_name": "Time-mean 10 metre U wind component",             "units": "m s**-1"},
    "avg_10v":      {"dims": D_SFC, "long_name": "Time-mean 10 metre V wind component",             "units": "m s**-1"},
    "avg_2t":       {"dims": D_SFC, "long_name": "Time-mean 2 metre temperature",                   "units": "K"},
    "avg_2d":       {"dims": D_SFC, "long_name": "Time-mean 2 metre dewpoint temperature",          "units": "K"},
    "avg_10ws":     {"dims": D_SFC, "long_name": "Time-mean 10 metre wind speed",                   "units": "m s**-1"},
    "avg_skt":      {"dims": D_SFC, "long_name": "Time-mean skin temperature",                      "units": "K"},
    # Hourly-mean params in clte
    "avg_surfror":  {"dims": D_SFC, "long_name": "Time-mean surface runoff rate",                   "units": "kg m**-2 s**-1"},
    "avg_ssurfror": {"dims": D_SFC, "long_name": "Time-mean sub-surface runoff rate",               "units": "kg m**-2 s**-1"},
    "avg_tsrwe":    {"dims": D_SFC, "long_name": "Time-mean total snowfall rate water equivalent",  "units": "kg m**-2 s**-1"},
    "avg_ishf":     {"dims": D_SFC, "long_name": "Time-mean surface sensible heat flux",            "units": "W m**-2"},
    "avg_slhtf":    {"dims": D_SFC, "long_name": "Time-mean surface latent heat flux",              "units": "W m**-2"},
    "avg_sdswrf":   {"dims": D_SFC, "long_name": "Time-mean surface downward short-wave radiation flux",  "units": "W m**-2"},
    "avg_sdlwrf":   {"dims": D_SFC, "long_name": "Time-mean surface downward long-wave radiation flux",   "units": "W m**-2"},
    "avg_snswrf":   {"dims": D_SFC, "long_name": "Time-mean surface net short-wave radiation flux",       "units": "W m**-2"},
    "avg_snlwrf":   {"dims": D_SFC, "long_name": "Time-mean surface net long-wave radiation flux",        "units": "W m**-2"},
    "avg_tnswrf":   {"dims": D_SFC, "long_name": "Time-mean top net short-wave radiation flux",     "units": "W m**-2"},
    "avg_tnlwrf":   {"dims": D_SFC, "long_name": "Time-mean top net long-wave radiation flux",      "units": "W m**-2"},
    "avg_iews":     {"dims": D_SFC, "long_name": "Time-mean eastward turbulent surface stress",     "units": "N m**-2"},
    "avg_inss":     {"dims": D_SFC, "long_name": "Time-mean northward turbulent surface stress",    "units": "N m**-2"},
    "avg_ie":       {"dims": D_SFC, "long_name": "Time-mean moisture flux",                         "units": "kg m**-2 s**-1"},
    "avg_tnswrfcs": {"dims": D_SFC, "long_name": "Time-mean top net short-wave radiation flux, clear sky",     "units": "W m**-2"},
    "avg_tnlwrcs":  {"dims": D_SFC, "long_name": "Time-mean top net long-wave radiation flux, clear sky",      "units": "W m**-2"},
    "avg_snswrfcs": {"dims": D_SFC, "long_name": "Time-mean surface net short-wave radiation flux, clear sky", "units": "W m**-2"},
    "avg_snlwrcs":  {"dims": D_SFC, "long_name": "Time-mean surface net long-wave radiation flux, clear sky",  "units": "W m**-2"},
    "avg_tdswrf":   {"dims": D_SFC, "long_name": "Time-mean top downward short-wave radiation flux","units": "W m**-2"},
    "avg_tprate":   {"dims": D_SFC, "long_name": "Time-mean total precipitation rate",              "units": "kg m**-2 s**-1"},
}

# ── Pressure levels (pl) ───────────────────────────────────────────
# 19 levels: 1000, 925, 850, 700, 600, 500, 400, 300, 250, 200, 150, 100, 70, 50, 30, 20, 10, 5, 1
PL_VARIABLES = {
    "avg_pv":   {"dims": D_LEV, "long_name": "Time-mean potential vorticity",                "units": "K m**2 kg**-1 s**-1"},
    "avg_z":    {"dims": D_LEV, "long_name": "Time-mean geopotential",                       "units": "m**2 s**-2"},
    "avg_t":    {"dims": D_LEV, "long_name": "Time-mean temperature",                        "units": "K"},
    "avg_u":    {"dims": D_LEV, "long_name": "Time-mean U component of wind",                "units": "m s**-1"},
    "avg_v":    {"dims": D_LEV, "long_name": "Time-mean V component of wind",                "units": "m s**-1"},
    "avg_q":    {"dims": D_LEV, "long_name": "Time-mean specific humidity",                  "units": "kg kg**-1"},
    "avg_w":    {"dims": D_LEV, "long_name": "Time-mean vertical velocity",                  "units": "Pa s**-1"},
    "avg_r":    {"dims": D_LEV, "long_name": "Time-mean relative humidity",                  "units": "%"},
    "avg_clwc": {"dims": D_LEV, "long_name": "Time-mean specific cloud liquid water content","units": "kg kg**-1"},
}

# ── Height levels (hl) ─────────────────────────────────────────────
# Height levels >10 m only (to avoid overlap with sfc 10u/10v/2t).
# IFS-only in Phase 1 and Phase 2.
HL_VARIABLES = {
    "avg_u": {"dims": D_LEV, "long_name": "Time-mean U component of wind", "units": "m s**-1"},
    "avg_v": {"dims": D_LEV, "long_name": "Time-mean V component of wind", "units": "m s**-1"},
}

# ── Soil / snow levels (sol) ────────────────────────────────────────
# Snow: 5 levels (1–5), IFS-only
# Soil: 4 levels for IFS (1–4), 5 for ICON (1–5)
SOL_VARIABLES = {
    "avg_sd":  {"dims": D_LEV, "long_name": "Time-mean snow depth water equivalent",   "units": "kg m**-2"},
    "avg_vsw": {"dims": D_LEV, "long_name": "Time-mean volumetric soil moisture",      "units": "m**3 m**-3"},
}

# ── 2-D ocean / sea ice (o2d) ──────────────────────────────────────
O2D_VARIABLES = {
    # Sea ice
    "avg_sithick": {"dims": D_O2D, "long_name": "Time-mean sea ice thickness",                      "units": "m"},
    "avg_siconc":  {"dims": D_O2D, "long_name": "Time-mean sea ice area fraction",                  "units": "Fraction"},
    "avg_siue":    {"dims": D_O2D, "long_name": "Time-mean eastward sea ice velocity",               "units": "m s**-1"},
    "avg_sivn":    {"dims": D_O2D, "long_name": "Time-mean northward sea ice velocity",              "units": "m s**-1"},
    "avg_sivol":   {"dims": D_O2D, "long_name": "Time-mean sea ice volume per unit area",            "units": "m**3 m**-2"},
    "avg_snvol":   {"dims": D_O2D, "long_name": "Time-mean snow volume over sea ice per unit area",  "units": "m**3 m**-2"},
    # Ocean surface
    "avg_sos":       {"dims": D_O2D, "long_name": "Time-mean sea surface practical salinity",                              "units": "g kg**-1"},
    "avg_tos":       {"dims": D_O2D, "long_name": "Time-mean sea surface temperature",                                    "units": "K"},
    "avg_mlotst030": {"dims": D_O2D, "long_name": "Time-mean ocean mixed layer depth defined by sigma theta 0.03 kg m-3", "units": "m"},
    "avg_hc300m":    {"dims": D_O2D, "long_name": "Time-mean vertically-integrated heat content in the upper 300 m",      "units": "J m**-2"},
    "avg_hc700m":    {"dims": D_O2D, "long_name": "Time-mean vertically-integrated heat content in the upper 700 m",      "units": "J m**-2"},
    "avg_hcbtm":     {"dims": D_O2D, "long_name": "Time-mean total column heat content",                                  "units": "J m**-2"},
    "avg_zos":       {"dims": D_O2D, "long_name": "Time-mean sea surface height",                                         "units": "m"},
}

# ── 3-D ocean (o3d) ────────────────────────────────────────────────
# Level count varies by model: ICON 72, FESOM 69, NEMO 75.
# We use 1–75 (NEMO max); out-of-range levels return NaN.
O3D_VARIABLES = {
    "avg_so":     {"dims": D_LEV, "long_name": "Time-mean sea water practical salinity",       "units": "g kg**-1"},
    "avg_thetao": {"dims": D_LEV, "long_name": "Time-mean sea water potential temperature",    "units": "K"},
    "avg_von":    {"dims": D_LEV, "long_name": "Time-mean northward sea water velocity",      "units": "m s**-1"},
    "avg_uoe":    {"dims": D_LEV, "long_name": "Time-mean eastward sea water velocity",       "units": "m s**-1"},
    "avg_wo":     {"dims": D_LEV, "long_name": "Time-mean upward sea water velocity",         "units": "m s**-1"},
}

# ── Master catalogue ───────────────────────────────────────────────
PORTFOLIO = {
    "sfc": {
        "levtype": "sfc",
        "levels": None,
        "variables": SFC_VARIABLES,
    },
    "pl": {
        "levtype": "pl",
        "levels": [1000, 925, 850, 700, 600, 500, 400, 300, 250, 200, 150, 100, 70, 50, 30, 20, 10, 5, 1],
        "variables": PL_VARIABLES,
    },
    "hl": {
        "levtype": "hl",
        "levels": [100],  # 100 m above ground; IFS-only
        "variables": HL_VARIABLES,
    },
    "sol": {
        "levtype": "sol",
        "levels": [1, 2, 3, 4, 5],  # snow uses 1-5; soil uses 1-4 (IFS) / 1-5 (ICON)
        "variables": SOL_VARIABLES,
    },
    "o2d": {
        "levtype": "o2d",
        "levels": None,
        "variables": O2D_VARIABLES,
    },
    "o3d": {
        "levtype": "o3d",
        "levels": list(range(1, 76)),  # NEMO 75, ICON 72, FESOM 69 — padded to max
        "variables": O3D_VARIABLES,
    },
}
