"""
Virtual Zarr store backed by Polytope.

Presents DestinE Climate DT data as an xarray Dataset where metadata is
available instantly and data chunks are fetched lazily on access (e.g.
when plotting or calling .values).

Usage:
    from polytope_zarr import PolytopeZarrStore
    store = PolytopeZarrStore(address=..., collection=..., base_request=...,
                              coords=..., variables=..., internal_dims=...)
    ds = store.open()
"""

import json
import logging
import numpy as np
import pandas as pd
from collections.abc import MutableMapping

# Try to import VLenUTF8 for proper string coordinates
try:
    from numcodecs import VLenUTF8
    _HAS_VLENUTF8 = True
except ImportError:
    _HAS_VLENUTF8 = False


class PolytopeZarrStore(MutableMapping):
    """A read-only zarr v2 store that fetches data from Polytope on demand."""

    def __init__(self, address, collection, base_request, coords, variables,
                 internal_dims=None, time_fields=None):
        """
        Parameters
        ----------
        address : str or dict
            Polytope server URL, or {model_name: url} for per-model routing.
        collection : str
            Polytope collection name (e.g. "destination-earth").
        base_request : dict
            Fixed request fields (class, dataset, experiment, …).
            Do NOT include year/month/day/time/param/model — those are per-chunk.
        coords : dict
            {dim_name: array-like} — e.g. {"time": pd.date_range(...),
            "cell": range(196608), "model": ["ICON", "IFS-FESOM"]}.
        variables : dict
            {var_name: {"dims": ("model", "time", "cell")}}.
        internal_dims : list, optional
            Dims that form a single chunk (e.g. ["cell"]).
        time_fields : list, optional
            Request fields extracted from timestamps. Default ["year", "month"].
        """
        self._address = address
        self._collection = collection
        self._base_request = dict(base_request)
        self._internal_dims = set(internal_dims or [])
        self._time_fields = time_fields or ["year", "month"]
        self._cache = {}
        self._kv = {}

        # Normalise coordinates to numpy arrays
        self._coords = {}
        self._dim_sizes = {}
        for name, vals in coords.items():
            if name == "time":
                arr = np.array(pd.DatetimeIndex(vals), dtype="datetime64[ns]")
            elif hasattr(vals, '__len__') and not isinstance(vals, str):
                arr = np.array(list(vals))
            else:
                arr = np.array(vals)
            self._coords[name] = arr
            self._dim_sizes[name] = len(arr)

        self._variables = dict(variables)
        self._build_metadata()

    # ── Metadata synthesis ──────────────────────────────────────────────

    def _build_metadata(self):
        """Populate self._kv with zarr v2 metadata and coordinate chunks."""
        self._kv[".zgroup"] = json.dumps({"zarr_format": 2}).encode()
        self._kv[".zattrs"] = json.dumps({}).encode()

        meta = {"zarr_consolidated_format": 1, "metadata": {
            ".zgroup": {"zarr_format": 2}, ".zattrs": {}
        }}

        # Coordinate arrays
        for name, arr in self._coords.items():
            self._write_coord(name, arr, meta)

        # Data variables (metadata only — chunks are lazy)
        for var_name, spec in self._variables.items():
            self._write_var_meta(var_name, spec, meta)

        self._kv[".zmetadata"] = json.dumps(meta).encode()

    def _write_coord(self, name, arr, meta):
        """Write a coordinate array's metadata + data chunk into the store."""
        if arr.dtype.kind == "M":  # datetime
            data = arr.astype("int64")
            zarray = self._zarray(shape=(len(arr),), chunks=(len(arr),),
                                  dtype="|i8", fill_value=0)
            zattrs = {"_ARRAY_DIMENSIONS": [name],
                       "units": "nanoseconds since 1970-01-01",
                       "calendar": "proleptic_gregorian"}
        elif arr.dtype.kind == "U" or arr.dtype.kind == "O":  # strings
            zarray, data = self._encode_strings(arr)
            zattrs = {"_ARRAY_DIMENSIONS": [name]}
        else:  # numeric
            data = arr.astype("int32") if arr.dtype.kind == "i" else arr
            zarray = self._zarray(shape=(len(arr),), chunks=(len(arr),),
                                  dtype=data.dtype.str, fill_value=None)
            zattrs = {"_ARRAY_DIMENSIONS": [name]}

        key_prefix = name
        self._kv[f"{key_prefix}/.zarray"] = json.dumps(zarray).encode()
        self._kv[f"{key_prefix}/.zattrs"] = json.dumps(zattrs).encode()
        self._kv[f"{key_prefix}/0"] = (
            bytes(data) if isinstance(data, (bytes, bytearray)) else data.tobytes()
        )

        meta["metadata"][f"{key_prefix}/.zarray"] = zarray
        meta["metadata"][f"{key_prefix}/.zattrs"] = zattrs

    def _write_var_meta(self, var_name, spec, meta):
        """Write a data variable's zarr metadata (no data — chunks are lazy)."""
        dims = spec["dims"]
        shape = tuple(self._dim_sizes[d] for d in dims)
        chunks = tuple(
            self._dim_sizes[d] if d in self._internal_dims else 1
            for d in dims
        )
        zarray = self._zarray(shape=shape, chunks=chunks,
                              dtype="<f4", fill_value="NaN")
        zattrs = {"_ARRAY_DIMENSIONS": list(dims)}
        # Propagate extra attributes (long_name, units, etc.) from spec
        for attr_key in ("long_name", "units"):
            if attr_key in spec:
                zattrs[attr_key] = spec[attr_key]

        self._kv[f"{var_name}/.zarray"] = json.dumps(zarray).encode()
        self._kv[f"{var_name}/.zattrs"] = json.dumps(zattrs).encode()

        meta["metadata"][f"{var_name}/.zarray"] = zarray
        meta["metadata"][f"{var_name}/.zattrs"] = zattrs

    @staticmethod
    def _zarray(shape, chunks, dtype, fill_value, compressor=None):
        return {"zarr_format": 2, "shape": list(shape),
                "chunks": list(chunks), "dtype": dtype,
                "fill_value": fill_value, "order": "C",
                "compressor": compressor, "filters": None}

    def _encode_strings(self, arr):
        """Encode a string array for zarr v2, using VLenUTF8 if available."""
        vals = [str(v) for v in arr]
        if _HAS_VLENUTF8:
            codec = VLenUTF8()
            data_bytes = codec.encode(np.array(vals, dtype=object))
            zarray = self._zarray(
                shape=(len(vals),), chunks=(len(vals),), dtype="|O",
                fill_value="", compressor=None)
            zarray["filters"] = [codec.get_config()]
            return zarray, data_bytes
        # Fallback: fixed-length bytes
        maxlen = max(len(v) for v in vals)
        data = np.array(vals, dtype=f"|S{maxlen}")
        zarray = self._zarray(shape=(len(vals),), chunks=(len(vals),),
                              dtype=data.dtype.str, fill_value="")
        return zarray, data.tobytes()

    # ── Lazy chunk fetching ─────────────────────────────────────────────

    def _fetch_chunk(self, var_name, chunk_key):
        """Translate a zarr chunk key into a Polytope request and fetch data."""
        spec = self._variables[var_name]
        dims = spec["dims"]
        indices = [int(i) for i in chunk_key.split(".")]

        request = dict(self._base_request)
        request["param"] = var_name
        chunk_shape = []

        for dim, idx in zip(dims, indices):
            size = self._dim_sizes[dim]
            if dim in self._internal_dims:
                chunk_shape.append(size)
                continue
            coord_val = self._coords[dim][idx]
            chunk_shape.append(1)

            if dim == "time":
                ts = pd.Timestamp(coord_val)
                field_map = {"year": str(ts.year), "month": str(ts.month),
                             "day": str(ts.day),
                             "time": ts.strftime("%H:%M")}
                for f in self._time_fields:
                    request[f] = field_map[f]
            elif dim == "model":
                request["model"] = str(coord_val)
            elif dim == "level":
                request["levelist"] = str(int(coord_val))
            else:
                request[dim] = str(coord_val)

        # Resolve address
        address = self._address
        if isinstance(address, dict):
            model = request.get("model", "")
            address = address.get(model, list(address.values())[0])

        total = 1
        for s in chunk_shape:
            total *= s

        try:
            import earthkit.data
            data = earthkit.data.from_source(
                "polytope", self._collection, request,
                address=address, stream=False)
            values = data.to_numpy().flatten().astype(np.float32)
            if values.size != total:
                raise ValueError(
                    f"Size mismatch: got {values.size}, expected {total}")
        except Exception as e:
            print(f"  ⚠ fetch {var_name} chunk {chunk_key}: {e}")
            values = np.full(total, np.nan, dtype=np.float32)

        return values.tobytes()

    # ── MutableMapping interface ────────────────────────────────────────

    def __getitem__(self, key):
        if key in self._kv:
            return self._kv[key]
        if key in self._cache:
            return self._cache[key]
        # Try to parse as a data variable chunk: "var_name/0.1.0"
        if "/" in key:
            parts = key.split("/", 1)
            var_name, chunk_key = parts[0], parts[1]
            if var_name in self._variables and "." in chunk_key:
                data = self._fetch_chunk(var_name, chunk_key)
                self._cache[key] = data
                return data
        raise KeyError(key)

    def __setitem__(self, key, value):
        self._kv[key] = value

    def __delitem__(self, key):
        if key in self._kv:
            del self._kv[key]
        elif key in self._cache:
            del self._cache[key]
        else:
            raise KeyError(key)

    def __iter__(self):
        return iter(self._kv)

    def __len__(self):
        return len(self._kv)

    def __contains__(self, key):
        if key in self._kv:
            return True
        # Report data variable chunks as existing so zarr fetches them
        if "/" in key:
            parts = key.split("/", 1)
            if parts[0] in self._variables and "." in parts[1]:
                return True
        return False

    def listdir(self, prefix=""):
        """List entries under a prefix (used by zarr for discovery)."""
        if prefix:
            prefix = prefix.rstrip("/") + "/"
        entries = set()
        for k in self._kv:
            if k.startswith(prefix):
                rest = k[len(prefix):]
                entries.add(rest.split("/")[0])
        return sorted(entries)

    # ── Convenience ─────────────────────────────────────────────────────

    def open(self):
        """Open this store as an xarray Dataset (lazy — no data fetched)."""
        import xarray as xr
        return xr.open_dataset(self, engine="zarr", consolidated=True)

    def clear_cache(self):
        """Free cached data chunks."""
        self._cache.clear()

    def __repr__(self):
        dims = ", ".join(f"{k}={v}" for k, v in self._dim_sizes.items())
        nvars = len(self._variables)
        return f"<PolytopeZarrStore {nvars} variables ({dims})>"
