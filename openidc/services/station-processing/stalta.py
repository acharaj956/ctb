"""STA/LTA detector: Python wrapper around the C kernel, with a NumPy fallback.

`recursive_sta_lta` prefers the compiled C kernel (libstalta.so) for speed and
falls back to a pure-NumPy implementation if the library is not present, so the
service runs in any environment. `detect_triggers` turns the characteristic
function into discrete detections (onset sample + peak ratio).
"""
from __future__ import annotations

import ctypes
import os
from typing import List, Tuple

import numpy as np

_LIB = None
_LIB_PATH = os.path.join(os.path.dirname(__file__), "libstalta.so")


def _load_lib():
    global _LIB
    if _LIB is not None:
        return _LIB
    if not os.path.exists(_LIB_PATH):
        return None
    lib = ctypes.CDLL(_LIB_PATH)
    lib.recursive_sta_lta.restype = None
    lib.recursive_sta_lta.argtypes = [
        ctypes.POINTER(ctypes.c_double),  # data
        ctypes.c_size_t,                  # n
        ctypes.c_size_t,                  # nsta
        ctypes.c_size_t,                  # nlta
        ctypes.POINTER(ctypes.c_double),  # out
    ]
    _LIB = lib
    return lib


def _recursive_sta_lta_numpy(data: np.ndarray, nsta: int, nlta: int) -> np.ndarray:
    """Pure-NumPy reference implementation (matches the C kernel exactly)."""
    csta, clta = 1.0 / nsta, 1.0 / nlta
    sq = data.astype(np.float64) ** 2
    sta = np.zeros_like(sq)
    lta = np.zeros_like(sq)
    s, l = 0.0, 1e-12
    for i in range(sq.size):
        s = csta * sq[i] + (1.0 - csta) * s
        l = clta * sq[i] + (1.0 - clta) * l
        sta[i], lta[i] = s, l
    charfct = sta / lta
    charfct[:nlta] = 0.0
    return charfct


def recursive_sta_lta(data: np.ndarray, nsta: int, nlta: int) -> np.ndarray:
    """STA/LTA characteristic function. Uses the C kernel when available."""
    data = np.ascontiguousarray(data, dtype=np.float64)
    lib = _load_lib()
    if lib is None:
        return _recursive_sta_lta_numpy(data, nsta, nlta)

    out = np.zeros_like(data)
    lib.recursive_sta_lta(
        data.ctypes.data_as(ctypes.POINTER(ctypes.c_double)),
        data.size,
        nsta,
        nlta,
        out.ctypes.data_as(ctypes.POINTER(ctypes.c_double)),
    )
    return out


def using_c_kernel() -> bool:
    return _load_lib() is not None


def detect_triggers(
    charfct: np.ndarray, on: float, off: float
) -> List[Tuple[int, float]]:
    """Classic on/off trigger logic.

    A detection opens when the characteristic function rises above `on` and
    closes when it falls back below `off`. Returns (onset_sample, peak_ratio)
    for each detection — onset is the sample where the ratio first exceeded `on`.
    """
    triggers: List[Tuple[int, float]] = []
    in_trigger = False
    onset = 0
    peak = 0.0
    for i, v in enumerate(charfct):
        if not in_trigger and v >= on:
            in_trigger = True
            onset = i
            peak = v
        elif in_trigger:
            peak = max(peak, v)
            if v < off:
                triggers.append((onset, float(peak)))
                in_trigger = False
    if in_trigger:
        triggers.append((onset, float(peak)))
    return triggers
