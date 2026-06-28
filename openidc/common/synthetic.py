"""Synthetic seismic waveform helpers.

Shared by the producer, the on-demand injector, and the dashboard so that the
definition of "what a seismic event looks like" lives in exactly one place.
"""
from __future__ import annotations

import numpy as np


def noise(n: int, rng=None, sigma: float = 1.0) -> np.ndarray:
    """Background seismic noise: Gaussian samples around zero."""
    rng = rng if rng is not None else np.random.default_rng()
    return rng.normal(0.0, sigma, int(n))


def add_event(
    samples: np.ndarray,
    sample_rate: float,
    amp: float,
    freq: float,
    onset_frac: float = 0.4,
    duration: float = 2.0,
):
    """Add a damped-sinusoid wavelet (a simulated phase arrival) into `samples`.

    Returns (samples, onset_sample). `onset_frac` places the onset as a fraction
    of the frame length; `amp` should sit well above the noise floor (~1.0) for
    the detector to trigger.
    """
    n = len(samples)
    onset = int(onset_frac * n)
    length = min(int(duration * sample_rate), n - onset)
    if length > 0:
        t = np.arange(length) / sample_rate
        wavelet = amp * np.sin(2 * np.pi * freq * t) * np.exp(-3.0 * t)
        samples[onset:onset + length] = samples[onset:onset + length] + wavelet
    return samples, onset
