"""Shared waveform-frame model and PKI signing helpers.

A *frame* is one contiguous block of samples from one station/channel — the
OpenIDC analogue of a CD-1.1 data frame. Real IMS stations cryptographically
sign their data frames so the IDC can prove the data is authentic and untampered;
we model that here with RSA signatures over the canonical frame bytes.

Both the ingestion (producer) and station-processing (consumer) services import
this module so the wire format and the signing scheme stay in lockstep.
"""
from __future__ import annotations

import base64
import binascii
import json
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

STA_RE = re.compile(r"^[A-Z0-9]{1,6}$")
CHAN_RE = re.compile(r"^[A-Z0-9]{1,8}$")
MAX_SAMPLES_PER_FRAME = 200_000


# --------------------------------------------------------------------------- #
# Frame model
# --------------------------------------------------------------------------- #
@dataclass
class Frame:
    """One waveform segment plus the metadata a detector needs."""

    sta: str                 # station code, e.g. "ARCES"
    chan: str                # channel code, e.g. "BHZ"
    start_time: float        # epoch seconds of the first sample
    sample_rate: float       # Hz
    samples: List[float]     # the waveform itself

    @property
    def end_time(self) -> float:
        return self.start_time + (len(self.samples) - 1) / self.sample_rate

    @property
    def jdate(self) -> int:
        """Julian date (yyyyddd) of the first sample — CSS 3.0 convention."""
        dt = datetime.fromtimestamp(self.start_time, tz=timezone.utc)
        return dt.year * 1000 + dt.timetuple().tm_yday

    # --- serialisation ----------------------------------------------------- #
    def header(self) -> dict:
        return {
            "sta": self.sta,
            "chan": self.chan,
            "start_time": self.start_time,
            "sample_rate": self.sample_rate,
            "nsamp": len(self.samples),
        }

    def signing_bytes(self) -> bytes:
        """Canonical bytes that get signed/verified (header + samples)."""
        body = {"header": self.header(), "samples": self.samples}
        return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()

    def to_wire(self, signature: Optional[str] = None) -> bytes:
        payload = {"header": self.header(), "samples": self.samples}
        if signature is not None:
            payload["sig"] = signature
        return json.dumps(payload, separators=(",", ":")).encode()

    @classmethod
    def from_wire(cls, raw: bytes) -> "tuple[Frame, Optional[str]]":
        try:
            obj = json.loads(raw)
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("frame payload is not valid JSON") from exc
        if not isinstance(obj, dict):
            raise ValueError("frame payload must be a JSON object")

        h = obj.get("header")
        if not isinstance(h, dict):
            raise ValueError("frame header must be an object")

        samples = obj.get("samples")
        if not isinstance(samples, list):
            raise ValueError("frame samples must be an array")
        if not samples:
            raise ValueError("frame must contain at least one sample")
        if len(samples) > MAX_SAMPLES_PER_FRAME:
            raise ValueError(f"frame exceeds {MAX_SAMPLES_PER_FRAME} samples")

        sta = h.get("sta")
        chan = h.get("chan")
        if not isinstance(sta, str) or not STA_RE.fullmatch(sta):
            raise ValueError("station code must match ^[A-Z0-9]{1,6}$")
        if not isinstance(chan, str) or not CHAN_RE.fullmatch(chan):
            raise ValueError("channel code must match ^[A-Z0-9]{1,8}$")

        start_raw = h.get("start_time")
        rate_raw = h.get("sample_rate")
        if isinstance(start_raw, bool) or isinstance(rate_raw, bool):
            raise ValueError("frame start_time and sample_rate must be numeric")
        try:
            start_time = float(start_raw)
            sample_rate = float(rate_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("frame start_time and sample_rate must be numeric") from exc
        if not math.isfinite(start_time) or start_time <= 0:
            raise ValueError("frame start_time must be a positive finite epoch time")
        if not math.isfinite(sample_rate) or sample_rate <= 0:
            raise ValueError("frame sample_rate must be positive and finite")

        nsamp = h.get("nsamp")
        if nsamp is not None:
            if isinstance(nsamp, bool) or not isinstance(nsamp, int):
                raise ValueError("frame nsamp must be an integer")
            if nsamp != len(samples):
                raise ValueError("frame nsamp does not match sample count")

        try:
            if any(isinstance(x, bool) for x in samples):
                raise ValueError
            clean_samples = [float(x) for x in samples]
        except (TypeError, ValueError) as exc:
            raise ValueError("frame samples must be numeric") from exc
        if not all(math.isfinite(x) for x in clean_samples):
            raise ValueError("frame samples must be finite")

        sig = obj.get("sig")
        if sig is not None and not isinstance(sig, str):
            raise ValueError("frame signature must be a string")

        frame = cls(
            sta=sta,
            chan=chan,
            start_time=start_time,
            sample_rate=sample_rate,
            samples=clean_samples,
        )
        return frame, sig


# --------------------------------------------------------------------------- #
# PKI: RSA key management + sign / verify
# --------------------------------------------------------------------------- #
PRIVATE_KEY_FILE = "private.pem"
PUBLIC_KEY_FILE = "public.pem"


def _write_public_key(key: rsa.RSAPrivateKey, pub_path: str) -> None:
    expected = _public_key_bytes(key)
    with open(pub_path, "wb") as fh:
        fh.write(expected)
    os.chmod(pub_path, 0o644)


def _public_key_bytes(key: rsa.RSAPrivateKey) -> bytes:
    return key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )


def ensure_keypair(pki_dir: str) -> rsa.RSAPrivateKey:
    """Load the producer keypair, generating one on first run.

    The private key stays with the producer; the public key is written to the
    shared PKI directory so the consumer can verify signatures — exactly the
    trust model of station-signed IMS data.
    """
    os.makedirs(pki_dir, exist_ok=True)
    priv_path = os.path.join(pki_dir, PRIVATE_KEY_FILE)
    pub_path = os.path.join(pki_dir, PUBLIC_KEY_FILE)

    if os.path.exists(priv_path):
        with open(priv_path, "rb") as fh:
            key = serialization.load_pem_private_key(fh.read(), password=None)
        try:
            os.chmod(priv_path, 0o600)
        except PermissionError:
            pass
        public_matches = False
        if os.path.exists(pub_path):
            with open(pub_path, "rb") as fh:
                public_matches = fh.read() == _public_key_bytes(key)
        if not public_matches:
            _write_public_key(key, pub_path)
        return key

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    with open(priv_path, "wb") as fh:
        fh.write(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
    os.chmod(priv_path, 0o600)
    _write_public_key(key, pub_path)
    return key


def wait_for_public_key(pki_dir: str, timeout: float = 60.0):
    """Block until the producer has published its public key (consumer side)."""
    pub_path = os.path.join(pki_dir, PUBLIC_KEY_FILE)
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(pub_path):
            with open(pub_path, "rb") as fh:
                return serialization.load_pem_public_key(fh.read())
        time.sleep(1.0)
    raise TimeoutError(f"public key not found in {pki_dir} after {timeout}s")


def sign(frame: Frame, private_key: rsa.RSAPrivateKey) -> str:
    sig = private_key.sign(
        frame.signing_bytes(),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode()


def verify(frame: Frame, signature: str, public_key) -> bool:
    from cryptography.exceptions import InvalidSignature

    try:
        public_key.verify(
            base64.b64decode(signature, validate=True),
            frame.signing_bytes(),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
            hashes.SHA256(),
        )
        return True
    except (InvalidSignature, binascii.Error, TypeError, ValueError):
        return False
