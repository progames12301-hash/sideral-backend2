from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import os
import re
import struct
import threading
import time
import xml.etree.ElementTree as ET
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import requests

try:
    import numpy as np
    from netCDF4 import Dataset
except ImportError:  # permite /health funcionar antes da instalação das dependências
    np = None
    Dataset = None

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8080"))
BUCKET = "noaa-goes19"
S3_HOST = f"{BUCKET}.s3.amazonaws.com"
CACHE_DIR = Path(os.getenv("SATELLITE_CACHE_DIR", "/tmp/sideral-satellite-cache"))
CATALOG_TTL = 90
MAX_RAW_BYTES = 320 * 1024 * 1024
HEADERS = {"User-Agent": "SideralSatellite/1.0", "Accept": "application/xml"}
PRODUCTS = {
    "ir": {"id": "C13", "channel": 13, "label": "C13 — Clean Longwave IR", "units": "K", "scale": 0.01, "offset": 150.0, "native_km": 2.0, "wavelength_um": 10.3},
    "vis": {"id": "C02", "channel": 2, "label": "C02 — Red Visible", "units": "reflectance", "scale": 0.0001, "offset": 0.0, "native_km": 0.5, "wavelength_um": 0.64},
}
catalog_cache: dict[tuple[str, int], tuple[float, dict]] = {}
catalog_lock = threading.Lock()
download_lock = threading.Lock()
processing_locks: dict[str, threading.Lock] = {}
grid_semaphore = threading.BoundedSemaphore(1)


def product_name(value: str) -> str:
    key = value.lower()
    return {"c13": "ir", "realcada": "ir", "c02": "vis"}.get(key, key)


def config_for(value: str) -> dict:
    key = product_name(value)
    if key not in PRODUCTS:
        raise ValueError("Produto GOES-19 inválido. Use C13/ir ou C02/vis.")
    return PRODUCTS[key]


def allowed_key(key: str, channel: int | None = None) -> bool:
    match = re.fullmatch(
        r"ABI-L2-CMIPF/(20\d{2})/(\d{3})/(\d{2})/"
        r"OR_ABI-L2-CMIPF-M\dC(\d{2})_G19_s\d{14}_e\d{14}_c\d{14}\.nc",
        key,
    )
    return bool(match and (channel is None or int(match.group(4)) == channel))


def key_time(key: str) -> dt.datetime:
    match = re.search(r"_s(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})", key)
    if not match:
        raise ValueError("Timestamp ausente no arquivo GOES-19")
    year, day, hour, minute, second = map(int, match.groups())
    return dt.datetime(year, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(days=day - 1, hours=hour, minutes=minute, seconds=second)


def catalog(product: str, limit: int = 36) -> dict:
    cfg = config_for(product)
    limit = max(1, min(72, int(limit)))
    cache_key = (product_name(product), limit)
    with catalog_lock:
        cached = catalog_cache.get(cache_key)
        if cached and time.monotonic() - cached[0] < CATALOG_TTL:
            return cached[1]
    now = dt.datetime.now(dt.timezone.utc)
    entries: list[dict] = []
    seen: set[str] = set()
    for hours_back in range(8):
        instant = now - dt.timedelta(hours=hours_back)
        prefix = f"ABI-L2-CMIPF/{instant.year}/{instant.timetuple().tm_yday:03d}/{instant.hour:02d}/OR_ABI-L2-CMIPF-M6C{cfg['channel']:02d}_G19"
        response = requests.get(f"https://{S3_HOST}/", params={"list-type": "2", "prefix": prefix, "max-keys": "100"}, headers=HEADERS, timeout=20)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        for node in root.findall("{*}Contents"):
            key = (node.findtext("{*}Key") or "").strip()
            if key in seen or not allowed_key(key, cfg["channel"]):
                continue
            seen.add(key)
            stamp = key_time(key)
            entries.append({"key": key, "timestamp": stamp.isoformat().replace("+00:00", "Z"), "data": stamp.isoformat().replace("+00:00", "Z"), "bytes": int(node.findtext("{*}Size") or 0), "original": f"https://{S3_HOST}/{key}"})
        if len(entries) >= limit:
            break
    entries.sort(key=lambda item: item["timestamp"])
    payload = {"status": True, "satellite": "GOES-19", "provider": "NOAA/NODD", "instrument": "ABI", "product": cfg["label"], "channel": cfg["channel"], "units": cfg["units"], "nativeResolutionKm": cfg["native_km"], "frames": entries[-limit:]}
    with catalog_lock:
        catalog_cache[cache_key] = (time.monotonic(), payload)
    return payload


def local_file(key: str) -> Path:
    if not allowed_key(key):
        raise ValueError("Arquivo GOES-19 inválido")
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    target = CACHE_DIR / Path(key).name
    if target.exists() and target.stat().st_size > 1_000_000:
        return target
    with download_lock:
        if target.exists() and target.stat().st_size > 1_000_000:
            return target
        part = target.with_suffix(".part")
        with requests.get(f"https://{S3_HOST}/{key}", headers={"User-Agent": HEADERS["User-Agent"]}, stream=True, timeout=180) as response:
            response.raise_for_status()
            expected = int(response.headers.get("Content-Length", "0") or 0)
            if expected > MAX_RAW_BYTES:
                raise ValueError("Arquivo bruto excede o limite operacional")
            with part.open("wb") as output:
                for chunk in response.iter_content(1024 * 1024):
                    if chunk:
                        output.write(chunk)
        if part.stat().st_size < 1_000_000:
            part.unlink(missing_ok=True)
            raise ValueError("Arquivo GOES-19 incompleto")
        part.replace(target)
        files = sorted(CACHE_DIR.glob("*.nc"), key=lambda item: item.stat().st_mtime, reverse=True)
        total = 0
        for old in files:
            total += old.stat().st_size
            if total > 450 * 1024 * 1024 and old != target:
                old.unlink(missing_ok=True)
    return target


def project(lon, lat, projection):
    lon = np.radians(lon); lat = np.radians(lat)
    a = float(projection.semi_major_axis); b = float(projection.semi_minor_axis)
    h = float(projection.perspective_point_height) + a
    lon0 = math.radians(float(projection.longitude_of_projection_origin))
    e2 = (a * a - b * b) / (a * a)
    geoc = np.arctan((b * b / a**2) * np.tan(lat))
    radius = b / np.sqrt(1 - e2 * np.cos(geoc) ** 2)
    dl = lon - lon0
    sx = h - radius * np.cos(geoc) * np.cos(dl)
    sy = -radius * np.cos(geoc) * np.sin(dl)
    sz = radius * np.sin(geoc)
    visible = h * (h - sx) >= sy * sy + (a / b) ** 2 * sz * sz + (h - sx) ** 2
    denominator = np.sqrt(sx * sx + sy * sy + sz * sz)
    return np.where(visible, np.arcsin(-sy / denominator), np.nan), np.where(visible, np.arctan2(sz, sx), np.nan)


def grid(product: str, key: str, bbox: list[float], width: int) -> tuple[dict, bytes]:
    if np is None or Dataset is None:
        raise RuntimeError("Dependências numéricas não instaladas")
    cfg = config_for(product)
    if not allowed_key(key, cfg["channel"]):
        raise ValueError("Arquivo não corresponde ao produto")
    west, south, east, north = bbox
    # Evita que várias ampliações simultâneas esgotem a memória do serviço.
    width = max(256, min(1536, int(width)))
    height = max(256, min(2048, int(round(width * (north - south) / max(.01, (east - west) * math.cos(math.radians((south + north) / 2)))))))
    digest = hashlib.sha1(f"{key}|{product_name(product)}|{bbox}|{width}x{height}".encode()).hexdigest()
    cache_path = CACHE_DIR / f"grid-{digest}.bin"
    if cache_path.exists() and cache_path.stat().st_size > 1024:
        body = cache_path.read_bytes(); size = struct.unpack("<I", body[:4])[0]; return json.loads(body[4:4+size]), body
    path = local_file(key)
    with Dataset(path) as ds:
        projection = ds.variables["goes_imager_projection"]
        x_axis = np.asarray(ds.variables["x"][:], dtype=np.float64); y_axis = np.asarray(ds.variables["y"][:], dtype=np.float64)
        variable = ds.variables["CMI"]
        # Lê apenas o recorte da órbita que cobre o bbox pedido. Isso evita
        # acessos aleatórios ao Full Disk durante cada linha da grade.
        edge_lon = np.concatenate((np.linspace(west, east, 181), np.full(181, west), np.full(181, east), np.linspace(west, east, 181)))
        edge_lat = np.concatenate((np.full(181, south), np.linspace(south, north, 181), np.linspace(south, north, 181), np.full(181, north)))
        edge_x, edge_y = project(edge_lon, edge_lat, projection)
        valid_edge = np.isfinite(edge_x) & np.isfinite(edge_y)
        if not valid_edge.any():
            raise ValueError("Região fora da área visível do GOES-19")
        ix_edge = np.rint((edge_x[valid_edge] - x_axis[0]) / (x_axis[-1] - x_axis[0]) * (len(x_axis) - 1)).astype(int)
        iy_edge = np.rint((edge_y[valid_edge] - y_axis[0]) / (y_axis[-1] - y_axis[0]) * (len(y_axis) - 1)).astype(int)
        pad = 4
        x0, x1 = max(0, int(ix_edge.min()) - pad), min(len(x_axis), int(ix_edge.max()) + pad + 1)
        y0, y1 = max(0, int(iy_edge.min()) - pad), min(len(y_axis), int(iy_edge.max()) + pad + 1)
        source_stride = max(1, int(math.ceil(max((x1 - x0) / width, (y1 - y0) / height))))
        if hasattr(variable, "set_auto_maskandscale"):
            variable.set_auto_maskandscale(False)
        crop = np.ma.filled(variable[y0:y1:source_stride, x0:x1:source_stride], 65535).astype(np.float32, copy=False)
        encoded = np.full((height, width), 65535, dtype="<u2")
        lons = west + (np.arange(width) + .5) * (east - west) / width
        m_n = math.asinh(math.tan(math.radians(north))); m_s = math.asinh(math.tan(math.radians(south)))
        for row in range(height):
            my = m_n - (row + .5) * (m_n - m_s) / height
            lat = math.degrees(math.atan(math.sinh(my)))
            px, py = project(lons, np.full(width, lat), projection)
            ix = np.rint(np.nan_to_num((px - x_axis[0]) / (x_axis[-1] - x_axis[0]) * (len(x_axis) - 1), nan=-1)).astype(np.int32)
            iy = np.rint(np.nan_to_num((py - y_axis[0]) / (y_axis[-1] - y_axis[0]) * (len(y_axis) - 1), nan=-1)).astype(np.int32)
            valid = np.isfinite(px) & np.isfinite(py) & (ix >= 0) & (ix < len(x_axis)) & (iy >= 0) & (iy < len(y_axis))
            crop_x = (ix - x0) // source_stride; crop_y = (iy - y0) // source_stride
            valid &= (crop_x >= 0) & (crop_x < crop.shape[1]) & (crop_y >= 0) & (crop_y < crop.shape[0])
            if valid.any():
                values = np.full(width, 65535, dtype=np.float32); values[valid] = crop[crop_y[valid], crop_x[valid]]
                finite = np.isfinite(values) & (values != 65535)
                encoded[row, finite] = np.clip(np.rint(values[finite]), 0, 65534).astype("<u2")
    metadata = {"format": "sideral-grid-u16-v1", "width": width, "height": height, "bbox": bbox, "scale": cfg["scale"], "offset": cfg["offset"], "nodata": 65535, "units": cfg["units"], "channel": cfg["channel"], "nativeResolutionKm": cfg["native_km"], "projection": "EPSG:3857", "bboxCRS": "EPSG:4326", "resampling": "nearest", "observedAt": key_time(key).isoformat().replace("+00:00", "Z")}
    header = json.dumps(metadata, separators=(",", ":")).encode(); body = struct.pack("<I", len(header)) + header + encoded.tobytes(order="C")
    CACHE_DIR.mkdir(parents=True, exist_ok=True); cache_path.write_bytes(body)
    # Mantém somente as grades mais recentes; os NetCDF continuam com a poda
    # própria em local_file().
    cached_grids = sorted(CACHE_DIR.glob("grid-*.bin"), key=lambda item: item.stat().st_mtime, reverse=True)
    for old_grid in cached_grids[6:]:
        old_grid.unlink(missing_ok=True)
    return metadata, body


class Handler(BaseHTTPRequestHandler):
    server_version = "SideralSatellite/1.0"
    def log_message(self, fmt, *args): print("[SAT] " + fmt % args)
    def json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode(); self.send_response(status); self.send_header("Content-Type", "application/json; charset=utf-8"); self.send_header("Access-Control-Allow-Origin", os.getenv("CORS_ORIGIN", "*")); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
    def do_OPTIONS(self): self.send_response(204); self.send_header("Access-Control-Allow-Origin", os.getenv("CORS_ORIGIN", "*")); self.send_header("Access-Control-Allow-Methods", "GET,OPTIONS"); self.send_header("Access-Control-Allow-Headers", "Content-Type"); self.end_headers()
    def do_GET(self):
        parsed = urlparse(self.path); query = parse_qs(parsed.query); path = parsed.path
        try:
            if path in ("/", "/api/health", "/api/satellite/status"):
                frame = None
                try: frame = catalog("ir", 1)["frames"][-1]
                except Exception: pass
                self.json(200, {"status": "ok" if frame or path == "/api/health" else "degraded", "service": "sideral-satellite", "satellite": "GOES-19", "latest_scan": frame.get("timestamp") if frame else None, "cache": {"directory": str(CACHE_DIR)}}); return
            if path == "/api/satellite/products":
                self.json(200, {"satellite": "GOES-19", "instrument": "ABI", "provider": "NOAA/NODD", "products": [{"id": v["id"], "product": k, "label": v["label"], "wavelength_um": v["wavelength_um"], "units": v["units"], "nativeResolutionKm": v["native_km"], "available": True} for k, v in PRODUCTS.items()]}); return
            if path in ("/api/satellite/frames", "/api/goes19/catalog"):
                self.json(200, catalog(query.get("product", ["C13"])[0], int(query.get("limit", ["36"])[0]))); return
            if path == "/api/satellite/latest":
                frames = catalog(query.get("product", ["C13"])[0], 1)["frames"]
                if not frames: self.json(404, {"status": "unavailable"}); return
                frame = frames[-1]; stamp = dt.datetime.fromisoformat(frame["timestamp"].replace("Z", "+00:00")); age = max(0, int((dt.datetime.now(dt.timezone.utc) - stamp).total_seconds() / 60)); product = product_name(query.get("product", ["C13"])[0])
                self.json(200, {"satellite": "GOES-19", "product": product.upper(), "timestamp": frame["timestamp"], "age_minutes": age, "key": frame["key"], "image_url": f"/api/satellite/image?product={product}&key={frame['key']}", "status": "ok"}); return
            if path in ("/api/satellite/image", "/api/goes19/grid", "/api/satellite/raw-grid"):
                product = product_name(query.get("product", ["C13"])[0]); key = unquote(query.get("key", [""])[0]); bbox = [float(v) for v in query.get("bbox", ["-90,-60,-30,15"])[0].split(",")]
                # Apenas um processamento pesado por vez. Requests adicionais
                # aguardam brevemente em vez de abrirem várias grades na RAM.
                if not grid_semaphore.acquire(timeout=35):
                    self.json(429, {"status": "busy", "error": "O processamento do satélite está ocupado; tente novamente em alguns segundos."}); return
                try:
                    _, body = grid(product, key, bbox, int(query.get("width", ["1024"])[0]))
                finally:
                    grid_semaphore.release()
                self.send_response(200); self.send_header("Content-Type", "application/vnd.sideral.raster+octet-stream"); self.send_header("Access-Control-Allow-Origin", os.getenv("CORS_ORIGIN", "*")); self.send_header("Cache-Control", "public, max-age=86400, immutable"); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body); return
            if path in ("/api/goes19/file", "/api/satellite/original"):
                file = local_file(unquote(query.get("key", [""])[0])); self.send_response(200); self.send_header("Content-Type", "application/x-netcdf"); self.send_header("Access-Control-Allow-Origin", os.getenv("CORS_ORIGIN", "*")); self.send_header("Content-Disposition", f'attachment; filename="{file.name}"'); self.send_header("Content-Length", str(file.stat().st_size)); self.end_headers(); self.wfile.write(file.read_bytes()); return
            if path == "/api/satellite/value":
                raise NotImplementedError("Use a grade para inspeção; endpoint de valor será ativado após validação do primeiro deployment.")
            self.json(404, {"error": "Endpoint não encontrado"})
        except Exception as exc:
            self.json(502, {"status": "error", "error": str(exc)})


if __name__ == "__main__":
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[SAT] Sideral Satellite listening on {HOST}:{PORT}")
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
