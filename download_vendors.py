#!/usr/bin/env python3
"""Download pinned frontend vendor libraries for offline use.

Versions are pinned and every file is checked against a SHA-256 checksum, so a
build always ships the same assets and a changed or compromised CDN file cannot
slip in. Update the version and hash together when bumping.
"""

import hashlib
import urllib.error
import urllib.request
from pathlib import Path

CDN = "https://cdn.jsdelivr.net/npm"

# dest (relative to VENDOR_DIR) -> (path on the CDN, sha256)
VENDOR_FILES = {
    "chart.min.js": (
        "chart.js@4.6.1/dist/chart.umd.min.js",
        "87364cc2b2a76a6684929e7d6b80b18a897794db38400caff1fcf1791c6925d2",
    ),
    "chartjs-plugin-zoom.min.js": (
        "chartjs-plugin-zoom@2.2.0/dist/chartjs-plugin-zoom.min.js",
        "e4a088e5bab93be6ee47c939eeb9ebaa80e0b39156d4bdfd1af9c844be81b6c4",
    ),
    "flatpickr.min.js": (
        "flatpickr@4.6.13/dist/flatpickr.min.js",
        "1eeab1cb779471a0b0aaa93dd91c2eb1aa537d696f01ab05ea9dabc55e8525a1",
    ),
    "flatpickr.min.css": (
        "flatpickr@4.6.13/dist/flatpickr.min.css",
        "1b34a42552c96f10e4dfaaa4a367276b03868aacff63c1ac42ffe331352bc754",
    ),
}

VENDOR_DIR = Path(__file__).parent / "static" / "vendor"
VENDOR_DIR.mkdir(parents=True, exist_ok=True)


def download_file(url: str, dest: Path, expected_sha256: str) -> None:
    print(f"Downloading {url} -> {dest.name}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        urllib.request.urlretrieve(url, dest)
    except urllib.error.URLError as e:
        raise RuntimeError(f"Failed to download {url}: {e.reason}") from e

    actual = hashlib.sha256(dest.read_bytes()).hexdigest()
    if actual != expected_sha256:
        dest.unlink(missing_ok=True)
        raise RuntimeError(f"Checksum mismatch for {url}: expected {expected_sha256}, got {actual}")
    print(f"Downloaded {dest.name}")


def main() -> None:
    print("Downloading vendor libraries...")
    try:
        for dest_rel, (remote, sha256) in VENDOR_FILES.items():
            download_file(f"{CDN}/{remote}", VENDOR_DIR / dest_rel, sha256)
        print("Done.")
    except Exception as e:
        print(f"Error: {e}")
        raise SystemExit(1) from e


if __name__ == "__main__":
    main()
