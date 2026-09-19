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
        "chart.js@4.5.1/dist/chart.umd.min.js",
        "48444a82d4edcb5bec0f1965faacdde18d9c17db3063d042abada2f705c9f54a",
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
    "dark.css": (
        "flatpickr@4.6.13/dist/themes/dark.css",
        "47798b76a38ac3a62b1ae658c566e0ed3b4cbcb115173ae620f0db8952f93612",
    ),
    "fonts/space-grotesk-700.woff2": (
        "@fontsource/space-grotesk@5.3.0/files/space-grotesk-latin-700-normal.woff2",
        "35f8aec56cfd5cbfdb03cc68733a54a0b05bb3617ffcd5fd332badc0b045ca55",
    ),
    "fonts/ibm-plex-mono-500.woff2": (
        "@fontsource/ibm-plex-mono@5.3.0/files/ibm-plex-mono-latin-500-normal.woff2",
        "01d285447409c8a588692162439a038b8cbd7871309ee20267b0d2d91c6e8e22",
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
