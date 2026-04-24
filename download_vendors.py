#!/usr/bin/env python3
"""Download frontend vendor libraries for offline use."""

import json
import urllib.error
import urllib.request
from pathlib import Path

VENDOR_DIR = Path(__file__).parent / "static" / "vendor"
VENDOR_DIR.mkdir(parents=True, exist_ok=True)


def get_latest_github_release(repo: str) -> str:
    url = f"https://api.github.com/repos/{repo}/releases/latest"
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode())
            return data["tag_name"].lstrip("v")
    except urllib.error.HTTPError as e:
        if e.code == 403:
            raise RuntimeError(
                "GitHub API rate limit exceeded. "
                "Please wait a few minutes or provide a GITHUB_TOKEN."
            ) from e
        if e.code == 404:
            raise RuntimeError(f"Repository {repo} not found on GitHub.") from e
        raise RuntimeError(f"GitHub API error: {e.code}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"Network error while contacting GitHub API: {e.reason}"
        ) from e


def download_file(url: str, dest: Path) -> None:
    print(f"Downloading {url} -> {dest.name}")
    try:
        urllib.request.urlretrieve(url, dest)
    except urllib.error.URLError as e:
        raise RuntimeError(f"Failed to download {url}: {e.reason}") from e
    print(f"✓ Downloaded {dest.name}")


def download_chartjs() -> None:
    version = get_latest_github_release("chartjs/Chart.js")
    print(f"Latest Chart.js version: {version}")

    url = f"https://cdn.jsdelivr.net/npm/chart.js@{version}/dist/chart.umd.min.js"
    dest = VENDOR_DIR / "chart.min.js"
    download_file(url, dest)

    zoom_version = get_latest_github_release("chartjs/chartjs-plugin-zoom")
    print(f"Latest chartjs-plugin-zoom version: {zoom_version}")
    zoom_url = (
        f"https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom@{zoom_version}"
        f"/dist/chartjs-plugin-zoom.min.js"
    )
    zoom_dest = VENDOR_DIR / "chartjs-plugin-zoom.min.js"
    download_file(zoom_url, zoom_dest)


def download_flatpickr() -> None:
    version = get_latest_github_release("flatpickr/flatpickr")
    print(f"Latest Flatpickr version: {version}")

    base_url = f"https://cdn.jsdelivr.net/npm/flatpickr@{version}/dist"

    files = {
        "flatpickr.min.js": f"{base_url}/flatpickr.min.js",
        "flatpickr.min.css": f"{base_url}/flatpickr.min.css",
        "dark.css": f"{base_url}/themes/dark.css",
    }

    for filename, url in files.items():
        dest = VENDOR_DIR / filename
        download_file(url, dest)


def main() -> None:
    print("Downloading vendor libraries...")
    try:
        download_chartjs()
        download_flatpickr()
        print("Done.")
    except Exception as e:
        print(f"Error: {e}")
        raise SystemExit(1) from e


if __name__ == "__main__":
    main()
