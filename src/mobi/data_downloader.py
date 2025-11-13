"""
Module for downloading historic trip data from Mobi by Rogers website.

This module provides functions to scrape and download CSV files containing
historic bike share trip data from https://www.mobibikes.ca/en/system-data
"""

import re
import shutil
import zipfile
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


class MobiDataDownloaderError(Exception):
    """Base exception for Mobi data downloader errors."""

    pass


def get_available_data_files(
    base_url: str = "https://www.mobibikes.ca/en/system-data",
    timeout: int = 30,
) -> list[dict]:
    """
    Scrape the Mobi system data page to find all available CSV download links.

    Args:
        base_url: URL of the Mobi system data page
        timeout: Request timeout in seconds

    Returns:
        List of dicts containing file metadata (url, month, year, filename)

    Raises:
        MobiDataDownloaderError: If the page cannot be accessed or parsed
    """
    try:
        response = requests.get(base_url, timeout=timeout)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise MobiDataDownloaderError(f"Failed to fetch data page: {e}")

    try:
        soup = BeautifulSoup(response.content, "html.parser")
    except Exception as e:
        raise MobiDataDownloaderError(f"Failed to parse HTML: {e}")

    # Find all links that look like data download links
    data_files = []
    links = soup.find_all("a", href=True)

    for link in links:
        href = link["href"]
        link_text = link.get_text(strip=True)

        # Look for Google Drive links or direct CSV/ZIP links
        is_gdrive = "drive.google.com" in href
        is_csv_or_zip = ".csv" in href.lower() or ".zip" in href.lower()

        if is_gdrive or is_csv_or_zip:
            # Try to parse month and year from link text
            month_year_match = re.search(
                r"(January|February|March|April|May|June|July|August|"
                r"September|October|November|December)\s+(\d{4})",
                link_text,
                re.IGNORECASE,
            )

            if month_year_match:
                month = month_year_match.group(1)
                year = month_year_match.group(2)
            else:
                # Try to extract from href
                month_year_match = re.search(r"(\d{4})[-_]?(\d{2})", href)
                if month_year_match:
                    year = month_year_match.group(1)
                    month_num = month_year_match.group(2)
                    month_names = [
                        "January",
                        "February",
                        "March",
                        "April",
                        "May",
                        "June",
                        "July",
                        "August",
                        "September",
                        "October",
                        "November",
                        "December",
                    ]
                    month = month_names[int(month_num) - 1]
                else:
                    month = "Unknown"
                    year = "Unknown"

            # Convert Google Drive view link to download link
            if is_gdrive and "/file/d/" in href:
                # Extract file ID from Google Drive link
                match = re.search(r"/file/d/([^/]+)", href)
                if match:
                    file_id = match.group(1)
                    download_url = (
                        f"https://drive.google.com/uc?export=download&id={file_id}"
                    )
                else:
                    download_url = href
            else:
                # Make absolute URL for direct links
                download_url = urljoin(base_url, href)

            # Create filename
            filename = f"mobi_{year}_{month}.csv".replace(" ", "_")

            data_files.append(
                {
                    "url": download_url,
                    "month": month,
                    "year": year,
                    "filename": filename,
                    "link_text": link_text,
                }
            )

    return data_files


def download_file(
    url: str,
    output_path: Path,
    timeout: int = 120,
    chunk_size: int = 8192,
) -> Path:
    """
    Download a file from a URL to the specified output path.

    Args:
        url: URL of the file to download
        output_path: Local path where the file should be saved
        timeout: Request timeout in seconds
        chunk_size: Size of chunks to download at a time (bytes)

    Returns:
        Path to the downloaded file

    Raises:
        MobiDataDownloaderError: If the download fails
    """
    try:
        response = requests.get(url, timeout=timeout, stream=True)
        response.raise_for_status()

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)

        return output_path

    except requests.exceptions.RequestException as e:
        raise MobiDataDownloaderError(f"Failed to download {url}: {e}")
    except IOError as e:
        raise MobiDataDownloaderError(f"Failed to save file to {output_path}: {e}")


def download_all_trip_data(
    output_dir: Path,
    base_url: str = "https://www.mobibikes.ca/en/system-data",
    overwrite: bool = False,
) -> list[Path]:
    """
    Download all available historic trip data CSV files.

    Args:
        output_dir: Directory where files should be saved
        base_url: URL of the Mobi system data page
        overwrite: Whether to overwrite existing files

    Returns:
        List of paths to downloaded files

    Raises:
        MobiDataDownloaderError: If download fails
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Finding available data files from {base_url}...")
    data_files = get_available_data_files(base_url)
    print(f"Found {len(data_files)} data file(s)")

    downloaded_files = []

    for i, file_info in enumerate(data_files, 1):
        filename = file_info["filename"]
        output_path = output_dir / filename

        if output_path.exists() and not overwrite:
            print(f"[{i}/{len(data_files)}] Skipping {filename} (already exists)")
            downloaded_files.append(output_path)
            continue

        print(
            f"[{i}/{len(data_files)}] Downloading {filename} "
            f"({file_info['month']} {file_info['year']})..."
        )

        try:
            downloaded_path = download_file(file_info["url"], output_path)
            downloaded_files.append(downloaded_path)
            print(f"  ✓ Saved to {downloaded_path}")
        except MobiDataDownloaderError as e:
            print(f"  ✗ Failed: {e}")
            continue

    print(f"\nDownloaded {len(downloaded_files)} file(s) to {output_dir}")
    return downloaded_files


def restore_from_backup(volume_root: Path, bundle_path: Path) -> dict[str, Path]:
    """
    Restore bundled assets (trip data and mobi site content) into the target volume.

    This function unpacks ``data.zip`` into a temporary directory, moves the
    ``trip_data`` and ``mobi_site`` directories to the top level of ``volume_root``,
    and overwrites any existing directories with the bundled copies.

    Args:
        volume_root: Unity Catalog volume root (e.g., ``/Volumes/.../raw_data``).
        bundle_path: Path to the project-supplied ``data.zip`` archive.

    Returns:
        Mapping of restored directory names to their destination paths.

    Raises:
        FileNotFoundError: If ``bundle_path`` does not exist.
        RuntimeError: If required directories are missing from the archive.
    """
    from tempfile import TemporaryDirectory

    volume_root = Path(volume_root)
    bundle_path = Path(bundle_path)

    if not bundle_path.exists():
        raise FileNotFoundError(f"Fallback bundle not found at {bundle_path}")

    volume_root.mkdir(parents=True, exist_ok=True)

    restored: dict[str, Path] = {}
    with TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        with zipfile.ZipFile(bundle_path, "r") as archive:
            archive.extractall(tmpdir_path)

        base_dir = tmpdir_path / "data"
        if not base_dir.exists():
            base_dir = tmpdir_path

        for dirname in ("trip_data", "mobi_site"):
            src_dir = base_dir / dirname
            if not src_dir.exists():
                raise RuntimeError(
                    f"Fallback archive is missing the '{dirname}' directory"
                )

            dst_dir = volume_root / dirname
            if dst_dir.exists():
                shutil.rmtree(dst_dir)

            shutil.move(str(src_dir), str(dst_dir))
            restored[dirname] = dst_dir

        leftover_data_dir = volume_root / "data"
        if leftover_data_dir.exists() and leftover_data_dir.is_dir():
            try:
                leftover_data_dir.rmdir()
            except OSError:
                shutil.rmtree(leftover_data_dir, ignore_errors=True)

    return restored

