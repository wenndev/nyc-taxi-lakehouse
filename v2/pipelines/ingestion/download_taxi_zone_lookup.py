# Resumo:
# - Baixa o CSV oficial Taxi Zone Lookup da NYC TLC.
# - Essa fonte pequena enriquece a dim_localizacao na Gold.

from __future__ import annotations

import argparse
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlretrieve

from v2.config.paths import taxi_zone_lookup_raw_dir
from v2.config.sources import TAXI_ZONE_LOOKUP_FILENAME, taxi_zone_lookup_url


def main() -> int:
    parser = argparse.ArgumentParser(description="Download NYC TLC Taxi Zone Lookup")
    parser.add_argument("--output", default=None)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    output_dir = resolve_output_dir(args.output, taxi_zone_lookup_raw_dir())
    return download_taxi_zone_lookup(
        output_dir=output_dir,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
    )


def download_taxi_zone_lookup(
    output_dir: Path,
    overwrite: bool = False,
    dry_run: bool = False,
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)

    url = taxi_zone_lookup_url()
    destination = output_dir / TAXI_ZONE_LOOKUP_FILENAME

    if dry_run:
        print(f"{url} -> {destination}")
        return 0

    if destination.exists() and not overwrite:
        print(f"SKIP existing: {destination}")
        print(f"Taxi Zone Lookup raw file ready at: {destination}")
        return 0

    print(f"Downloading: {url}")
    try:
        download_file(url, destination)
    except (HTTPError, URLError) as exc:
        print(f"FAILED: {url}")
        print(exc)
        return 1

    print(f"Saved: {destination}")
    print(f"Taxi Zone Lookup raw file ready at: {destination}")
    return 0


def download_file(url: str, destination: Path) -> None:
    temporary_destination = destination.with_suffix(f"{destination.suffix}.part")

    if temporary_destination.exists():
        temporary_destination.unlink()

    try:
        urlretrieve(url, temporary_destination)
        temporary_destination.replace(destination)
    except Exception:
        if temporary_destination.exists():
            temporary_destination.unlink()
        raise


def resolve_output_dir(output: str | None, default_output: Path) -> Path:
    if not output:
        return default_output

    if output.startswith("abfss://"):
        raise ValueError(
            "Direct abfss:// output is not supported by this raw Python downloader. "
            "On Databricks, use a filesystem path backed by ADLS, such as /Volumes/... "
            "or /dbfs/mnt/..."
        )

    return Path(normalize_databricks_path(output))


def normalize_databricks_path(path: str) -> str:
    if path.startswith("dbfs:/"):
        return f"/dbfs/{path.removeprefix('dbfs:/').lstrip('/')}"

    return path


if __name__ == "__main__":
    raise SystemExit(main())
