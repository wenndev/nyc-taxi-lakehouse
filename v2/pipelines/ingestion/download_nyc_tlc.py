from __future__ import annotations

import argparse
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlretrieve

from v2.config.paths import nyc_tlc_raw_dir
from v2.config.sources import nyc_tlc_yellow_filename, nyc_tlc_yellow_url


def main() -> int:
    parser = argparse.ArgumentParser(description="Download NYC TLC yellow trip data")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--start-month", type=int, default=1)
    parser.add_argument("--end-month", type=int, default=12)
    parser.add_argument("--output", default=None)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    validate_month_range(args.start_month, args.end_month)

    output_dir = resolve_output_dir(args.output, nyc_tlc_raw_dir(args.year))

    return download_files(
        year=args.year,
        start_month=args.start_month,
        end_month=args.end_month,
        output_dir=output_dir,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
    )


def validate_month_range(start_month: int, end_month: int) -> None:
    if not 1 <= start_month <= 12:
        raise ValueError(f"Mes inicial invalido: {start_month}")
    if not 1 <= end_month <= 12:
        raise ValueError(f"Mes final invalido: {end_month}")
    if start_month > end_month:
        raise ValueError("Mes inicial nao pode ser maior que mes final")


def download_files(
    year: int,
    start_month: int,
    end_month: int,
    output_dir: Path,
    overwrite: bool = False,
    dry_run: bool = False,
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)

    for month in range(start_month, end_month + 1):
        url = nyc_tlc_yellow_url(year, month)
        filename = nyc_tlc_yellow_filename(year, month)
        destination = output_dir / filename

        if dry_run:
            print(f"{url} -> {destination}")
            continue

        if destination.exists() and not overwrite:
            print(f"SKIP existing: {destination}")
            continue

        print(f"Downloading: {url}")
        try:
            download_file(url, destination)
        except (HTTPError, URLError) as exc:
            print(f"FAILED: {url}")
            print(exc)
            return 1

        print(f"Saved: {destination}")

    print(f"NYC TLC raw files ready at: {output_dir}")
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
