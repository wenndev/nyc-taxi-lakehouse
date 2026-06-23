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

    output_dir = Path(args.output) if args.output else nyc_tlc_raw_dir(args.year)
    output_dir.mkdir(parents=True, exist_ok=True)

    for month in range(args.start_month, args.end_month + 1):
        url = nyc_tlc_yellow_url(args.year, month)
        filename = nyc_tlc_yellow_filename(args.year, month)
        destination = output_dir / filename

        if args.dry_run:
            print(f"{url} -> {destination}")
            continue

        if destination.exists() and not args.overwrite:
            print(f"SKIP existing: {destination}")
            continue

        print(f"Downloading: {url}")
        try:
            urlretrieve(url, destination)
        except (HTTPError, URLError) as exc:
            print(f"FAILED: {url}")
            print(exc)
            return 1

        print(f"Saved: {destination}")

    print(f"NYC TLC raw files ready at: {output_dir}")
    return 0


def validate_month_range(start_month: int, end_month: int) -> None:
    if not 1 <= start_month <= 12:
        raise ValueError(f"Mes inicial invalido: {start_month}")
    if not 1 <= end_month <= 12:
        raise ValueError(f"Mes final invalido: {end_month}")
    if start_month > end_month:
        raise ValueError("Mes inicial nao pode ser maior que mes final")


if __name__ == "__main__":
    raise SystemExit(main())

