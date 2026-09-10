# Resumo:
# - Baixa dados climaticos da NOAA usando a API CDO.
# - Resolve a paginacao por offset para nao ficar limitado a poucos registros.

from __future__ import annotations

import argparse
import json
import os
import random
import time
from datetime import date
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from v2.config.paths import noaa_raw_dir
from v2.config.sources import (
    NOAA_CDO_DATA_URL,
    NOAA_GHCND_DATASET_ID,
    NOAA_GHCND_NYC_STORAGE_ID,
    NOAA_NYC_LOCATION_ID,
)


DEFAULT_DATATYPES = ["PRCP", "TMAX", "TMIN", "SNOW", "SNWD"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Download NOAA CDO weather data")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--datasetid", default=NOAA_GHCND_DATASET_ID)
    parser.add_argument("--datatypeid", action="append", default=None)
    parser.add_argument("--stationid", action="append", default=None)
    parser.add_argument("--locationid", action="append", default=None)
    parser.add_argument("--units", default="metric", choices=["metric", "standard"])
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--initial-offset", type=int, default=1)
    parser.add_argument("--output", default=None)
    parser.add_argument("--storage-datasetid", default=None)
    parser.add_argument("--token", default=None)
    parser.add_argument("--token-env", default="NOAA_TOKEN")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-global", action="store_true")
    parser.add_argument("--sleep-seconds", type=float, default=0.25)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--retry-jitter-seconds", type=float, default=0.0)
    args = parser.parse_args()

    default_to_nyc_location(args)

    start_date, end_date = resolve_date_range(args.year, args.start_date, args.end_date)
    datatypes = args.datatypeid or DEFAULT_DATATYPES
    storage_datasetid = resolve_storage_datasetid(
        api_datasetid=args.datasetid,
        stationids=args.stationid,
        locationids=args.locationid,
        storage_datasetid=args.storage_datasetid,
    )
    output_dir = resolve_output_dir(
        args.output,
        noaa_raw_dir(args.year, storage_datasetid),
    )

    validate_args(args, datatypes)

    base_params = build_base_params(
        datasetid=args.datasetid,
        datatypes=datatypes,
        stationids=args.stationid,
        locationids=args.locationid,
        start_date=start_date,
        end_date=end_date,
        units=args.units,
    )

    first_page_url = build_url(base_params, limit=args.limit, offset=args.initial_offset)
    print(f"Output : {output_dir}")
    print(f"Dataset: {args.datasetid}")
    print(f"Storage: {storage_datasetid}")
    print(f"Dates  : {start_date} -> {end_date}")
    print(f"Types  : {', '.join(datatypes)}")
    if args.stationid:
        print(f"Stations : {', '.join(args.stationid)}")
    if args.locationid:
        print(f"Locations: {', '.join(args.locationid)}")
    print(f"First page: {first_page_url}")

    if args.dry_run:
        print("Dry run only. No NOAA request was executed.")
        return 0

    token = resolve_token(args.token, args.token_env, args.env_file)
    if not token:
        print(
            f"NOAA token not found. Set {args.token_env}, pass --token, "
            f"or add it to {args.env_file}."
        )
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)
    return download_pages(
        token=token,
        base_params=base_params,
        output_dir=output_dir,
        limit=args.limit,
        initial_offset=args.initial_offset,
        storage_datasetid=storage_datasetid,
        overwrite=args.overwrite,
        sleep_seconds=args.sleep_seconds,
        max_retries=args.max_retries,
        retry_jitter_seconds=args.retry_jitter_seconds,
    )


def resolve_date_range(
    year: int,
    start_date: str | None,
    end_date: str | None,
) -> tuple[str, str]:
    return start_date or f"{year}-01-01", end_date or f"{year}-12-31"


def default_to_nyc_location(args: argparse.Namespace) -> None:
    if args.stationid or args.locationid or args.allow_global:
        return

    args.locationid = [NOAA_NYC_LOCATION_ID]


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


def resolve_storage_datasetid(
    api_datasetid: str,
    stationids: list[str] | None,
    locationids: list[str] | None,
    storage_datasetid: str | None,
) -> str:
    if storage_datasetid:
        return storage_datasetid

    if not stationids and locationids and NOAA_NYC_LOCATION_ID in locationids:
        return NOAA_GHCND_NYC_STORAGE_ID

    return api_datasetid


def normalize_databricks_path(path: str) -> str:
    if path.startswith("dbfs:/"):
        return f"/dbfs/{path.removeprefix('dbfs:/').lstrip('/')}"

    return path


def validate_args(args: argparse.Namespace, datatypes: list[str]) -> None:
    validate_iso_date(args.start_date) if args.start_date else None
    validate_iso_date(args.end_date) if args.end_date else None

    if not 1 <= args.limit <= 1000:
        raise ValueError("NOAA CDO limit must be between 1 and 1000")

    if args.initial_offset < 1:
        raise ValueError("Initial offset must be >= 1")

    if args.sleep_seconds < 0:
        raise ValueError("Sleep seconds must be >= 0")

    if args.max_retries < 1:
        raise ValueError("Max retries must be >= 1")

    if args.retry_jitter_seconds < 0:
        raise ValueError("Retry jitter seconds must be >= 0")

    if not datatypes:
        raise ValueError("At least one datatypeid is required")

    has_scope = bool(args.stationid or args.locationid)
    if not has_scope and not args.allow_global:
        raise ValueError(
            "Pass at least one --stationid or --locationid. "
            "Use --allow-global only if you really want an unscoped NOAA request."
        )


def validate_iso_date(value: str) -> None:
    date.fromisoformat(value)


def resolve_token(token: str | None, token_env: str, env_file: str | None) -> str | None:
    return token or os.environ.get(token_env) or read_env_file_value(env_file, token_env)


def read_env_file_value(env_file: str | None, key: str) -> str | None:
    if not env_file:
        return None

    path = Path(env_file)
    if not path.exists():
        return None

    with path.open("r", encoding="utf-8") as file:
        for raw_line in file:
            line = raw_line.strip()

            if not line or line.startswith("#") or "=" not in line:
                continue

            env_key, env_value = line.split("=", 1)
            env_key = env_key.removeprefix("export ").strip()

            if env_key != key:
                continue

            return clean_env_value(env_value)

    return None


def clean_env_value(value: str) -> str:
    value = value.strip()

    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]

    return value


def build_base_params(
    datasetid: str,
    datatypes: list[str],
    stationids: list[str] | None,
    locationids: list[str] | None,
    start_date: str,
    end_date: str,
    units: str,
) -> list[tuple[str, str]]:
    params = [
        ("datasetid", datasetid),
        ("startdate", start_date),
        ("enddate", end_date),
        ("units", units),
    ]

    for datatype in datatypes:
        params.append(("datatypeid", datatype))

    for stationid in stationids or []:
        params.append(("stationid", stationid))

    for locationid in locationids or []:
        params.append(("locationid", locationid))

    return params


def build_url(base_params: list[tuple[str, str]], limit: int, offset: int) -> str:
    params = [*base_params, ("limit", str(limit)), ("offset", str(offset))]
    return f"{NOAA_CDO_DATA_URL}?{urlencode(params)}"


def download_pages(
    token: str,
    base_params: list[tuple[str, str]],
    output_dir: Path,
    limit: int,
    initial_offset: int,
    storage_datasetid: str,
    overwrite: bool,
    sleep_seconds: float,
    max_retries: int,
    retry_jitter_seconds: float,
) -> int:
    offset = initial_offset
    page_number = 1
    expected_count: int | None = None
    downloaded_results = 0
    pages = 0
    status = "failed"
    download_complete = False
    existing_pages = set(output_dir.glob("page_*_offset_*.json"))
    if not overwrite:
        validate_resume_manifest(
            output_dir, base_params, limit, initial_offset, storage_datasetid
        )

    def save_progress(current_status: str, complete: bool = False) -> None:
        write_manifest(
            output_dir=output_dir,
            base_params=base_params,
            limit=limit,
            initial_offset=initial_offset,
            sleep_seconds=sleep_seconds,
            max_retries=max_retries,
            retry_jitter_seconds=retry_jitter_seconds,
            storage_datasetid=storage_datasetid,
            pages=pages,
            downloaded_results=downloaded_results,
            expected_count=expected_count,
            status=current_status,
            download_complete=complete,
        )

    if overwrite:
        # Invalidate the old identity before cleanup, including interrupted cleanup.
        (output_dir / "_manifest.json").unlink(missing_ok=True)
        for path in existing_pages:
            path.unlink()
        existing_pages.clear()

    # Register the query before saving pages so an interrupted run can resume.
    save_progress("in_progress")
    try:
        visited_pages: set[Path] = set()
        while True:
            url = build_url(base_params, limit=limit, offset=offset)
            destination = output_dir / f"page_{page_number:06d}_offset_{offset:09d}.json"
            cached = destination.exists()

            if cached:
                print(f"SKIP existing: {destination}")
                payload = read_json(destination)
            else:
                print(f"Downloading page {page_number}: offset={offset}")
                payload = request_json(
                    url=url,
                    token=token,
                    max_retries=max_retries,
                    sleep_seconds=sleep_seconds,
                    retry_jitter_seconds=retry_jitter_seconds,
                )

            page_count, result_count = validate_page(payload, limit, offset)
            if expected_count is not None and page_count != expected_count:
                raise ValueError(
                    "NOAA result count changed between pages. "
                    "Use a new --output or restart with --overwrite."
                )
            expected_count = page_count
            if not cached:
                write_json(destination, payload)
                print(f"Saved: {destination}")
            visited_pages.add(destination)
            pages += 1
            downloaded_results += result_count

            print(
                f"Page {page_number}: results={result_count}, "
                f"downloaded={downloaded_results}, expected={expected_count}"
            )

            if result_count == 0 or offset + result_count > expected_count:
                break
            if result_count < limit:
                break

            offset += limit
            page_number += 1
            time.sleep(sleep_seconds)

        if existing_pages - visited_pages:
            raise ValueError(
                "NOAA output contains pages outside this download. "
                "Use a new --output or restart with --overwrite."
            )
        download_complete = is_download_complete(downloaded_results, expected_count)
        status = "success" if download_complete else "incomplete"
    finally:
        save_progress(status, download_complete)

    if not download_complete:
        print(
            "NOAA download incomplete: "
            f"downloaded_results={downloaded_results}, expected_count={expected_count}"
        )
        return 1

    print(f"NOAA raw files ready at: {output_dir}")
    return 0


def validate_resume_manifest(
    output_dir: Path,
    base_params: list[tuple[str, str]],
    limit: int,
    initial_offset: int,
    storage_datasetid: str,
) -> None:
    manifest_path = output_dir / "_manifest.json"
    if not manifest_path.exists():
        if any(output_dir.glob("page_*_offset_*.json")):
            raise ValueError(
                "NOAA pages have no manifest to identify their query. "
                "Use a new --output or restart with --overwrite."
            )
        return

    try:
        manifest = read_json(manifest_path)
        matches = (
            isinstance(manifest, dict)
            and manifest.get("endpoint") == NOAA_CDO_DATA_URL
            and sorted(manifest["params"]) == sorted([list(pair) for pair in base_params])
            and manifest.get("limit") == limit
            and manifest.get("initial_offset") == initial_offset
            and manifest.get("storage_datasetid") == storage_datasetid
        )
    except (ValueError, KeyError, TypeError):
        matches = False

    if not matches:
        raise ValueError(
            "NOAA manifest does not match this query or pagination settings. "
            "Use a new --output or restart with --overwrite."
        )


def validate_page(payload: dict[str, Any], limit: int, offset: int) -> tuple[int, int]:
    try:
        resultset = payload["metadata"]["resultset"]
        count = resultset["count"]
        results = payload["results"]
        valid = (
            type(count) is int
            and count >= 0
            and resultset["offset"] == offset
            and resultset["limit"] == limit
            and isinstance(results, list)
            and len(results) <= min(limit, max(0, count - offset + 1))
        )
    except (KeyError, TypeError):
        valid = False

    if not valid:
        raise ValueError(f"Invalid NOAA page metadata or results at offset={offset}.")
    return count, len(results)


def request_json(
    url: str,
    token: str,
    max_retries: int,
    sleep_seconds: float,
    retry_jitter_seconds: float = 0.0,
) -> dict[str, Any]:
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            request = Request(url, headers={"token": token})
            with urlopen(request, timeout=60) as response:
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError) as exc:
            last_error = exc
            wait_seconds = calculate_retry_wait_seconds(
                attempt=attempt,
                sleep_seconds=sleep_seconds,
                retry_jitter_seconds=retry_jitter_seconds,
            )
            print(
                f"Attempt {attempt}/{max_retries} failed: {exc}. "
                f"Retrying in {wait_seconds:.2f}s"
            )
            time.sleep(wait_seconds)

    raise RuntimeError(f"NOAA request failed after {max_retries} attempts") from last_error


def calculate_retry_wait_seconds(
    attempt: int,
    sleep_seconds: float,
    retry_jitter_seconds: float = 0.0,
) -> float:
    base_wait = sleep_seconds * attempt

    if retry_jitter_seconds <= 0:
        return base_wait

    return base_wait + random.uniform(0, retry_jitter_seconds)


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    temporary_path = path.with_suffix(f"{path.suffix}.part")

    with temporary_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)

    temporary_path.replace(path)


def is_download_complete(downloaded_results: int, expected_count: int | None) -> bool:
    if expected_count is None:
        return True

    return downloaded_results == expected_count


def write_manifest(
    output_dir: Path,
    base_params: list[tuple[str, str]],
    limit: int,
    initial_offset: int,
    sleep_seconds: float,
    max_retries: int,
    retry_jitter_seconds: float,
    storage_datasetid: str,
    pages: int,
    downloaded_results: int,
    expected_count: int | None,
    status: str,
    download_complete: bool,
) -> None:
    manifest = {
        "source": "NOAA CDO API v2",
        "endpoint": NOAA_CDO_DATA_URL,
        "params": base_params,
        "storage_datasetid": storage_datasetid,
        "limit": limit,
        "initial_offset": initial_offset,
        "sleep_seconds": sleep_seconds,
        "max_retries": max_retries,
        "retry_jitter_seconds": retry_jitter_seconds,
        "pages": pages,
        "downloaded_results": downloaded_results,
        "expected_count": expected_count,
        "status": status,
        "download_complete": download_complete,
    }
    write_json(output_dir / "_manifest.json", manifest)


if __name__ == "__main__":
    raise SystemExit(main())
