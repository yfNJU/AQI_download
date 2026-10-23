"""Collect CNEMC hourly observations and keep an append-only station history.

Standard library only. All persisted observation and detection times use UTC.
"""

import argparse
import copy
import csv
import io
import json
import os
import re
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

UTC = timezone.utc
BASE_URL = "https://air.cnemc.cn:18007"
CODE = re.compile(r"\d{4}A\Z")
FIELDS = ("AQI", "PM2_5", "PM10", "SO2", "NO2", "CO", "O3")
META = ("StationCode", "PositionName", "Area", "Longitude", "Latitude")
HEADER = ("Time_UTC", *FIELDS, "Quality_flags")


def utc_text(value):
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_time(value):
    """.NET dates are Unix milliseconds; naive server ISO times are UTC+08."""
    match = re.fullmatch(r"/Date\((-?\d+)(?:[+-]\d{4})?\)/", str(value))
    if match:
        return datetime.fromtimestamp(int(match[1]) / 1000, UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone(timedelta(hours=8)))
    return parsed.astimezone(UTC)


def number(value):
    """Return a canonical nonnegative number, or None for a missing value."""
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value).strip())
    except InvalidOperation:
        return None
    if not parsed.is_finite() or parsed < 0:
        return None
    if parsed == 0:
        return "0"
    text = format(parsed, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def station_metadata(data):
    result = {}
    if not isinstance(data, list) or not data:
        raise ValueError("Empty or malformed station list")
    for item in data:
        if any(item.get(key) is None for key in META):
            raise ValueError("Missing station metadata")
        row = {key: str(item.get(key, "")).strip() for key in META}
        code = row["StationCode"]
        if not CODE.fullmatch(code) or code in result:
            raise ValueError(f"Invalid or duplicate station code: {code}")
        if not row["PositionName"] or not row["Area"]:
            raise ValueError(f"Missing name/area for {code}")
        for field, limit in (("Longitude", 180), ("Latitude", 90)):
            value = number(row[field])
            if value is None or not 0 < Decimal(value) <= limit:
                raise ValueError(f"Invalid {field} for {code}")
            row[field] = value
        result[code] = row
    return result


def update_metadata(previous, snapshot, observed_at):
    """Keep baseline immutable; compare with current so events are not repeated."""
    state = copy.deepcopy(previous)
    stamp = utc_text(observed_at)
    for code, meta in snapshot.items():
        if code not in state:
            state[code] = {
                "baseline": meta.copy(), "current": meta.copy(),
                "first_seen_utc": stamp, "name_changes": [],
                "coordinate_changes": [],
            }
        item = state[code]
        old = item["current"]
        if old["PositionName"] != meta["PositionName"]:
            item["name_changes"].append({"name": meta["PositionName"], "time": stamp})
        if any(old[key] != meta[key] for key in ("Longitude", "Latitude")):
            item["coordinate_changes"].append({
                "Longitude": meta["Longitude"], "Latitude": meta["Latitude"], "time": stamp,
            })
        item["current"] = meta.copy()
        item["last_seen_utc"] = stamp
    for code, item in state.items():
        item["present_in_latest_list"] = code in snapshot
    return state


def metadata_table(state):
    columns = list(META)
    versions = max((len(s["coordinate_changes"]) for s in state.values()), default=0)
    for n in range(1, versions + 1):
        columns.extend((f"Lon_new{n}", f"Lat_new{n}", "change_time" if n == 1 else f"change_time{n}"))
    columns.append("Sta_history")
    rows = []
    for code, station in sorted(state.items()):
        row = station["baseline"].copy()
        for n, event in enumerate(station["coordinate_changes"], 1):
            row[f"Lon_new{n}"] = event["Longitude"]
            row[f"Lat_new{n}"] = event["Latitude"]
            row["change_time" if n == 1 else f"change_time{n}"] = event["time"]
        row["Sta_history"] = "; ".join(
            f"change name to {e['name']} on {parse_time(e['time']):%y.%m.%d}"
            for e in station["name_changes"]
        )
        rows.append(row)
    return columns, rows


def history_rows(data, code, now, max_age_hours=6):
    if not isinstance(data, list) or not data:
        raise ValueError(f"{code}: empty history")
    result = {}
    for item in data:
        if item.get("StationCode") != code:
            raise ValueError(f"{code}: response contains another station")
        stamp = parse_time(item["TimePoint"])
        if stamp.minute or stamp.second or stamp.microsecond or stamp > now + timedelta(hours=1):
            raise ValueError(f"{code}: invalid observation time {stamp}")
        key = utc_text(stamp)
        if key in result:
            raise ValueError(f"{code}: duplicate observation time {key}")
        row = {"Time_UTC": key}
        flags = {}
        for field in FIELDS:
            value = number(item.get(field))
            row[field] = value if value is not None else "NA"
            if value is None:
                flags[field] = item.get(field)
        row["Quality_flags"] = json.dumps(flags, ensure_ascii=False, sort_keys=True)
        result[key] = row
    latest = parse_time(max(result))
    if latest < now - timedelta(hours=max_age_hours):
        raise ValueError(f"{code}: stale history, latest={utc_text(latest)}")
    # Only the most recent 24 hourly slots, even if the server returns extra rows.
    cutoff = utc_text(latest - timedelta(hours=23))
    return {key: result[key] for key in sorted(result) if key >= cutoff}


def merge_rows(existing, incoming):
    merged = copy.deepcopy(existing)
    stats = Counter()
    for stamp, new in incoming.items():
        if stamp not in merged:
            merged[stamp] = new.copy()
            stats["added_rows"] += 1
            stats["invalid_values"] += sum(number(new[f]) is None for f in FIELDS)
            continue
        old = merged[stamp]
        flags = json.loads(old["Quality_flags"])
        new_flags = json.loads(new["Quality_flags"])
        for field in FIELDS:
            valid_new, valid_old = number(new[field]), number(old[field])
            if valid_new is not None:
                if valid_new != valid_old:
                    stats["filled_values" if valid_old is None else "updated_values"] += 1
                old[field] = valid_new
                flags.pop(field, None)
            elif valid_old is not None:
                stats["preserved_values"] += 1
            else:
                old[field] = "NA"
                flags[field] = new_flags.get(field)
            if valid_new is None:
                stats["invalid_values"] += 1
        old["Quality_flags"] = json.dumps(flags, ensure_ascii=False, sort_keys=True)
    return dict(sorted(merged.items())), stats


def table_text(columns, rows, delimiter=","):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=columns, delimiter=delimiter, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue()


def atomic_write(path, text):
    if path.exists() and path.read_text(encoding="utf-8") == text:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    try:
        with temp.open("w", encoding="utf-8", newline="") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def write_json(path, data):
    atomic_write(path, json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def read_series(path):
    if not path.exists():
        return {}
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream, delimiter="\t")
        if reader.fieldnames != list(HEADER):
            raise ValueError(f"Unexpected columns in {path}")
        rows = {}
        for row in reader:
            key = row["Time_UTC"]
            if key in rows or utc_text(parse_time(key)) != key or None in row.values() or None in row:
                raise ValueError(f"Corrupt/duplicate row in {path}")
            if not isinstance(json.loads(row["Quality_flags"]), dict):
                raise ValueError(f"Invalid quality flags in {path}")
            if any(row[f] != "NA" and number(row[f]) is None for f in FIELDS):
                raise ValueError(f"Invalid saved value in {path}")
            rows[key] = row
        return rows


def save_station(root, code, incoming, archived_years):
    stats = Counter()
    for year in sorted({int(stamp[:4]) for stamp in incoming}):
        subset = {key: value for key, value in incoming.items() if int(key[:4]) == year}
        if year in archived_years:
            stats["archived_rows_skipped"] += len(subset)
            continue
        path = root / str(year) / f"{code}_{year}.txt"
        existing = read_series(path)
        if any(int(stamp[:4]) != year for stamp in existing):
            raise ValueError(f"Wrong year in {path}")
        merged, changes = merge_rows(existing, subset)
        atomic_write(path, table_text(HEADER, merged.values(), "\t"))
        stats.update(changes)
    return stats


class Client:
    """A shared start-rate limiter applies to all workers and retry requests."""

    def __init__(self, config):
        self.config = config
        self.lock = threading.Lock()
        self.next_request = 0.0

    def post(self, route):
        for attempt in range(self.config["attempts"]):
            with self.lock:
                delay = max(0, self.next_request - time.monotonic())
                if delay:
                    time.sleep(delay)
                self.next_request = time.monotonic() + self.config["request_interval_seconds"]
            request = Request(BASE_URL + route, data=b"", headers={
                "User-Agent": "AQI-download/1.0 (+https://github.com/yfNJU/AQI_download)",
                "Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded",
            })
            wait = 2 ** attempt
            try:
                with urlopen(request, timeout=self.config["timeout_seconds"]) as response:
                    return json.loads(response.read().decode("utf-8-sig"))
            except HTTPError as error:
                if error.code not in (408, 429, 500, 502, 503, 504):
                    raise
                retry_after = error.headers.get("Retry-After", "")
                if retry_after.isdigit():
                    wait = max(wait, min(int(retry_after), 60))
                if attempt + 1 == self.config["attempts"]:
                    raise
            except (URLError, TimeoutError, json.JSONDecodeError, UnicodeError):
                if attempt + 1 == self.config["attempts"]:
                    raise
            time.sleep(wait)


def load_config(path):
    config = json.loads(path.read_text(encoding="utf-8-sig"))
    for field in ("workers", "attempts", "timeout_seconds", "max_data_age_hours"):
        if type(config.get(field)) is not int or config[field] <= 0:
            raise ValueError(f"Invalid config: {field}")
    if not isinstance(config.get("request_interval_seconds"), (int, float)) or config["request_interval_seconds"] < 0.2:
        raise ValueError("request_interval_seconds must be >= 0.2")
    if not isinstance(config.get("archived_years"), list) or any(type(y) is not int or not 1900 <= y <= 9999 for y in config["archived_years"]):
        raise ValueError("archived_years must be a list of integer years")
    return config


def collect(root, config, selected, report, allow_large_drop=False):
    client = Client(config)
    raw = client.post("/HourChangesPublish/GetAllAQIPublishLive")
    snapshot = station_metadata(raw)
    now = datetime.now(UTC)
    times = [parse_time(item["TimePoint"]) for item in raw]
    if max(times) < now - timedelta(hours=config["max_data_age_hours"]) or max(times) > now + timedelta(hours=1):
        raise ValueError("Station list is stale or has a future timestamp")
    state_path = root / "state.json"
    initial_path = root / "stations_initial.csv"
    if state_path.exists():
        document = json.loads(state_path.read_text(encoding="utf-8"))
        if document.get("schema_version") != 1:
            raise ValueError("Unsupported state schema")
        previous = document["stations"]
    else:
        previous = {}
        if initial_path.exists():
            raise ValueError("state.json is missing; restore it to preserve history")
    last_count = sum(item["present_in_latest_list"] for item in previous.values())
    if last_count and len(snapshot) < last_count * 0.8 and not allow_large_drop:
        raise ValueError("Station count dropped >20%; verify source before --allow-large-drop")
    codes = sorted(snapshot if selected is None else set(selected))
    if any(code not in snapshot for code in codes):
        raise ValueError("Requested station is absent from the current list")
    state = update_metadata(previous, snapshot, now)
    # State is authoritative; a crash before CSV export can be repaired on next run.
    write_json(state_path, {"schema_version": 1, "stations": state})
    if not initial_path.exists():
        initial_rows = [state[c]["baseline"] for c in sorted(state)]
        atomic_write(initial_path, table_text(META, initial_rows))
    columns, rows = metadata_table(state)
    atomic_write(root / "stations.csv", table_text(columns, rows))
    report.update({"stations_in_list": len(snapshot), "requested": len(codes), "succeeded": 0,
                   "failed": [], "missing_hours": {}, "all_missing_stations": [], "counters": {},
                   "station_list_time_utc": utc_text(max(times))})
    stats = Counter()
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=config["workers"]) as pool:
        tasks = {pool.submit(client.post, f"/HourChangesPublish/GetAqiHistoryByCondition?stationCode={code}"): code for code in codes}
        for index, future in enumerate(as_completed(tasks), 1):
            code = tasks[future]
            try:
                incoming = history_rows(future.result(), code, datetime.now(UTC), config["max_data_age_hours"])
                stats.update(save_station(root, code, incoming, config["archived_years"]))
                if len(incoming) < 24:
                    report["missing_hours"][code] = 24 - len(incoming)
                if all(row[field] == "NA" for row in incoming.values() for field in FIELDS):
                    report["all_missing_stations"].append(code)
                report["succeeded"] += 1
            except Exception as error:
                report["failed"].append({"StationCode": code, "error": str(error)})
                print(f"FAILED {code}: {error}", flush=True)
            report["counters"] = dict(stats)
            if index % 50 == 0 or index == len(codes):
                print(f"{index}/{len(codes)}: ok={report['succeeded']} failed={len(report['failed'])} elapsed={time.monotonic()-started:.0f}s", flush=True)
            if len(report["failed"]) >= 20 and report["succeeded"] == 0:
                for pending in tasks:
                    pending.cancel()
                raise RuntimeError("Stopped after 20 failures without a successful station")
    report["complete"] = not report["failed"] and report["succeeded"] == len(codes)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    base = Path(__file__).resolve().parent
    parser.add_argument("--output-dir", type=Path, default=base)
    parser.add_argument("--config", type=Path, default=base / "config.json")
    parser.add_argument("--stations", nargs="+", help="Only fetch these stations; metadata still covers the full list")
    parser.add_argument("--allow-large-drop", action="store_true")
    args = parser.parse_args()
    root = args.output_dir.resolve()
    root.mkdir(parents=True, exist_ok=True)
    lock = root / ".run.lock"
    try:
        with lock.open("x", encoding="utf-8") as stream:
            stream.write(str(os.getpid()))
    except FileExistsError:
        print(f"Another run may be active: {lock}. Check before removing this lock.", file=sys.stderr)
        return 2
    report = {"started_utc": utc_text(datetime.now(UTC)), "complete": False}
    try:
        config = load_config(args.config)
        collect(root, config, args.stations, report, args.allow_large_drop)
    except Exception as error:
        report["fatal_error"] = str(error)
        print(f"ERROR: {error}", file=sys.stderr)
    finally:
        report["finished_utc"] = utc_text(datetime.now(UTC))
        try:
            write_json(root / "last_run.json", report)
        finally:
            lock.unlink(missing_ok=True)
    print(json.dumps(report, ensure_ascii=False), flush=True)
    return 0 if report["complete"] else 1


if __name__ == "__main__":
    sys.exit(main())
