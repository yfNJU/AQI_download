"""Offline checks for data loss, duplicate events, and year boundaries."""
import copy
import io
import json
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import collect as c


NOW = datetime(2026, 10, 24, 0, tzinfo=c.UTC)


def station(code="4166A", name="Old name", lon="88.1198", lat="47.7931"):
    return dict(zip(c.META, (code, name, "Altay", lon, lat)))


def observation(hour=0, code="4166A", **values):
    row = {"StationCode": code, "TimePoint": c.utc_text(NOW - timedelta(hours=hour))}
    row.update(dict.fromkeys(c.FIELDS, "10"))
    row.update(values)
    return row


def rows(**values):
    return c.history_rows([observation(**values)], "4166A", NOW)


class MergeTests(unittest.TestCase):
    def test_new_valid_value_overwrites_but_na_does_not(self):
        old = rows(AQI="20", PM10="30", CO="NA")
        new = rows(AQI="15", PM10="NA", CO="0")
        result, stats = c.merge_rows(old, new)
        row = result[c.utc_text(NOW)]
        self.assertEqual((row["AQI"], row["PM10"], row["CO"]), ("15", "30", "0"))
        self.assertEqual(stats["updated_values"], 1)
        self.assertEqual(stats["filled_values"], 1)
        self.assertEqual(stats["preserved_values"], 1)
        self.assertNotIn("CO", json.loads(row["Quality_flags"]))
        self.assertEqual(old[c.utc_text(NOW)]["AQI"], "20")

    def test_repeated_merge_is_idempotent(self):
        original = rows(PM2_5="低于检出限", NO2="NA")
        merged, _ = c.merge_rows({}, original)
        again, stats = c.merge_rows(merged, original)
        self.assertEqual(merged, again)
        self.assertEqual(stats["added_rows"] + stats["updated_values"], 0)

    def test_missing_values_keep_the_original_annotation(self):
        data = rows(PM2_5="低于检出限", NO2=None)
        merged, _ = c.merge_rows({}, data)
        row = merged[c.utc_text(NOW)]
        self.assertEqual(row["PM2_5"], "NA")
        self.assertEqual(json.loads(row["Quality_flags"])["PM2_5"], "低于检出限")

    def test_zero_and_normalization(self):
        for value in (0, "0", "0.000", "-0"):
            self.assertEqual(c.number(value), "0")
        self.assertEqual(c.number("116.3000"), "116.3")
        for value in ("", "NA", None, True, "nan", "inf", "-1", "低于检出限3"):
            self.assertIsNone(c.number(value))

    def test_rows_are_sorted_and_old_hours_are_not_removed(self):
        old = c.history_rows([observation(2), observation(1)], "4166A", NOW)
        merged, _ = c.merge_rows(old, rows())
        self.assertEqual(list(merged), sorted(merged))
        self.assertEqual(len(merged), 3)


class MetadataTests(unittest.TestCase):
    def test_no_duplicate_name_events_and_return_to_original(self):
        first = c.update_metadata({}, {"4166A": station()}, NOW)
        second = c.update_metadata(first, {"4166A": station(name="New name")}, NOW)
        third = c.update_metadata(second, {"4166A": station(name="New name")}, NOW)
        back = c.update_metadata(third, {"4166A": station()}, NOW + timedelta(days=1))
        self.assertEqual(len(third["4166A"]["name_changes"]), 1)
        self.assertEqual(len(back["4166A"]["name_changes"]), 2)
        self.assertEqual(back["4166A"]["baseline"]["PositionName"], "Old name")
        self.assertEqual(first["4166A"]["name_changes"], [])
        columns, data = c.metadata_table(back)
        self.assertEqual(columns[-1], "Sta_history")
        self.assertIn("change name to New name on 26.10.24", data[0]["Sta_history"])
        self.assertIn("change name to Old name on 26.10.25", data[0]["Sta_history"])

    def test_coordinate_versions_are_shared_across_table(self):
        initial = {"4166A": station(), "1001A": station("1001A")}
        state = c.update_metadata({}, initial, NOW)
        changed = copy.deepcopy(initial)
        changed["4166A"]["Longitude"] = "88.2"
        state = c.update_metadata(state, changed, NOW)
        changed["1001A"]["Latitude"] = "47.9"
        state = c.update_metadata(state, changed, NOW)
        columns, _ = c.metadata_table(state)
        self.assertIn("Lon_new1", columns)
        self.assertNotIn("Lon_new2", columns)
        changed["4166A"]["Latitude"] = "47.8"
        state = c.update_metadata(state, changed, NOW + timedelta(days=1))
        columns, data = c.metadata_table(state)
        self.assertIn("Lon_new2", columns)
        self.assertIn("change_time2", columns)
        row = next(r for r in data if r["StationCode"] == "4166A")
        self.assertEqual((row["Lon_new2"], row["Lat_new2"]), ("88.2", "47.8"))
        self.assertEqual(row["Longitude"], "88.1198")

    def test_numeric_formatting_is_not_a_coordinate_change(self):
        initial = c.station_metadata([station(lon="88.1000")])
        new = c.station_metadata([station(lon="88.1")])
        state = c.update_metadata(c.update_metadata({}, initial, NOW), new, NOW)
        self.assertEqual(state["4166A"]["coordinate_changes"], [])

    def test_missing_station_is_retained_and_new_station_gets_baseline(self):
        state = c.update_metadata({}, {"4166A": station()}, NOW)
        state = c.update_metadata(state, {"1001A": station("1001A")}, NOW)
        self.assertFalse(state["4166A"]["present_in_latest_list"])
        self.assertEqual(state["1001A"]["baseline"]["StationCode"], "1001A")
        state = c.update_metadata(state, {"4166A": station()}, NOW)
        self.assertEqual(state["4166A"]["name_changes"], [])

    def test_invalid_snapshot_is_rejected(self):
        for data in ([], [station(), station()], [station(lon="NA")], [station(code="../x")], [station(name=None)]):
            with self.subTest(data=data), self.assertRaises(ValueError):
                c.station_metadata(data)


class TimeAndFileTests(unittest.TestCase):
    def test_dotnet_and_naive_china_time(self):
        self.assertEqual(c.utc_text(c.parse_time("/Date(1792684800000)/")), "2026-10-22T16:00:00Z")
        self.assertEqual(c.utc_text(c.parse_time("2026-10-23T00:00:00")), "2026-10-22T16:00:00Z")

    def test_wrong_station_duplicate_stale_and_future_are_rejected(self):
        cases = [[observation(code="1001A")], [observation(), observation()],
                 [observation(8)], [observation(-2)]]
        for data in cases:
            with self.subTest(data=data), self.assertRaises(ValueError):
                c.history_rows(data, "4166A", NOW)

    def test_extra_old_rows_are_filtered_and_missing_hours_not_invented(self):
        result = c.history_rows([observation(0), observation(2), observation(24)], "4166A", NOW)
        self.assertEqual(len(result), 2)

    @patch.object(c, "atomic_write")
    @patch.object(c, "read_series", return_value={})
    def test_cross_year_writes_and_archive_skip(self, read, write):
        new_year = datetime(2027, 1, 1, 1, tzinfo=c.UTC)
        raw = [observation(TimePoint="2026-12-31T23:00:00Z"),
               observation(TimePoint="2027-01-01T00:00:00Z")]
        incoming = c.history_rows(raw, "4166A", new_year)
        c.save_station(Path("output"), "4166A", incoming, [])
        self.assertEqual({x.args[0].parent.name for x in write.call_args_list}, {"2026", "2027"})
        write.reset_mock()
        stats = c.save_station(Path("output"), "4166A", incoming, [2026])
        self.assertEqual(write.call_count, 1)
        self.assertEqual(write.call_args.args[0].name, "4166A_2027.txt")
        self.assertEqual(stats["archived_rows_skipped"], 1)

    def test_csv_quotes_station_names_and_history(self):
        state = c.update_metadata({}, {"4166A": station(name='Name, "A"')}, NOW)
        columns, data = c.metadata_table(state)
        parsed = list(c.csv.DictReader(io.StringIO(c.table_text(columns, data))))
        self.assertEqual(parsed[0]["PositionName"], 'Name, "A"')

    @patch.object(c.Path, "exists", return_value=True)
    @patch.object(c.Path, "open")
    def test_corrupt_existing_file_is_rejected_not_overwritten(self, opened, exists):
        opened.return_value = io.StringIO("Time_UTC\tAQI\n2026-10-24T00:00:00Z\t5\n")
        with self.assertRaises(ValueError):
            c.read_series(Path("old.txt"))


class TransportTests(unittest.TestCase):
    @patch.object(c.time, "sleep")
    @patch.object(c, "urlopen")
    def test_transient_network_errors_are_retried(self, request, sleep):
        response = io.BytesIO(b"[]")
        request.side_effect = [c.URLError("temporary"), response]
        client = c.Client({"attempts": 3, "timeout_seconds": 2, "request_interval_seconds": 0.5})
        self.assertEqual(client.post("/test"), [])
        self.assertEqual(request.call_count, 2)

    @patch.object(c, "urlopen", side_effect=c.HTTPError("https://example.invalid", 403, "denied", {}, None))
    def test_permission_error_is_not_retried(self, request):
        client = c.Client({"attempts": 3, "timeout_seconds": 2, "request_interval_seconds": 0.5})
        with self.assertRaises(c.HTTPError):
            client.post("/test")
        self.assertEqual(request.call_count, 1)


if __name__ == "__main__":
    unittest.main()
