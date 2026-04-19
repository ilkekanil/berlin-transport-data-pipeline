# etl/load_timetable_changes.py

import re
from pathlib import Path
from datetime import datetime
from collections import Counter

import psycopg2
from psycopg2.extras import execute_values
import xml.etree.ElementTree as ET

from config import DB

from load_fact_timetables import (
    PROJECT_ROOT,
    slugify,
    normalize_station_from_filename,
    normalize_station_from_xml_station,
    build_station_lookup,
    parse_snapshot_ts_from_folder,
    get_time_id,
)

CHANGES_ROOT = PROJECT_ROOT / "data" / "timetable_changes"


# Parsing helpers


PT_IN_TRIP_ID = re.compile(r"-(\d{10})-")  # matches -2509021527- inside id

def build_station_lookup_by_eva(cur) -> dict[str, int]:
    cur.execute("SELECT station_id, eva_number FROM public.dim_station WHERE eva_number IS NOT NULL;")
    return {str(eva).strip(): station_id for station_id, eva in cur.fetchall() if str(eva).strip()}



def extract_planned_pt_from_trip_id(trip_id: str) -> datetime | None:
    """
    In timetable_changes, planned time is embedded in s.id:
      id="RANDOM-YYMMDDHHMM-XX"
    """
    if not trip_id:
        return None
    m = PT_IN_TRIP_ID.search(trip_id)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%y%m%d%H%M")
    except Exception:
        return None


def parse_ct(ct: str) -> datetime | None:
    if not ct:
        return None
    try:
        return datetime.strptime(ct.strip(), "%y%m%d%H%M")
    except Exception:
        return None


def is_cancelled_from_attrs(attrs: dict) -> bool:
    """
    DB change files encode cancellation in different ways depending on feed.
    We implement a robust detector:
      - c="c" or c="true" or c="1"
      - cs="c" etc.
      - clt present sometimes indicates cancellation or changed line text
    """
    for k in ("c", "cs", "cancelled", "is_cancelled"):
        v = attrs.get(k)
        if v is None:
            continue
        v = str(v).strip().lower()
        if v in ("c", "true", "1", "yes", "y"):
            return True
    return False


def parse_change_file(fp: Path):
    """
    Returns rows to update planned fact rows:
      (station_id, time_id, trip_id, is_arrival, is_departure,
       actual_time, delay_minutes, is_cancelled, platform, snapshot_ts)
    """
    try:
        root = ET.parse(fp).getroot()
    except Exception:
        return []

    out = []
    for s in root.findall("./s"):
        trip_id = s.attrib.get("id")
        if not trip_id:
            continue

        planned_ts = extract_planned_pt_from_trip_id(trip_id)
        if planned_ts is None:
            continue

        # arrival change
        ar = s.find("./ar")
        if ar is not None:
            ct = parse_ct(ar.attrib.get("ct"))
            if ct is not None:
                delay = int((ct - planned_ts).total_seconds() // 60)
            else:
                delay = None
            out.append((
                "AR",
                trip_id,
                planned_ts,
                ct,
                delay,
                is_cancelled_from_attrs(ar.attrib),
                ar.attrib.get("l"),
            ))

        # departure change
        dp = s.find("./dp")
        if dp is not None:
            ct = parse_ct(dp.attrib.get("ct"))
            if ct is not None:
                delay = int((ct - planned_ts).total_seconds() // 60)
            else:
                delay = None
            out.append((
                "DP",
                trip_id,
                planned_ts,
                ct,
                delay,
                is_cancelled_from_attrs(dp.attrib),
                dp.attrib.get("l"),
            ))

    return out



# Batch updater


def update_fact_batch(cur, rows):
    """
    rows = list of:
      (station_id, time_id, trip_id, is_arrival, is_departure,
       actual_time, delay_minutes, is_cancelled, platform, snapshot_ts)
    """
    sql = """
    WITH upd(station_id, time_id, trip_id, is_arrival, is_departure,
             actual_time, delay_minutes, is_cancelled, platform, snapshot_ts) AS (
      VALUES %s
    )
    UPDATE public.fact_train_movement f
    SET
      actual_time   = COALESCE(upd.actual_time, f.actual_time),
      delay_minutes = COALESCE(upd.delay_minutes, f.delay_minutes),
      is_cancelled  = (f.is_cancelled OR upd.is_cancelled),
      platform      = COALESCE(upd.platform, f.platform),
      snapshot_ts   = GREATEST(f.snapshot_ts, upd.snapshot_ts)
    FROM upd
    WHERE f.station_id   = upd.station_id
      AND f.time_id      = upd.time_id
      AND f.trip_id      = upd.trip_id
      AND f.is_arrival   = upd.is_arrival
      AND f.is_departure = upd.is_departure;
    """
    execute_values(cur, sql, rows, page_size=5000)
    return len(rows)


# Main loader


def load_timetable_changes(limit_files: int | None = None):
    xml_files = list(CHANGES_ROOT.rglob("*_change.xml"))
    print(f"Found {len(xml_files)} change XML files under {CHANGES_ROOT}")
    if not xml_files:
        return

    if limit_files:
        xml_files = xml_files[:limit_files]
        print(f"DEBUG: limiting to {len(xml_files)} files")

    conn = psycopg2.connect(**DB)
    conn.autocommit = False

    parsed_files = 0
    skipped_station = 0
    skipped_snapshot = 0
    updated_rows = 0
    empty_change_files = 0

    matched_by_eva = 0
    matched_by_filename = 0
    matched_by_xml_attr = 0

    unmapped_nonempty = Counter()
    unmapped_examples = {}

    try:
        with conn.cursor() as cur:
            station_lookup = build_station_lookup(cur)
            station_lookup_eva = build_station_lookup_by_eva(cur)
            batch = []

            for fp in xml_files:
                parsed_files += 1

                station_id = None
                matched_by = None

                root = None
                try:
                    root = ET.parse(fp).getroot()
                except Exception:
                    root = None

                xml_station = None
                xml_eva = None
                if root is not None:
                    xml_station = root.attrib.get("station")
                    xml_eva = root.attrib.get("eva")

                # 1) EVA-based mapping
                if xml_eva:
                    station_id = station_lookup_eva.get(str(xml_eva).strip())
                    if station_id is not None:
                        matched_by = "eva"

                # 2) filename mapping
                station_key = normalize_station_from_filename(fp.name)
                if station_id is None:
                    station_id = station_lookup.get(station_key)
                    if station_id is not None:
                        matched_by = "filename"

                # 3) XML station name fallback
                if station_id is None and xml_station:
                    for k in normalize_station_from_xml_station(xml_station):
                        station_id = station_lookup.get(k)
                        if station_id is not None:
                            matched_by = "xml"
                            break

                if station_id is None:
                    skipped_station += 1
                    try:
                        if fp.stat().st_size > 60:
                            # log something: eva or xml_station if available
                            debug_key = (
                                f"eva_{xml_eva}" if xml_eva else
                                (normalize_station_from_filename(xml_station) if xml_station else station_key)
                            )
                            unmapped_nonempty[debug_key] += 1
                            unmapped_examples.setdefault(debug_key, fp.name)
                    except Exception:
                        pass
                    continue

                if matched_by == "eva":
                    matched_by_eva += 1
                elif matched_by == "filename":
                    matched_by_filename += 1
                elif matched_by == "xml":
                    matched_by_xml_attr += 1

                snapshot_ts = parse_snapshot_ts_from_folder(fp.parent.name)
                if snapshot_ts is None:
                    skipped_snapshot += 1
                    continue

                events = parse_change_file(fp)
                if not events:
                    empty_change_files += 1
                    continue

                for etype, trip_id, planned_ts, actual_ts, delay_min, cancelled, platform in events:
                    time_id = get_time_id(cur, planned_ts)

                    batch.append((
                        station_id,
                        time_id,
                        trip_id,
                        etype == "AR",
                        etype == "DP",
                        actual_ts,
                        delay_min,
                        cancelled,
                        platform,
                        snapshot_ts,
                    ))

                    if len(batch) >= 5000:
                        updated_rows += update_fact_batch(cur, batch)
                        batch.clear()

            if batch:
                updated_rows += update_fact_batch(cur, batch)
                batch.clear()

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
        
        
    #print(f"Parsed change files: {parsed_files}")
    #print(f"Station mapped by filename: {matched_by_filename}")
    #print(f"Station mapped by XML attr fallback: {matched_by_xml_attr}")
    #print(f"Skipped files (station not mapped): {skipped_station}")
    #print(f"Skipped files (snapshot_ts parse failed): {skipped_snapshot}")
    #print(f"Updated fact rows attempted: {updated_rows}")

    print("\nTop unmapped (NON-EMPTY) station keys:")
    for k, c in unmapped_nonempty.most_common(30):
        print(f"  {c:6d}  {k}   (example: {unmapped_examples[k]})")

    print(" timetable_changes update done.")


if __name__ == "__main__":
    load_timetable_changes()
