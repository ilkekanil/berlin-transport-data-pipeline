# etl/load_trains.py
import os
from pathlib import Path
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_values

from config import DB  

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TIMETABLES_DIR = PROJECT_ROOT / "data" / "timetables"


def connect():
    return psycopg2.connect(**DB)


def safe_get(d: dict, *keys, default=None):
    """Try multiple keys in order."""
    for k in keys:
        if k in d and d[k] not in (None, "", "null"):
            return d[k]
    return default


def extract_train_fields_from_stop(stop_elem: ET.Element) -> dict | None:
    """
    DB timetables often store train info on stop nodes or their children.
    We try a bunch of common attribute names so this works even if XML differs slightly.
    """
    a = stop_elem.attrib

    # common possibilities 
    train_number = safe_get(a, "n", "trainNumber", "train_number", "number")
    operator_code = safe_get(a, "o", "operator", "operatorCode", "operator_code")
    train_category = safe_get(a, "cat", "category", "trainCategory", "train_category")
    trip_type = safe_get(a, "t", "tripType", "trip_type", "type")
    line = safe_get(a, "l", "line", "lineName", "line_name")

    # If not found on the stop, sometimes it’s stored in a child node
    # scan children once and pick from their attribs too
    if not any([train_number, operator_code, train_category, trip_type, line]):
        for child in list(stop_elem):
            ca = child.attrib
            train_number = train_number or safe_get(ca, "n", "trainNumber", "train_number", "number")
            operator_code = operator_code or safe_get(ca, "o", "operator", "operatorCode", "operator_code")
            train_category = train_category or safe_get(ca, "cat", "category", "trainCategory", "train_category")
            trip_type = trip_type or safe_get(ca, "t", "tripType", "trip_type", "type")
            line = line or safe_get(ca, "l", "line", "lineName", "line_name")
            if any([train_number, operator_code, train_category, trip_type, line]):
                break

    # We need *something* to consider it a train record
    if not any([train_number, line, train_category]):
        return None

    return {
        "train_number": str(train_number) if train_number is not None else None,
        "operator_code": str(operator_code) if operator_code is not None else None,
        "train_category": str(train_category) if train_category is not None else None,
        "trip_type": str(trip_type) if trip_type is not None else None,
        "line": str(line) if line is not None else None,
    }


def find_timetable_xml_files() -> list[Path]:
    if not TIMETABLES_DIR.exists():
        raise FileNotFoundError(f"Timetables folder not found: {TIMETABLES_DIR}")

    # a structure: timetables/<timestamp_folder>/*.xml 
    files = sorted(TIMETABLES_DIR.glob("**/*.xml"))
    return files


def load_trains(limit_files: int | None = None):
    xml_files = find_timetable_xml_files()
    if limit_files:
        xml_files = xml_files[:limit_files]

    if not xml_files:
        print(f"No .xml files found under: {TIMETABLES_DIR}")
        return

    # Collect unique train rows
    unique = set()
    missing_count = 0
    parsed_files = 0

    for fp in xml_files:
        try:
            tree = ET.parse(fp)
            root = tree.getroot()
            parsed_files += 1
        except Exception as e:
            print(f"[WARN] Failed to parse XML: {fp} -> {e}")
            continue

        # Common: stop nodes are <s ...> 
        stops = root.findall(".//s")
        if not stops:
            # fallback: any elements that look like stops
            stops = list(root.iter())

        for s in stops:
            rec = extract_train_fields_from_stop(s)
            if not rec:
                missing_count += 1
                continue

            key = (
                rec["train_number"],
                rec["operator_code"],
                rec["train_category"],
                rec["trip_type"],
                rec["line"],
            )
            unique.add(key)

    rows = list(unique)

    # Upsert into dim_train
    sql = """
    INSERT INTO public.dim_train (train_number, operator_code, train_category, trip_type, line)
    VALUES %s
    ON CONFLICT (train_number, operator_code, train_category, trip_type, line)
    DO NOTHING;
    """

    with connect() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=5000)

    #print(f"Parsed files: {parsed_files}")
    #print(f"Unique train rows inserted/kept: {len(rows)}")
    #print(f"Skipped stop nodes (no train info found): {missing_count}")
    print("dim_train load done.")


if __name__ == "__main__":
    # Tip: start small for speed while testing:
    # load_trains(limit_files=20)
    load_trains()
