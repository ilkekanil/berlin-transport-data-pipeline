# etl/load_fact_timetables.py
#A small number of timetable files (355 out of 135692) could not be mapped to any station dimension entry 
# due to missing/corrupted station identifiers. These files were logged and skipped to preserve referential integrity.

import re
from pathlib import Path
from datetime import datetime
from collections import Counter

import psycopg2
from psycopg2.extras import execute_values
import xml.etree.ElementTree as ET

from config import DB

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TIMETABLES_ROOT = PROJECT_ROOT / "data" / "timetables"

UMLAUT_MAP = {
    "ä": "ae", "ö": "oe", "ü": "ue",
    "Ä": "ae", "Ö": "oe", "Ü": "ue",
    "ß": "ss",
}
ALIAS_MAP = {
    "dende": "berlin_suedende",
    "n_ldnerplatz": "berlin_noeldnerplatz",
    "yorckstrasse_gro_g_rschenstrasse": "berlin_yorckstr_s1",  # <-- change here
    "berlin_schoeneweide": "berlin_schoeneweide",
}



# Normalization helpers
def resolve_station_id_by_name_search(cur, xml_station: str) -> int | None:
    if not xml_station:
        return None

    token = re.sub(r"\s*\([^)]*\)\s*", " ", xml_station).strip()
    token = token.replace("Berlin-", "").replace("Berlin ", "").strip()
    token = slugify(token).replace("_", "")

    if not token:
        return None

    tokens_to_try = {token, token.replace("oe", "o").replace("ae", "a").replace("ue", "u")}

    sql = """
        SELECT station_id
        FROM public.dim_station
        WHERE
          REPLACE(
            REPLACE(
              REPLACE(
                REPLACE(
                  TRANSLATE(LOWER(name), 'äöüß', 'aouss'),
                '.', ''),
              ' ', ''),
            '-', ''),
          '_', '')
          LIKE %s
        ORDER BY LENGTH(name) ASC
        LIMIT 1;
    """

    for tok in tokens_to_try:
        cur.execute(sql, (f"%{tok}%",))
        row = cur.fetchone()
        if row:
            return row[0]

    return None




def slugify(s: str) -> str:
    """Deterministic slug: umlauts expanded, lowercase, non-alnum->_, collapse _"""
    if not s:
        return ""
    s = s.strip()
    for k, v in UMLAUT_MAP.items():
        s = s.replace(k, v)
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def add_umlaut_variants(key: str) -> set[str]:
    """
    Generate extra keys to match cases where station names appear as:
      schoeneweide  vs schoneweide
      noeldnerplatz vs noldnerplatz
    """
    variants = {key}
    variants.add(key.replace("ae", "a"))
    variants.add(key.replace("oe", "o"))
    variants.add(key.replace("ue", "u"))
    return variants


def _apply_db_filename_fixes(raw: str) -> str:
    """
    Fix DB-specific filename encodings BEFORE slugify.

    Examples from your logs:
      warschauer_stra_e  -> warschauer_strasse
      julius-leber-br_cke -> julius-leber-bruecke
      sch_neberg -> schoeneberg
      gr_nbergallee -> gruenbergallee
      k_llnische -> koellnische
      s_d -> sued
      *_pbf -> strip suffix
    """
    s = raw.lower()

    # common DB encodings for German letters / words
    s = s.replace("stra_e", "strasse")   # straße
    s = s.replace("br_cke", "bruecke")   # brücke

    # schoene*
    s = s.replace("sch_ne", "schoene")
    s = s.replace("sch_nh", "schoenh")   # schoenhaus...
    s = s.replace("sch_n", "schoen")    

    # gruen*
    s = s.replace("gr_n", "gruen")

    # koelln*
    s = s.replace("k_lln", "koelln")

    # sued token: s_d
    s = re.sub(r"(^|_)s_d(_|$)", r"\1sued\2", s)

    # strip dataset suffix used in some station identifiers
    s = re.sub(r"_pbf$", "", s)

    # Plänterwald
    s = re.sub(r"^pl_nterwald$", "plaenterwald", s)

    return s


def normalize_station_from_filename(filename: str) -> str:
    """
    Convert a DB API station filename into a canonical key.

    Handles:
    - suffixes: *_timetable.xml / *_change.xml
    - prefixes: s_/u_/s+u_ (if present)
    - DB encodings: stra_e, br_cke, sch_, gr_n, k_lln, s_d, *_pbf, etc.
    - token-based abbreviation expansion (no substring replacements!)
    - final deterministic alias resolution for a few legacy identifiers
    """
    name = filename.lower()

    # strip suffixes
    if name.endswith("_timetable.xml"):
        name = name[:-len("_timetable.xml")]
    elif name.endswith("_change.xml"):
        name = name[:-len("_change.xml")]
    elif name.endswith(".xml"):
        name = name[:-len(".xml")]

    # remove transport prefixes
    for p in ("s+u_", "s_u_", "s_", "u_"):
        if name.startswith(p):
            name = name[len(p):]
            break

    # apply DB-specific fixes before slugify
    name = _apply_db_filename_fixes(name)

    # slugify after fixes
    name = slugify(name)

    # token-based abbreviation expansion 
    parts = name.split("_")
    token_map = {
        "hbf": "hauptbahnhof",
        "bhf": "bahnhof",
        "str": "strasse",
        "pl": "platz",
    }
    parts = [token_map.get(p, p) for p in parts]
    name = "_".join(parts)
    name = re.sub(r"_+", "_", name).strip("_")

    #final deterministic alias resolution
    name = ALIAS_MAP.get(name, name)

    return name



def read_station_attr_quick(fp: Path) -> str | None:
    """Fallback: read <timetable station="..."> attribute (cheap)."""
    try:
        root = ET.parse(fp).getroot()
        return root.attrib.get("station")
    except Exception:
        return None

def normalize_station_from_xml_station(station: str) -> list[str]:
    """
    Normalize <timetable station="..."> into a list of lookup keys to try.
    This is station-name normalization (NOT filename normalization).
    """
    if not station:
        return []

    base = station.strip()

    # produce a few "semantic" variants:
    # original
    # without parentheses content like "Berlin Yorckstr.(S1)" -> "Berlin Yorckstr."
    no_paren = re.sub(r"\s*\([^)]*\)\s*", " ", base).strip()

    candidates_raw = [base, no_paren]

    keys = []
    for s in candidates_raw:
        k = slugify(s)
        if not k:
            continue

        # add umlaut style variants (oe->o etc)
        for v in add_umlaut_variants(k):
            keys.append(v)
            if v.startswith("berlin_"):
                keys.append(v.replace("berlin_", "", 1))
            else:
                keys.append("berlin_" + v)

    # unique preserve order
    seen = set()
    out = []
    for k in keys:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


# Timestamp parsing


def parse_snapshot_ts_from_folder(folder_name: str) -> datetime | None:
    try:
        return datetime.strptime(folder_name, "%y%m%d%H%M")
    except Exception:
        return None


def parse_pt(pt: str) -> datetime | None:
    if not pt:
        return None
    try:
        return datetime.strptime(pt.strip(), "%y%m%d%H%M")
    except Exception:
        return None



# DB helpers


def build_station_lookup(cur) -> dict[str, int]:
    cur.execute("SELECT station_id, name FROM public.dim_station;")
    lookup: dict[str, int] = {}

    def add(key: str, station_id: int):
        if key:
            lookup.setdefault(key, station_id)

    def add_with_berlin_variants(key: str, station_id: int):
        if not key:
            return

        for k in add_umlaut_variants(key):
            add(k, station_id)

            if k.startswith("berlin_"):
                add(k.replace("berlin_", "", 1), station_id)
            else:
                add("berlin_" + k, station_id)

    for station_id, name in cur.fetchall():
        k1 = slugify(name)
        k2 = normalize_station_from_filename(name)

        add_with_berlin_variants(k1, station_id)
        add_with_berlin_variants(k2, station_id)

        if "hauptbahnhof" in k2:
            add_with_berlin_variants(k2.replace("hauptbahnhof", "hbf"), station_id)

    return lookup




def get_time_id(cur, ts: datetime) -> int:
    cur.execute("SELECT time_id FROM public.dim_time WHERE ts = %s;", (ts,))
    row = cur.fetchone()
    if row:
        return row[0]

    weekday = ts.isoweekday()
    day_type = "weekend" if weekday in (6, 7) else "weekday"

    cur.execute(
        """
        INSERT INTO public.dim_time (ts, date, year, month, day, hour, minute, weekday, day_type)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING time_id;
        """,
        (ts, ts.date(), ts.year, ts.month, ts.day, ts.hour, ts.minute, weekday, day_type),
    )
    return cur.fetchone()[0]


def get_or_create_train_id(cur, train_number, operator_code, train_category, trip_type, line) -> int:
    cur.execute(
        """
        SELECT train_id
        FROM public.dim_train
        WHERE train_number = %s
          AND operator_code = %s
          AND train_category IS NOT DISTINCT FROM %s
          AND trip_type IS NOT DISTINCT FROM %s
          AND line IS NOT DISTINCT FROM %s
        LIMIT 1;
        """,
        (train_number, operator_code, train_category, trip_type, line),
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        """
        INSERT INTO public.dim_train (train_number, operator_code, train_category, trip_type, line)
        VALUES (%s,%s,%s,%s,%s)
        RETURNING train_id;
        """,
        (train_number, operator_code, train_category, trip_type, line),
    )
    return cur.fetchone()[0]



# XML parsing

def parse_one_file(fp: Path):
    try:
        root = ET.parse(fp).getroot()
    except Exception:
        return []

    events = []
    for s in root.findall("./s"):
        trip_id = s.attrib.get("id")
        if not trip_id:
            continue

        tl = s.find("./tl")
        if tl is None:
            continue

        train_number = tl.attrib.get("n")
        operator_code = tl.attrib.get("o")
        train_category = tl.attrib.get("c")
        trip_type = tl.attrib.get("t")

        ar = s.find("./ar")
        if ar is not None:
            events.append((
                "AR", trip_id,
                ar.attrib.get("pt"),
                ar.attrib.get("pp"),
                ar.attrib.get("l"),
                train_number, operator_code, train_category, trip_type
            ))

        dp = s.find("./dp")
        if dp is not None:
            events.append((
                "DP", trip_id,
                dp.attrib.get("pt"),
                dp.attrib.get("pp"),
                dp.attrib.get("l"),
                train_number, operator_code, train_category, trip_type
            ))

    return events


def upsert_fact_batch(cur, rows):
    sql = """
    INSERT INTO public.fact_train_movement
      (station_id, time_id, train_id, is_arrival, is_departure,
       actual_time, delay_minutes, is_cancelled,
       platform, trip_id, snapshot_ts)
    VALUES %s
    ON CONFLICT ON CONSTRAINT uq_fact
    DO UPDATE SET
      platform = COALESCE(EXCLUDED.platform, public.fact_train_movement.platform),
      snapshot_ts = GREATEST(public.fact_train_movement.snapshot_ts, EXCLUDED.snapshot_ts);
    """
    execute_values(cur, sql, rows, page_size=5000)
    return len(rows)



# Main loader

def load_fact_timetables(limit_files: int | None = None):
    xml_files = list(TIMETABLES_ROOT.rglob("*_timetable.xml"))
    print(f"Found {len(xml_files)} timetable XML files under {TIMETABLES_ROOT}")
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
    skipped_events_no_pt = 0
    inserted = 0

    matched_by_filename = 0
    matched_by_xml_attr = 0

    # debugging: unmapped non-empty
    unmapped_nonempty = Counter()
    unmapped_examples = {}

    try:
        with conn.cursor() as cur:
            station_lookup = build_station_lookup(cur)
            batch = []

            for fp in xml_files:
                parsed_files += 1

                station_key = normalize_station_from_filename(fp.name)
                station_id = station_lookup.get(station_key)

                matched_by = None
                if station_id is not None:
                    matched_by = "filename"
                else:
                    xml_station = read_station_attr_quick(fp)
                    if xml_station:
                        # 1) try key-based variants
                        for k in normalize_station_from_xml_station(xml_station):
                            station_id = station_lookup.get(k)
                            if station_id is not None:
                                matched_by = "xml"
                                break

                        # 2) last resort: deterministic DB name token search
                        if station_id is None:
                            station_id = resolve_station_id_by_name_search(cur, xml_station)
                            if station_id is not None:
                                matched_by = "xml"





                if station_id is None:
                    skipped_station += 1
                    try:
                        if fp.stat().st_size > 60:
                            unmapped_nonempty[station_key] += 1
                            unmapped_examples.setdefault(station_key, fp.name)
                    except Exception:
                        pass
                    continue

                if matched_by == "filename":
                    matched_by_filename += 1
                elif matched_by == "xml":
                    matched_by_xml_attr += 1

                snapshot_ts = parse_snapshot_ts_from_folder(fp.parent.name)
                if snapshot_ts is None:
                    skipped_snapshot += 1
                    continue

                events = parse_one_file(fp)
                if not events:
                    continue

                for (etype, trip_id, pt, platform, line,
                     train_number, operator_code, train_category, trip_type) in events:

                    planned_ts = parse_pt(pt)
                    if planned_ts is None:
                        skipped_events_no_pt += 1
                        continue

                    time_id = get_time_id(cur, planned_ts)
                    train_id = get_or_create_train_id(
                        cur, train_number, operator_code, train_category, trip_type, line
                    )

                    batch.append((
                        station_id,
                        time_id,
                        train_id,
                        etype == "AR",
                        etype == "DP",
                        None,
                        None,
                        False,
                        platform,
                        trip_id,
                        snapshot_ts,
                    ))

                    if len(batch) >= 5000:
                        inserted += upsert_fact_batch(cur, batch)
                        batch.clear()

            if batch:
                inserted += upsert_fact_batch(cur, batch)
                batch.clear()

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    #print(f"Parsed timetable files: {parsed_files}")
    #print(f"Station mapped by filename: {matched_by_filename}")
    #print(f"Station mapped by XML attr fallback: {matched_by_xml_attr}")
    #print(f"Skipped files (station not mapped): {skipped_station}")
    #print(f"Skipped files (snapshot_ts parse failed): {skipped_snapshot}")
    #print(f"Skipped events (pt not parsed): {skipped_events_no_pt}")
    #print(f"Inserted/updated fact rows: {inserted}")

    print("\nTop unmapped (NON-EMPTY) station keys:")
    for k, c in unmapped_nonempty.most_common(30):
        print(f"  {c:6d}  {k}   (example: {unmapped_examples[k]})")

    print("fact_train_movement planned load done.")
    
    print("\nSample unmapped filenames:")
    for k, c in unmapped_nonempty.most_common(20):
        print(f"{k} -> {unmapped_examples[k]}")



if __name__ == "__main__":
    load_fact_timetables()
