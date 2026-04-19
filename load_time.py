import os
from datetime import datetime
import psycopg2

from config import DB

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

TIMETABLES_DIR = os.path.join(PROJECT_ROOT, "data", "timetables")
CHANGES_DIR = os.path.join(PROJECT_ROOT, "data", "timetable_changes")


def parse_folder_timestamp(folder_name: str) -> datetime | None:
    """
    Accept:
      - 10 digits: YYMMDDHHMM  (e.g. 2509021600 -> 2025-09-02 16:00)
      - 12 digits: YYMMDDHHMMSS (rare, but handle safely)
    """
    if not folder_name.isdigit():
        return None
    if len(folder_name) == 10:
        return datetime.strptime(folder_name, "%y%m%d%H%M")
    if len(folder_name) == 12:
        return datetime.strptime(folder_name, "%y%m%d%H%M%S")
    return None


def iter_timestamp_folders(base_dir: str):
    """
    base_dir structure examples:
      data/timetables/250902_250909/2509021600/*.xml
      data/timetable_changes/250902_250909/2509021615/*.xml

    We want the timestamp folders (2509021600, 2509021615, ...)
    """
    if not os.path.isdir(base_dir):
        return

    for week in sorted(os.listdir(base_dir)):
        week_path = os.path.join(base_dir, week)
        if not os.path.isdir(week_path):
            continue

        # inside week folder: many timestamp folders
        for ts_folder in sorted(os.listdir(week_path)):
            ts_path = os.path.join(week_path, ts_folder)
            if not os.path.isdir(ts_path):
                continue

            dt = parse_folder_timestamp(ts_folder)
            if dt is None:
                continue

            yield dt


def upsert_time_rows(conn, timestamps: list[datetime]):
    """
    Insert into dim_time(ts, date, year, month, day, hour, minute, weekday, day_type)
    Assumes your dim_time has:
      time_id SERIAL PK
      ts TIMESTAMP UNIQUE (recommended)
    """
    if not timestamps:
        return 0

    rows = []
    for ts in timestamps:
        rows.append((
            ts,
            ts.date(),
            ts.year,
            ts.month,
            ts.day,
            ts.hour,
            ts.minute,
            ts.isoweekday(),         # 1=Mon..7=Sun
            "weekend" if ts.isoweekday() in (6, 7) else "weekday"
        ))

    sql = """
    INSERT INTO public.dim_time
      (ts, date, year, month, day, hour, minute, weekday, day_type)
    VALUES
      (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (ts) DO NOTHING;
    """

    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    return len(rows)


def load_time():
    all_ts = set()

    for dt in iter_timestamp_folders(TIMETABLES_DIR):
        all_ts.add(dt)

    for dt in iter_timestamp_folders(CHANGES_DIR):
        all_ts.add(dt)

    if not all_ts:
        print(f"No timestamp folders found in:\n- {TIMETABLES_DIR}\n- {CHANGES_DIR}")
        return

    timestamps = sorted(all_ts)

    conn = psycopg2.connect(**DB)
    try:
        inserted = upsert_time_rows(conn, timestamps)
        print(f"Inserted {inserted} time rows. Unique timestamps: {len(timestamps)}")
    finally:
        conn.close()


if __name__ == "__main__":
    load_time()
