import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values

from config import DB 


# db_project/
#   data/station_data.json
#   etl/load_stations.py
DATA_PATH = (Path(__file__).resolve().parents[1] / "data" / "station_data.json")


def normalize_station_json(raw: Any) -> List[Dict[str, Any]]:
    """
    station_data.json can be:
      - {"result": [ ...stations... ], ...}   <-- your case
      - [ ...stations... ]                   <-- sometimes datasets are a list
    """
    if isinstance(raw, dict):
        if "result" in raw and isinstance(raw["result"], list):
            return raw["result"]
        raise TypeError(f"station_data.json has dict format but no 'result' list. Keys={list(raw.keys())}")

    if isinstance(raw, list):
        return raw

    raise TypeError(f"station_data.json unexpected type: {type(raw)}")


def extract_main_eva_and_coords(station_obj: Dict[str, Any]) -> Tuple[Optional[int], Optional[float], Optional[float]]:
    """
    Returns: (eva_number, latitude, longitude)
    Coordinates in file are [lon, lat].
    """
    eva_list = station_obj.get("evaNumbers") or []
    if not isinstance(eva_list, list) or len(eva_list) == 0:
        return None, None, None

    # prefer isMain==true, else first
    main = None
    for e in eva_list:
        if isinstance(e, dict) and e.get("isMain") is True:
            main = e
            break
    if main is None:
        main = eva_list[0] if isinstance(eva_list[0], dict) else None

    if not isinstance(main, dict):
        return None, None, None

    eva_number = main.get("number")
    if not isinstance(eva_number, int):
        eva_number = None

    geo = main.get("geographicCoordinates") or {}
    coords = None
    if isinstance(geo, dict):
        coords = geo.get("coordinates")

    lat = lon = None
    # coordinates are [lon, lat]
    if isinstance(coords, list) and len(coords) >= 2:
        if isinstance(coords[0], (int, float)) and isinstance(coords[1], (int, float)):
            lon = float(coords[0])
            lat = float(coords[1])

    return eva_number, lat, lon


def load_stations() -> None:
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Could not find: {DATA_PATH}")

    with open(DATA_PATH, "r", encoding="utf-8") as f:
        raw = json.load(f)

    stations = normalize_station_json(raw)

    rows = []
    for st in stations:
        if not isinstance(st, dict):
            continue

        station_id = st.get("number") 
        name = st.get("name")
        category = st.get("category")

        if not isinstance(station_id, int):
            continue
        if not isinstance(name, str):
            name = None
        if not isinstance(category, int):
            category = None

        eva_number, lat, lon = extract_main_eva_and_coords(st)

        rows.append((station_id, name, eva_number, lat, lon, category))

    if not rows:
        print("No station rows found to insert.")
        return

    sql = """
        INSERT INTO public.dim_station
            (station_id, name, eva_number, latitude, longitude, category)
        VALUES %s
        ON CONFLICT (station_id) DO UPDATE SET
            name = EXCLUDED.name,
            eva_number = EXCLUDED.eva_number,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            category = EXCLUDED.category;
    """

    conn = psycopg2.connect(**DB)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
        conn.commit()
        print(f"Inserted {len(rows)} stations into dim_station")
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    load_stations()
