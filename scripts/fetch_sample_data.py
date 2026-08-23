"""Regenerate examples/data/*.

Trims the public NYC TLC Yellow Taxi Trip Records (January 2024) and the
Taxi Zone Lookup Table down to the small, offline-friendly fixtures the
examples run against by default. See examples/data/README.md.

Run: `uv run python scripts/fetch_sample_data.py`
"""

from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "examples" / "data"

TRIPS_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")

    sample_path = DATA_DIR / "nyc_taxi_sample.parquet"
    con.execute(
        f"""
        COPY (
            SELECT
                VendorID,
                tpep_pickup_datetime,
                tpep_dropoff_datetime,
                passenger_count,
                trip_distance,
                PULocationID,
                DOLocationID,
                payment_type,
                fare_amount,
                tip_amount,
                total_amount
            FROM read_parquet('{TRIPS_URL}')
            WHERE tpep_pickup_datetime >= '2024-01-01'
              AND tpep_pickup_datetime < '2024-01-08'
            USING SAMPLE 60000 ROWS
        ) TO '{sample_path}' (FORMAT PARQUET)
        """
    )
    n = con.execute(f"SELECT count(*) FROM '{sample_path}'").fetchone()[0]
    print(f"wrote {sample_path} ({n} rows)")

    zones_path = DATA_DIR / "taxi_zone_lookup.csv"
    con.execute(
        f"COPY (SELECT * FROM read_csv('{ZONES_URL}')) TO '{zones_path}' (HEADER, FORMAT CSV)"
    )
    print(f"wrote {zones_path}")

    con.close()


if __name__ == "__main__":
    main()
