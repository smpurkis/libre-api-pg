import json
import psycopg2
from libre_link_up import LibreLinkUpClient
from libre_link_up.custom_types import GlucoseSensorReading, LibreLinkUpUrl
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
local_timezone = "Europe/London"


def make_db_connection():
    conn = psycopg2.connect(
        host=os.environ["PG_HOST"],
        port=os.environ["PG_PORT"],
        database=os.environ["PG_DATABASE"],
        user=os.environ["PG_USERNAME"],
        password=os.environ["PG_PASSWORD"],
    )
    return conn


def batch_insert_glucose_readings(conn, readings: list[GlucoseSensorReading]):
    cur = conn.cursor()
    for reading in readings:
        # data = json.loads(reading.model_dump_json())
        
        unix_timestamp = reading.unix_timestamp
        value = reading.value
        value_in_mg_per_dl = reading.value_in_mg_per_dl
        iso_datetime = datetime.utcfromtimestamp(reading.unix_timestamp).isoformat() + "Z"
        # Assuming source is available on the object
        source = getattr(reading, "source", None)
        if source:
            source = str(source.value) if hasattr(source, "value") else str(source)
        
        try:
            cur.execute(
                """
                INSERT INTO glucose_reading (unix_timestamp, value, value_in_mg_per_dl, iso_datetime, source)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (unix_timestamp, source) DO NOTHING
                """,
                (unix_timestamp, value, value_in_mg_per_dl, iso_datetime, source)
            )
            if cur.rowcount > 0:
                print(f"Inserting reading for time: {iso_datetime}")
        except Exception as e:
            print(f"Error inserting reading: {e}")
            conn.rollback()
            continue
            
    conn.commit()
    cur.close()


def run():
    """Example of how to use the Postgres client."""
    conn = make_db_connection()
    client = LibreLinkUpClient(
        username=os.environ["LIBRE_LINK_UP_USERNAME"],
        password=os.environ["LIBRE_LINK_UP_PASSWORD"],
        url=LibreLinkUpUrl.EU.value,
    )
    client.login()

    readings = client.get_logbook_readings()
    if len(readings) > 0:
        print("Successful run")
    batch_insert_glucose_readings(conn, readings)

    readings = client.get_graph_readings()
    batch_insert_glucose_readings(conn, readings)

    latest_reading = client.get_latest_reading()
    batch_insert_glucose_readings(conn, [latest_reading])
    conn.close()


if __name__ == "__main__":
    run()
