import json
from surrealdb import Surreal
from libre_link_up import LibreLinkUpClient
from libre_link_up.types import GlucoseSensorReading, LibreLinkUpUrl
import hashlib
import os
import pytz
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
local_timezone = "Europe/London"


async def make_db_connection() -> Surreal:
    db = Surreal(url=f"ws://{os.environ['DB_URL']}/rpc")
    await db.connect()
    await db.signin(
        {
            "user": os.environ["DB_USERNAME"],
            "pass": os.environ["DB_PASSWORD"],
            "NS": os.environ["DB_NAMESPACE"],
            "DB": os.environ["DB_DATABASE"],
        }
    )
    return db


async def generate_tables_with_schema(db: Surreal):
    table_name = "glucose_reading"

    # check if table exists
    if table_name in (await db.query("INFO FOR DB"))[0]["result"]["tables"]:
        return

    print(f"Creating table: {table_name}")
    await db.query(f"CREATE {table_name}")
    for field_name, field_type in [
        ("unix_timestamp", "float"),
        ("value", "float"),
        ("value_in_mg_per_dl", "float"),
        ("iso_datetime", "datetime"),
        ("source", "string"),
    ]:
        await db.query(
            f"DEFINE FIELD {field_name} ON TABLE {table_name} TYPE {field_type}"
        )


async def batch_insert_glucose_readings(
    db: Surreal, readings: list[GlucoseSensorReading]
):
    for reading in readings:
        data = json.loads(reading.model_dump_json())
        print(reading.unix_timestamp)
        iso_datetime = (
            pytz.timezone(local_timezone)
            .localize(datetime.fromtimestamp(reading.unix_timestamp))
            .astimezone(pytz.utc)
            .strftime("%Y-%m-%dT%H:%M:%SZ")
        )
        data["iso_datetime"] = iso_datetime
        data_hash = hashlib.sha256(str(data).encode()).hexdigest()
        data_id = f"glucose_reading:{data_hash}"
        if await db.select(data_id):
            continue

        print(f"Inserting reading for time: {iso_datetime}")
        await db.create(data_id, data)


async def run():
    """Example of how to use the SurrealDB client."""
    # db = await make_db_connection()
    db = None
    client = LibreLinkUpClient(
        username=os.environ["LIBRE_LINK_UP_USERNAME"],
        password=os.environ["LIBRE_LINK_UP_PASSWORD"],
        url=LibreLinkUpUrl.EU.value,
    )
    client.login()
    # await generate_tables_with_schema(db)

    # readings = client.get_logbook_readings()
    # if len(readings) > 0:
    #     print("Successful run")
    # await batch_insert_glucose_readings(db, readings)

    readings = client.get_graph_readings()
    await batch_insert_glucose_readings(db, readings)

    latest_reading = client.get_latest_reading()
    await batch_insert_glucose_readings(db, [latest_reading])
    # await db.close()


if __name__ == "__main__":
    import asyncio

    asyncio.run(run())
