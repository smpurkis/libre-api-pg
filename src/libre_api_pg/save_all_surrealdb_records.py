import json
from surrealdb import Surreal
import os
from dotenv import load_dotenv
import asyncio
import bson
from tqdm import tqdm
import datetime
import dateutil.parser

load_dotenv()

STATE_FILE = "export_state.json"
OUTPUT_FILENAME = "glucose_readings_export.bson"
TABLE_NAME = "glucose_reading"
# IMPORTANT: Replace 'created_at' with the actual sortable timestamp field in your table
TIMESTAMP_FIELD = "created_at"
BATCH_SIZE = 1000


def load_last_timestamp(state_file: str) -> str | None:
    """Loads the last exported timestamp from the state file."""
    if not os.path.exists(state_file):
        return None
    try:
        with open(state_file, "r") as f:
            data = json.load(f)
            return data.get("last_timestamp")
    except (json.JSONDecodeError, FileNotFoundError, KeyError):
        print(f"Warning: Could not read or parse state file '{state_file}'.")
        return None


def save_last_timestamp(state_file: str, timestamp: str):
    """Saves the last exported timestamp to the state file."""
    try:
        with open(state_file, "w") as f:
            json.dump({"last_timestamp": timestamp}, f)
    except IOError as e:
        print(f"Error: Could not write to state file '{state_file}': {e}")


async def make_db_connection() -> Surreal:
    """Establishes connection to the SurrealDB database."""
    db_url = os.environ.get("DB_URL")
    db_user = os.environ.get("DB_USERNAME")
    db_pass = os.environ.get("DB_PASSWORD")
    db_ns = os.environ.get("DB_NAMESPACE")
    db_db = os.environ.get("DB_DATABASE")

    if not all([db_url, db_user, db_pass, db_ns, db_db]):
        raise ValueError(
            "Missing one or more database environment variables (DB_URL, DB_USERNAME, DB_PASSWORD, DB_NAMESPACE, DB_DATABASE)"
        )

    db = Surreal(url=f"ws://{db_url}/rpc")
    await db.connect()
    await db.signin(
        {
            "user": db_user,
            "pass": db_pass,
            "NS": db_ns,
            "DB": db_db,
        }
    )
    print(f"Connected to SurrealDB - Namespace: {db_ns}, Database: {db_db}")
    return db


async def run():
    """
    Fetches only new records from the specified table since the last export,
    appends them as individual BSON documents to a file, and shows progress.
    """
    db = None
    last_timestamp_str = load_last_timestamp(STATE_FILE)
    new_records_count = 0
    max_timestamp_in_batch = (
        last_timestamp_str  # Initialize with the last known timestamp
    )

    try:
        db = await make_db_connection()

        # --- Build Query Conditions ---
        where_clause = ""
        params = {}
        if last_timestamp_str:
            # Validate timestamp format before using in query
            try:
                # Attempt to parse to ensure it's a valid datetime string SurrealDB might understand
                # Note: SurrealDB datetime handling can be specific. Adjust parsing/formatting if needed.
                dateutil.parser.isoparse(last_timestamp_str)
                where_clause = f"WHERE {TIMESTAMP_FIELD} > $last_ts"
                params["last_ts"] = last_timestamp_str
                print(f"Fetching records newer than {last_timestamp_str}")
            except ValueError:
                print(
                    f"Warning: Invalid timestamp format found in state file: '{last_timestamp_str}'. Performing full fetch."
                )
                last_timestamp_str = None  # Reset to trigger full fetch logic
                # Optionally: Delete or rename the invalid state file here
        else:
            print("No previous timestamp found. Fetching all records.")
            # If no timestamp, fetch all records (first run or state file issue)

        # --- Get Count of New Records ---
        print(f"Counting new records in '{TABLE_NAME}'...")
        count_query = (
            f"SELECT count() AS total FROM {TABLE_NAME} {where_clause} GROUP ALL;"
        )
        count_response = await db.query(count_query, params)

        # --- Validate Count Response ---
        total_new_records = 0
        try:
            if (
                not count_response
                or not isinstance(count_response, list)
                or len(count_response) == 0
                or count_response[0].get("status") != "OK"
                or not isinstance(count_response[0].get("result"), list)
            ):
                raise ValueError(
                    f"Invalid response structure received for count query: {count_response}"
                )

            count_result = count_response[0]["result"]
            if count_result:
                if "total" not in count_result[0]:
                    raise ValueError(f"Unexpected count result format: {count_result}")
                total_new_records = count_result[0]["total"]

        except Exception as e:
            print(f"Error: Could not retrieve or parse new record count: {e}")
            return

        if total_new_records == 0:
            print(f"No new records found in '{TABLE_NAME}' to export.")
            return
        else:
            print(f"Found {total_new_records} new records to export.")
        # --- End Get Count ---

        print(f"\nStarting export append to {OUTPUT_FILENAME}...")
        start_at = 0
        records_processed_this_run = 0

        # Determine file mode: 'ab' (append binary) if state exists and is valid, otherwise 'wb' (write binary)
        file_mode = (
            "ab" if last_timestamp_str and os.path.exists(OUTPUT_FILENAME) else "wb"
        )
        if file_mode == "wb":
            print(
                f"Performing initial full export or overwriting due to missing state/output file."
            )

        # --- Use tqdm for Progress Bar ---
        with (
            tqdm(
                total=total_new_records,
                unit="record",
                desc=f"Appending {TABLE_NAME}",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]",
                ncols=100,
            ) as pbar,
            open(OUTPUT_FILENAME, file_mode) as f,  # Use determined file mode
        ):
            while records_processed_this_run < total_new_records:
                # Fetch batch - IMPORTANT: Order by timestamp to correctly track the latest
                query = f"SELECT * FROM {TABLE_NAME} {where_clause} ORDER BY {TIMESTAMP_FIELD} ASC LIMIT $limit START $start"
                batch_params = {**params, "limit": BATCH_SIZE, "start": start_at}
                response = await db.query(query, batch_params)

                # --- Response Handling ---
                if not response or not isinstance(response, list) or len(response) == 0:
                    pbar.set_postfix_str(
                        "Warning: Received unexpected empty response", refresh=True
                    )
                    await asyncio.sleep(0.1)
                    continue

                query_result = response[0]
                if query_result.get("status") != "OK":
                    error_message = query_result.get("result", "Unknown error")
                    pbar.set_postfix_str(f"DB Error: {error_message}", refresh=True)
                    print(f"\nError fetching data batch: {error_message}")
                    break

                records = query_result.get("result", [])
                # --- End Response Handling ---

                if not records:
                    # Should not happen if count was accurate and loop condition is correct, but check anyway
                    print(
                        f"\nWarning: No more records found via query at start {start_at}, "
                        f"but expected {total_new_records - records_processed_this_run} more."
                    )
                    break

                num_fetched = len(records)
                batch_latest_ts = None

                # Process and write batch
                for record in records:
                    if isinstance(record, dict):
                        try:
                            # Track the latest timestamp in this batch
                            current_ts = record.get(TIMESTAMP_FIELD)
                            if current_ts:
                                # Basic string comparison works for ISO formats
                                if (
                                    max_timestamp_in_batch is None
                                    or current_ts > max_timestamp_in_batch
                                ):
                                    max_timestamp_in_batch = current_ts

                            bson_doc = bson.dumps(record)
                            f.write(bson_doc)
                        except Exception as bson_err:
                            pbar.set_postfix_str(
                                f"BSON Error: {bson_err}", refresh=True
                            )
                    else:
                        pbar.set_postfix_str(
                            f"Skipping non-dict: {type(record)}", refresh=True
                        )

                records_processed_this_run += num_fetched
                # NOTE: For `START` with `ORDER BY`, START refers to the Nth record in the *sorted* result set,
                # so incrementing it by num_fetched is correct here.
                start_at += num_fetched
                pbar.update(num_fetched)

        # --- Final Summary ---
        print(f"\nExport finished.")
        if records_processed_this_run != total_new_records:
            print(
                f"Warning: Expected to process {total_new_records} new records, but {records_processed_this_run} were written."
            )
        else:
            print(f"Successfully appended {records_processed_this_run} new records.")

        # Save the latest timestamp processed in this run
        if max_timestamp_in_batch and records_processed_this_run > 0:
            # Only update state if new records were actually processed and a valid timestamp was found
            if (
                max_timestamp_in_batch != last_timestamp_str
            ):  # Check if timestamp actually changed
                save_last_timestamp(STATE_FILE, max_timestamp_in_batch)
                print(f"Updated last timestamp to: {max_timestamp_in_batch}")
            else:
                print(
                    "No newer timestamp found in the processed records. State file not updated."
                )
        elif records_processed_this_run > 0:
            print(
                f"Warning: Processed {records_processed_this_run} records but couldn't determine a new latest timestamp (missing '{TIMESTAMP_FIELD}'?). State file not updated."
            )

        print(f"Data saved to: {OUTPUT_FILENAME}")

    except FileNotFoundError:
        print("Error: Could not find .env file.")
    except ValueError as ve:
        print(f"Configuration or Data Error: {ve}")
    except ConnectionError as ce:
        print(f"Database Connection Error: {ce}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        import traceback

        traceback.print_exc()
    finally:
        # Check if the db object was successfully created before trying to close
        if db:
            try:
                print("Closing database connection...")
                await db.close()
                print("Database connection closed.")
            except Exception as close_err:
                # Log error if closing fails, but don't prevent script exit
                print(f"Error closing database connection: {close_err}")


if __name__ == "__main__":
    print("Running SurrealDB incremental data export script...")
    # Ensure TIMESTAMP_FIELD is set correctly
    if TIMESTAMP_FIELD == "created_at":  # Default placeholder check
        print(
            f"Warning: Using default timestamp field '{TIMESTAMP_FIELD}'. Ensure this field exists and is sortable in your '{TABLE_NAME}' table."
        )
    elif not TIMESTAMP_FIELD:
        print(
            f"Error: TIMESTAMP_FIELD constant is not set. Please define the sortable timestamp field name."
        )
        exit()  # Exit if not configured

    asyncio.run(run())
    print("\nScript finished.")

    # --- Added Section: Print Example SQL Schema ---
    print("\n--- Example SQL Schema (PostgreSQL) ---")
    print(f"""
-- This is a generic example schema based on common glucose reading fields.
-- You MUST adapt this to your actual data structure exported from SurrealDB.
-- Data types (especially for IDs and timestamps) might need adjustment based on the target SQL database.

CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    surreal_id VARCHAR(255) PRIMARY KEY, -- Assuming SurrealDB ID like 'glucose_reading:xxxx'
    value INTEGER,                      -- Or FLOAT/DECIMAL depending on precision needed
    unit VARCHAR(50),
    timestamp TIMESTAMPTZ,              -- Timestamp of the reading itself
    device_id VARCHAR(100),
    user_id VARCHAR(100),               -- Or link to a users table
    created_at TIMESTAMPTZ,             -- Record creation timestamp (used for sorting)
    -- Add other fields from your SurrealDB table here...
    raw_bson BYTEA                      -- Optional: Store the original BSON document
);

-- Optional: Add indexes for faster querying
CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_timestamp ON {TABLE_NAME} (timestamp);
CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_created_at ON {TABLE_NAME} (created_at);
CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_user_device ON {TABLE_NAME} (user_id, device_id);

""")
    # --- End Added Section ---
