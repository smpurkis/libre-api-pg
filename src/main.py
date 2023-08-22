import dotenv
import os
from LibrelinkClient import LibrelinkClient

# Load environment variables
dotenv.load_dotenv()

# Replace with your login credentials
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")

assert username, "Username not set"
assert password, "Password not set"

client = LibrelinkClient(username, password)
client.login()
latest_reading = client.get_latest_reading()
print(f"Latest glucose reading: {latest_reading}")

percentage_in_range = client.percentage_in_range()
print(f"Percentage in range in the last 24 hours: {percentage_in_range}%")

cgm_data = client.get_cgm_data()
client.save_to_json(cgm_data, "cgm_data.json")

logbook_data = client.get_logbook_data()
client.save_to_json(logbook_data, "logbook_data.json")
