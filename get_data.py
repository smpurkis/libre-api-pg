import requests
import json

import dotenv

import os

# Load environment variables
dotenv.load_dotenv()

# Replace with your login credentials
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")

assert username, "Username not set"
assert password, "Password not set"

# Replace with the URL of the login endpoint
url = "https://api-eu2.libreview.io"

version = "4.7.0"

headers = {
    "product": "llu.android",
    "version": version,
    "accept-encoding": "gzip",
    "cache-control": "no-cache",
    "connection": "Keep-Alive",
    "content-type": "application/json",
}

# Login and acquire JWT token
response = requests.post(
    f"{url}/llu/auth/login",
    headers=headers,
    json={"email": username, "password": password},
)
assert response.ok, response.reason
data = response.json()
jwt_token = data["data"]["authTicket"]["token"]

# add token to headers
headers["Authorization"] = f"Bearer {jwt_token}"

# # Get patient ID
response = requests.get(f"{url}/llu/connections", headers=headers)
assert response.ok, response.reason
connections = response.json()
patient_id = connections["data"][0]["patientId"]
with open(f"connections_{version}.json", "w") as f:
    json.dump(connections, f, indent=4, sort_keys=True)
# patient_id = "3927f270-4f00-11ed-96e1-0242ac110007"


# Get CGM data
response = requests.get(f"{url}/llu/connections/{patient_id}/graph", headers=headers)
assert response.ok, response.reason
cgm_data = response.json()

# Save CGM data as JSON
with open(f"cgm_data_{version}.json", "w") as f:
    json.dump(cgm_data, f, indent=4, sort_keys=True)


# Get CGM data
response = requests.get(f"{url}/llu/connections/{patient_id}/logbook", headers=headers)
assert response.ok, response.reason
cgm_data = response.json()

# Save CGM data as JSON
with open(f"logbook_{version}.json", "w") as f:
    json.dump(cgm_data, f, indent=4, sort_keys=True)
