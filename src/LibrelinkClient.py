import requests
import json
import os
import dotenv
from datetime import datetime, timedelta


def convert_timestamp_string_to_datetime(timestamp):
    # "8/16/2023 10:16:34 AM"
    return datetime.strptime(timestamp, "%m/%d/%Y %I:%M:%S %p")


class LibrelinkClient:
    def __init__(
        self, username, password, url="https://api-eu2.libreview.io", version="4.7.0"
    ):
        self.username = username
        self.password = password
        self.url = url
        self.version = version
        self.headers = {
            "product": "llu.android",
            "version": version,
            "accept-encoding": "gzip",
            "cache-control": "no-cache",
            "connection": "Keep-Alive",
            "content-type": "application/json",
        }
        self.jwt_token = None
        self.login()
        self.patient_id = self.get_patient_id()
        self.low_bound = 3.5
        self.high_bound = 8.5

    def login(self):
        response = requests.post(
            f"{self.url}/llu/auth/login",
            headers=self.headers,
            json={"email": self.username, "password": self.password},
        )
        response.raise_for_status()
        data = response.json()
        self.jwt_token = data["data"]["authTicket"]["token"]
        self.headers["Authorization"] = f"Bearer {self.jwt_token}"

    def get_patient_id(self):
        response = requests.get(f"{self.url}/llu/connections", headers=self.headers)
        response.raise_for_status()
        connections = response.json()
        return connections["data"][0]["patientId"]

    def get_cgm_data(self):
        response = requests.get(
            f"{self.url}/llu/connections/{self.patient_id}/graph", headers=self.headers
        )
        response.raise_for_status()
        cgm_data = response.json()
        return cgm_data

    def get_logbook_data(self):
        response = requests.get(
            f"{self.url}/llu/connections/{self.patient_id}/logbook",
            headers=self.headers,
        )
        response.raise_for_status()
        logbook_data = response.json()
        return logbook_data

    def save_to_json(self, data, filename):
        with open(filename, "w") as f:
            json.dump(data, f, indent=4, sort_keys=True)

    def get_latest_reading(self):
        cgm_data = self.get_cgm_data()
        latest_reading = cgm_data["data"]["connection"]["glucoseItem"]["Value"]
        return latest_reading

    def is_in_range(self, reading: float, d) -> bool:
        # in_range = self.low_bound <= reading <= self.high_bound
        # print(f"Reading: {reading}, in range: {in_range}, d: {d}")
        return self.low_bound <= reading <= self.high_bound

    def percentage_in_range(self, source: str = "graph"):
        if source == "logbook":
            cgm_data = self.get_logbook_data()
            graph_data = cgm_data["data"]
        else:
            cgm_data = self.get_cgm_data()
            graph_data = cgm_data["data"]["graphData"]
        now = datetime.now()
        last_24_hours = now - timedelta(hours=23)
        readings_in_last_24_hours = [
            d
            for d in graph_data
            if datetime.strptime(d["Timestamp"], "%m/%d/%Y %I:%M:%S %p") > last_24_hours
            and d.get("Value", None)
        ]
        percent_in_range = round(
            100.0
            * sum(
                [
                    self.is_in_range(d["Value"], d["Timestamp"])
                    for d in readings_in_last_24_hours
                ]
            )
            / len(readings_in_last_24_hours),
            1,
        )
        return percent_in_range
