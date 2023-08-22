import streamlit as st
import pandas as pd
from src.LibrelinkClient import LibrelinkClient
import dotenv
import os
import datetime
import arrow

time_now = arrow.now(tz="Europe/London")

# Load environment variables
dotenv.load_dotenv()

# Replace with your login credentials
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")
client = LibrelinkClient(username, password)


def get_latest_reading():
    latest_reading = client.get_latest_reading()
    return latest_reading


def percentage_in_range():
    percent_in_range = client.percentage_in_range()
    return percent_in_range


st.title("LibreLink App")

st.write(f"Last updated: {time_now.humanize()} ({time_now})")

latest_reading = get_latest_reading()
st.write(f"Latest reading: {latest_reading}")

percent_in_range = percentage_in_range()
red = "#f63366"
green = "#00cc96"
color = red if percent_in_range < 70 else green
st.write(
    # f"Percentage of readings in range in last 24 hours: {percent_in_range}%"
    f"""
    <div style="background-color:{color};padding:10px">
    <h2 style="color:white;text-align:center;">Percentage of readings in range in last 24 hours</h2>
    <h1 style="color:white;text-align:center;">{percent_in_range}%</h1>
    </div>
    """,
    unsafe_allow_html=True,
)
