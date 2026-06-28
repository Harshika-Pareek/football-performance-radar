"""
Run this FIRST, before anything else, to confirm your API key works.
The /status endpoint does NOT count against your daily quota, so this is free to run.
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ["API_FOOTBALL_KEY"]

resp = requests.get(
    "https://v3.football.api-sports.io/status",
    headers={"x-apisports-key": API_KEY},
    timeout=15,
)

print("Status code:", resp.status_code)
print(resp.json())