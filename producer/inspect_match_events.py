import json
import os

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ["API_FOOTBALL_KEY"]
FIXTURE_ID = 855735  # England 6-2 Iran

resp = requests.get(
    "https://v3.football.api-sports.io/fixtures/events",
    headers={"x-apisports-key": API_KEY},
    params={"fixture": FIXTURE_ID},
    timeout=15,
)

events = resp.json()["response"]

for e in events:
    print(f"{e['time']['elapsed']}' - {e['type']} - {e['team']['name']} - "
          f"{e['player']['name']} - {e.get('detail', '')}")