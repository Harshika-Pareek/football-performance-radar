import json
import os

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ["API_FOOTBALL_KEY"]

resp = requests.get(
    "https://v3.football.api-sports.io/fixtures",
    headers={"x-apisports-key": API_KEY},
    params={"league": 1, "season": 2022},
    timeout=15,
)

data = resp.json()

matches = []
for f in data["response"]:
    matches.append({
        "id": f["fixture"]["id"],
        "date": f["fixture"]["date"][:10],
        "home": f["teams"]["home"]["name"],
        "away": f["teams"]["away"]["name"],
        "score": f"{f['goals']['home']}-{f['goals']['away']}",
    })

print(json.dumps(matches[:15], indent=2))