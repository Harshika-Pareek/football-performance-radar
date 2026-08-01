import requests
import json
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "producer" / ".env")
key = os.environ["FOOTBALL_DATA_API_KEY"]

resp = requests.get(
    "https://api.football-data.org/v4/competitions/PL/matches",
    headers={"X-Auth-Token": key},
    params={"season": 2025, "status": "FINISHED"},
    timeout=15,
)

data = resp.json()

# See the top-level keys
print("Top level keys:", data.keys())

# See first match raw
print("\nFirst match raw JSON:")
print(json.dumps(data["matches"][0], indent=2))