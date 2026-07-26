import requests, os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / "producer" / ".env")

key = os.environ["FOOTBALL_DATA_API_KEY"]

resp = requests.get(
    "https://api.football-data.org/v4/competitions/PL/matches",
    headers={"X-Auth-Token": key},
    params={"season": 2025, "status": "FINISHED"},
    timeout=15
)

data = resp.json()
matches = data.get("matches", [])
print(f"2025/26 PL completed matches: {len(matches)}")
if matches:
    m = matches[0]
    print(f"Sample: {m['homeTeam']['name']} {m['score']['fullTime']['home']}-{m['score']['fullTime']['away']} {m['awayTeam']['name']}")