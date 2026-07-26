"""
Test if 2025/26 Premier League historical data is accessible
on the free API-Football tier.

Run this first thing tomorrow before doing anything else.
"""

import requests
import os
import json
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent.parent / "producer" / ".env")

API_KEY = os.environ["API_FOOTBALL_KEY"]
HEADERS = {"x-apisports-key": API_KEY}
BASE_URL = "https://v3.football.api-sports.io"

def test_pl_access():
    print("Testing Premier League 2025/26 access...")
    
    resp = requests.get(
        f"{BASE_URL}/fixtures",
        headers=HEADERS,
        params={
            "league": 39,      # Premier League
            "season": 2025,    # 2025/26 season
            "round": "Regular Season - 1"
        },
        timeout=15
    )
    
    data = resp.json()
    
    if data.get("errors"):
        print(f"❌ BLOCKED: {data['errors']}")
        print("→ Will use World Cup 2022 data for training instead")
        return False
    
    fixtures = data.get("response", [])
    print(f"✅ ACCESSIBLE: {len(fixtures)} fixtures found")
    print(f"Requests used today: {data['paging']}")
    
    if fixtures:
        first = fixtures[0]
        print(f"\nSample fixture:")
        print(f"  {first['teams']['home']['name']} vs {first['teams']['away']['name']}")
        print(f"  Date: {first['fixture']['date']}")
        print(f"  Score: {first['goals']['home']}-{first['goals']['away']}")
    
    return True

def test_wc2022_access():
    print("\nTesting World Cup 2022 access (backup)...")
    
    resp = requests.get(
        f"{BASE_URL}/fixtures",
        headers=HEADERS,
        params={"league": 1, "season": 2022},
        timeout=15
    )
    
    data = resp.json()
    fixtures = data.get("response", [])
    print(f"✅ World Cup 2022: {len(fixtures)} fixtures confirmed accessible")
    return len(fixtures) > 0

def check_remaining_quota():
    resp = requests.get(
        f"{BASE_URL}/status",
        headers=HEADERS,
        timeout=15
    )
    data = resp.json()
    requests_used = data["response"]["requests"]["current"]
    requests_limit = data["response"]["requests"]["limit_day"]
    print(f"\nAPI quota: {requests_used}/{requests_limit} used today")

if __name__ == "__main__":
    check_remaining_quota()
    pl_available = test_pl_access()
    wc_available = test_wc2022_access()
    
    print("\n" + "="*50)
    print("TOMORROW'S DATA PLAN:")
    if pl_available:
        print("✅ Use Premier League 2025/26 — train on full season")
        print("✅ Predict 2026/27 Week 1 fixtures (21 Aug)")
    else:
        print("⚠️  Premier League blocked on free tier")
        print("✅ Load all 64 World Cup 2022 matches instead")
        print("✅ Still predict 2026/27 Week 1 using team patterns")
    print("="*50)