"""
Verify both data sources are accessible before building the ML pipeline.
Run this before anything else in Layer 3.
"""

from pathlib import Path
from dotenv import load_dotenv
import os
import requests

load_dotenv(Path(__file__).parent.parent / "producer" / ".env")

def check_statsbomb():
    print("\n1. StatsBomb Open Data")
    print("─" * 40)
    try:
        from statsbombpy import sb
        comps = sb.competitions()
        pl = comps[
            (comps.competition_name == "Premier League") &
            (comps.country_name == "England")
        ]
        print(f"✅ StatsBomb accessible")
        print(f"   Premier League seasons available:")
        for _, row in pl.iterrows():
            print(f"   → {row.season_name} (season_id: {row.season_id})")
        return True
    except Exception as e:
        print(f"❌ StatsBomb failed: {e}")
        return False

def check_football_data_org():
    print("\n2. football-data.org")
    print("─" * 40)
    key = os.environ.get("FOOTBALL_DATA_API_KEY")
    if not key:
        print("❌ FOOTBALL_DATA_API_KEY not found in .env")
        return False
    try:
        resp = requests.get(
            "https://api.football-data.org/v4/competitions/PL/matches",
            headers={"X-Auth-Token": key},
            params={"season": 2026},
            timeout=15
        )
        data = resp.json()
        if resp.status_code != 200:
            print(f"❌ API error: {data.get('message')}")
            return False
        matches = data.get("matches", [])
        print(f"✅ football-data.org accessible")
        print(f"   2026/27 PL matches found: {len(matches)}")
        if matches:
            first = matches[0]
            print(f"   First match: {first['homeTeam']['name']} vs {first['awayTeam']['name']}")
            print(f"   Date: {first['utcDate'][:10]}")
        return True
    except Exception as e:
        print(f"❌ football-data.org failed: {e}")
        return False

if __name__ == "__main__":
    print("Verifying data sources for Layer 3 ML pipeline")
    print("=" * 50)
    sb_ok = check_statsbomb()
    fd_ok = check_football_data_org()
    print("\n" + "=" * 50)
    print("SUMMARY:")
    print(f"  StatsBomb:          {'✅' if sb_ok else '❌'}")
    print(f"  football-data.org:  {'✅' if fd_ok else '❌'}")
    if sb_ok and fd_ok:
        print("\n  Both sources ready — proceed with Layer 3")
    else:
        print("\n  Fix failing sources before proceeding")