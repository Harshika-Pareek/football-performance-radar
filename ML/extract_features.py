"""
Extract per-team attack/defence strength features from 2025/26 Premier League
match results (football-data.org) and save them to ml/pl_features_2025.csv.
"""
import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# Step 1: Load the API key the same way test_pl_2025_data.py does -
# the key lives in producer/.env, not in this ml/ folder, so we point
# load_dotenv at that file explicitly.
load_dotenv(Path(__file__).parent.parent / "producer" / ".env")
key = os.environ["FOOTBALL_DATA_API_KEY"]

# Step 2: Fetch every FINISHED match of the 2025/26 PL season in one call.
# football-data.org returns the whole competition's matches as a single
# JSON payload, so no pagination is needed here.
resp = requests.get(
    "https://api.football-data.org/v4/competitions/PL/matches",
    headers={"X-Auth-Token": key},
    params={"season": 2025, "status": "FINISHED"},
    timeout=15,
)
resp.raise_for_status()
matches = resp.json().get("matches", [])
print(f"Fetched {len(matches)} finished 2025/26 PL matches")

# Step 3: Flatten the nested JSON into one row per match with just the
# fields we need (team names + final score). Working with a flat list of
# dicts makes it trivial to hand off to pandas next.
rows = []
for m in matches:
    home_goals = m["score"]["fullTime"]["home"]
    away_goals = m["score"]["fullTime"]["away"]
    # Guard against any in-progress/edge-case match that slipped through
    # without a final score.
    if home_goals is None or away_goals is None:
        continue
    rows.append(
        {
            "home_team": m["homeTeam"]["name"],
            "away_team": m["awayTeam"]["name"],
            "home_goals": home_goals,
            "away_goals": away_goals,
        }
    )

df = pd.DataFrame(rows)

# Step 4: Compute each team's home-venue stats by grouping all matches
# where the team played at home. groupby + mean gives us, per team, the
# average goals it scored (attack) and the average goals it conceded
# (defence) while playing at home.
home_stats = (
    df.groupby("home_team")
    .agg(home_attack=("home_goals", "mean"), home_defence=("away_goals", "mean"))
    .reset_index()
    .rename(columns={"home_team": "team"})
)

# Step 5: Same idea for away-venue stats - group by away_team instead,
# so "attack" here means goals scored while away and "defence" means
# goals conceded while away.
away_stats = (
    df.groupby("away_team")
    .agg(away_attack=("away_goals", "mean"), away_defence=("home_goals", "mean"))
    .reset_index()
    .rename(columns={"away_team": "team"})
)

# Step 6: Merge the two halves into one row per team so every team has
# all four strength numbers side by side.
features = home_stats.merge(away_stats, on="team", how="outer").round(2)

# Step 7: Print the requested summaries.
# Top 5 strongest home teams = highest average goals scored at home.
print("\nTop 5 strongest home attacks:")
print(
    features.sort_values("home_attack", ascending=False)
    .head(5)[["team", "home_attack"]]
    .to_string(index=False)
)

# Top 5 weakest away defences = highest average goals conceded away
# (conceding more goals means a weaker defence).
print("\nTop 5 weakest away defences:")
print(
    features.sort_values("away_defence", ascending=False)
    .head(5)[["team", "away_defence"]]
    .to_string(index=False)
)

# Step 8: Persist the full feature table for downstream use (e.g. the ML
# model in this branch) as a CSV next to this script.
out_path = Path(__file__).parent / "pl_features_2025.csv"
features.to_csv(out_path, index=False)
print(f"\nSaved features for {len(features)} teams to {out_path}")
