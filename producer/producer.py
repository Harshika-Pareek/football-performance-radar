"""
World Cup match event producer.

Polls API-Football for live World Cup fixtures + events and pushes
each fixture's current state onto a Kafka topic.

Free tier = 100 requests/day, so this defaults to polling every 90 seconds.
"""

import json
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

API_KEY = os.environ["API_FOOTBALL_KEY"]
BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_KEY}

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "worldcup_match_events")

WORLD_CUP_LEAGUE_ID = 1
WORLD_CUP_SEASON = 2026

POLL_INTERVAL_SECONDS = 90


def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def fetch_live_fixtures():
    resp = requests.get(
        f"{BASE_URL}/fixtures",
        headers=HEADERS,
        params={"league": WORLD_CUP_LEAGUE_ID, "season": WORLD_CUP_SEASON, "live": "all"},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()

    if data.get("errors"):
        print(f"[WARN] API returned errors: {data['errors']}")

    return data.get("response", [])


def fetch_fixture_events(fixture_id):
    resp = requests.get(
        f"{BASE_URL}/fixtures/events",
        headers=HEADERS,
        params={"fixture": fixture_id},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("response", [])


def build_message(fixture, events):
    teams = fixture["teams"]
    fixture_info = fixture["fixture"]
    goals = fixture["goals"]

    return {
        "fixture_id": fixture_info["id"],
        "status": fixture_info["status"]["short"],
        "elapsed_minutes": fixture_info["status"]["elapsed"],
        "home_team": teams["home"]["name"],
        "away_team": teams["away"]["name"],
        "home_goals": goals["home"],
        "away_goals": goals["away"],
        "events": events,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def run():
    producer = get_producer()
    print(f"Producer started. Polling every {POLL_INTERVAL_SECONDS}s. "
          f"Pushing to topic '{TOPIC_NAME}' on {KAFKA_BOOTSTRAP_SERVERS}")

    while True:
        try:
            live_fixtures = fetch_live_fixtures()

            if not live_fixtures:
                print(f"[{datetime.now().isoformat()}] No live World Cup fixtures right now.")
            else:
                for fixture in live_fixtures:
                    fixture_id = fixture["fixture"]["id"]
                    events = fetch_fixture_events(fixture_id)
                    message = build_message(fixture, events)

                    producer.send(TOPIC_NAME, key=str(fixture_id), value=message)
                    print(f"[{datetime.now().isoformat()}] Sent fixture {fixture_id}: "
                          f"{message['home_team']} {message['home_goals']}-"
                          f"{message['away_goals']} {message['away_team']} "
                          f"({message['elapsed_minutes']}')")

                producer.flush()

        except requests.HTTPError as e:
            print(f"[ERROR] API request failed: {e}")
        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()