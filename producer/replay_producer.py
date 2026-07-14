"""
Replay simulator: replays a real historical World Cup match's events into
Kafka, paced out over time, to simulate what a live feed would look like.

This exists because the free API-Football tier doesn't allow querying the
live 2026 season — so we replay real 2022 World Cup data instead. Kafka and
everything downstream cannot tell the difference between this and genuine
live data; messages just arrive over time either way.
"""

print("Script started")

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
TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "football.match.events")

FIXTURE_ID = 855735  # England 6-2 Iran, 2022 World Cup
SECONDS_PER_MATCH_MINUTE = 2  # 1 match-minute = 2 real seconds


def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def fetch_match_events(fixture_id):
    resp = requests.get(
        f"{BASE_URL}/fixtures/events",
        headers=HEADERS,
        params={"fixture": fixture_id},
        timeout=15,
    )
    resp.raise_for_status()
    events = resp.json()["response"]
    # sort by elapsed minute, earliest first, so we replay in correct order
    events.sort(key=lambda e: e["time"]["elapsed"])
    return events


def build_message(fixture_id, event):
    return {
        "fixture_id": fixture_id,
        "minute": event["time"]["elapsed"],
        "event_type": event["type"],
        "detail": event.get("detail", ""),
        "team": event["team"]["name"],
        "player": event["player"]["name"],
        "replayed_at": datetime.now(timezone.utc).isoformat(),
    }


def run():
    producer = get_producer()
    events = fetch_match_events(FIXTURE_ID)

    print(f"Replaying fixture {FIXTURE_ID} — {len(events)} events found.")
    print(f"Pushing to topic '{TOPIC_NAME}' on {KAFKA_BOOTSTRAP_SERVERS}")

    previous_minute = 0

    for event in events:
        current_minute = event["time"]["elapsed"]

        # wait an amount of time proportional to how much match-time has
        # passed since the last event, so events arrive at a realistic pace
        minutes_elapsed = current_minute - previous_minute
        wait_seconds = max(minutes_elapsed, 1) * SECONDS_PER_MATCH_MINUTE
        time.sleep(wait_seconds)

        message = build_message(FIXTURE_ID, event)
        producer.send(TOPIC_NAME, key=str(FIXTURE_ID), value=message)
        producer.flush()

        print(f"[{current_minute}'] Sent: {message['event_type']} - "
              f"{message['team']} - {message['player']} - {message['detail']}")

        previous_minute = current_minute

    print("Replay finished — all events sent.")


if __name__ == "__main__":
    run()