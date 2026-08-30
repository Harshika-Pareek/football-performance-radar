"""
News producer for Sprint 3's RAG layer.

Fetches Premier League news from two RSS feeds (BBC Sport, Guardian
Football), classifies each entry by which PL team(s) it mentions, and
publishes matching entries to Kafka topic "news.premier_league" so the
RAG pipeline has a stream of team-tagged articles to index.
"""

import csv
import json
import os
import uuid
from calendar import timegm
from datetime import datetime, timezone
from pathlib import Path

import feedparser
from dotenv import load_dotenv
from kafka import KafkaProducer

# producer/.env holds KAFKA_BOOTSTRAP_SERVERS (same file replay_producer.py
# reads). This script lives in ML/rag, not producer/, so a bare
# load_dotenv() wouldn't find it — resolve the path explicitly instead.
ENV_PATH = Path(__file__).resolve().parents[2] / "producer" / ".env"
load_dotenv(ENV_PATH)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
TOPIC_NAME = "news.premier_league"

# Team names come from the features CSV so they stay in sync with the rest
# of the pipeline (e.g. "Arsenal FC", "AFC Bournemouth").
TEAM_NAMES_CSV = Path(__file__).resolve().parents[1] / "pl_features_2025.csv"

FEEDS = {
    "BBC Sport": "https://feeds.bbci.co.uk/sport/football/rss.xml",
    "Guardian": "https://www.theguardian.com/football/rss",
}

# The CSV stores official club names ("Arsenal FC", "Manchester United FC"),
# but headlines use short forms ("Arsenal", "Man Utd", "Spurs") — matching
# the literal CSV name against article text matched 0/134 real entries in
# testing. These aliases are the nickname/shorthand variants to also check.
#
# Deliberately excluded as too ambiguous:
#   - "Blues"  (Chelsea) — also Ipswich Town's nickname; confirmed false
#     positive in testing (matched a Man Utd v Ipswich headline)
#   - "United" standalone (Man Utd) — shared by Newcastle United, Leeds
#     United, West Ham United; "Manchester United"/"Man Utd"/"Man United"
#     still cover real headlines
#   - "City" standalone (Man City) — shared by Leicester City, and any
#     other "*City" club the feeds cover; "Manchester City"/"Man City"
#     still cover real headlines
TEAM_ALIASES = {
    "AFC Bournemouth": ["Bournemouth"],
    "Arsenal FC": ["Arsenal", "Gunners"],
    "Aston Villa FC": ["Aston Villa", "Villa"],
    "Brentford FC": ["Brentford", "Bees"],
    "Brighton & Hove Albion FC": ["Brighton", "Brighton & Hove Albion", "Seagulls"],
    "Burnley FC": ["Burnley", "Clarets"],
    "Chelsea FC": ["Chelsea"],
    "Crystal Palace FC": ["Crystal Palace", "Palace", "Eagles"],
    "Everton FC": ["Everton", "Toffees"],
    "Fulham FC": ["Fulham", "Cottagers"],
    "Leeds United FC": ["Leeds United", "Leeds"],
    "Liverpool FC": ["Liverpool", "Reds"],
    "Manchester City FC": ["Manchester City", "Man City"],
    "Manchester United FC": ["Manchester United", "Man Utd", "Man United"],
    "Newcastle United FC": ["Newcastle United", "Newcastle", "Magpies"],
    "Nottingham Forest FC": ["Nottingham Forest", "Forest"],
    "Sunderland AFC": ["Sunderland", "Black Cats"],
    "Tottenham Hotspur FC": ["Tottenham Hotspur", "Tottenham", "Spurs"],
    "West Ham United FC": ["West Ham United", "West Ham", "Hammers"],
    "Wolverhampton Wanderers FC": ["Wolverhampton Wanderers", "Wolverhampton", "Wolves"],
}


def load_team_names(csv_path):
    """Read the 'team' column out of the features CSV."""
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [row["team"] for row in reader]


def build_team_aliases(team_names):
    """
    Map each official CSV team name to its full list of match candidates:
    the official name itself plus every known nickname/shorthand. Falls
    back to just the official name if a team has no entry in TEAM_ALIASES
    (keeps this from silently dropping a team if the CSV ever changes).
    """
    return {team: [team] + TEAM_ALIASES.get(team, []) for team in team_names}


def classify_relevance(text, team_aliases):
    """
    Shared classification logic, used for both BBC and Guardian entries.
    Checks `text` (case-insensitive) against every alias of every team, not
    just the official CSV name. Returns a list of (team, matched_alias)
    pairs — one entry per team that matched, using whichever alias hit
    first, so callers can report which alias triggered the match.
    """
    text_lower = text.lower()
    matches = []
    for team, aliases in team_aliases.items():
        for alias in aliases:
            if alias.lower() in text_lower:
                matches.append((team, alias))
                break
    return matches


def parse_published_at(entry):
    """
    Convert the entry's published date to an ISO-8601 UTC string.
    feedparser already normalizes the many RSS date formats into
    entry.published_parsed (a UTC struct_time), so we just need to guard
    against it being absent or unparseable rather than parse dates
    ourselves.
    """
    parsed = entry.get("published_parsed")
    if not parsed:
        return None
    try:
        return datetime.fromtimestamp(timegm(parsed), tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OverflowError):
        return None


def build_message(team, entry, source):
    """Build the Kafka payload for one (matched team, article) pair."""
    return {
        "team_mentioned": team,
        "published_at": parse_published_at(entry),
        "article_id": str(uuid.uuid4()),
        "title": entry.get("title", ""),
        "summary": entry.get("summary", ""),
        "source": source,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def get_producer():
    # Same serializer setup as replay_producer.py: JSON-encode values,
    # UTF-8 encode the string key (team name here instead of fixture id).
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def process_feed(source, url, team_aliases, producer):
    """
    Fetch one RSS feed and classify every entry against team_aliases.
    An entry can mention more than one team (e.g. a match preview), so we
    send one Kafka message per matched team, each keyed by that team —
    that's what lets downstream consumers filter the topic per-team.
    """
    feed = feedparser.parse(url)
    entries = feed.entries
    print(f"{source}: fetched {len(entries)} entries")

    matched_count = 0
    for entry in entries:
        title = entry.get("title", "")
        summary = entry.get("summary", "")
        combined_text = f"{title} {summary}"

        matched_teams = classify_relevance(combined_text, team_aliases)
        if not matched_teams:
            continue
        matched_count += 1

        for team, alias in matched_teams:
            message = build_message(team, entry, source)
            producer.send(TOPIC_NAME, key=team, value=message)
            producer.flush()
            print(f"  sent -> [{team}] (matched \"{alias}\") "
                  f"{message['article_id']}: {message['title']}")

    print(f"{source}: matched {matched_count}/{len(entries)} entries")
    return matched_count


def run():
    team_names = load_team_names(TEAM_NAMES_CSV)
    team_aliases = build_team_aliases(team_names)
    producer = get_producer()

    print(f"Loaded {len(team_names)} team names from {TEAM_NAMES_CSV.name}")
    print(f"Publishing to topic '{TOPIC_NAME}' on {KAFKA_BOOTSTRAP_SERVERS}")
    print()

    for source, url in FEEDS.items():
        process_feed(source, url, team_aliases, producer)
        print()

    print("Done.")


if __name__ == "__main__":
    run()
