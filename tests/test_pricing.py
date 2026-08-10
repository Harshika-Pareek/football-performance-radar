import pandas as pd

from pricing.fair_odds import apply_margin, fair_odds_from_probability
from pricing.margin import calculate_margin, price_match

# A tiny, hand-built features table stands in for train_model.py's real
# output. Both teams are present, so predict_match() has real data for
# each side and should flag confidence as HIGH - that keeps this test
# deterministic and free of the live football-data.org API.
FEATURES_DF = pd.DataFrame(
    {
        "team": ["Team A", "Team B"],
        "home_attack_strength": [1.2, 0.9],
        "home_defence_strength": [0.8, 1.1],
        "away_attack_strength": [1.0, 1.0],
        "away_defence_strength": [1.0, 1.0],
    }
)
LEAGUE_AVG_HOME = 1.5
LEAGUE_AVG_AWAY = 1.2


def test_fair_odds_from_probability():
    # A 50% chance should be fair at even-money-doubled odds of 2.0.
    assert fair_odds_from_probability(0.5) == 2.0


def test_apply_margin():
    # Shading 2.0 fair odds by a 10% margin should leave 1.8.
    assert apply_margin(2.0, 0.10) == 1.8


def test_calculate_margin_low_wider_than_high():
    # LOW confidence must always price a wider (larger) margin than HIGH.
    assert calculate_margin(0.05, "LOW") > calculate_margin(0.05, "HIGH")


def test_calculate_margin_low_value():
    # LOW confidence is exactly 1.5x the base margin: 0.05 * 1.5 = 0.075.
    assert calculate_margin(0.05, "LOW") == 0.075


def test_price_match_end_to_end_known_teams():
    # Both teams have real rows in FEATURES_DF, so this should price as
    # a HIGH-confidence match at the default (unwidened) base margin.
    priced = price_match("Team A", "Team B", FEATURES_DF, LEAGUE_AVG_HOME, LEAGUE_AVG_AWAY)

    assert priced["home_team"] == "Team A"
    assert priced["away_team"] == "Team B"
    assert priced["confidence"] == "HIGH"
    assert priced["margin"] == 0.05

    # All three outcomes must be priced, and every priced price must sit
    # strictly below its fair price - that gap is the bookmaker's margin.
    for outcome in ("home", "draw", "away"):
        assert priced["priced_odds"][outcome] < priced["fair_odds"][outcome]
        assert priced["priced_odds"][outcome] > 1.0


def test_price_match_widens_margin_for_unknown_team():
    # "Team C" has no row in FEATURES_DF, so predict_match() falls back
    # to league-average strengths for it and flags confidence as LOW -
    # the priced margin should widen accordingly.
    priced = price_match("Team A", "Team C", FEATURES_DF, LEAGUE_AVG_HOME, LEAGUE_AVG_AWAY)

    assert priced["confidence"] == "LOW"
    assert priced["margin"] == 0.075
