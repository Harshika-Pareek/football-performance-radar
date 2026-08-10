"""
Confidence-linked margin sizing and end-to-end match pricing.

A prediction built on a team with no real training history (see
ml.train_model._get_strength's fallback to league-average) is less
trustworthy than one built entirely from observed data. Pricing
should protect the book against that extra uncertainty by widening
the margin - the same "flag it, then act on the flag" pattern
train_model.py already uses for missing-team confidence, applied here
to a pricing decision instead of just a display label.
"""
from ML.train_model import predict_match
from pricing.fair_odds import apply_margin, fair_odds_from_probability

# Widen the margin by 50% when confidence is LOW, to compensate for
# the extra uncertainty of a prediction that leaned on a league-average
# fallback rather than the team's own observed data.
LOW_CONFIDENCE_MULTIPLIER = 1.5


def calculate_margin(base_margin: float, confidence: str) -> float:
    """Scale the standard margin up for low-confidence predictions.

    HIGH confidence (both teams have real training history) charges
    the standard base_margin. LOW confidence (either team's strength
    fell back to a league-average placeholder) charges 1.5x that
    margin, since the model is pricing a match it understands less
    well and the book needs a wider buffer against that risk.

    Rounded to 10 decimal places - margins are percentages with no
    real-world need for more precision than that, and rounding avoids
    binary floating-point noise like 0.05 * 1.5 == 0.07500000000000001."""
    if confidence == "LOW":
        return round(base_margin * LOW_CONFIDENCE_MULTIPLIER, 10)
    return base_margin


def price_match(
    home_team: str,
    away_team: str,
    features_df,
    league_avg_home: float,
    league_avg_away: float,
    base_margin: float = 0.05,
) -> dict:
    """Price a single match end-to-end: predict -> fair odds -> margin.

    Runs the Poisson model via predict_match() to get win/draw/loss
    probabilities plus its HIGH/LOW confidence flag, converts each
    probability to fair (breakeven) decimal odds, widens the margin
    when confidence is LOW, and applies that margin to produce the
    final priced odds a customer would actually see."""
    prediction = predict_match(home_team, away_team, features_df, league_avg_home, league_avg_away)
    confidence = prediction["confidence"]
    margin = calculate_margin(base_margin, confidence)

    fair_odds = {
        "home": fair_odds_from_probability(prediction["home_win_prob"]),
        "draw": fair_odds_from_probability(prediction["draw_prob"]),
        "away": fair_odds_from_probability(prediction["away_win_prob"]),
    }
    priced_odds = {outcome: apply_margin(odds, margin) for outcome, odds in fair_odds.items()}

    return {
        "home_team": home_team,
        "away_team": away_team,
        "confidence": confidence,
        "margin": margin,
        "fair_odds": fair_odds,
        "priced_odds": priced_odds,
    }
