"""
Train a Poisson goal-scoring model on 2023/24 + 2024/25 Premier League
results (football-data.org), then back-test it on the held-out 2025/26
season to get a genuine out-of-sample accuracy figure. Logs the run to
MLflow and finishes with a sample prediction from a separate model
refit on 2025/26 alone.
"""
import os
import sys
from itertools import product
from pathlib import Path

import mlflow
import pandas as pd
import requests
from dotenv import load_dotenv
from scipy.stats import poisson

# MLflow prints emoji (e.g. the run-URL banner) to stdout, which crashes
# on Windows' default cp1252 console encoding. Force UTF-8 so that
# doesn't take down an otherwise-successful run.
sys.stdout.reconfigure(encoding="utf-8")

# Step 1: Load the API key the same way extract_features.py does - the
# key lives in producer/.env, not in this ml/ folder, so we point
# load_dotenv at that file explicitly.
load_dotenv(Path(__file__).parent.parent / "producer" / ".env")
key = os.environ["FOOTBALL_DATA_API_KEY"]

MAX_GOALS = 6  # 7x7 matrix covers scorelines 0-0 through 6-6


def fetch_season_matches(season: int) -> pd.DataFrame:
    """Fetch every FINISHED PL match for one season and flatten it into
    a DataFrame with just home/away team names and the final score -
    same shape extract_features.py builds, just factored into a
    function so we can call it once per season."""
    resp = requests.get(
        "https://api.football-data.org/v4/competitions/PL/matches",
        headers={"X-Auth-Token": key},
        params={"season": season, "status": "FINISHED"},
        timeout=15,
    )
    resp.raise_for_status()
    matches = resp.json().get("matches", [])

    rows = []
    for m in matches:
        home_goals = m["score"]["fullTime"]["home"]
        away_goals = m["score"]["fullTime"]["away"]
        # Guard against any in-progress/edge-case match that slipped
        # through without a final score.
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
    return pd.DataFrame(rows)


def build_features(matches_df: pd.DataFrame) -> tuple[pd.DataFrame, float, float]:
    """Build per-team attack/defence strength features from a set of
    matches, plus the league-average home/away goals those strengths
    are normalised against. This is the same groupby logic as
    extract_features.py, with an extra normalisation step so teams can
    be compared on a common "1.0 = average" scale."""
    # Home-venue stats: for each team, average goals scored (attack)
    # and conceded (defence) while playing at home.
    home_stats = (
        matches_df.groupby("home_team")
        .agg(home_attack=("home_goals", "mean"), home_defence=("away_goals", "mean"))
        .reset_index()
        .rename(columns={"home_team": "team"})
    )

    # Away-venue stats: same idea, grouped by away_team instead.
    away_stats = (
        matches_df.groupby("away_team")
        .agg(away_attack=("away_goals", "mean"), away_defence=("home_goals", "mean"))
        .reset_index()
        .rename(columns={"away_team": "team"})
    )

    features_df = home_stats.merge(away_stats, on="team", how="outer")

    # League averages, computed from this same match set only - these
    # are the denominators that turn raw goals-per-game into a
    # relative "strength" (1.0 = league-average team).
    league_avg_home_goals = matches_df["home_goals"].mean()
    league_avg_away_goals = matches_df["away_goals"].mean()

    # Step 5: normalise into strength values. Attack strength compares
    # a team's scoring rate to the league's average scoring rate at
    # that venue; defence strength compares goals conceded to the
    # league's average scoring rate by opponents at that venue.
    features_df["home_attack_strength"] = features_df["home_attack"] / league_avg_home_goals
    features_df["home_defence_strength"] = features_df["home_defence"] / league_avg_away_goals
    features_df["away_attack_strength"] = features_df["away_attack"] / league_avg_away_goals
    features_df["away_defence_strength"] = features_df["away_defence"] / league_avg_home_goals

    return features_df.round(3), league_avg_home_goals, league_avg_away_goals


def _get_strength(features_df: pd.DataFrame, team: str, column: str) -> float:
    """Look up one strength value for a team, falling back to 1.0
    (league-average) if the team never appeared in the training data -
    e.g. a newly promoted/relegated side we have no history for."""
    row = features_df.loc[features_df["team"] == team]
    if row.empty or pd.isna(row.iloc[0][column]):
        return 1.0
    return float(row.iloc[0][column])


def predict_match(
    home_team: str,
    away_team: str,
    features_df: pd.DataFrame,
    league_avg_home: float,
    league_avg_away: float,
) -> dict:
    """Predict a single match's outcome probabilities with an
    independent-Poisson model: each side's expected goals (lambda) is
    the league-average goals at that venue, scaled by the attacking
    team's attack strength and the defending team's defence strength."""
    home_attack = _get_strength(features_df, home_team, "home_attack_strength")
    home_defence = _get_strength(features_df, home_team, "home_defence_strength")
    away_attack = _get_strength(features_df, away_team, "away_attack_strength")
    away_defence = _get_strength(features_df, away_team, "away_defence_strength")

    home_lambda = league_avg_home * home_attack * away_defence
    away_lambda = league_avg_away * away_attack * home_defence

    # Build the 7x7 grid of P(home scores i) * P(away scores j), for
    # every scoreline from 0-0 to 6-6, assuming the two scores are
    # independent Poisson variables.
    home_goal_probs = [poisson.pmf(i, home_lambda) for i in range(MAX_GOALS + 1)]
    away_goal_probs = [poisson.pmf(j, away_lambda) for j in range(MAX_GOALS + 1)]

    home_win_prob = draw_prob = away_win_prob = 0.0
    for i, j in product(range(MAX_GOALS + 1), repeat=2):
        p = home_goal_probs[i] * away_goal_probs[j]
        if i > j:
            home_win_prob += p
        elif i == j:
            draw_prob += p
        else:
            away_win_prob += p

    return {
        "home_win_prob": home_win_prob,
        "draw_prob": draw_prob,
        "away_win_prob": away_win_prob,
        "home_lambda": home_lambda,
        "away_lambda": away_lambda,
    }


def actual_outcome(home_goals: int, away_goals: int) -> str:
    if home_goals > away_goals:
        return "H"
    if home_goals < away_goals:
        return "A"
    return "D"


def predicted_outcome(prediction: dict) -> str:
    probs = {
        "H": prediction["home_win_prob"],
        "D": prediction["draw_prob"],
        "A": prediction["away_win_prob"],
    }
    return max(probs, key=probs.get)


if __name__ == "__main__":
    # Step 1/2: fetch training seasons (2023/24, 2024/25) and the held-out
    # test season (2025/26) as separate, non-overlapping pulls.
    print("Fetching training data (2023/24 + 2024/25 seasons)...")
    train_2023 = fetch_season_matches(2023)
    train_2024 = fetch_season_matches(2024)
    training_df = pd.concat([train_2023, train_2024], ignore_index=True)

    print("Fetching held-out test data (2025/26 season)...")
    test_df = fetch_season_matches(2025)

    print("\n=== Training data summary ===")
    print(f"2023/24 matches: {len(train_2023)}")
    print(f"2024/25 matches: {len(train_2024)}")
    print(f"Combined training matches: {len(training_df)}")
    print(f"Held-out 2025/26 test matches: {len(test_df)}")

    # Step 3/4/5: build attack/defence strength features and league
    # averages from TRAINING data only - the test season never touches
    # this step, which is what makes the evaluation below out-of-sample.
    features_df, league_avg_home, league_avg_away = build_features(training_df)
    print(f"\nLeague avg home goals (training): {league_avg_home:.3f}")
    print(f"League avg away goals (training): {league_avg_away:.3f}")
    print(f"\nTrained features for {len(features_df)} teams:")
    print(features_df.to_string(index=False))

    # Step 7: out-of-sample evaluation - predict every 2025/26 match
    # using ONLY the training-derived features, then compare the
    # predicted most-likely outcome to what actually happened.
    outcomes = ["H", "D", "A"]
    confusion = pd.DataFrame(0, index=outcomes, columns=outcomes)  # rows=actual, cols=predicted

    correct = 0
    per_class_total = {o: 0 for o in outcomes}
    per_class_correct = {o: 0 for o in outcomes}

    for _, match in test_df.iterrows():
        prediction = predict_match(
            match["home_team"], match["away_team"], features_df, league_avg_home, league_avg_away
        )
        actual = actual_outcome(match["home_goals"], match["away_goals"])
        predicted = predicted_outcome(prediction)

        confusion.loc[actual, predicted] += 1
        per_class_total[actual] += 1
        if actual == predicted:
            correct += 1
            per_class_correct[actual] += 1

    total = len(test_df)
    overall_accuracy = correct / total
    home_win_accuracy = per_class_correct["H"] / per_class_total["H"] if per_class_total["H"] else float("nan")
    draw_accuracy = per_class_correct["D"] / per_class_total["D"] if per_class_total["D"] else float("nan")
    away_win_accuracy = per_class_correct["A"] / per_class_total["A"] if per_class_total["A"] else float("nan")

    print("\n=== Out-of-sample evaluation (2025/26, held out) ===")
    print(f"Overall accuracy: {overall_accuracy:.1%} ({correct}/{total})")
    print(f"Home win accuracy: {home_win_accuracy:.1%} ({per_class_correct['H']}/{per_class_total['H']})")
    print(f"Draw accuracy: {draw_accuracy:.1%} ({per_class_correct['D']}/{per_class_total['D']})")
    print(f"Away win accuracy: {away_win_accuracy:.1%} ({per_class_correct['A']}/{per_class_total['A']})")
    print("\nConfusion matrix (rows=actual, cols=predicted):")
    print(confusion.to_string())

    # Step 8: log the backtest to MLflow. Wrapped in try/except since
    # this depends on a tracking server running at localhost:5000 - if
    # it's not up, we still want the rest of this script (including
    # the demo prediction below) to run and print.
    try:
        mlflow.set_tracking_uri("http://localhost:5000")
        mlflow.set_experiment("football-poisson")
        with mlflow.start_run():
            mlflow.log_param("training_seasons", "2023,2024")
            mlflow.log_param("test_season", 2025)
            mlflow.log_param("num_training_matches", len(training_df))

            mlflow.log_metric("out_of_sample_accuracy", overall_accuracy)
            mlflow.log_metric("home_win_accuracy", home_win_accuracy)
            mlflow.log_metric("draw_accuracy", draw_accuracy)
            mlflow.log_metric("away_win_accuracy", away_win_accuracy)

            features_csv_path = Path(__file__).parent / "training_features.csv"
            features_df.to_csv(features_csv_path, index=False)
            mlflow.log_artifact(str(features_csv_path))

            mlflow.log_text(
                "This run backtests a Poisson model trained on the 2023/24 and "
                "2024/25 seasons against the 2025/26 season, which was never "
                "seen during feature-building. Accuracy figures above are "
                "genuine out-of-sample performance, not in-sample fit on the "
                "same data the strengths were computed from.",
                "backtest_note.txt",
            )
        print("\nLogged backtest run to MLflow (experiment: football-poisson).")
    except Exception as exc:
        print(f"\nSkipped MLflow logging - could not reach tracking server: {exc}")

    # Step 9: final demo prediction. This is a SEPARATE model, refit on
    # 2025/26 alone, so it reflects current squad strength rather than
    # the 2023-24/2024-25 backtest model evaluated above.
    print("\n=== Final model for 2026/27 predictions (refit on 2025/26 alone) ===")
    final_features_df, final_avg_home, final_avg_away = build_features(test_df)

    demo = predict_match("Arsenal FC", "Coventry City FC", final_features_df, final_avg_home, final_avg_away)
    print("Arsenal FC (home) vs Coventry City FC (away):")
    print(f"  home_lambda={demo['home_lambda']:.3f}, away_lambda={demo['away_lambda']:.3f}")
    print(
        f"  home_win_prob={demo['home_win_prob']:.1%}, "
        f"draw_prob={demo['draw_prob']:.1%}, "
        f"away_win_prob={demo['away_win_prob']:.1%}"
    )
    print(
        "  Note: Coventry City FC has no 2025/26 PL history, so its strengths "
        "fall back to 1.0 (league average) - this demonstrates the missing-team handling."
    )
