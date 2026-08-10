"""
Convert model win/draw/loss probabilities into priced decimal odds.

A "fair" price reflects the model's true probability with no
bookmaker edge: decimal odds of 1/prob mean the market breaks even in
expectation. Real books never quote fair odds - they shade every
price short by a margin (also called the "overround" or "vig") so
that the probabilities implied by their odds sum to more than 100%.
That gap is the book's structural edge, independent of who wins.
apply_margin() is what turns a fair, breakeven price into a priced,
profitable one.
"""


def fair_odds_from_probability(prob: float) -> float:
    """Convert a win probability into fair (breakeven) decimal odds.

    Decimal odds are the inverse of probability - e.g. a 50% chance
    is fair at 2.0 (stake back double: 1 unit staked returns 2 units
    including the stake). No margin is applied here; this is the
    theoretical price before any bookmaker edge is added."""
    return 1 / prob


def apply_margin(fair_odds: float, margin_pct: float) -> float:
    """Shade fair odds down by a margin to build in the bookmaker's edge.

    Cutting the payout by margin_pct (e.g. 0.10 for 10%) means the
    book pays out less than the fair price would, which is exactly
    how real-world bookmaker overround works: shorten every outcome's
    price slightly so the implied probabilities across all outcomes
    sum to more than 100%, and the excess is the book's expected
    margin regardless of the result."""
    return fair_odds * (1 - margin_pct)
