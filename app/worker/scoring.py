"""The risk formula from the synopsis.

    Risk Score = 100 x (0.25 D + 0.25 B + 0.25 F + 0.25 C)

Pure and deterministic: same inputs, same output. Each feature's share of the
score is returned so any score can be explained term by term.
"""
WEIGHTS = {"D": 0.25, "B": 0.25, "F": 0.25, "C": 0.25}


def risk_score(d: float, b: float, f: float, c: float) -> float:
    for name, v in (("D", d), ("B", b), ("F", f), ("C", c)):
        if not 0.0 <= v <= 1.0:
            raise ValueError(f"feature {name}={v} is outside [0, 1]")
    return 100 * (WEIGHTS["D"] * d + WEIGHTS["B"] * b + WEIGHTS["F"] * f + WEIGHTS["C"] * c)


def contributions(d: float, b: float, f: float, c: float) -> dict[str, float]:
    """Points each feature adds to the 0-100 score (they sum to the score)."""
    values = {"D": d, "B": b, "F": f, "C": c}
    return {k: 100 * WEIGHTS[k] * v for k, v in values.items()}
