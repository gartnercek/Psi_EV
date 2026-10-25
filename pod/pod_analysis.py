"""Analysis of the POD bot bet log.

Usage: python pod_analysis.py bet_log.csv

Reads the exported bet log, keeps settled bets, and compares realized profit
with the expected profit implied by the logged EV (price / no-vig price - 1).
Prints a markdown report to stdout.
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd


def money(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(r"[$,]", "", regex=True), errors="coerce")


def load(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["stake"] = money(df["Stake"])
    df["profit"] = money(df["Profit"])
    df["ev"] = pd.to_numeric(df["Logged EV"].astype(str).str.rstrip("%"), errors="coerce") / 100
    df["exp_profit"] = df["stake"] * df["ev"]
    df["settled"] = df["Result"].isin(["WIN", "LOSS", "PUSH", "HALF_WIN", "VOID"])
    # movement between the opening no-vig price and the no-vig price when the bet was logged
    df["nvp_move"] = df["Logged NVP"] / df["Opening NVP"] - 1
    return df


def summarize(g: pd.DataFrame) -> dict:
    n = len(g)
    stake = g["stake"].sum()
    profit = g["profit"].sum()
    exp = g["exp_profit"].sum()
    # per-bet return on stake, for a rough standard error of the yield
    r = g["profit"] / g["stake"]
    se = r.std(ddof=1) / np.sqrt(n) if n > 1 else float("nan")
    return {
        "bets": n,
        "staked": stake,
        "profit": profit,
        "yield": profit / stake if stake else float("nan"),
        "exp_yield": exp / stake if stake else float("nan"),
        "se_yield": se,
        "win_rate": (g["Result"] == "WIN").mean(),
    }


def table(df: pd.DataFrame, by: str | pd.Series, title: str) -> str:
    rows = []
    for key, g in df.groupby(by, observed=True):
        s = summarize(g)
        rows.append(
            f"| {key} | {s['bets']:,} | ${s['staked']:,.0f} | ${s['profit']:,.2f} | "
            f"{s['yield']:+.2%} | {s['exp_yield']:+.2%} | ±{s['se_yield']:.2%} |"
        )
    head = (
        f"\n### {title}\n\n| | Bets | Staked | Profit | Yield | Expected yield | Std. error of yield |\n"
        "|---|---:|---:|---:|---:|---:|---:|\n"
    )
    return head + "\n".join(rows) + "\n"


def main(path: str) -> None:
    raw = load(path)
    pending = (~raw["settled"]).sum()
    df = raw[raw["settled"]].copy()

    print("# POD bot settled record\n")
    print(
        f"Source: exported bet log, bets with start times from {raw['Start Time'].min()[:10]} "
        f"to {raw['Start Time'].max()[:10]}. {len(raw):,} rows, {pending} unsettled and excluded. "
        "Amounts in dollars, stakes of $20 or $25. Expected profit = stake x logged EV, where EV = bet odds / "
        "devigged Pinnacle price - 1.\n"
    )

    s = summarize(df)
    print("### All settled bets\n")
    print("| Bets | Staked | Profit | Yield | Expected yield | Std. error of yield | Win rate |")
    print("|---:|---:|---:|---:|---:|---:|---:|")
    print(
        f"| {s['bets']:,} | ${s['staked']:,.0f} | ${s['profit']:,.2f} | {s['yield']:+.2%} | "
        f"{s['exp_yield']:+.2%} | ±{s['se_yield']:.2%} | {s['win_rate']:.1%} |"
    )

    bins = [-np.inf, 0, 0.03, 0.05, 0.10, np.inf]
    labels = ["EV below 0%", "0% to 3%", "3% to 5%", "5% to 10%", "10% and above"]
    df["ev_band"] = pd.cut(df["ev"], bins=bins, labels=labels, right=False)
    print(table(df, "ev_band", "By logged EV"))
    print(table(df, "Sport", "By sport"))
    print(table(df, "Market", "By market"))

    pos = df[df["ev"] > 0]
    print("\n### Only bets with positive logged EV\n")
    s = summarize(pos)
    print(
        f"{s['bets']:,} bets, ${s['staked']:,.0f} staked, profit ${s['profit']:,.2f}, "
        f"yield {s['yield']:+.2%} against {s['exp_yield']:+.2%} expected (std. error ±{s['se_yield']:.2%}).\n"
    )

    # Did the sharp price keep moving in our favor after the bet?  Only the opening and the logged price
    # are in the file, so this compares the logged price with the opening price, not the closing price.
    print("### Sharp price move between opening and the logged bet\n")
    mv = df.dropna(subset=["nvp_move"])
    print(
        f"Median change in the devigged price from opening to the bet: {mv['nvp_move'].median():+.2%}. "
        f"Share of bets where the price had shortened (our side became more likely): "
        f"{(mv['nvp_move'] < 0).mean():.1%}.\n"
    )


if __name__ == "__main__":
    main(sys.argv[1])
