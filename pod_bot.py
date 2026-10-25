"""
Pinnacle Odds Dropper (POD) Betting Bot
──────────────────────────────────────
Monitors POD terminal alerts, matches on Rainbet/Duel (BetBy), and bets when EV >= threshold.

Phases:
  1. POD alert ingestion (AG Grid scrape)
  2. Soft-book event match + EV check
  3. Auto bet placement (--bet)
"""

from __future__ import annotations

import vpn_asyncio  # noqa: F401  — Proton blocks 127.0.0.1 socketpair
import argparse
import asyncio
import hashlib
import json
import logging
import random
import re
import sys
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from typing import Any

from playwright.async_api import TimeoutError as PlaywrightTimeout
from playwright.async_api import async_playwright

# ── Configuration ───────────────────────────────────────────────────────────────
POD_TERMINAL_URL = "https://www.pinnacleoddsdropper.com/terminal"
RAINBET_BASE_URL = "https://rainbet.com"
RAINBET_SPORTS_URL = f"{RAINBET_BASE_URL}/sportsbook"
DUEL_BASE_URL = "https://duel.com"
DUEL_SPORTS_URL = f"{DUEL_BASE_URL}/sports"
BCGAME_BASE_URL = "https://bc.game"
BCGAME_SPORTS_URL = f"{BCGAME_BASE_URL}/sports"

POLL_INTERVAL = 2.0          # seconds between terminal scrapes
MIN_EV_PCT = 1.7           # minimum EV % before betting
# Soft-book EV this high on a main market is almost always the wrong event/market
MAX_PLAUSIBLE_EV_PCT = 40.0
FLAT_STAKE = 25.0           # euros per bet on Rainbet/Duel; dollars on BC.Game
# Coinbase BTC-USD spot on 2026-10-04. Frozen — BC.Game slips are in BTC.
BCGAME_BTC_USD = 84803.695
MAX_ALERT_AGE_SEC = 60.0    # only act on drops newer than this (seconds)
MAX_CONCURRENT_SESSIONS = 2  # parallel soft-book worker tabs
SESSION_START_STAGGER = 3.5  # seconds between worker starts (anti-burst)
RAINBET_SEARCH_COOLDOWN = 2.2  # base gap between searches (anti-captcha)
RAINBET_SEARCH_JITTER = 0.7    # extra random delay on top of cooldown
RAINBET_OP_TIMEOUT = 35.0    # timeout per Rainbet search/navigate step
OUTCOME_LOOKUP_TIMEOUT = 8.0  # plate match should finish fast (inventory is in-memory)
OUTCOME_LOOKUP_FAST_TIMEOUT = 3.0  # when plates were already scraped once
RAINBET_SEARCH_MIN_WAIT = 0.55      # debounce after typing before first check
RAINBET_SEARCH_RESULT_TIMEOUT = 6.0 # max wait for result cards to appear
RAINBET_SEARCH_POLL_INTERVAL = 0.28
RAINBET_SEARCH_MAX_RETRIES = 2      # re-queue failed opens this many times
RAINBET_ALERT_PAUSE = (0.2, 0.55)   # pause between markets on same event
RAINBET_MATCH_IDLE_PAUSE = (0.7, 1.6)  # pause after finishing a match
HUMAN_CLICK_PAUSE = (0.04, 0.14)
PRELOADER_WAIT_TIMEOUT = 8.0
HUMAN_TYPE_DELAY_MS = (12, 32)  # keep some jitter, but stay fast for drops
CAPTCHA_POLL_SEC = 4.0
CAPTCHA_LOG_EVERY = 20.0
DEAD_PAGE_CONFIRM_SEC = 2.5   # re-check before reloading, so slow loads aren't killed
DEAD_PAGE_READY_TIMEOUT = 40.0  # wait for the BetBy widget to mount after a reload
DEAD_PAGE_MAX_ATTEMPTS = 3
IDLE_HEALTH_CHECK_SEC = 20.0  # how often an idle worker probes its tab
KEEPALIVE_IDLE_SEC = 8 * 60   # poke BetBy before its ~30 min idle expiry
POD_HEARTBEAT_SEC = 10 * 60   # log that POD polling is still alive
POD_KEEPALIVE_SEC = 8 * 60    # mouse nudge so POD session/websocket stays up
POD_STALE_GRID_SEC = 40 * 60  # same AG Grid row-ids this long → reload terminal
POD_STALE_DROP_SEC = 40 * 60  # no new drop this long (frozen grid still shows rows) → hard-refresh POD + books
POD_EMPTY_GRID_POLLS = 4      # consecutive empty scrapes before POD reload
POD_QUIET_GRID_TIMEOUT_MS = 2_500  # don't block 15s every poll when the grid is missing
ALT_LINE_MAX_DIST = 1.0  # quick-look nearby window (e.g. -2.75 → include -2.0); ranked by EV, ≤2 book hits
ALT_ATTEMPT_MAX = 2       # max soft-book plate lookups when exact line is missing
POD_MUTE_DONT_ASK_AGAIN = True  # tick POD's "Don't ask again" so later mutes are 1 click
KICKOFF_TOLERANCE_MIN = 10       # minutes — default hard gate
# When BOTH team names + league already match, allow ~1h drift (POD/Rainbet TZ)
KICKOFF_SAME_FIXTURE_TOLERANCE_MIN = 75
ODDS_DRIFT_TOL = 0.05
DEBUG_DUMP_HTML = False      # dump terminal HTML once for selector discovery
SEEN_ALERTS_FILE = "pod_seen_alerts.json"

# ── Logging ─────────────────────────────────────────────────────────────────────
def _safe_text(text: str) -> str:
    if not text:
        return text
    return text.encode("ascii", errors="replace").decode("ascii")


class SafeConsoleHandler(logging.StreamHandler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            if isinstance(record.msg, str):
                record.msg = _safe_text(record.msg)
            super().emit(record)
        except UnicodeEncodeError:
            record.msg = _safe_text(str(record.msg))
            record.args = ()
            super().emit(record)


if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    handlers=[
        logging.FileHandler("pod_betting_log.txt", encoding="utf-8"),
        SafeConsoleHandler(sys.stdout),
    ],
    force=True,
)
log = logging.getLogger()

_html_dumped = False
_runtime_max_alert_age_sec = MAX_ALERT_AGE_SEC


def get_max_alert_age_sec() -> float:
    return _runtime_max_alert_age_sec


def set_max_alert_age_sec(value: float) -> None:
    global _runtime_max_alert_age_sec
    _runtime_max_alert_age_sec = value


@dataclass(frozen=True)
class SoftBook:
    """Soft book shell around the shared BetBy sports widget."""

    name: str
    base_url: str
    sports_url: str
    domain: str
    sports_path: str
    nav_hrefs: tuple[str, ...]
    widget_hosts: tuple[str, ...]


BOOK_RAINBET = SoftBook(
    name="Rainbet",
    base_url=RAINBET_BASE_URL,
    sports_url=RAINBET_SPORTS_URL,
    domain="rainbet.com",
    sports_path="/sportsbook",
    nav_hrefs=(
        'a[href="/sportsbook"]',
        'a[href*="/sportsbook"]',
        'a[href*="rainbet.com/sportsbook"]',
        '[data-editor-id*="Sportsbook" i]',
        'nav a:has-text("Sportsbook")',
        'a:has-text("Sportsbook")',
    ),
    widget_hosts=("#betby", "#bt-root"),
)
BOOK_DUEL = SoftBook(
    name="Duel",
    base_url=DUEL_BASE_URL,
    sports_url=DUEL_SPORTS_URL,
    domain="duel.com",
    sports_path="/sports",
    nav_hrefs=(
        'a[href="/sports"]',
        'a[href*="/sports"]',
        'a[href*="duel.com/sports"]',
        'nav a:has-text("Sports")',
        'a:has-text("Sports")',
    ),
    widget_hosts=("#bt-root", "#betby"),
)
BOOK_BCGAME = SoftBook(
    name="BC.Game",
    base_url=BCGAME_BASE_URL,
    sports_url=BCGAME_SPORTS_URL,
    domain="bc.game",
    sports_path="/sports",
    nav_hrefs=(
        'a[href="/sports"]',
        'a[href^="/sports"]',
        'a[href*="bc.game/sports"]',
        'nav a:has-text("Sports")',
        'a:has-text("Sports Home")',
    ),
    widget_hosts=("#betby", "#bt-root"),
)

_SOFT_BOOK: SoftBook = BOOK_RAINBET


def get_book() -> SoftBook:
    return _SOFT_BOOK


def set_book(book: SoftBook) -> None:
    global _SOFT_BOOK
    _SOFT_BOOK = book


def book_tag() -> str:
    return get_book().name


@dataclass
class PODAlert:
    """Structured drop alert from the POD terminal."""

    alert_id: str
    scraped_at: str
    home_team: str = ""
    away_team: str = ""
    event: str = ""
    league: str = ""
    sport: str = ""
    kickoff: str = ""
    period: str = ""          # Match, 1st half, Game, 1st set, ...
    market: str = ""          # moneyline | spread | total | ...
    selection: str = ""       # team name, over/under, etc.
    line: str | None = None   # spread/total line when applicable
    old_odds: float | None = None
    new_odds: float | None = None
    no_vig_odds: float | None = None
    pinned_odds: float | None = None  # sharp price from quick-look alt rows
    drop_pct: float | None = None
    ev_pct: float | None = None
    soft_odds: float | None = None
    alert_age_sec: float | None = None   # age POD displayed ("17 seconds ago") when read
    age_captured_at: float = field(default_factory=time.monotonic)
    alert_type: str = "DROP"  # DROP | ALT (quick-look fallback)
    raw_text: str = ""
    raw_fields: dict[str, Any] = field(default_factory=dict)

    @property
    def is_parsed(self) -> bool:
        return bool(self.selection and self.no_vig_odds and self.new_odds)

    @property
    def live_age_sec(self) -> float | None:
        """POD's own drop age, advanced by the time spent queued in the bot."""
        if self.alert_age_sec is None:
            return None
        return self.alert_age_sec + max(0.0, time.monotonic() - self.age_captured_at)

    @property
    def is_fresh(self) -> bool:
        age = self.live_age_sec
        if age is None:
            return True
        return age <= get_max_alert_age_sec()

    @property
    def sort_age(self) -> float:
        """Lower = newer. Unknown age sorts as newest."""
        age = self.live_age_sec
        return -1.0 if age is None else age

    def age_label(self) -> str:
        age = self.live_age_sec
        if age is None:
            return "just now"
        queued = age - (self.alert_age_sec or 0.0)
        if queued >= 5.0:
            return f"{age:.0f}s ago (POD {self.alert_age_sec:.0f}s + {queued:.0f}s queued)"
        return f"{age:.0f}s ago"

    @property
    def row_id(self) -> str:
        return str(self.raw_fields.get("row_id", ""))

    @property
    def match_key(self) -> str:
        """Stable key for same-event grouping across markets."""
        home = normalise_team_name(self.home_team)
        away = normalise_team_name(self.away_team)
        kick = re.sub(r"\s+", " ", (self.kickoff or "").strip().lower())
        return f"{home}|{away}|{kick}"

    @property
    def is_actionable(self) -> bool:
        if self.ev_pct is None:
            return False
        return self.ev_pct >= MIN_EV_PCT

    def summary(self) -> str:
        matchup = self.event or "Unknown event"
        if self.home_team and self.away_team:
            matchup = f"{self.home_team} vs {self.away_team}"
        parts = [matchup]
        if self.league:
            parts.append(f"({self.league})")
        market_bits = []
        if self.period and self.period.lower() not in ("match", "game"):
            market_bits.append(self.period)
        market_bits.append(self.market or "market?")
        if self.selection:
            market_bits.append(self.selection)
        # Keep line 0 visible — it is Handicap 0 / DNB, not "no line"
        if self.line not in (None, ""):
            market_bits.append(f"line {self.line}")
        parts.append("| " + " ".join(market_bits))
        if self.old_odds is not None and self.new_odds is not None:
            parts.append(f"| {self.old_odds:.3f}->{self.new_odds:.3f}")
        if self.no_vig_odds is not None:
            parts.append(f"| NV {self.no_vig_odds:.3f}")
        if self.ev_pct is not None:
            parts.append(f"| EV {self.ev_pct:+.1f}%")
        elif self.drop_pct is not None:
            parts.append(f"| drop {self.drop_pct:.1f}%")
        return " ".join(parts)


def parse_args():
    parser = argparse.ArgumentParser(description="Pinnacle Odds Dropper Betting Bot")
    parser.add_argument(
        "--debug-html",
        action="store_true",
        help="Dump terminal HTML once for selector discovery",
    )
    parser.add_argument(
        "--poll",
        type=float,
        default=POLL_INTERVAL,
        help=f"Seconds between terminal polls (default {POLL_INTERVAL})",
    )
    parser.add_argument(
        "--min-ev",
        type=float,
        default=MIN_EV_PCT,
        help=f"Minimum EV %% to flag as actionable (default {MIN_EV_PCT})",
    )
    parser.add_argument(
        "--bet",
        action="store_true",
        help="Place bets when EV >= min threshold (default: dry-run, log only)",
    )
    book_group = parser.add_mutually_exclusive_group()
    book_group.add_argument(
        "--duel",
        action="store_true",
        help="Use Duel.com sports (BetBy) instead of Rainbet",
    )
    book_group.add_argument(
        "--bc",
        "--bcgame",
        dest="bcgame",
        action="store_true",
        help="Use BC.Game sports (BetBy) instead of Rainbet",
    )
    parser.add_argument(
        "--no-rainbet",
        action="store_true",
        help="POD scrape only — skip soft-book matching",
    )
    parser.add_argument(
        "--stake",
        type=float,
        default=FLAT_STAKE,
        help=f"Stake per bet in euros (default {FLAT_STAKE})",
    )
    parser.add_argument(
        "--max-alert-age",
        type=float,
        default=MAX_ALERT_AGE_SEC,
        help=f"Only process alerts newer than this many seconds (default {MAX_ALERT_AGE_SEC})",
    )
    parser.add_argument(
        "--probe",
        action="store_true",
        help="Scrape once after ENTER and print parsed alerts, then exit",
    )
    parser.add_argument(
        "--reset-seen",
        action="store_true",
        help="Clear pod_seen_alerts.json before starting",
    )
    parser.add_argument(
        "--sessions",
        type=int,
        default=MAX_CONCURRENT_SESSIONS,
        help=f"Parallel Rainbet worker tabs (default {MAX_CONCURRENT_SESSIONS})",
    )
    parser.add_argument(
        "--auto-start",
        action="store_true",
        help="Skip ENTER prompt — wait up to 2 min for POD grid after login",
    )
    parser.add_argument(
        "--proxy",
        "--vpn",
        dest="proxy_server",
        help="Proxy server URL, e.g. http://host:port or socks5://host:port",
    )
    parser.add_argument("--proxy-user", dest="proxy_user", help="Proxy username")
    parser.add_argument("--proxy-pass", dest="proxy_pass", help="Proxy password")
    return parser.parse_args()


def _parse_float(value: str | None) -> float | None:
    if not value:
        return None
    cleaned = value.strip().replace(",", ".")
    cleaned = re.sub(r"[^0-9.+\-]", "", cleaned)
    if not cleaned or cleaned in (".", "-", "+"):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None

def _normalise_market(text: str) -> str:
    t = text.strip().lower()
    if t in ("match", "moneyline", "money line", "ml", "match odds", "1x2"):
        return "moneyline"
    if any(k in t for k in ("spread", "handicap", "asian handicap", "ah")):
        return "spread"
    if any(k in t for k in ("total", "over", "under", "ou", "o/u")):
        return "total"
    if "half" in t or "set" in t or "quarter" in t or "period" in t:
        return "unknown"
    return t or "unknown"


def _alert_fingerprint(parts: list[str]) -> str:
    blob = "||".join(p.strip().lower() for p in parts if p and p.strip())
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()[:16]


def _parse_match_cell(text: str) -> dict[str, str]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    kickoff = lines[0] if lines else ""
    home_team = ""
    away_team = ""
    league = ""

    for ln in lines[1:]:
        if re.match(r"^H:\s*", ln, re.I):
            home_team = re.sub(r"^H:\s*", "", ln, flags=re.I).strip()
        elif re.match(r"^A:\s*", ln, re.I):
            away_team = re.sub(r"^A:\s*", "", ln, flags=re.I).strip()
        elif not league:
            league = ln

    event = ""
    if home_team and away_team:
        event = f"{home_team} vs {away_team}"
    elif lines:
        event = lines[-1]

    return {
        "kickoff": kickoff,
        "home_team": home_team,
        "away_team": away_team,
        "league": league,
        "event": event,
    }


def _parse_outcome_cell(text: str) -> dict[str, str | None]:
    if "|" in text:
        parts = [p.strip() for p in re.split(r"\s*\|\s*", text) if p.strip()]
    else:
        parts = [ln.strip() for ln in text.splitlines() if ln.strip()]

    period_raw = parts[0] if parts else ""
    selection = parts[1] if len(parts) > 1 else ""
    line = parts[2] if len(parts) > 2 else None

    # POD sometimes prefixes selection with the period again
    selection = re.sub(
        r"^(1st\s*half|2nd\s*half|first\s*half|second\s*half|"
        r"1st\s*set|2nd\s*set|3rd\s*set|first\s*set|second\s*set|third\s*set)\s+",
        "",
        selection,
        flags=re.I,
    ).strip()

    ou = re.match(r"^(over|under)\s+([0-9.]+)$", selection, re.I)
    if ou:
        return {
            "period": period_raw,
            "market": "total",
            "selection": ou.group(1).title(),
            "line": ou.group(2),
        }

    if line and selection.lower() in ("over", "under") and re.match(r"^[0-9.]+$", line):
        return {
            "period": period_raw,
            "market": "total",
            "selection": selection.title(),
            "line": line,
        }

    # Explicit moneyline label in 3rd column: "Moneyline (3-way)"
    if line and re.search(r"moneyline", line, re.I):
        return {
            "period": period_raw,
            "market": "moneyline",
            "selection": selection,
            "line": None,
        }

    market = _normalise_market(period_raw)

    # Numeric 3rd column is always a handicap/spread line — including 0 / 0.0
    # (POD Handicap 0 / DNB). Never strip "0" into moneyline.
    if line is not None and re.match(r"^[+-]?\d+(?:\.\d+)?$", str(line).strip()):
        if market in ("moneyline", "unknown") or "half" in market or "set" in market:
            market = "spread"
    elif market not in ("moneyline", "spread", "total"):
        # Bare team under a period header → moneyline for that period
        market = "moneyline"

    return {
        "period": period_raw,
        "market": market,
        "selection": selection,
        "line": line,
    }


def _parse_price_cell(text: str) -> dict[str, float | None]:
    nums = [_parse_float(m.group(1)) for m in re.finditer(r"([0-9]+\.[0-9]+)", text)]
    nums = [n for n in nums if n is not None]
    old_odds = nums[0] if len(nums) >= 1 else None
    new_odds = nums[1] if len(nums) >= 2 else None
    drop_pct = None
    if old_odds and new_odds and old_odds > 0:
        drop_pct = round((1 - new_odds / old_odds) * 100, 2)
    return {"old_odds": old_odds, "new_odds": new_odds, "drop_pct": drop_pct}


def _parse_alert_cell(text: str) -> dict[str, Any]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    sport = lines[0] if lines else ""
    age_sec: float | None = None
    lower = text.lower()
    if "year ago" in lower or "month ago" in lower or "week ago" in lower:
        age_sec = 999_999.0
    elif "just now" in lower or "a moment ago" in lower:
        age_sec = 0.0
    else:
        for ln in lines[1:]:
            ln_lower = ln.lower()
            if "just now" in ln_lower or "a moment ago" in ln_lower:
                age_sec = 0.0
                break
            m = re.search(
                r"(\d+)\s*(sec(?:ond)?s?|min(?:ute)?s?|hr?s?|hours?)\s+ago",
                ln,
                re.I,
            )
            if m:
                n = int(m.group(1))
                unit = m.group(2).lower()
                if unit.startswith("sec"):
                    age_sec = float(n)
                elif unit.startswith("min"):
                    age_sec = float(n * 60)
                else:
                    age_sec = float(n * 3600)
                break
    return {"sport": sport, "alert_age_sec": age_sec}


def _is_stale_row(cells: dict[str, str]) -> bool:
    blob = " ".join(cells.values()).lower()
    return any(k in blob for k in ("year ago", "month ago", "week ago"))


_IGNORED_MARKET_MARKERS = (
    "corner",
    "corners",
    "booking",
    "bookings",
    "card",
    "cards",
    "yellow card",
    "red card",
    "throw-in",
    "throw in",
    "offsides",
    "fouls",
    "shots",
    "saves",
)


def is_ignored_special_market(alert: PODAlert | None = None, **fields: str) -> bool:
    """
    Skip corners / bookings / specialty prop alerts for now.
    These share team names with the main match and must not bind to goal markets.
    """
    parts = [
        fields.get("home_team", ""),
        fields.get("away_team", ""),
        fields.get("league", ""),
        fields.get("event", ""),
        fields.get("selection", ""),
        fields.get("period", ""),
        fields.get("sport", ""),
    ]
    if alert is not None:
        parts.extend(
            [
                alert.home_team,
                alert.away_team,
                alert.league,
                alert.event,
                alert.selection,
                alert.period,
                alert.sport,
                alert.raw_text,
            ]
        )
    blob = " ".join(p for p in parts if p).lower()
    return any(m in blob for m in _IGNORED_MARKET_MARKERS)


def _build_alert_from_col_ids(
    cells: dict[str, str],
    row_id: str,
    row_text: str,
) -> PODAlert | None:
    if any("drag here" in v.lower() for v in cells.values()):
        return None

    if _is_stale_row(cells):
        return None

    match_text = cells.get("match", "")
    outcome_text = cells.get("rowOutcome", cells.get("outcome", ""))
    price_text = cells.get("current", cells.get("price", ""))
    novig_text = cells.get("noVigPrice", cells.get("no_vig_price", ""))
    alert_text = cells.get("alert", "")
    drop_text = cells.get("percentageChange", "")

    no_vig_odds = _parse_float(novig_text)
    if not match_text or not outcome_text or no_vig_odds is None:
        return None

    match_fields = _parse_match_cell(match_text)
    outcome_fields = _parse_outcome_cell(outcome_text)
    price_fields = _parse_price_cell(price_text)
    alert_fields = _parse_alert_cell(alert_text)

    if is_ignored_special_market(
        home_team=match_fields.get("home_team", ""),
        away_team=match_fields.get("away_team", ""),
        league=match_fields.get("league", ""),
        event=match_fields.get("event", ""),
        selection=str(outcome_fields.get("selection") or ""),
        period=str(outcome_fields.get("period") or ""),
        sport=str(alert_fields.get("sport") or ""),
    ):
        return None

    drop_pct = _parse_float(drop_text.replace("%", "")) if drop_text else None
    if drop_pct is None:
        drop_pct = price_fields.get("drop_pct")

    if not outcome_fields.get("selection"):
        return None

    alert_id = _alert_fingerprint(
        [
            row_id,
            match_fields.get("event", ""),
            outcome_fields.get("market", ""),
            outcome_fields.get("selection", ""),
            str(outcome_fields.get("line", "")),
        ]
    )

    return PODAlert(
        alert_id=alert_id,
        scraped_at=datetime.now().isoformat(timespec="seconds"),
        home_team=match_fields.get("home_team", ""),
        away_team=match_fields.get("away_team", ""),
        event=match_fields.get("event", ""),
        league=match_fields.get("league", ""),
        sport=alert_fields.get("sport", ""),
        kickoff=match_fields.get("kickoff", ""),
        period=outcome_fields.get("period", ""),
        market=outcome_fields.get("market", ""),
        selection=outcome_fields.get("selection", ""),
        line=outcome_fields.get("line"),
        old_odds=price_fields.get("old_odds"),
        new_odds=price_fields.get("new_odds"),
        no_vig_odds=no_vig_odds,
        drop_pct=drop_pct,
        alert_age_sec=alert_fields.get("alert_age_sec"),
        raw_text=row_text,
        raw_fields={"row_id": row_id, "cells": cells},
    )


def compute_ev_pct(soft_odds: float, no_vig_odds: float) -> float:
    return round((soft_odds / no_vig_odds - 1) * 100, 2)


# ── Rainbet matching ────────────────────────────────────────────────────────────
_TEAM_STOPWORDS = {
    "fc", "sc", "cf", "ac", "bc", "cd", "ec", "the", "de", "la", "el", "vs",
    "club", "united", "city", "town", "athletic", "atletico", "team",
}


def _normalize_team_for_match(name: str) -> str:
    name = normalise_team_name(name)
    name = name.replace("-", " ").replace("_", " ")
    name = re.sub(r"\bii\b", "2", name)
    name = re.sub(r"\biii\b", "3", name)
    name = re.sub(r"\biv\b", "4", name)
    name = re.sub(r"\bfc\b|\bsc\b|\bcf\b|\bac\b", "", name)
    return re.sub(r"\s+", " ", name).strip()


def team_tokens(name: str) -> set[str]:
    name = _normalize_team_for_match(name)
    return {
        tok
        for tok in re.findall(r"[a-z0-9]+", name)
        if len(tok) > 2 and tok not in _TEAM_STOPWORDS
    }


def _whole_token_in(tok: str, text: str) -> bool:
    """True when tok is a whole word in text — 'lee' must not hit 'bulleen'."""
    if not tok or not text:
        return False
    return re.search(rf"(^|[^a-z0-9]){re.escape(tok)}([^a-z0-9]|$)", text) is not None


def _nordic_fold(name: str) -> str:
    """Fold å/aa, æ/ae, ø/oe so Kjelsås/Kjelsaas matches Kjelsas."""
    s = (name or "").lower()
    s = s.replace("å", "aa").replace("æ", "ae").replace("ø", "oe")
    s = s.replace("aa", "a").replace("ae", "a").replace("oe", "o")
    return s


def _one_edit_apart(a: str, b: str) -> bool:
    """True when strings are equal or one insertion/deletion/substitution apart."""
    if a == b:
        return True
    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False
    if la > lb:
        a, b, la, lb = b, a, lb, la
    i = j = diffs = 0
    while i < la and j < lb:
        if a[i] == b[j]:
            i += 1
            j += 1
            continue
        diffs += 1
        if diffs > 1:
            return False
        if la == lb:
            i += 1
        j += 1
    if j < lb or i < la:
        diffs += (lb - j) + (la - i)
    return diffs <= 1


def _ordered_abbrev_in(short: str, long: str) -> bool:
    """
    'ham kam' / 'ham-kam' inside 'hamarkameratene' — parts in order, each >= 3.
    Requires 2+ parts so a lone 'ham' cannot match random teams.
    """
    parts = [p for p in re.split(r"[-_\s]+", short) if len(p) >= 3]
    if len(parts) < 2:
        return False
    pos = 0
    for part in parts:
        i = long.find(part, pos)
        if i < 0:
            return False
        pos = i + len(part)
    return True


def _team_name_sides(text: str) -> list[str]:
    t = _normalize_team_for_match(text)
    if " vs " in t:
        return [p.strip() for p in t.split(" vs ") if p.strip()]
    return [t] if t else []


def team_match_score(team: str, text: str) -> int:
    team_n = _normalize_team_for_match(team)
    text_n = _normalize_team_for_match(text)
    if not team_n or not text_n:
        return 0

    def _score_against(hay: str) -> int:
        if not hay:
            return 0
        if team_n in hay:
            if " " in team_n or len(team_n) >= 6:
                return 10
            if _whole_token_in(team_n, hay):
                return 10
        team_f = _nordic_fold(team_n)
        hay_f = _nordic_fold(hay)
        if team_f and team_f == hay_f:
            return 10
        if len(team_f) >= 6 and team_f in hay_f:
            return 8
        if len(team_f) >= 6 and _one_edit_apart(team_f, hay_f):
            return 8
        if _ordered_abbrev_in(team_f, hay_f) or _ordered_abbrev_in(hay_f, team_f):
            return 8
        toks = team_tokens(team)
        hay_toks = team_tokens(hay)
        score = 0
        for tok in toks:
            if _whole_token_in(tok, hay):
                score += 1
                continue
            tok_f = _nordic_fold(tok)
            if len(tok_f) >= 6 and any(
                _one_edit_apart(tok_f, _nordic_fold(ht)) for ht in hay_toks
            ):
                score += 1
        return score

    # Score against each side of "Home vs Away" so 'ham'+'kam' cannot
    # stitch across two different teams.
    sides = _team_name_sides(text)
    return max(_score_against(side) for side in sides) if sides else _score_against(text_n)


def teams_match_text(home: str, away: str, text: str) -> bool:
    home_score = team_match_score(home, text)
    away_score = team_match_score(away, text)
    return home_score >= 1 and away_score >= 1


def normalise_team_name(name: str) -> str:
    name = re.sub(r"\([^)]*\)", "", name)
    name = re.sub(r"\s+", " ", name).strip().lower()
    return name


def league_tokens(league: str) -> set[str]:
    return {
        tok
        for tok in re.findall(r"[a-z0-9]+", league.lower())
        if len(tok) > 2 and tok not in _TEAM_STOPWORDS
    }


def league_overlap_score(pod_league: str, rainbet_category: str) -> int:
    return len(league_tokens(pod_league) & league_tokens(rainbet_category))


_GENERIC_SPORTS = frozenset({"", "games", "game", "other", "sport", "sports"})
_URL_SPORT_MAP = {
    "soccer": "soccer",
    "football": "soccer",
    "tennis": "tennis",
    "ice-hockey": "ice hockey",
    "hockey": "ice hockey",
    "basketball": "basketball",
    "american-football": "american football",
    "baseball": "baseball",
    "volleyball": "volleyball",
    "handball": "handball",
    "table-tennis": "table tennis",
    "esports": "esports",
    "e-soccer": "esports",
    "esoccer": "esports",
    "e-basketball": "esports",
    "ebasketball": "esports",
    "e-cricket": "esports",
    "ecricket": "esports",
    "mma": "mma",
    "ufc": "mma",
    "cricket": "cricket",
}
_OT_MONEYLINE_SPORTS = frozenset({
    "ice hockey", "basketball", "american football", "baseball",
})


def canonical_sport(text: str) -> str:
    """Map POD / Rainbet sport or league text to a small canonical set."""
    raw = (text or "").strip().lower()
    if not raw or raw in _GENERIC_SPORTS:
        return ""
    t = re.sub(r"[^a-z0-9]+", " ", raw).strip()
    if not t or t in _GENERIC_SPORTS:
        return ""
    hyphen = t.replace(" ", "-")
    if hyphen in _URL_SPORT_MAP:
        return _URL_SPORT_MAP[hyphen]
    if t in _URL_SPORT_MAP:
        return _URL_SPORT_MAP[t]
    if "table tennis" in t:
        return "table tennis"
    if "tennis" in t:
        return "tennis"
    if re.search(r"\b(atp|wta|itf)\b", t):
        return "tennis"
    if "ice hockey" in t or re.search(r"\b(nhl|khl)\b", t):
        return "ice hockey"
    if re.search(r"\bhockey\b", t):
        return "ice hockey"
    if "basket" in t or re.search(r"\b(nba|wnba)\b", t):
        return "basketball"
    if "american football" in t or re.search(r"\bnfl\b", t):
        return "american football"
    if "baseball" in t or re.search(r"\bmlb\b", t):
        return "baseball"
    if "soccer" in t:
        return "soccer"
    if "football" in t:
        return "soccer"
    return ""


def sport_from_href(href: str) -> str:
    """Sport slug from a Rainbet/Duel event URL or card href."""
    if not href:
        return ""
    low = href.lower()
    m = re.search(r"/(?:sportsbook|sports)/([a-z0-9-]+)/", low)
    if not m:
        m = re.search(r"bt-path=/+([a-z0-9-]+)/", low)
    if not m:
        m = re.search(
            r"/(soccer|tennis|ice-hockey|basketball|american-football|"
            r"baseball|volleyball|handball|esports|e-soccer|esoccer|"
            r"e-basketball|ebasketball|e-cricket|ecricket|table-tennis|"
            r"mma|cricket)/",
            low,
        )
    if not m:
        return ""
    slug = m.group(1)
    return _URL_SPORT_MAP.get(slug, canonical_sport(slug))


def alert_canonical_sport(alert: PODAlert) -> str:
    return (
        canonical_sport(alert.sport)
        or canonical_sport(alert.league)
        or canonical_sport(alert.event)
    )


def card_canonical_sport(card: dict[str, str]) -> str:
    return sport_from_href(card.get("href", "")) or canonical_sport(
        card.get("category", "")
    )


def sports_agree(pod_sport: str, book_sport: str) -> bool:
    """Unknown on either side is not a reject; known disagreement is."""
    if not pod_sport or not book_sport:
        return True
    return pod_sport == book_sport


_MONTH_ABBR = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _abs_minutes(dt: datetime) -> int:
    return int(dt.timestamp() // 60)


def _resolve_year(month: int, day: int, hour: int, minute: int, now: datetime) -> datetime:
    """Pick the year that puts this date closest to now (handles Dec->Jan rollover)."""
    best: datetime | None = None
    for year in (now.year - 1, now.year, now.year + 1):
        try:
            candidate = datetime(year, month, day, hour, minute)
        except ValueError:
            continue
        if best is None or abs((candidate - now).total_seconds()) < abs((best - now).total_seconds()):
            best = candidate
    return best or now


def parse_pod_kickoff_dt(kickoff: str) -> datetime | None:
    """Parse POD '00:30 27/07' (HH:MM DD/MM) into a concrete datetime."""
    m = re.match(r"(\d{1,2}):(\d{2})\s+(\d{1,2})/(\d{1,2})", kickoff.strip())
    if not m:
        return None
    hour, minute, day, month = map(int, m.groups())
    return _resolve_year(month, day, hour, minute, datetime.now())


def parse_rainbet_dt(time_str: str) -> datetime | None:
    """
    Resolve a Rainbet card label to a concrete datetime.
    Handles 'Today, 19:00', 'Tomorrow, 01:15', 'In 2 days, 20:00', 'Aug 1, 18:30'.
    Live/finished labels ('3rd set', "70' 2nd half", 'Ended') return None.
    """
    text = time_str.strip()
    lower = text.lower()
    now = datetime.now()

    def at(day_offset: int, hour: int, minute: int) -> datetime:
        return (now + timedelta(days=day_offset)).replace(
            hour=hour, minute=minute, second=0, microsecond=0
        )

    m_in = re.search(r"in\s+(\d+)\s+days?,?\s*(\d{1,2}):(\d{2})", lower)
    if m_in:
        return at(int(m_in.group(1)), int(m_in.group(2)), int(m_in.group(3)))

    m_named = re.match(r"([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{1,2}):(\d{2})", text)
    if m_named and m_named.group(1)[:3].lower() in _MONTH_ABBR:
        month = _MONTH_ABBR[m_named.group(1)[:3].lower()]
        day = int(m_named.group(2))
        hour, minute = int(m_named.group(3)), int(m_named.group(4))
        return _resolve_year(month, day, hour, minute, now)

    m = re.search(r"(\d{1,2}):(\d{2})", text)
    if not m:
        return None
    hour, minute = int(m.group(1)), int(m.group(2))

    if "tomorrow" in lower:
        return at(1, hour, minute)
    if "yesterday" in lower:
        return at(-1, hour, minute)
    if "today" in lower:
        return at(0, hour, minute)

    # Live/finished states carry a clock-like token but no real kickoff
    if any(k in lower for k in ("half", "set", "inning", "quarter", "period", "ended", "'")):
        return None

    return at(0, hour, minute)


def parse_pod_kickoff_minutes(kickoff: str) -> int | None:
    dt = parse_pod_kickoff_dt(kickoff)
    return None if dt is None else _abs_minutes(dt)


def parse_rainbet_time_minutes(time_str: str) -> int | None:
    dt = parse_rainbet_dt(time_str)
    return None if dt is None else _abs_minutes(dt)


def _fmt_kickoff(dt: datetime | None) -> str:
    return "?" if dt is None else dt.strftime("%b %d %H:%M")


def kickoff_diff_minutes(pod_kickoff: str, rainbet_time: str) -> int | None:
    pod_dt = parse_pod_kickoff_dt(pod_kickoff)
    rb_dt = parse_rainbet_dt(rainbet_time)
    if pod_dt is None or rb_dt is None:
        return None
    return abs(int((pod_dt - rb_dt).total_seconds() // 60))


def _is_rainbet_imminent(time_str: str) -> bool:
    """Rainbet labels meaning the fixture is about to start / already warming up."""
    lower = (time_str or "").strip().lower()
    if not lower:
        return False
    return any(
        k in lower
        for k in (
            "warmup",
            "warm-up",
            "warm up",
            "about to start",
            "starting soon",
            "starts soon",
            "to begin",
        )
    )


def _is_rainbet_live_marker(time_str: str) -> bool:
    """Live / in-play Rainbet card labels (no wall-clock kickoff)."""
    lower = (time_str or "").strip().lower()
    if not lower:
        return False
    if any(
        k in lower
        for k in (
            "1st set", "2nd set", "3rd set", "4th set", "5th set",
            "first set", "second set", "third set", "fourth set", "fifth set",
            "1st half", "2nd half", "first half", "second half",
            "half time", "halftime", "half-time",
            "1st quarter", "2nd quarter", "3rd quarter", "4th quarter",
            "live", "in play", "in-play", "in progress",
        )
    ):
        return True
    if re.search(r"\bq[1-4]\b", lower):
        return True
    # Soccer/basketball clock like 70' / 12′
    if re.search(r"\d+['′]", lower):
        return True
    return False


def kickoff_status(
    pod_kickoff: str,
    rainbet_time: str,
    *,
    both_teams_matched: bool = False,
    league_matched: bool = False,
) -> tuple[bool, str]:
    """
    (matches, reason). The kickoff is a hard gate: relative Rainbet labels
    ('Tomorrow, 03:30') are resolved to a real date first, then the start time
    must line up with POD's.

    Exceptions (only with strong identity elsewhere):
    - Rainbet 'Warmup' / imminent labels — both teams must match
    - Live markers ('1st set', clocks) — both teams must match
    - ~1h clock drift — both teams AND league must match (timezone skew)
    """
    pod_dt = parse_pod_kickoff_dt(pod_kickoff)
    if pod_dt is None:
        return True, "no POD kickoff to validate"

    rb_dt = parse_rainbet_dt(rainbet_time)
    if rb_dt is None:
        if both_teams_matched and _is_rainbet_imminent(rainbet_time):
            return True, f"imminent '{rainbet_time}' + both teams matched"
        if both_teams_matched and _is_rainbet_live_marker(rainbet_time):
            return True, f"live '{rainbet_time}' + both teams matched"
        return False, f"can't parse Rainbet time '{rainbet_time}'"

    diff = abs(int((pod_dt - rb_dt).total_seconds() // 60))
    if diff <= KICKOFF_TOLERANCE_MIN:
        return True, "kickoff match"
    if (
        both_teams_matched
        and league_matched
        and diff <= KICKOFF_SAME_FIXTURE_TOLERANCE_MIN
    ):
        return True, f"kickoff ~timezone ({diff}min, both teams+league)"
    return (
        False,
        f"kickoff mismatch ({diff}min apart: POD {pod_kickoff} = {_fmt_kickoff(pod_dt)} "
        f"vs Rainbet '{rainbet_time}' = {_fmt_kickoff(rb_dt)})",
    )


def kickoff_matches(pod_kickoff: str, rainbet_time: str) -> bool:
    return kickoff_status(pod_kickoff, rainbet_time)[0]


_NON_TEAM_SELECTIONS = {
    "draw", "x", "over", "under", "1", "2", "home", "away", "yes", "no",
}


def pick_search_query(alert: PODAlert) -> str:
    """Always search by a team name — never by Draw/Over/Under.

    Prefer the longer side when one name is a short code (e.g. LBK) — BetBy
    search often returns nothing for 2–3 letter queries.
    """
    candidates: list[str] = []
    for candidate in (alert.home_team, alert.away_team):
        clean = re.sub(r"\([^)]*\)", "", candidate or "").strip()
        if clean:
            candidates.append(clean)
    if candidates:
        # Prefer a single distinctive token (Kjelsas) over hyphenated nicknames
        # that BetBy expands into 30+ fuzzy hits (Ham Kam → every "ham" team).
        candidates.sort(
            key=lambda s: (
                len(s) < 4,
                bool(re.search(r"[-_\s]", s)),
                -len(s),
            )
        )
        return candidates[0]
    sel = re.sub(r"\([^)]*\)", "", alert.selection or "").strip()
    if sel and sel.lower() not in _NON_TEAM_SELECTIONS:
        return sel
    return alert.home_team or alert.away_team or ""


_LEADING_CLUB_PREFIX = re.compile(
    r"^(al|el|fc|sc|cf|ac|hk|hc|fk|as|ssc|ud|cd|afc|cfc)\s+",
    re.I,
)


def _search_name_variants(name: str) -> list[str]:
    """Hyphen/space/Al- fallbacks so 'Al-Qadisiyah' can hit 'Al Qadsiah'."""
    clean = re.sub(r"\([^)]*\)", "", name or "").strip()
    if not clean:
        return []
    out: list[str] = []
    seen: set[str] = set()

    def add(q: str) -> None:
        q = re.sub(r"\s+", " ", q).strip()
        key = q.lower()
        if len(q) < 4 or key in seen:
            return
        seen.add(key)
        out.append(q)

    spaced = re.sub(r"[-_]+", " ", clean)
    stripped = _LEADING_CLUB_PREFIX.sub("", spaced).strip()
    # Distinctive remainder first — 'Al-Tai' search is drowned by every Al-* team
    add(stripped)
    add(spaced)
    add(clean)
    return out


def event_search_queries(alert: PODAlert) -> list[str]:
    """Ordered search strings: distinctive tokens, then full names, then both."""
    primary = pick_search_query(alert)
    other = ""
    for name in (alert.away_team, alert.home_team):
        clean = re.sub(r"\([^)]*\)", "", name or "").strip()
        if clean and clean.lower() != primary.lower():
            other = clean
            break
    queries: list[str] = []
    seen: set[str] = set()

    def add(q: str) -> None:
        q = re.sub(r"\s+", " ", q).strip()
        key = q.lower()
        if len(q) < 4 or key in seen:
            return
        seen.add(key)
        queries.append(q)

    for name in (primary, other):
        for variant in _search_name_variants(name):
            add(variant)
            if len(queries) >= 3:
                break
        if len(queries) >= 3:
            break
    h = (_search_name_variants(alert.home_team) or [""])[0]
    a = (_search_name_variants(alert.away_team) or [""])[0]
    if h and a and h.lower() != a.lower():
        add(f"{h} {a}")
    return queries[:4]


_rainbet_last_search_by_page: dict[int, float] = {}
_captcha_last_log_by_page: dict[int, float] = {}


async def human_pause(lo: float = 0.2, hi: float | None = None) -> None:
    """Jittered sleep — greyhound-style anti-robotic cadence."""
    if hi is None:
        hi = lo + 0.35
    await asyncio.sleep(random.uniform(lo, max(lo, hi)))


async def _rainbet_throttle(page=None) -> None:
    """Per-tab throttle with jitter so parallel sessions don't burst."""
    key = id(page) if page is not None else 0
    need = RAINBET_SEARCH_COOLDOWN + random.uniform(0.0, RAINBET_SEARCH_JITTER)
    elapsed = time.monotonic() - _rainbet_last_search_by_page.get(key, 0.0)
    if elapsed < need:
        await asyncio.sleep(need - elapsed)
    _rainbet_last_search_by_page[key] = time.monotonic()


CAPTCHA_DETECT_JS = """
() => {
  // Only flag a REAL interstitial / blocked page.
  // Soft books often embed invisible Turnstile/Cloudflare iframes on normal
  // sports pages - those alone must NEVER count as captcha.
  const bodyText = (document.body?.innerText || '').replace(/\\s+/g, ' ').trim().toLowerCase();
  const title = (document.title || '').toLowerCase();

  const hasSearch = !!document.querySelector(
    'svg[data-cy="ic-search"], [data-cy="ic-search"], input[type="search"]'
  );
  const hasSportsChrome = !!document.querySelector(
    '[data-editor-id="eventCard"], [data-editor-id="navbarIcon"], ' +
    '[data-editor-id="simpleMarketTitle"], [data-editor-id="marketCard"], ' +
    '[data-editor-id="marketCardHeader"], #bt-root, #betby, ' +
    'a[href*="/sportsbook"], a[href*="/sports"]'
  );

  // If sportsbook UI is usable, we are NOT on a captcha page
  if (hasSearch || hasSportsChrome) {
    return { hit: false, reason: '' };
  }

  const phrases = [
    'verify you are human',
    'confirm you are human',
    'just a moment',
    'checking your browser',
    'attention required',
    'please complete the security check',
    'enable javascript and cookies',
    'are you a robot',
    'cf-browser-verification',
  ];
  for (const p of phrases) {
    if (bodyText.includes(p) || title.includes(p)) {
      return { hit: true, reason: p };
    }
  }

  // Full-page challenge form (sports chrome already confirmed missing)
  const challengeRoot = document.querySelector(
    '#cf-challenge-running, #challenge-form, #challenge-stage, #cf-wrapper, .cf-error-details'
  );
  if (challengeRoot) {
    return { hit: true, reason: 'cf-challenge-page' };
  }

  // BC.Game (and some other shells) put a visible hCaptcha over the BetBy mount.
  // Ignore the invisible checkbox iframe; only a sized challenge counts.
  for (const frame of document.querySelectorAll('iframe')) {
    const src = (frame.src || '').toLowerCase();
    if (!src.includes('hcaptcha.com') && !src.includes('challenges.cloudflare.com')) continue;
    if (src.includes('checkbox-invisible') || src.includes('frame=checkbox')) continue;
    const r = frame.getBoundingClientRect();
    if (r.width > 40 && r.height > 40) {
      return { hit: true, reason: 'hcaptcha' };
    }
  }

  // Bare interstitial: almost no content + Cloudflare fingerprints
  if (bodyText.length < 600) {
    if (
      bodyText.includes('ray id') ||
      bodyText.includes('cloudflare') ||
      title.includes('moment') ||
      title.includes('attention') ||
      title.includes('just a')
    ) {
      return { hit: true, reason: 'challenge-interstitial' };
    }
  }

  return { hit: false, reason: '' };
}
"""


async def detect_rainbet_captcha(page) -> str | None:
    """
    True captcha/challenge interstitial only.
    Background Turnstile/Cloudflare iframes on a normal sportsbook page are ignored.
    """
    try:
        result = await page.evaluate(CAPTCHA_DETECT_JS)
        if isinstance(result, dict) and result.get("hit"):
            return str(result.get("reason") or "captcha")
    except Exception:
        pass
    return None


async def wait_out_captcha(page, *, label: str | None = None) -> bool:
    """
    If a real captcha interstitial is showing, pause until it clears.
    Never reloads/refreshes while captcha is up.
    """
    label = label or book_tag()
    reason = await detect_rainbet_captcha(page)
    if not reason:
        return False

    key = id(page)
    now = time.monotonic()
    if now - _captcha_last_log_by_page.get(key, 0.0) >= CAPTCHA_LOG_EVERY:
        log.warning(
            f"  [{label}] Captcha/challenge detected ({reason}) - "
            f"waiting for you to solve it (no page refresh)"
        )
        _captcha_last_log_by_page[key] = now

    while True:
        await asyncio.sleep(CAPTCHA_POLL_SEC)
        reason = await detect_rainbet_captcha(page)
        if not reason:
            log.info(f"  [{label}] Captcha cleared - resuming")
            _captcha_last_log_by_page.pop(key, None)
            await human_pause(0.8, 1.6)
            return True
        now = time.monotonic()
        if now - _captcha_last_log_by_page.get(key, 0.0) >= CAPTCHA_LOG_EVERY:
            log.warning(
                f"  [{label}] Still on captcha ({reason}) - solve it in the browser tab"
            )
            _captcha_last_log_by_page[key] = now


DEAD_PAGE_DETECT_JS = """
([domain, sportsPath]) => {
  const url = (location.href || '').toLowerCase();
  if (!url.includes(String(domain || '').toLowerCase())) {
    return { dead: false, reason: '' };
  }

  const body = document.body;
  const text = (body?.innerText || '').replace(/\\s+/g, ' ').trim();
  if (text.length < 40) return { dead: true, reason: 'blank-document' };

  // BetBy mounts under #betby (open shadow) and/or as #bt-root in light DOM.
  const deepQuery = (root, selector) => {
    if (!root) return null;
    try {
      const hit = root.querySelector?.(selector);
      if (hit) return hit;
    } catch (e) {}
    if (root.shadowRoot) {
      const nested = deepQuery(root.shadowRoot, selector);
      if (nested) return nested;
    }
    const nodes = root.querySelectorAll ? root.querySelectorAll('*') : [];
    for (const el of nodes) {
      if (el.shadowRoot) {
        const nested = deepQuery(el.shadowRoot, selector);
        if (nested) return nested;
      }
    }
    return null;
  };

  const widgetChrome = 'iframe, [data-editor-id], #bt-root, svg[data-cy="ic-search"], [data-cy="ic-search"]';
  const sportsPathLc = String(sportsPath || '').toLowerCase();
  const onSportsPath = sportsPathLc && url.includes(sportsPathLc) && !url.includes('/casino');
  const looksLikeSports = onSportsPath
    || url.includes('/sports')
    || /\\/(soccer|tennis|basketball|hockey|volleyball|baseball|american-football|handball|esports|greyhound|horse)\\//.test(url);

  // Duel (and some Rainbet layouts): #bt-root is already in the light DOM.
  // Require real sports chrome — not just a "Back online" check icon.
  const btRoot = document.querySelector('#bt-root');
  if (btRoot && (
    deepQuery(btRoot, widgetChrome)
    || btRoot.querySelector(
      '[data-editor-id], [data-cy="sport-page"], [data-cy="betslip"],'
      + ' svg[data-cy="ic-search"], [data-cy="ic-search"], svg[data-cy^="ic-"],'
      + ' svg[data-cy^="sport-"]'
    )
  )) {
    return { dead: false, reason: '' };
  }

  const host = document.querySelector('#betby');
  if (!host) {
    if (looksLikeSports) {
      return { dead: true, reason: 'sportsbook-widget-missing' };
    }
    return { dead: false, reason: '' };
  }

  // Live widget chrome inside the shadow tree, or a direct iframe under #betby.
  if (deepQuery(host, widgetChrome)) {
    return { dead: false, reason: '' };
  }

  if (host.children.length === 0) {
    return { dead: true, reason: 'betby-container-empty' };
  }
  // Wrapper div present but shadow/widget never mounted (idle timeout)
  return { dead: true, reason: 'sportsbook-widget-missing' };
}
"""


async def _probe_dead_page(page) -> str | None:
    book = get_book()
    try:
        result = await page.evaluate(
            DEAD_PAGE_DETECT_JS, [book.domain, book.sports_path]
        )
    except Exception:
        return None
    if isinstance(result, dict) and result.get("dead"):
        return str(result.get("reason") or "dead-page")
    return None


async def detect_rainbet_dead_page(page, *, confirm: bool = True) -> str | None:
    """
    Soft-book sports widget silently unmounts after long idle periods: the
    site navbar/footer still render but BetBy (#betby / #bt-root) has no live
    tree, so no search or event card ever appears again.
    """
    reason = await _probe_dead_page(page)
    if not reason or not confirm:
        return reason
    # A page mid-load looks identical — give it a moment before condemning it
    await asyncio.sleep(DEAD_PAGE_CONFIRM_SEC)
    return await _probe_dead_page(page)


async def _wait_sportsbook_mounted(page, timeout: float = DEAD_PAGE_READY_TIMEOUT) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await detect_rainbet_captcha(page):
            return False
        if not await _probe_dead_page(page):
            return True
        await asyncio.sleep(1.0)
    return not await _probe_dead_page(page)


SESSION_EXPIRED_DETECT_JS = """
() => {
  const isVisible = (el) => {
    if (!el || !el.getBoundingClientRect) return false;
    try {
      const s = window.getComputedStyle(el);
      if (s && (s.display === 'none' || s.visibility === 'hidden' || Number(s.opacity) === 0)) {
        return false;
      }
    } catch (e) {}
    const r = el.getBoundingClientRect();
    return r.width > 8 && r.height > 8;
  };
  const walk = (root) => {
    if (!root) return false;
    try {
      const svg = root.querySelector?.('svg[data-cy="unauthorized"]');
      if (svg && isVisible(svg)) return true;
    } catch (e) {}
    try {
      const nodes = root.querySelectorAll ? root.querySelectorAll('h1, h2, h3, p, span, div, button') : [];
      for (const el of nodes) {
        if (!isVisible(el)) continue;
        const t = (el.innerText || el.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();
        if (!t || t.length > 80) continue;
        if (t.includes('your session has expired')) return true;
      }
    } catch (e) {}
    if (root.shadowRoot && walk(root.shadowRoot)) return true;
    const all = root.querySelectorAll ? root.querySelectorAll('*') : [];
    for (const el of all) {
      if (el.shadowRoot && walk(el.shadowRoot)) return true;
    }
    return false;
  };
  return walk(document.body);
}
"""


async def detect_rainbet_session_expired(page) -> bool:
    """
    BetBy idle-timeout interstitial: 'Your session has expired' + Refresh now.
    Still has data-editor-id chrome (betslip), so the dead-page probe misses it.
    Only count *visible* copy so leftover hidden nodes don't trigger F5 loops.
    """
    try:
        return bool(await page.evaluate(SESSION_EXPIRED_DETECT_JS))
    except Exception:
        return False


async def _find_session_refresh_button(page):
    """Locate BetBy's 'Refresh now' brand button (shadow-piercing)."""
    btn, target = await _first_locator(
        page,
        [
            'button:has-text("Refresh now")',
            'button[data-editor-id="brandButton"]',
        ],
    )
    if not btn:
        return None, page
    try:
        text = ((await btn.inner_text()) or "").strip().lower()
    except Exception:
        text = ""
    # brandButton is also used elsewhere — require Refresh wording
    if "refresh" in text:
        return btn, target
    btn2, target2 = await _first_locator(page, ['button:has-text("Refresh now")'])
    if btn2:
        return btn2, target2
    return None, page


async def _click_session_refresh_js(page) -> bool:
    """Click visible 'Refresh now' even when it lives in open shadow."""
    try:
        return bool(
            await page.evaluate(
                """() => {
                  const isVisible = (el) => {
                    if (!el) return false;
                    const r = el.getBoundingClientRect();
                    return r.width > 8 && r.height > 8;
                  };
                  const walk = (root) => {
                    if (!root) return null;
                    const btns = root.querySelectorAll ? root.querySelectorAll('button') : [];
                    for (const b of btns) {
                      const t = (b.innerText || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                      if (t.includes('refresh') && isVisible(b)) return b;
                    }
                    if (root.shadowRoot) {
                      const hit = walk(root.shadowRoot);
                      if (hit) return hit;
                    }
                    const all = root.querySelectorAll ? root.querySelectorAll('*') : [];
                    for (const el of all) {
                      if (el.shadowRoot) {
                        const hit = walk(el.shadowRoot);
                        if (hit) return hit;
                      }
                    }
                    return null;
                  };
                  const btn = walk(document.body);
                  if (!btn) return false;
                  btn.click();
                  return true;
                }"""
            )
        )
    except Exception:
        return False


async def _sportsbook_ui_ready(page) -> bool:
    """True when the expired overlay is gone and BetBy chrome is mounted."""
    if await detect_rainbet_session_expired(page):
        return False
    # Shadow-aware mount check — Playwright often cannot see ic-search on Duel
    return not await _probe_dead_page(page)


async def keepalive_sportsbook(page, *, label: str | None = None) -> None:
    """
    Light real-pointer activity so BetBy's ~30 min idle timeout does not fire.
    Prefer a search open/close when the icon is reachable; otherwise mouse + wheel.
    """
    label = label or book_tag()
    if await detect_rainbet_captcha(page):
        return
    if await detect_rainbet_session_expired(page) or await _probe_dead_page(page):
        return
    try:
        box = await page.evaluate(
            """() => {
              const host = document.querySelector('#bt-root, #betby');
              if (!host) return null;
              const r = host.getBoundingClientRect();
              if (r.width < 40 || r.height < 40) return null;
              return { x: r.left + r.width * 0.32, y: r.top + Math.min(140, r.height * 0.22) };
            }"""
        )
        if isinstance(box, dict) and box.get("x") is not None:
            await page.mouse.move(float(box["x"]), float(box["y"]))
            await human_pause(0.05, 0.12)
            await page.mouse.move(float(box["x"]) + 16, float(box["y"]) + 10)
            await page.mouse.wheel(0, 70)
            await asyncio.sleep(0.12)
            await page.mouse.wheel(0, -70)
        icon, _ = await _first_locator(
            page,
            [
                'svg[data-cy="ic-search"]',
                '[data-cy="ic-search"]',
                'div[data-editor-id="navbarIcon"]:has(svg[data-cy="ic-search"])',
            ],
        )
        if icon:
            try:
                await human_click(icon, timeout=2000)
                await human_pause(0.15, 0.35)
            except Exception:
                pass
            await close_rainbet_search(page)
        log.info(f"  [{label}] Sportsbook keepalive")
    except Exception as e:
        log.debug(f"  [{label}] Keepalive skipped: {e}")


async def recover_rainbet_session_expired(
    page, *, label: str | None = None
) -> bool:
    """
    Click 'Refresh now' on the BetBy session-expired interstitial.
    Only F5 if the widget is still actually missing after remount grace.
    """
    label = label or book_tag()
    if not await detect_rainbet_session_expired(page):
        return True

    log.warning(f"  [{label}] Sportsbook session expired — clicking Refresh now")
    clicked = False
    btn, _ = await _find_session_refresh_button(page)
    if btn:
        try:
            await human_click(btn, timeout=5000)
            clicked = True
        except Exception as e:
            log.warning(f"  [{label}] Refresh click failed ({e})")
    if not clicked:
        clicked = await _click_session_refresh_js(page)
    if not clicked:
        log.warning(f"  [{label}] Refresh now button missing — reloading tab (F5)")
        return await recover_rainbet_page(page, label=label, force=True)

    await human_pause(1.0, 2.0)
    await wait_out_captcha(page, label=label)

    # Give BetBy time to remount — do not F5 just because Playwright
    # cannot pierce the search icon.
    if await _wait_sportsbook_mounted(page) and not await detect_rainbet_session_expired(page):
        log.info(f"  [{label}] Session refreshed")
        _rainbet_last_search_by_page.pop(id(page), None)
        return True

    if await _sportsbook_ui_ready(page):
        log.info(f"  [{label}] Session refreshed")
        _rainbet_last_search_by_page.pop(id(page), None)
        return True

    # Overlay gone but chrome still mounting — wait a bit more before F5
    extra_deadline = time.monotonic() + 12.0
    while time.monotonic() < extra_deadline:
        if await _sportsbook_ui_ready(page):
            log.info(f"  [{label}] Session refreshed")
            _rainbet_last_search_by_page.pop(id(page), None)
            return True
        await asyncio.sleep(0.8)

    log.warning(f"  [{label}] Still blank after Refresh now — reloading tab (F5)")
    try:
        await page.reload(wait_until="domcontentloaded")
    except Exception as e:
        log.warning(f"  [{label}] F5 failed ({e}) — trying full navigation")
        return await recover_rainbet_page(page, label=label, force=True)

    await human_pause(1.0, 2.0)
    await wait_out_captcha(page, label=label)

    if await detect_rainbet_session_expired(page):
        btn2, _ = await _find_session_refresh_button(page)
        if btn2:
            try:
                await human_click(btn2, timeout=5000)
                await human_pause(1.0, 2.0)
            except Exception:
                pass
        else:
            await _click_session_refresh_js(page)

    if await _wait_sportsbook_mounted(page) and await _sportsbook_ui_ready(page):
        log.info(f"  [{label}] Sportsbook recovered after F5")
        _rainbet_last_search_by_page.pop(id(page), None)
        return True

    return await recover_rainbet_page(page, label=label, force=True)


async def _hard_reload_page(page, url: str) -> None:
    """
    Hard refresh: disable the HTTP cache, reload, fall back to a fresh navigation.
    Soft reload leaves a frozen POD/BetBy websocket sitting on the same document.
    """
    client = None
    reloaded = False
    try:
        client = await page.context.new_cdp_session(page)
        await client.send("Network.enable")
        await client.send("Network.setCacheDisabled", {"cacheDisabled": True})
        await page.reload(wait_until="domcontentloaded")
        reloaded = True
    except Exception:
        reloaded = False
    finally:
        if client is not None:
            try:
                await client.send("Network.setCacheDisabled", {"cacheDisabled": False})
            except Exception:
                pass
            try:
                await client.detach()
            except Exception:
                pass
    if not reloaded:
        await page.goto(url, wait_until="domcontentloaded")


async def recover_rainbet_page(
    page, *, label: str | None = None, force: bool = False, hard: bool = False
) -> bool:
    """
    Reload a hollow soft-book tab. Returns True if the page is usable afterwards.
    Never reloads while a captcha is up — that would just re-trigger the check.
    Pass force=True to reload a tab that looks intact but is behaving as if dead.
    Pass hard=True for a cache-bypassing refresh (frozen idle sessions).
    """
    label = label or book_tag()
    if hard:
        reason = "hard refresh"
    else:
        reason = await detect_rainbet_dead_page(page)
        if not reason:
            if not force:
                return True
            reason = "sportsbook unresponsive"

    for attempt in range(1, DEAD_PAGE_MAX_ATTEMPTS + 1):
        if await detect_rainbet_captcha(page):
            await wait_out_captcha(page, label=label)
            if not hard and not await detect_rainbet_dead_page(page):
                return True

        log.warning(
            f"  [{label}] Sportsbook widget not loaded ({reason}) — "
            f"{'hard-' if hard else ''}reloading tab "
            f"(attempt {attempt}/{DEAD_PAGE_MAX_ATTEMPTS})"
        )
        try:
            if hard or attempt > 1:
                await _hard_reload_page(page, get_book().sports_url)
            else:
                await page.reload(wait_until="domcontentloaded")
        except Exception as e:
            log.warning(f"  [{label}] Reload failed: {e}")
            try:
                await page.goto(get_book().sports_url, wait_until="domcontentloaded")
            except Exception:
                pass

        await human_pause(1.0, 2.0)
        await wait_out_captcha(page, label=label)

        if await _wait_sportsbook_mounted(page):
            log.info(f"  [{label}] Sportsbook recovered")
            _rainbet_last_search_by_page.pop(id(page), None)
            return True

        reason = await _probe_dead_page(page) or reason
        await asyncio.sleep(min(15.0, 3.0 * attempt))

    log.error(f"  [{label}] Sportsbook still not loading after {DEAD_PAGE_MAX_ATTEMPTS} attempts")
    return False


async def ensure_rainbet_alive(page, *, label: str | None = None) -> bool:
    """Cheap pre-flight: session-expired first, then hollow-widget recovery."""
    label = label or book_tag()
    if await detect_rainbet_session_expired(page):
        if not await recover_rainbet_session_expired(page, label=label):
            return False
    if not await _probe_dead_page(page):
        return True
    return await recover_rainbet_page(page, label=label)


async def human_click(el, *, timeout: float = 3000, force: bool = False) -> None:
    """
    Hover briefly, pause, then click — less robotic than instant spam clicks.
    If the normal click is intercepted (sticky header / overlay / shadow quirks),
    fall back to force click, then a DOM click.
    """
    try:
        frame = await el.owner_frame()
        page = frame.page if frame is not None else None
        if page is not None:
            await wait_for_preloader_gone(page, timeout=min(4.0, PRELOADER_WAIT_TIMEOUT))
    except Exception:
        pass

    try:
        await el.scroll_into_view_if_needed(timeout=timeout)
    except Exception:
        pass

    if not force:
        try:
            await el.hover(timeout=min(timeout, 2000))
        except Exception:
            pass
        await human_pause(*HUMAN_CLICK_PAUSE)

    try:
        await el.click(timeout=timeout, force=force)
        return
    except Exception as first_err:
        # "performing click action" timeouts usually mean something is covering
        # the plate (header, toast, betslip, #preloader). Force + DOM click bypass that.
        try:
            await el.click(timeout=timeout, force=True)
            return
        except Exception:
            pass
        try:
            await el.evaluate("node => { node.click(); }")
            return
        except Exception:
            pass
        try:
            box = await el.bounding_box()
            frame = await el.owner_frame()
            if box and frame is not None:
                page = frame.page
                await page.mouse.click(
                    box["x"] + box["width"] / 2,
                    box["y"] + box["height"] / 2,
                )
                return
        except Exception:
            pass
        raise first_err


async def wait_for_preloader_gone(page, timeout: float = PRELOADER_WAIT_TIMEOUT) -> bool:
    """Wait until Rainbet #preloader is hidden/detached so clicks are not intercepted."""
    deadline = time.monotonic() + timeout
    js = """() => {
      const els = [
        ...document.querySelectorAll('#preloader, section#preloader, .z-preloader')
      ];
      for (const el of els) {
        const st = getComputedStyle(el);
        if (st.display === 'none' || st.visibility === 'hidden' || Number(st.opacity) === 0) {
          continue;
        }
        const r = el.getBoundingClientRect();
        if (r.width > 2 && r.height > 2) return true;
      }
      return false;
    }"""
    while time.monotonic() < deadline:
        blocking = False
        for target in [page, *getattr(page, "frames", [])]:
            try:
                if await target.evaluate(js):
                    blocking = True
                    break
            except Exception:
                continue
        if not blocking:
            return True
        await asyncio.sleep(0.2)
    return False


async def soft_return_sportsbook(page, *, allow_goto: bool = False) -> None:
    """
    Leave event/search UI without a hard reload when possible.
    Hard goto only when allow_goto and not on a captcha page.
    """
    book = get_book()
    await wait_out_captcha(page)
    await close_rainbet_search(page)
    await human_pause(0.25, 0.6)

    url = (page.url or "").lower()
    if (
        book.domain in url
        and book.sports_path in url
        and "/casino" not in url
    ):
        # Already on sports — Escape is enough; avoid reload loops
        return

    # Prefer clicking Sports nav over a full navigation reload
    for sel in book.nav_hrefs:
        el, target = await _first_locator(page, [sel])
        if not el:
            continue
        try:
            await human_click(el)
            await human_pause(0.6, 1.1)
            await wait_out_captcha(page)
            return
        except Exception:
            try:
                await target.evaluate("(node) => node.click()", el)
                await human_pause(0.6, 1.1)
                return
            except Exception:
                continue

    if allow_goto and not await detect_rainbet_captcha(page):
        await page.goto(book.sports_url, wait_until="domcontentloaded")
        await human_pause(0.8, 1.4)
        await wait_out_captcha(page)


EVENT_CARDS_JS = """
() => {
  const extractTeams = (link) => {
    if (!link) return '';
    const rows = [...link.querySelectorAll('div.sc-1gh6pgx-2')];
    const names = rows
      .map((row) => row.innerText.replace(/\\s+/g, ' ').trim())
      .filter((t) => t && !/^\\d+\\.\\d+$/.test(t));
    if (names.length >= 2) return names.slice(0, 2).join(' vs ');
    if (names.length === 1) return names[0];
  const teamDivs = [...link.querySelectorAll('div[class*="jss1784"], [data-editor-id="searchResultHighlightItem"]')];
    if (teamDivs.length) {
      const parentRows = [...new Set(teamDivs.map((el) => el.closest('div.sc-1gh6pgx-2') || el.parentElement))];
      const fromParents = parentRows.map((el) => el?.innerText?.replace(/\\s+/g, ' ')?.trim()).filter(Boolean);
      if (fromParents.length >= 2) return fromParents.slice(0, 2).join(' vs ');
    }
    const raw = link.innerText.replace(/\\s+/g, ' ').trim();
    const timeEl = link.querySelector('[data-editor-id="eventCardStatusLabel"]');
    const time = timeEl?.innerText?.trim() || '';
    return time ? raw.replace(time, '').trim() : raw;
  };

    return [...document.querySelectorAll('div[data-editor-id="eventCard"]')].map((card) => {
    const link = card.querySelector('a[data-editor-id="eventCardContent"]');
    const href = link?.getAttribute('href') || '';
    const category = card.querySelector('[data-editor-id="eventCardCategory"]')?.innerText?.replace(/\\s+/g, ' ').trim() || '';
    const time = card.querySelector('[data-editor-id="eventCardStatusLabel"]')?.innerText?.replace(/\\s+/g, ' ').trim() || '';
    const teams = extractTeams(link);
    return { category, time, teams, href };
  }).filter((c) => {
    if (!c.href) return false;
    const teams = (c.teams || '').toLowerCase();
    if (!teams) return false;
    // League/competition shortcuts have a title and no fixture time
    if (!teams.includes(' vs ') && !c.time) return false;
    return true;
  });
}
"""


def score_event_card(card: dict[str, str], alert: PODAlert) -> int:
    """Kickoff + sport + both teams are hard gates — never open a mismatch."""
    teams = card.get("teams", "")
    home_score = team_match_score(alert.home_team, teams)
    away_score = team_match_score(alert.away_team, teams)
    team_score = home_score + away_score
    both_teams = home_score >= 1 and away_score >= 1
    league_score = league_overlap_score(alert.league, card.get("category", ""))
    pod_sport = alert_canonical_sport(alert)
    book_sport = card_canonical_sport(card)

    if not sports_agree(pod_sport, book_sport):
        return -1

    ok, _reason = kickoff_status(
        alert.kickoff,
        card.get("time", ""),
        both_teams_matched=both_teams,
        league_matched=league_score > 0,
    )
    if not ok:
        return -1

    # One team token is not enough (tennis 'Lee' used to open soccer 'Bulleen')
    if not both_teams:
        return -1

    score = 20  # passed kickoff gate
    score += team_score * 5
    score += league_score * 3
    score += 15  # both teams required
    if pod_sport and book_sport and pod_sport == book_sport:
        score += 10
    return score


def explain_card_rejection(card: dict[str, str], alert: PODAlert) -> str:
    teams = card.get("teams", "")
    home_score = team_match_score(alert.home_team, teams)
    away_score = team_match_score(alert.away_team, teams)
    both_teams = home_score >= 1 and away_score >= 1
    league_matched = league_overlap_score(alert.league, card.get("category", "")) > 0
    pod_sport = alert_canonical_sport(alert)
    book_sport = card_canonical_sport(card)
    if not sports_agree(pod_sport, book_sport):
        return (
            f"sport mismatch (POD {pod_sport or '?'} vs Rainbet {book_sport or '?'})"
        )
    ok, kickoff_reason = kickoff_status(
        alert.kickoff,
        card.get("time", ""),
        both_teams_matched=both_teams,
        league_matched=league_matched,
    )
    if not ok:
        return kickoff_reason
    if not both_teams:
        return (
            "kickoff OK but both teams not matched "
            f"(home={home_score} away={away_score})"
        )
    score = score_event_card(card, alert)
    if score < 0:
        return "rejected"
    return f"score {score}"


async def _with_timeout(coro, seconds: float, label: str):
    try:
        return await asyncio.wait_for(coro, timeout=seconds)
    except asyncio.TimeoutError:
        log.error(f"  [{book_tag()}] Timed out after {seconds:.0f}s: {label}")
        return None
    except Exception as e:
        log.error(f"  [{book_tag()}] Error during {label}: {e}")
        return None


async def close_rainbet_search(page) -> None:
    try:
        await page.keyboard.press("Escape")
        await human_pause(0.2, 0.45)
    except Exception:
        pass


async def _query_all(page, selector: str):
    found = []
    try:
        found.extend(await page.query_selector_all(selector))
    except Exception:
        pass
    for frame in page.frames:
        try:
            found.extend(await frame.query_selector_all(selector))
        except Exception:
            pass
    return found


async def _first_locator(page, selectors: list[str]):
    for sel in selectors:
        for target in [page, *page.frames]:
            try:
                el = await target.query_selector(sel)
                if el:
                    return el, target
            except Exception:
                pass
    return None, page


async def ensure_rainbet_sportsbook(page, *, force_home: bool = False) -> None:
    """
    Make sure navbar search is available.
    Never hard-reloads while a captcha/challenge is showing.
    Prefer soft return over force_home goto.
    """
    book = get_book()
    await wait_out_captcha(page)
    await ensure_rainbet_alive(page)

    url = (page.url or "").lower()
    needs_home = (
        force_home
        or book.domain not in url
        or "/casino" in url
        or book.sports_path not in url
    )
    if not needs_home:
        return

    if await detect_rainbet_captcha(page):
        await wait_out_captcha(page)
        return

    if force_home:
        # Soft first — avoid reload loops that re-trigger captcha
        await soft_return_sportsbook(page, allow_goto=False)
        url = (page.url or "").lower()
        if (
            book.domain in url
            and book.sports_path in url
            and "/casino" not in url
            and not await detect_rainbet_captcha(page)
        ):
            # If search icon is already usable, skip goto
            icon, _ = await _first_locator(
                page,
                [
                    'svg[data-cy="ic-search"]',
                    '[data-cy="ic-search"]',
                    'div[data-editor-id="navbarIcon"]:has(svg[data-cy="ic-search"])',
                ],
            )
            if icon:
                return

    if await detect_rainbet_captcha(page):
        await wait_out_captcha(page)
        return

    await page.goto(book.sports_url, wait_until="domcontentloaded")
    await human_pause(0.7, 1.3)
    await wait_out_captcha(page)


async def open_rainbet_search(page, *, _captcha_retry: int = 0) -> bool:
    await wait_out_captcha(page)
    await ensure_rainbet_sportsbook(page)
    await close_rainbet_search(page)
    await human_pause(0.25, 0.55)

    # Click the search SVG, or its navbarIcon parent
    for sel in (
        'svg[data-cy="ic-search"]',
        '[data-cy="ic-search"]',
        'div[data-editor-id="navbarIcon"]:has(svg[data-cy="ic-search"])',
    ):
        icon, target = await _first_locator(page, [sel])
        if not icon:
            continue
        try:
            handle = await icon.evaluate_handle(
                """(el) => el.closest('[data-editor-id="navbarIcon"]') || el"""
            )
            el = handle.as_element()
            if el:
                await human_click(el, timeout=3000)
            else:
                await human_click(icon, timeout=3000)
        except Exception:
            try:
                await target.evaluate("(el) => el.click()", icon)
            except Exception:
                continue
        await human_pause(0.45, 0.85)
        await wait_out_captcha(page)
        inp, _ = await _find_search_input(page)
        if inp:
            return True

    # Missing search chrome is often a challenge page — wait, don't spam reopen
    if _captcha_retry < 2 and await detect_rainbet_captcha(page):
        await wait_out_captcha(page)
        return await open_rainbet_search(page, _captcha_retry=_captcha_retry + 1)

    # BetBy "Your session has expired" still has chrome but no search navbar
    if _captcha_retry < 2 and await detect_rainbet_session_expired(page):
        if await recover_rainbet_session_expired(page):
            return await open_rainbet_search(page, _captcha_retry=_captcha_retry + 1)

    log.warning(f"  [{book_tag()}] Search icon/input not found (url={page.url})")
    return False


async def _find_search_input(page):
    inp, target = await _first_locator(
        page,
        [
            'input[type="search"]',
            'input[placeholder*="earch" i]',
            'input[aria-label*="earch" i]',
            'input[data-editor-id*="search" i]',
        ],
    )
    return inp, target


async def _type_search_query(inp, query: str) -> None:
    """Type search query quickly — drops need haste more than perfect cadence."""
    await human_click(inp, timeout=2000)
    await human_pause(0.04, 0.1)
    try:
        await inp.press("Control+A")
        await inp.press("Backspace")
    except Exception:
        try:
            await inp.fill("")
        except Exception:
            pass

    delay = random.randint(*HUMAN_TYPE_DELAY_MS)
    try:
        await inp.type(query, delay=delay)
    except Exception:
        await inp.fill(query)

    await inp.evaluate(
        """(el, q) => {
            el.focus();
            const setter = Object.getOwnPropertyDescriptor(
                window.HTMLInputElement.prototype, 'value'
            ).set;
            setter.call(el, q);
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
            el.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true }));
        }""",
        query,
    )
    await human_pause(0.08, 0.2)
    try:
        await inp.press("Enter")
    except Exception:
        pass
    await human_pause(0.06, 0.15)


async def _evaluate_event_cards(scope) -> list[dict[str, str]]:
    try:
        cards = await scope.evaluate(EVENT_CARDS_JS)
        return cards if isinstance(cards, list) else []
    except Exception:
        return []


async def _extract_card_from_element(card_el) -> dict[str, str] | None:
    try:
        link = await card_el.query_selector('a[data-editor-id="eventCardContent"]')
        if not link:
            return None
        href = await link.get_attribute("href") or ""
        if not href:
            return None

        cat_el = await card_el.query_selector('[data-editor-id="eventCardCategory"]')
        category = re.sub(r"\s+", " ", (await cat_el.inner_text()).strip()) if cat_el else ""

        time_el = await card_el.query_selector('[data-editor-id="eventCardStatusLabel"]')
        time_str = re.sub(r"\s+", " ", (await time_el.inner_text()).strip()) if time_el else ""

        team_rows = await link.query_selector_all("div.sc-1gh6pgx-2")
        names: list[str] = []
        for row in team_rows:
            text = re.sub(r"\s+", " ", (await row.inner_text()).strip())
            if text and not re.match(r"^\d+\.\d+$", text):
                names.append(text)
        teams = " vs ".join(names[:2]) if len(names) >= 2 else (names[0] if names else "")
        if not teams:
            raw = re.sub(r"\s+", " ", (await link.inner_text()).strip())
            teams = raw.replace(time_str, "").strip() if time_str else raw

        return {"category": category, "time": time_str, "teams": teams, "href": href}
    except Exception:
        return None


async def _scrape_event_cards(page, target) -> list[dict[str, str]]:
    """Scrape event cards from every frame — search overlay may not be in main document."""
    cards: list[dict[str, str]] = []
    seen_hrefs: set[str] = set()

    scopes: list = [page]
    if target is not page:
        scopes.append(target)
    for frame in page.frames:
        if frame not in scopes:
            scopes.append(frame)

    for scope in scopes:
        for card in await _evaluate_event_cards(scope):
            href = card.get("href", "")
            if href and href not in seen_hrefs:
                seen_hrefs.add(href)
                cards.append(card)

    if cards:
        return cards

    for card_el in await _query_all(page, 'div[data-editor-id="eventCard"]'):
        data = await _extract_card_from_element(card_el)
        if data and data["href"] not in seen_hrefs:
            seen_hrefs.add(data["href"])
            cards.append(data)

    return cards


async def _count_event_card_elements(page) -> int:
    return len(await _query_all(page, 'div[data-editor-id="eventCard"]'))


async def _wait_for_event_cards(page, target, query: str) -> list[dict[str, str]]:
    """Wait for Rainbet search debounce + result cards to render."""
    await asyncio.sleep(RAINBET_SEARCH_MIN_WAIT + random.uniform(0.0, 0.4))
    deadline = time.monotonic() + RAINBET_SEARCH_RESULT_TIMEOUT
    while time.monotonic() < deadline:
        if await detect_rainbet_captcha(page):
            await wait_out_captcha(page)
            deadline = time.monotonic() + RAINBET_SEARCH_RESULT_TIMEOUT
        visible = await _count_event_card_elements(page)
        cards = await _scrape_event_cards(page, target)
        if cards:
            log.info(
                f"  [{book_tag()}] Search '{query}' -> {len(cards)} result(s)"
                f"{f' ({visible} card element(s) in DOM)' if visible != len(cards) else ''}"
            )
            for card in cards[:3]:
                log.info(
                    f"  [{book_tag()}]     - {_safe_text(card.get('teams', '?'))} @ {_safe_text(card.get('time', '?'))}"
                    f" | {_safe_text(card.get('category', ''))}"
                )
            return cards
        if visible:
            log.debug(f"  [{book_tag()}] {visible} card element(s) visible but scrape incomplete, retrying...")
        await asyncio.sleep(RAINBET_SEARCH_POLL_INTERVAL + random.uniform(0.0, 0.15))
    visible = await _count_event_card_elements(page)
    log.warning(
        f"  [{book_tag()}] Search '{query}' -> no results after {RAINBET_SEARCH_RESULT_TIMEOUT:.0f}s"
        f"{f' ({visible} card element(s) in DOM — scrape failed)' if visible else ''}"
    )
    return []


async def search_rainbet_events(
    page,
    query: str,
    *,
    open_panel: bool = True,
) -> list[dict[str, str]]:
    await wait_out_captcha(page)
    if open_panel:
        if not await open_rainbet_search(page):
            # One soft recovery — never force-reload while captcha is up
            if await detect_rainbet_captcha(page):
                await wait_out_captcha(page)
            elif await detect_rainbet_session_expired(page):
                await recover_rainbet_session_expired(page)
            elif await detect_rainbet_dead_page(page):
                await recover_rainbet_page(page)
            else:
                await soft_return_sportsbook(page, allow_goto=True)
                await human_pause(0.5, 1.0)
            if not await open_rainbet_search(page):
                return []
    else:
        # Ensure input still exists; reopen if the panel closed
        inp_check, _ = await _find_search_input(page)
        if not inp_check:
            if not await open_rainbet_search(page):
                return []

    inp, target = await _find_search_input(page)
    if not inp:
        log.warning(f"  [{book_tag()}] Search input not found (url={page.url})")
        if await recover_rainbet_page(page, force=True) and await open_rainbet_search(page):
            inp, target = await _find_search_input(page)
        if not inp:
            return []

    await _type_search_query(inp, query)
    cards = await _wait_for_event_cards(page, target, query)
    if not cards:
        if await detect_rainbet_captcha(page):
            await wait_out_captcha(page)
        log.info(f"  [{book_tag()}] Empty search UI state url={page.url}")
    return cards


async def pick_best_event_card(cards: list[dict[str, str]], alert: PODAlert) -> dict[str, str] | None:
    best = None
    best_score = 0
    for card in cards:
        score = score_event_card(card, alert)
        if score > best_score:
            best_score = score
            best = card
    if best and best_score > 0:
        return best
    return None


async def navigate_rainbet_event(page, alert: PODAlert) -> bool:
    await wait_out_captcha(page)
    await wait_for_preloader_gone(page, timeout=PRELOADER_WAIT_TIMEOUT)
    await _rainbet_throttle(page)

    primary = pick_search_query(alert)
    queries = event_search_queries(alert)
    if not queries:
        queries = [primary]

    card = None
    last_found: list[dict[str, str]] = []
    for i, query in enumerate(queries):
        await wait_out_captcha(page)
        # Always reopen search between attempts — stale panels caused empty results
        found = await search_rainbet_events(page, query, open_panel=True)
        if found:
            last_found = found
            card = await pick_best_event_card(found, alert)
            if card:
                break
            # Already saw both names under this query — retyping the other
            # side won't fix a kickoff/league reject, only burns drop age
            both_seen = any(
                teams_match_text(alert.home_team, alert.away_team, c.get("teams", ""))
                for c in found
            )
            if both_seen:
                break
        await close_rainbet_search(page)
        await human_pause(0.15, 0.35)

    if not card:
        log.warning(
            f"  [{book_tag()}] No matching event for {alert.home_team} vs {alert.away_team}"
            f" (league={alert.league}, kickoff={alert.kickoff}, tried: {', '.join(queries)})"
        )
        for sample in last_found[:4]:
            reason = explain_card_rejection(sample, alert)
            log.info(
                f"  [{book_tag()}]   - {_safe_text(sample.get('teams', ''))} @ {_safe_text(sample.get('time', ''))}"
                f" | {_safe_text(sample.get('category', ''))} -- {reason}"
            )
        await close_rainbet_search(page)
        return False

    href = card["href"]
    link, target = await _first_locator(
        page,
        [f'a[data-editor-id="eventCardContent"][href="{href}"]', f'a[href="{href}"]'],
    )
    if link:
        try:
            await human_click(link)
        except Exception:
            await target.evaluate("(el) => el.click()", link)
    else:
        if await detect_rainbet_captcha(page):
            await wait_out_captcha(page)
            return False
        abs_href = _absolute_book_href(href)
        await page.goto(abs_href, wait_until="domcontentloaded")
    await human_pause(0.9, 1.6)
    await close_rainbet_search(page)
    await wait_out_captcha(page)
    # Leave search path if BetBy kept us on /search/
    try:
        url_now = (page.url or "").lower()
        if "/search/" in url_now or "bt-path=%2fsearch" in url_now:
            abs_href = _absolute_book_href(href)
            log.info(f"  [{book_tag()}] Still on search UI — goto {abs_href}")
            await page.goto(abs_href, wait_until="domcontentloaded")
            await human_pause(0.8, 1.4)
            await close_rainbet_search(page)
    except Exception:
        pass
    opened_sport = sport_from_href(page.url) or sport_from_href(href)
    pod_sport = alert_canonical_sport(alert)
    if not sports_agree(pod_sport, opened_sport):
        log.warning(
            f"  [{book_tag()}] Skip opened event — sport mismatch "
            f"(POD {pod_sport or '?'} vs Rainbet {opened_sport or '?'}): "
            f"{_safe_text(card.get('teams', ''))} url={_safe_text(page.url)}"
        )
        return False
    opened_teams = card.get("teams", "")
    if not teams_match_text(alert.home_team, alert.away_team, opened_teams):
        log.warning(
            f"  [{book_tag()}] Skip opened event — both teams not matched "
            f"(POD {alert.home_team} vs {alert.away_team} | "
            f"opened {_safe_text(opened_teams)})"
        )
        return False
    log.info(
        f"  [{book_tag()}] Opened: {_safe_text(card['teams'])} | {_safe_text(card['category'])}"
        f" | {_safe_text(card['time'])} = {_fmt_kickoff(parse_rainbet_dt(card.get('time', '')))}"
        f" (POD {alert.kickoff}) url={_safe_text(page.url)}"
    )
    return True


def _normalise_line_value(line: str | None) -> str | None:
    if line is None or line == "":
        return None
    val = _parse_float(line)
    if val is None:
        return line.strip()
    if val == int(val):
        return str(int(val))
    return f"{val:g}"


def _format_line_for_display(line: str | None) -> str:
    """Rainbet shows (1) / (-1) / (-1.5) — no leading + on positives."""
    norm = _normalise_line_value(line)
    return norm or ""


def _is_quarter_line(line: str | None) -> bool:
    """True for .25 / .75 Asian quarter lines (Rainbet: Handicap (Asian))."""
    val = _parse_float(line)
    if val is None:
        return False
    frac = abs(val) % 1.0
    return abs(frac - 0.25) < 1e-6 or abs(frac - 0.75) < 1e-6


def _is_half_line(line: str | None) -> bool:
    val = _parse_float(line)
    if val is None:
        return False
    return abs(abs(val) % 1.0 - 0.5) < 1e-6


def _is_zero_line(line: str | float | None) -> bool:
    """POD '0' = Handicap 0 / Draw No Bet — not moneyline."""
    val = _parse_float(str(line)) if line is not None else None
    return val is not None and abs(val) < 1e-9


def _lines_equal(a: str | float | None, b: str | float | None) -> bool:
    av = _parse_float(str(a)) if a is not None else None
    bv = _parse_float(str(b)) if b is not None else None
    if av is None or bv is None:
        return False
    return abs(av - bv) < 1e-6


def _total_outcome_label(selection: str, line: str | None) -> str | None:
    if not line:
        return None
    return f"{selection.strip().lower()} {_normalise_line_value(line)}"


def _handicap_outcome_label(selection: str, line: str | None) -> str | None:
    """Rainbet format: '(-1) Barcelona SC' / '(+0.5) Team'."""
    if line is None or line == "":
        return None
    norm = _format_line_for_display(line)
    val = _parse_float(norm)
    if val is not None and val > 0:
        return f"(+{norm}) {selection.strip()}"
    return f"({norm}) {selection.strip()}"


def _exact_total_plate_name(plate_name: str, selection: str, line: str | None) -> bool:
    name = _strip_embedded_odds(plate_name).strip().lower()
    m = re.match(r"^(over|under)\s+([0-9.]+)$", name)
    if not m:
        return False
    if m.group(1) != selection.strip().lower():
        return False
    return _lines_equal(line, m.group(2))


def _strip_embedded_odds(label: str, odds: float | None = None) -> str:
    """
    Rainbet often concatenates odds into the plate name:
      '1 1.70' / 'draw 4.40' / '-0.5 2.04' / '(-1) 2.98'
    Strip the trailing odds token so matching sees '1' / 'draw' / '-0.5'.
    """
    s = re.sub(r"\s+", " ", (label or "").strip())
    if not s:
        return s
    if odds is not None:
        for fmt in (f"{odds:.3f}", f"{odds:.2f}", f"{odds:.1f}", f"{odds:g}"):
            fmt = fmt.rstrip("0").rstrip(".") if "." in fmt else fmt
            candidates = {f"{odds:.2f}", f"{odds:.1f}", f"{odds:g}", fmt}
            for c in candidates:
                if not c:
                    continue
                if s.endswith(c) and len(s) > len(c):
                    head = s[: -len(c)].rstrip(" @")
                    if head:
                        return head
    # Trailing odds-like decimal (1.01–99.99). Keep totals 'over 147.5' intact
    # because lines >= 100 won't match \\d{1,2}\\.\\d+.
    m = re.match(r"^(.*?)\s+@?\s*(\d{1,2}\.\d{1,3})$", s)
    if m:
        head, num = m.group(1).strip(), float(m.group(2))
        if head and 1.01 <= num < 100:
            # Don't strip a bare Asian/HC line that is itself the whole label
            if re.fullmatch(r"[+-]?\d+(?:\.\d+)?", head):
                return head
            if re.fullmatch(r"\(\s*[+-]?\d+(?:\.\d+)?\s*\)", head):
                return head
            if re.fullmatch(r"(draw|x|tie|1|2|home|away|over|under|yes|no)", head, re.I):
                return head
            # '(-1) Team 2.98' → keep '(-1) Team'
            if not re.fullmatch(r"(over|under)\s+[0-9.]+", head, re.I):
                return head
    return s


def _parse_rainbet_handicap_plate(plate_name: str) -> tuple[float | None, str]:
    """
    Parse Rainbet/Duel handicap plate labels:
      '(-1) Barcelona SC' / '(+0.5) Team' / '(0) Team'
      '(-2)' / '-2' / '+1.5'  (line-only; team lives in column header)
      '-1 2.98' / '1.5 1.21'   (line + embedded odds — common overnight miss)
      'Barcelona SC (-1)'
    """
    s = _strip_embedded_odds(plate_name)
    if not s:
        return None, ""
    # (line) optional team — team may be empty for column-header layouts
    m = re.match(r"^\(\s*([+-]?\d+(?:\.\d+)?)\s*\)\s*(.*)$", s)
    if m:
        team = _strip_embedded_odds(m.group(2).strip())
        return _parse_float(m.group(1)), team
    # Team (line)
    m = re.match(r"^(.+?)\s*\(\s*([+-]?\d+(?:\.\d+)?)\s*\)$", s)
    if m and not re.match(r"^[+-]?\d", m.group(1).strip()):
        return _parse_float(m.group(2)), m.group(1).strip()
    # Bare line: -2 / +1.5 / 1.5
    m = re.match(r"^([+-]?\d+(?:\.\d+)?)$", s)
    if m:
        return _parse_float(m.group(1)), ""
    return None, s


def _looks_like_line_only_label(text: str) -> bool:
    t = _strip_embedded_odds(text or "").strip()
    if not t:
        return True
    return bool(
        re.fullmatch(r"[+-]?\d+(?:\.\d+)?", t)
        or re.fullmatch(r"\(\s*[+-]?\d+(?:\.\d+)?\s*\)", t)
    )


def _ml_plate_code(plate_name: str) -> str | None:
    """Map a 1x2/Winner plate label to '1' / '2' / 'draw', else None."""
    token = _strip_embedded_odds(plate_name).strip().lower()
    if token in ("1", "home", "h"):
        return "1"
    if token in ("2", "away", "a"):
        return "2"
    if token in ("draw", "x", "tie"):
        return "draw"
    return None


def _plate_side_from_col(col_index: int | None) -> str | None:
    """Home is usually the left column (even index), away the right (odd)."""
    if col_index is None:
        return None
    return "H" if (col_index % 2) == 0 else "A"


_PERIOD_ORDINAL = {
    "1": 1, "1st": 1, "first": 1,
    "2": 2, "2nd": 2, "second": 2,
    "3": 3, "3rd": 3, "third": 3,
    "4": 4, "4th": 4, "fourth": 4,
    "5": 5, "5th": 5, "fifth": 5,
}


def _period_scope(period: str) -> tuple[str, int | None]:
    """
    Parse POD period into (scope, number).
    scope: 'match' | 'half' | 'set' | 'quarter' | 'period'
    """
    p = (period or "").lower().strip()
    if not p or p in ("match", "game", "full time", "ft", "moneyline", "match odds"):
        return "match", None
    m = re.search(
        r"\b(1st|2nd|3rd|4th|5th|first|second|third|fourth|fifth|[1-5])\s*"
        r"(half|set|quarter|period|qtr)\b",
        p,
    )
    if m:
        n = _PERIOD_ORDINAL.get(m.group(1))
        kind = m.group(2)
        if kind == "qtr":
            kind = "quarter"
        return kind, n
    if "half" in p:
        return "half", None
    if "set" in p:
        return "set", None
    if "quarter" in p or re.search(r"\bq[1-4]\b", p):
        return "quarter", None
    return "match", None


def _period_flags(period: str) -> tuple[bool, bool]:
    """Legacy helper: first/second HALF only (sets are not halves)."""
    scope, n = _period_scope(period)
    return (scope == "half" and n == 1), (scope == "half" and n == 2)


def _title_period_number(title_l: str, scope: str) -> int | None:
    """Which half/set/quarter a Rainbet market title refers to, if any."""
    if scope == "half" and "half" not in title_l:
        return None
    if scope == "set" and "set" not in title_l:
        return None
    if scope == "quarter" and "quarter" not in title_l and not re.search(r"\bq[1-4]\b", title_l):
        return None
    m = re.search(
        r"\b(1st|2nd|3rd|4th|5th|first|second|third|fourth|fifth)\b",
        title_l,
    )
    if m:
        return _PERIOD_ORDINAL.get(m.group(1))
    m = re.search(r"\bq([1-4])\b", title_l)
    if m and scope == "quarter":
        return int(m.group(1))
    return None


def _period_title_ok(period: str, title_l: str) -> bool:
    """True when a Rainbet market title's period matches the POD alert period."""
    scope, n = _period_scope(period)
    has_half = "half" in title_l
    has_set = "set" in title_l
    has_quarter = "quarter" in title_l or bool(re.search(r"\bq[1-4]\b", title_l))
    # "first goal" etc. already filtered elsewhere; still treat ordinal-only
    # titles carefully via scope checks below.

    if scope == "match":
        # Full-match markets only — never 1st half / first set / Q1 boxes
        if has_half or has_set or has_quarter:
            return False
        if re.search(r"\b(1st|2nd|3rd|4th|5th|first|second|third|fourth|fifth)\b", title_l):
            return False
        return True

    if scope == "half":
        if not has_half or has_set:
            return False
        title_n = _title_period_number(title_l, "half")
        return title_n == n if n is not None else title_n is not None

    if scope == "set":
        if not has_set or has_half:
            return False
        title_n = _title_period_number(title_l, "set")
        return title_n == n if n is not None else title_n is not None

    if scope == "quarter":
        if not has_quarter:
            return False
        title_n = _title_period_number(title_l, "quarter")
        return title_n == n if n is not None else title_n is not None

    return False


def _effective_sport(alert: PODAlert, url: str = "") -> str:
    """POD sport/league, else the opened event URL (hockey friendlies, etc.)."""
    return alert_canonical_sport(alert) or sport_from_href(url)


def _alert_uses_ot_moneyline(alert: PODAlert, url: str = "") -> bool:
    """Hockey/basketball/etc. POD moneyline is 2-way including OT — not 1x2."""
    if (alert.market or "").lower() != "moneyline":
        return False
    sel = (alert.selection or "").strip().lower()
    if sel in ("draw", "x", "tie"):
        return False
    return _effective_sport(alert, url) in _OT_MONEYLINE_SPORTS


def _set_label(n: int | None) -> list[str]:
    """Common Rainbet phrasings for set N."""
    if n == 1:
        return ["first set", "1st set"]
    if n == 2:
        return ["second set", "2nd set"]
    if n == 3:
        return ["third set", "3rd set"]
    if n == 4:
        return ["fourth set", "4th set"]
    if n == 5:
        return ["fifth set", "5th set"]
    return ["set"]


def rainbet_market_patterns(alert: PODAlert, url: str = "") -> list[str]:
    """Map POD period + market to Rainbet market titles (for logging / preference)."""
    scope, n = _period_scope(alert.period)
    market = (alert.market or "").lower()

    if market == "total":
        if scope == "half" and n == 1:
            return ["1st half - total", "1st half total"]
        if scope == "half" and n == 2:
            return ["2nd half - total", "2nd half total"]
        if scope == "set":
            return [f"{s} - total" for s in _set_label(n)] + [f"{s} total" for s in _set_label(n)]
        if _is_quarter_line(alert.line):
            return ["total (asian)", "total"]
        return ["total"]

    if market == "spread":
        asian = _is_quarter_line(alert.line)
        half = _is_half_line(alert.line)
        zero = _is_zero_line(alert.line)
        if scope == "half" and n == 1:
            if asian:
                return ["1st half - handicap (asian)", "1st half handicap (asian)"]
            if half:
                return ["1st half - handicap", "1st half - handicap (asian)"]
            if zero:
                return ["1st half - handicap", "1st half - draw no bet", "1st half draw no bet"]
            return ["1st half - handicap", "1st half handicap"]
        if scope == "half" and n == 2:
            if asian:
                return ["2nd half - handicap (asian)"]
            if zero:
                return ["2nd half - handicap", "2nd half - draw no bet"]
            return ["2nd half - handicap", "2nd half handicap"]
        if scope == "set":
            labels = _set_label(n)
            out: list[str] = []
            for s in labels:
                if asian:
                    out.append(f"{s} - handicap (asian)")
                else:
                    out.append(f"{s} - handicap")
                    out.append(f"{s} handicap")
                    if zero:
                        out.append(f"{s} - draw no bet")
            return out or ["set - handicap"]
        if asian:
            return ["handicap (asian)"]
        if zero:
            # POD "0" ≈ Handicap 0; DNB is the same economic idea on some books
            return ["handicap", "draw no bet"]
        if half:
            return ["handicap", "handicap (asian)"]
        return ["handicap"]

    if market == "moneyline":
        if scope == "half" and n == 1:
            return ["1st half - 1x2", "1st half 1x2", "1st half - winner"]
        if scope == "half" and n == 2:
            return ["2nd half - 1x2", "2nd half 1x2", "2nd half - winner"]
        if scope == "set":
            out: list[str] = []
            for s in _set_label(n):
                out.extend([f"{s} - winner", f"{s} winner", f"{s} - moneyline"])
            return out
        if _alert_uses_ot_moneyline(alert, url):
            return [
                "winner (incl. overtime)",
                "winner (incl. overtime and penalties)",
                "winner (incl. extra innings)",
                "winner",
                "match winner",
            ]
        return ["1x2", "winner", "match winner"]

    return []


def _is_combo_market_title(title_l: str) -> bool:
    """
    Rainbet combo boxes like '1x2 & total' / 'winner & total' — POD never
    drops these; matching them burns EV on the wrong selection.
    """
    t = (title_l or "").lower()
    if not t:
        return False
    if "&" in t or " and " in t:
        return True
    has_result = any(k in t for k in ("1x2", "winner", "moneyline", "match odds"))
    has_total = "total" in t or "over/under" in t or "over under" in t
    has_hc = "handicap" in t or "spread" in t
    return (has_result and has_total) or (has_result and has_hc) or (has_total and has_hc)


def _looks_like_combo_plate(plate_name: str, plate_text: str = "") -> bool:
    """Combo outcome plates embed '&' / team+total in one button."""
    blob = f"{plate_name or ''} {plate_text or ''}".lower()
    if "&" in blob or " and " in blob:
        return True
    # e.g. 'Atlanta Dream over 90.5' without ampersand
    if re.search(r"\b(over|under)\s+\d", blob) and not re.match(
        r"^\s*(over|under)\s+\d", blob
    ):
        return True
    return False


# Exact Rainbet market-box titles we will click into (no substring fishing).
_ML_TITLES_MATCH = frozenset({
    "1x2", "moneyline", "match odds", "match winner", "winner",
    "winner (incl. overtime)", "winner (including overtime)",
    "winner (incl. overtime and penalties)",
    "winner (including overtime and penalties)",
    "winner (incl. extra innings)", "winner (including extra innings)",
    "moneyline (incl. overtime)", "match winner (incl. overtime)",
})
_ML_TITLES_OT = frozenset({
    "winner (incl. overtime)", "winner (including overtime)",
    "winner (incl. overtime and penalties)",
    "winner (including overtime and penalties)",
    "winner (incl. extra innings)", "winner (including extra innings)",
    "moneyline (incl. overtime)", "match winner (incl. overtime)",
    "winner", "match winner", "moneyline",
})
_ML_TITLES_1H = frozenset({
    "1st half - 1x2", "1st half 1x2", "1st half - winner", "1st half winner",
    "first half - 1x2", "first half - winner", "first half winner",
})
_ML_TITLES_2H = frozenset({
    "2nd half - 1x2", "2nd half 1x2", "2nd half - winner", "2nd half winner",
    "second half - 1x2", "second half - winner", "second half winner",
})
_ML_TITLES_SET = frozenset({
    "first set - winner", "1st set - winner", "first set winner", "1st set winner",
    "second set - winner", "2nd set - winner", "second set winner", "2nd set winner",
    "third set - winner", "3rd set - winner", "third set winner", "3rd set winner",
    "fourth set - winner", "4th set - winner",
    "fifth set - winner", "5th set - winner",
})
_TOTAL_TITLES_MATCH = frozenset({
    "total", "total (asian)", "asian total", "goals - total", "match total",
    "goals over/under", "over/under", "game total",
    "total (incl. overtime)", "total (including overtime)",
    "total incl. overtime", "total including overtime",
})
_TOTAL_TITLES_1H = frozenset({
    "1st half - total", "1st half total", "first half - total", "first half total",
    "1st half - total (asian)", "1st half total (asian)",
})
_TOTAL_TITLES_2H = frozenset({
    "2nd half - total", "2nd half total", "second half - total", "second half total",
})


def rainbet_market_title_matches(alert: PODAlert, title: str, url: str = "") -> bool:
    """
    Match Rainbet market box title to the POD alert with a strict allowlist.
    Combo markets (winner & total, etc.) are always rejected.
    """
    title_l = title.lower().strip()
    if not title_l:
        return False
    if _is_combo_market_title(title_l):
        return False

    market = (alert.market or "").lower()
    scope, n = _period_scope(alert.period)
    is_asian_title = "asian" in title_l
    want_asian = _is_quarter_line(alert.line)

    def period_ok() -> bool:
        return _period_title_ok(alert.period, title_l)

    if any(
        junk in title_l
        for junk in (
            "corner", "booking", "card", "correct score",
            "first goal", "both teams", "to score", "double chance", "exact score",
            "method of", "to lift", "race to", "which team", "sending off",
            "bookings", "corners",
        )
    ):
        return False

    # Draw No Bet is only valid for POD Handicap 0 — never for 1x2 moneyline
    if "draw no bet" in title_l or title_l in ("dnb",):
        return market == "spread" and _is_zero_line(alert.line) and period_ok()

    if market == "total":
        if scope == "half" and n == 1:
            return title_l in _TOTAL_TITLES_1H
        if scope == "half" and n == 2:
            return title_l in _TOTAL_TITLES_2H
        if scope == "set":
            return "set" in title_l and "total" in title_l and period_ok()
        if scope != "match":
            return False
        if title_l in _TOTAL_TITLES_MATCH:
            return True
        # Basketball: "Total (incl. overtime)" / POD "Game total"
        if title_l.startswith("total") and "overtime" in title_l:
            return True
        if title_l in ("game total", "match total"):
            return True
        # Asian total box — only when the line belongs there
        if title_l.startswith("total") and is_asian_title:
            return want_asian or _is_half_line(alert.line)
        return False

    if market == "spread":
        if "handicap" not in title_l and "spread" not in title_l:
            return False
        if "total" in title_l or "winner" in title_l or "1x2" in title_l:
            return False
        if want_asian and not is_asian_title:
            return False
        # Integer lines (incl. 0) belong on European Handicap, not Asian box
        if not want_asian and not _is_half_line(alert.line) and is_asian_title:
            return False
        return period_ok()

    if market == "moneyline":
        is_1x2_title = title_l == "1x2" or title_l.startswith("1x2 ")
        is_ot_title = (
            "overtime" in title_l
            or "extra innings" in title_l
            or "penalties" in title_l
        )
        sel_l = (alert.selection or "").strip().lower()
        is_draw = sel_l in ("draw", "x", "tie")
        if scope == "match":
            if is_draw:
                return is_1x2_title or title_l in ("match odds",)
            if _alert_uses_ot_moneyline(alert, url):
                # Regulation 1x2 is 3-way; POD hockey/NBA ML is 2-way incl. OT
                if is_1x2_title:
                    return False
                if title_l in _ML_TITLES_OT:
                    return True
                if "winner" in title_l and is_ot_title and "set" not in title_l:
                    return True
                return False
            if is_ot_title and _effective_sport(alert, url) == "soccer":
                return False
            if title_l in _ML_TITLES_MATCH:
                return True
            # Basketball moneyline often labelled Winner (incl. overtime)
            if "winner" in title_l and is_ot_title and "set" not in title_l:
                return True
            return False
        if scope == "half" and n == 1:
            return title_l in _ML_TITLES_1H
        if scope == "half" and n == 2:
            return title_l in _ML_TITLES_2H
        if scope == "set":
            if title_l in _ML_TITLES_SET:
                return period_ok()
            return "set" in title_l and "winner" in title_l and period_ok()
        return False

    return False


_NON_TEAM_PLATES = frozenset({
    "yes", "no", "y", "n", "over", "under", "draw", "x", "tie", "1", "2",
    "home", "away", "odd", "even",
})


def _selection_matches_team_plate(selection: str, plate_name: str) -> bool:
    """
    Strict team-name match for outcome plates.
    Critical: never let BTTS 'no' match a team like 'Noah' via substring.
    """
    plate = (plate_name or "").strip()
    sel = (selection or "").strip()
    if not plate or not sel:
        return False
    plate_l = plate.lower()
    if plate_l in _NON_TEAM_PLATES:
        return False
    if re.fullmatch(r"yes|no", plate_l):
        return False
    # Tiny plates are never team names (guards no⊂noah, etc.)
    if len(re.sub(r"[^a-z0-9]", "", plate_l)) < 3:
        return False

    plate_norm = _normalize_team_for_match(plate)
    sel_norm = _normalize_team_for_match(sel)
    if not plate_norm or not sel_norm:
        return False
    if plate_norm == sel_norm:
        return True

    # Containment only when the shorter string is a full token of the longer one
    # (never raw substring: 'no' must not hit 'noah')
    shorter, longer = (
        (plate_norm, sel_norm)
        if len(plate_norm) <= len(sel_norm)
        else (sel_norm, plate_norm)
    )
    if len(shorter) >= 3 and re.search(rf"(^|\s){re.escape(shorter)}(\s|$)", longer):
        return True

    p_toks = team_tokens(plate)
    s_toks = team_tokens(sel)
    if not p_toks or not s_toks:
        return False
    # Require meaningful overlap — at least one shared token of length >= 3
    return bool(p_toks & s_toks)


def _spread_plate_matches(
    plate_name: str,
    plate_text: str,
    alert: PODAlert,
    *,
    col_index: int | None = None,
) -> bool:
    """
    Match handicap plates by LINE FIRST, then home/away side.
    Rainbet overnight plates are often line-only with embedded odds:
      '-1 2.98' / '1.5 1.21' / '(-1)' — do NOT require the team string on the plate.
    Side comes from column header / 1|2 / col order (home = left = even index).
    """
    if _looks_like_combo_plate(plate_name, plate_text):
        return False

    raw = _strip_embedded_odds(plate_name)
    raw_l = raw.strip().lower()
    side = _alert_spread_side(alert)
    col_side = _plate_side_from_col(col_index)

    # DNB / Handicap 0 often shows bare 1/2 (or team names) with no (0) prefix
    if _is_zero_line(alert.line):
        code = _ml_plate_code(raw)
        if code == "1":
            return side == "H"
        if code == "2":
            return side == "A"
        if code == "draw":
            return False

    # Reject bare ML tokens for non-zero lines (avoid 1x2 leaking into HC samples)
    if raw_l in _NON_TEAM_PLATES and not _looks_like_line_only_label(raw):
        if raw_l not in ("1", "2", "home", "away", "h", "a"):
            return False

    line_val, team_name = _parse_rainbet_handicap_plate(raw)
    if line_val is None:
        if _is_zero_line(alert.line):
            if _selection_matches_team_plate(alert.selection, raw or plate_text):
                return True
            if col_side and side and col_side == side and _looks_like_line_only_label(raw):
                return True
            return False
        team_name = raw
        m = re.search(r"([+-]?\d+(?:\.\d+)?)", raw or "")
        line_val = _parse_float(m.group(1)) if m else None

    if not _lines_equal(alert.line, line_val):
        return False

    team_name = _strip_embedded_odds(team_name or "").strip()
    team_l = team_name.lower()

    # Explicit team / 1-2 on the plate (or enriched from column header)
    if team_name and not _looks_like_line_only_label(team_name):
        if team_l in ("1", "home", "h"):
            return side == "H" if side else True
        if team_l in ("2", "away", "a"):
            return side == "A" if side else True
        if _selection_matches_team_plate(alert.selection, team_name):
            return True
        # Wrong team label on this line — reject
        if any(c.isalpha() for c in team_name):
            return False

    # Line-only plate: disambiguate home/away via column order (home left).
    # When col_index is None the caller already decided the line is unique
    # (or side is unknown) — accept the line match.
    if side and col_side:
        return side == col_side
    if side == "H" and team_l in ("1", "home", "h"):
        return True
    if side == "A" and team_l in ("2", "away", "a"):
        return True

    blob = f"{raw} {plate_text or ''}".strip()
    if blob and _selection_matches_team_plate(alert.selection, blob):
        return True
    return True


def _moneyline_plate_matches(
    plate_name: str,
    plate_text: str,
    alert: PODAlert,
) -> bool:
    if _looks_like_combo_plate(plate_name, plate_text):
        return False
    sel_raw = (alert.selection or "").strip().lower()
    code = _ml_plate_code(plate_name)
    if code is None:
        # Also try first token of full cell text (name may be empty)
        code = _ml_plate_code((plate_text or "").split("\n")[0])

    # Draw / X on 1x2
    if sel_raw in ("draw", "x", "tie"):
        return code == "draw"

    # BetBy/Duel label match 1x2 / Winner as 1 / draw / 2 (odds often glued on)
    if code == "1":
        return bool(alert.home_team) and _selection_matches_team_plate(
            alert.selection, alert.home_team
        )
    if code == "2":
        return bool(alert.away_team) and _selection_matches_team_plate(
            alert.selection, alert.away_team
        )
    if code == "draw":
        return False

    plate_l = _strip_embedded_odds(plate_name).strip().lower()
    if plate_l in _NON_TEAM_PLATES:
        return False
    if re.search(r"\b(over|under|yes|no)\b", plate_l):
        return False
    return _selection_matches_team_plate(
        alert.selection, _strip_embedded_odds(plate_name)
    )


def _plate_matches_alert(
    plate_name: str,
    plate_text: str,
    alert: PODAlert,
    *,
    col_index: int | None = None,
) -> bool:
    if _looks_like_combo_plate(plate_name, plate_text):
        return False
    name = plate_name.strip()
    if alert.market == "total":
        return _exact_total_plate_name(name.lower(), alert.selection, alert.line)
    if alert.market == "spread":
        return _spread_plate_matches(
            name, plate_text, alert, col_index=col_index
        )
    if alert.market == "moneyline":
        return _moneyline_plate_matches(name, plate_text, alert)
    return False


async def _read_plate_name_and_odds(plate_el) -> tuple[str, str, float | None]:
    name = ""
    name_el = await plate_el.query_selector(
        '[data-editor-id="tableOutcomePlateName"], [data-editor-id="outcomePlateName"]'
    )
    if name_el:
        name = (await name_el.inner_text()).strip()
        name = re.sub(r"\s+", " ", name)
    full_text = (await plate_el.inner_text()).strip()
    if not name:
        name = full_text.split("\n")[0].strip()

    # Duel marketCard layout: plate name is often just the line ("2.5" / "-1.5")
    # while Over/Under or the team sits in marketCardOutcomeCellHeader columns.
    try:
        enriched = await plate_el.evaluate(
            """(plate) => {
              const clean = (t) => (t || '').replace(/\\s+/g, ' ').trim();
              const nameEl = plate.querySelector(
                '[data-editor-id="tableOutcomePlateName"], [data-editor-id="outcomePlateName"]'
              );
              let name = clean(nameEl?.innerText || '');
              const full = clean(plate.innerText || '');
              if (!name) name = (full.split('\\n')[0] || '').trim();
              const cell = plate.closest?.('[data-editor-id="marketCardOutcomeCell"]');
              if (!cell?.parentElement) return name;
              const parent = cell.parentElement;
              const headers = [...parent.querySelectorAll(
                ':scope > [data-editor-id="marketCardOutcomeCellHeader"]'
              )].map((h) => clean(h.innerText || '')).filter(Boolean);
              if (!headers.length) return name;
              const cells = [...parent.querySelectorAll(
                ':scope > [data-editor-id="marketCardOutcomeCell"]'
              )];
              const idx = cells.indexOf(cell);
              if (idx < 0) return name;
              const col = headers[idx % headers.length] || '';
              if (!col) return name;
              const colL = col.toLowerCase();
              if (/^(over|under)\\b/i.test(name)) return name;
              // Line-only plates: '-2', '+1.5', '(-2)', '(+0.5)', '(0)'
              const lineOnly =
                /^[+-]?\\d+(?:\\.\\d+)?$/.test(name)
                || /^\\(\\s*[+-]?\\d+(?:\\.\\d+)?\\s*\\)$/.test(name);
              if ((colL === 'over' || colL === 'under') && lineOnly) {
                const num = name.replace(/[()\\s]/g, '');
                return col + ' ' + num;
              }
              if (lineOnly && colL !== 'over' && colL !== 'under') {
                const numStr = name.replace(/[()\\s]/g, '');
                const num = parseFloat(numStr);
                if (!Number.isNaN(num)) {
                  const core = numStr.replace(/^\\+/, '');
                  const signed = num > 0 ? ('+' + core) : (numStr.startsWith('-') ? numStr : core);
                  return '(' + signed + ') ' + col;
                }
              }
              return name;
            }"""
        )
        if isinstance(enriched, str) and enriched.strip():
            name = enriched.strip()
    except Exception:
        pass

    odds = None
    for sel in (
        "div.sc-7elhv3-1 span",
        "span.jss280",
        "span.jss743",
        "span.jss627",
        "span.jss632",
        "span.jss1610",
    ):
        odds_el = await plate_el.query_selector(sel)
        if odds_el:
            odds = _parse_float((await odds_el.inner_text()).strip())
            if odds is not None:
                break
    if odds is None:
        nums = re.findall(r"([0-9]+\.[0-9]+)", full_text)
        if nums:
            odds = _parse_float(nums[-1])
    name = _strip_embedded_odds(name, odds)
    return name, full_text, odds


# Shadow-piercing BetBy market inventory. Playwright pierce is unreliable on
# Duel (#bt-root / nested shadows); search cards also expose outcomePlate and
# must be excluded so we don't treat search UI as an event page.
#
# Duel event pages may use marketCard + marketCardHeader, tableMarketWrapper,
# or a flatter layout where only outcomePlate exists near a title-like node.
MARKET_INVENTORY_JS = """
() => {
  const deepQueryAll = (root, selector) => {
    const out = [];
    const visit = (node) => {
      if (!node) return;
      try {
        if (node.querySelectorAll) out.push(...node.querySelectorAll(selector));
      } catch (e) {}
      if (node.shadowRoot) visit(node.shadowRoot);
      let kids = [];
      try { kids = node.querySelectorAll ? [...node.querySelectorAll('*')] : []; } catch (e) {}
      for (const el of kids) {
        if (el.shadowRoot) visit(el.shadowRoot);
      }
    };
    visit(root);
    return out;
  };

  const platesIn = (root, selector) => {
    // querySelectorAll does not pierce nested shadows — use deep walk
    return deepQueryAll(root, selector);
  };

  const titleSel =
    '[data-editor-id="simpleMarketTitle"], '
    + '[data-editor-id="tableMarketTitle"], '
    + '[data-editor-id="marketTitle"], '
    + '[data-editor-id="marketCardHeader"]';
  const plateSel =
    '[data-editor-id="outcomePlate"], [data-editor-id="tableOutcomePlate"], '
    + '[data-editor-id="bannerOutcomePlate"]';
  const wrapperSel =
    '[data-editor-id="tableMarketWrapper"], [data-editor-id="marketCard"]';

  const inEventCard = (el) => !!(el && el.closest && el.closest('[data-editor-id="eventCard"]'));
  const clean = (t) => (t || '').replace(/\\s+/g, ' ').trim();
  const TITLE_RE = /\\b(1x2|winner|moneyline|match odds|total|handicap|spread|draw no bet|over\\/under|asian|double chance)\\b/i;

  const readTitle = (el) => {
    if (!el) return '';
    const nested = el.querySelector?.(
      '.sc-1oesjbz-2, .sc-1w0fsw0-1, span'
    );
    return clean((nested || el).innerText || '').split('\\n')[0];
  };

  const enrichPlateName = (plate, rawName) => {
    const cell = plate.closest?.('[data-editor-id="marketCardOutcomeCell"]');
    let parent = cell?.parentElement || null;
    // Some Rainbet skins nest headers one level higher than the outcome cell
    if (!parent) {
      parent = plate.closest?.(
        '[data-editor-id="marketCard"], [data-editor-id="tableMarketWrapper"]'
      ) || plate.parentElement;
    }
    if (!parent) return rawName;
    let headers = [...parent.querySelectorAll(
      ':scope > [data-editor-id="marketCardOutcomeCellHeader"], '
      + '[data-editor-id="marketCardOutcomeCellHeader"]'
    )].map((h) => clean(h.innerText || '')).filter(Boolean);
    // Dedupe while preserving order (nested query can double-count)
    headers = [...new Set(headers)];
    if (!headers.length) return rawName;
    let cells = [...parent.querySelectorAll(
      ':scope > [data-editor-id="marketCardOutcomeCell"], '
      + '[data-editor-id="marketCardOutcomeCell"]'
    )];
    if (cell && cells.length) {
      // Prefer unique cells
      cells = [...new Set(cells)];
    }
    const idx = cell ? cells.indexOf(cell) : -1;
    if (idx < 0 && headers.length < 2) return rawName;
    // Fallback index: sibling plate order when cell chrome is missing
    let useIdx = idx;
    if (useIdx < 0) {
      const plates = [...(parent.querySelectorAll?.(
        '[data-editor-id="outcomePlate"], [data-editor-id="tableOutcomePlate"]'
      ) || [])];
      useIdx = plates.indexOf(plate);
    }
    if (useIdx < 0) return rawName;
    const col = headers[useIdx % headers.length] || '';
    if (!col) return rawName;
    const colL = col.toLowerCase();
    if (/^(over|under)\\b/i.test(rawName)) return rawName;
    // Already enriched '(line) Team'
    if (/^\\(\\s*[+-]?\\d+(?:\\.\\d+)?\\s*\\)\\s+\\S/.test(rawName)) return rawName;
    const lineOnly =
      /^[+-]?\\d+(?:\\.\\d+)?$/.test(rawName)
      || /^\\(\\s*[+-]?\\d+(?:\\.\\d+)?\\s*\\)$/.test(rawName);
    if ((colL === 'over' || colL === 'under') && lineOnly) {
      const num = rawName.replace(/[()\\s]/g, '');
      return col + ' ' + num;
    }
    if (lineOnly && colL !== 'over' && colL !== 'under') {
      const numStr = rawName.replace(/[()\\s]/g, '');
      const num = parseFloat(numStr);
      if (!Number.isNaN(num)) {
        const core = numStr.replace(/^\\+/, '');
        const signed = num > 0 ? ('+' + core) : (numStr.startsWith('-') ? numStr : core);
        return '(' + signed + ') ' + col;
      }
    }
    return rawName;
  };

  const readPlate = (plate, psiId, colIndex) => {
    plate.setAttribute('data-psi-plate-id', String(psiId));
    const nameEl = plate.querySelector(
      '[data-editor-id="tableOutcomePlateName"], [data-editor-id="outcomePlateName"]'
    );
    let name = clean(nameEl?.innerText || '');
    const full = clean(plate.innerText || '');
    if (!name) name = (full.split('\\n')[0] || '').trim();
    name = enrichPlateName(plate, name);
    let odds = null;
    for (const sel of [
      'div.sc-7elhv3-1 span', 'span.jss280', 'span.jss743', 'span.jss627',
      'span.jss632', 'span.jss1610',
    ]) {
      const o = plate.querySelector(sel);
      if (!o) continue;
      const v = parseFloat(clean(o.innerText || ''));
      if (!Number.isNaN(v) && v >= 1.01) { odds = v; break; }
    }
    if (odds == null) {
      const nums = [...full.matchAll(/([0-9]+\\.[0-9]+)/g)].map((m) => parseFloat(m[1]));
      const ok = nums.filter((n) => n >= 1.01 && n < 500);
      if (ok.length) odds = ok[ok.length - 1];
    }
    // Strip glued odds: '1 1.70' / '-0.5 2.04' / 'draw 4.40'
    if (odds != null && name) {
      const oddsStrs = [String(odds), odds.toFixed(2), odds.toFixed(1), odds.toFixed(3)];
      for (const os of oddsStrs) {
        const re = new RegExp('\\\\s+@?\\\\s*' + os.replace('.', '\\\\.') + '\\\\s*$');
        if (re.test(name)) {
          name = name.replace(re, '').trim();
          break;
        }
      }
      if (/\\s+\\d{1,2}\\.\\d{1,3}\\s*$/.test(name)) {
        const m = name.match(/^(.*?)\\s+(\\d{1,2}\\.\\d{1,3})\\s*$/);
        if (m && Math.abs(parseFloat(m[2]) - odds) < 0.011) name = m[1].trim();
      }
    }
    return { psiId: String(psiId), name, text: full, odds, colIndex: colIndex };
  };

  const markets = [];
  let psi = 0;
  const wrappers = deepQueryAll(document, wrapperSel).filter((w) => !inEventCard(w));
  if (wrappers.length) {
    for (const w of wrappers) {
      const tEl =
        w.querySelector?.(titleSel)
        || deepQueryAll(w, titleSel)[0]
        || deepQueryAll(w, '.sc-1wfrh04-3 span, .sc-1oesjbz-2')[0];
      const title = readTitle(tEl);
      const rawPlates = platesIn(w, plateSel).filter((p) => !inEventCard(p));
      const plates = rawPlates.map((p, i) => readPlate(p, ++psi, i));
      if (title || plates.length) markets.push({ title: title || '?', plates });
    }
  }

  if (!markets.length) {
    const titles = deepQueryAll(document, titleSel).filter((t) => !inEventCard(t));
    const usedPlates = new Set();
    for (const tEl of titles) {
      const title = readTitle(tEl);
      let scope = tEl.parentElement;
      let best = null;
      for (let i = 0; i < 10 && scope; i++) {
        const plates = platesIn(scope, plateSel).filter((p) => !inEventCard(p));
        const localTitles = platesIn(scope, titleSel).filter((t) => !inEventCard(t));
        if (plates.length) {
          if (localTitles.length === 1) { best = scope; break; }
          if (!best) best = scope;
        }
        scope = scope.parentElement;
      }
      scope = best || tEl.parentElement;
      const plates = platesIn(scope || document, plateSel)
        .filter((p) => !inEventCard(p) && !usedPlates.has(p))
        .map((p, i) => { usedPlates.add(p); return readPlate(p, ++psi, i); });
      if (title || plates.length) markets.push({ title: title || '?', plates });
    }
  }

  // Fallback: plates exist but known title/wrapper chrome is missing.
  // Group by nearby title-like text (1x2 / Total / Handicap / …).
  if (!markets.length) {
    const allEditor = deepQueryAll(document, '[data-editor-id]');
    const titleNodes = allEditor.filter((el) => {
      if (inEventCard(el)) return false;
      const eid = el.getAttribute('data-editor-id') || '';
      if (/plate|outcome|odd|button|icon|chip|tab|nav|input|stake|betslip/i.test(eid)) {
        return false;
      }
      const t = clean(el.innerText || '').split('\\n')[0];
      return !!(t && t.length >= 2 && t.length < 80 && TITLE_RE.test(t));
    });
    const usedPlates = new Set();
    for (const tEl of titleNodes) {
      const title = clean(tEl.innerText || '').split('\\n')[0];
      let scope = tEl.parentElement;
      let best = null;
      for (let i = 0; i < 14 && scope; i++) {
        const plates = platesIn(scope, plateSel).filter(
          (p) => !inEventCard(p) && !usedPlates.has(p)
        );
        if (plates.length) {
          best = { scope, plates };
          if (plates.length <= 16) break;
        }
        scope = scope.parentElement;
      }
      if (!best || !best.plates.length) continue;
      best.plates.forEach((p) => usedPlates.add(p));
      markets.push({
        title: title || '?',
        plates: best.plates.map((p, i) => readPlate(p, ++psi, i)),
      });
    }
  }

  const allPlates = deepQueryAll(document, plateSel);
  const eventCardPlates = allPlates.filter(inEventCard).length;
  const pagePlates = allPlates.length - eventCardPlates;

  const idCounts = {};
  for (const el of deepQueryAll(document, '[data-editor-id]')) {
    const id = el.getAttribute('data-editor-id') || '';
    if (!id) continue;
    idCounts[id] = (idCounts[id] || 0) + 1;
  }
  const editorIds = Object.entries(idCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 35)
    .map(([k, v]) => k + ':' + v);

  const plateAncestry = allPlates.filter((p) => !inEventCard(p)).slice(0, 2).map((p) => {
    const chain = [];
    let n = p;
    for (let i = 0; i < 10 && n; i++) {
      chain.push(n.getAttribute?.('data-editor-id') || n.tagName || '?');
      n = n.parentElement;
    }
    return {
      text: clean(p.innerText || '').slice(0, 70),
      chain,
    };
  });

  return {
    url: location.href,
    markets,
    titles: markets.map((m) => m.title).filter(Boolean).slice(0, 20),
    marketCount: markets.length,
    pagePlates,
    eventCardPlates,
    wrappers: wrappers.length,
    searchPath: /bt-path=\\/search\\//i.test(location.href || ''),
    editorIds,
    plateAncestry,
  };
}
"""


async def _scrape_market_inventory(page) -> dict:
    """Shadow-piercing market scrape across page + frames."""
    best: dict = {
        "markets": [],
        "titles": [],
        "marketCount": 0,
        "pagePlates": 0,
        "eventCardPlates": 0,
        "wrappers": 0,
        "url": "",
        "searchPath": False,
        "editorIds": [],
        "plateAncestry": [],
    }
    for target in [page, *page.frames]:
        try:
            info = await target.evaluate(MARKET_INVENTORY_JS)
        except Exception:
            continue
        if not isinstance(info, dict):
            continue
        # Prefer frames that actually grouped markets; don't let raw plate
        # count from an empty shell beat a titled market inventory.
        score = (
            int(info.get("marketCount") or 0) * 1000
            + int(info.get("wrappers") or 0) * 100
            + int(info.get("pagePlates") or 0)
        )
        best_score = (
            int(best.get("marketCount") or 0) * 1000
            + int(best.get("wrappers") or 0) * 100
            + int(best.get("pagePlates") or 0)
        )
        if score >= best_score:
            best = info
    return best


async def _locate_psi_plate(page, psi_id: str):
    """Resolve a scraped plate id back to an ElementHandle (shadow-aware)."""
    js = """(id) => {
      const find = (root) => {
        if (!root) return null;
        try {
          const hit = root.querySelector?.('[data-psi-plate-id="' + id + '"]');
          if (hit) return hit;
        } catch (e) {}
        let kids = [];
        try { kids = root.querySelectorAll ? [...root.querySelectorAll('*')] : []; } catch (e) {}
        for (const el of kids) {
          if (el.shadowRoot) {
            const n = find(el.shadowRoot);
            if (n) return n;
          }
        }
        return null;
      };
      return find(document);
    }"""
    for target in [page, *page.frames]:
        try:
            handle = await target.evaluate_handle(js, str(psi_id))
            el = handle.as_element() if handle else None
            if el:
                return el
        except Exception:
            continue
    return None


def _absolute_book_href(href: str) -> str:
    """Turn BetBy card hrefs into a navigable soft-book URL (Duel bt-path aware)."""
    book = get_book()
    raw = (href or "").strip()
    if not raw:
        return book.sports_url
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    if raw.startswith("?"):
        # e.g. ?bt-path=/soccer/... → https://duel.com/sports?bt-path=...
        base = book.sports_url.split("?")[0]
        return f"{base}{raw}"
    if raw.startswith("/"):
        return f"{book.base_url.rstrip('/')}{raw}"
    # Bare bt-path body
    if raw.startswith("bt-path="):
        return f"{book.sports_url.split('?')[0]}?{raw}"
    return f"{book.sports_url.split('?')[0]}?bt-path=/{raw.lstrip('/')}"


async def _get_market_wrappers(page):
    """
    BetBy market sections.
    Rainbet: tableMarketWrapper (+ simpleMarketTitle).
    Duel (2026): marketCard (+ marketCardHeader).
    Older Duel skins: simpleMarketTitle + outcomePlate without wrappers.
    """
    classic = await _query_all(
        page,
        'div[data-editor-id="tableMarketWrapper"], div[data-editor-id="marketCard"]',
    )
    # Prefer wrappers that are NOT inside search event cards
    filtered = []
    for w in classic:
        try:
            if await w.evaluate(
                "el => !el.closest('[data-editor-id=\"eventCard\"]')"
            ):
                filtered.append(w)
        except Exception:
            filtered.append(w)
    if filtered:
        return filtered

    titles = await _query_all(
        page,
        '[data-editor-id="simpleMarketTitle"], '
        '[data-editor-id="tableMarketTitle"], '
        '[data-editor-id="marketTitle"], '
        '[data-editor-id="marketCardHeader"]',
    )
    wrappers: list = []
    seen_ids: set[float] = set()
    for title_el in titles:
        try:
            if await title_el.evaluate(
                "el => !!el.closest('[data-editor-id=\"eventCard\"]')"
            ):
                continue
            handle = await title_el.evaluate_handle(
                """el => {
                  // marketCardHeader lives inside marketCard — prefer that
                  const card = el.closest?.('[data-editor-id="marketCard"]');
                  if (card) return card;
                  let best = null;
                  let n = el.parentElement;
                  for (let i = 0; i < 10 && n; i++) {
                    const plates = n.querySelectorAll(
                      '[data-editor-id="outcomePlate"], [data-editor-id="tableOutcomePlate"]'
                    );
                    if (plates.length) {
                      const titles = n.querySelectorAll(
                        '[data-editor-id="simpleMarketTitle"], '
                        + '[data-editor-id="tableMarketTitle"], '
                        + '[data-editor-id="marketTitle"], '
                        + '[data-editor-id="marketCardHeader"]'
                      );
                      if (titles.length === 1) return n;
                      if (!best) best = n;
                    }
                    n = n.parentElement;
                  }
                  return best || el.parentElement;
                }"""
            )
            el = handle.as_element() if handle else None
            if not el:
                continue
            uid = await el.evaluate(
                "e => { e.__psiUid = e.__psiUid || Math.random(); return e.__psiUid; }"
            )
            if uid in seen_ids:
                continue
            seen_ids.add(uid)
            wrappers.append(el)
        except Exception:
            continue
    return wrappers


async def _read_market_title(wrapper) -> str:
    for sel in (
        '[data-editor-id="marketCardHeader"] .sc-1oesjbz-2',
        '[data-editor-id="marketCardHeader"]',
        '[data-editor-id="simpleMarketTitle"]',
        '[data-editor-id="tableMarketTitle"]',
        '[data-editor-id="marketTitle"]',
        ".sc-1wfrh04-3 span",
        '[data-editor-id="simpleMarketTitle"] .sc-1w0fsw0-1',
        ".sc-1w0fsw0-1",
        ".sc-1oesjbz-2",
    ):
        try:
            el = await wrapper.query_selector(sel)
            if el:
                text = (await el.inner_text()).strip()
                if text:
                    return text.split("\n")[0].strip()
        except Exception:
            continue
    return ""


async def _log_market_dom_diagnostic(page) -> None:
    """When no market titles are found, dump shadow-aware inventory + editor ids."""
    try:
        info = await _scrape_market_inventory(page)
        log.info(
            f"  [{book_tag()}] DOM diag: wrappers={info.get('wrappers')} "
            f"markets={info.get('marketCount')} pagePlates={info.get('pagePlates')} "
            f"cardPlates={info.get('eventCardPlates')} search={info.get('searchPath')} "
            f"seen={info.get('titles') or []} url={_safe_text(info.get('url') or page.url)}"
        )
        editor_ids = info.get("editorIds") or []
        if editor_ids:
            log.info(f"  [{book_tag()}] editor-ids: {', '.join(editor_ids[:25])}")
        ancestry = info.get("plateAncestry") or []
        for i, sample in enumerate(ancestry[:2]):
            log.info(
                f"  [{book_tag()}] plate[{i}] '{_safe_text(sample.get('text', ''))}' "
                f"← {' > '.join(sample.get('chain') or [])}"
            )
        # Persist once for offline debugging when plates exist but markets don't
        if int(info.get("pagePlates") or 0) > 0 and int(info.get("marketCount") or 0) == 0:
            path = "debug_duel_market_dom.json"
            try:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(info, f, indent=2, ensure_ascii=False)
                log.info(f"  [{book_tag()}] Wrote {path}")
            except Exception:
                pass
    except Exception as e:
        log.info(f"  [{book_tag()}] DOM diag failed: {e}")


def _is_challenge_url(url: str | None) -> bool:
    u = (url or "").lower()
    return "challenges.cloudflare.com" in u or "/cdn-cgi/challenge" in u


def _inventory_markets_blank(inv: dict | None) -> bool:
    if not inv:
        return True
    return (
        int(inv.get("wrappers") or 0) == 0
        and int(inv.get("marketCount") or 0) == 0
        and int(inv.get("pagePlates") or 0) == 0
    )


async def recover_event_page_once(page) -> bool:
    """
    Mid-flow recovery when Cloudflare/empty DOM kills markets.
    Only call on miss/empty — never on the happy path.
    Waits for manual CF solve if needed, then reloads the event page once.
    """
    tag = book_tag()
    url = page.url or ""
    if _is_challenge_url(url) or await detect_rainbet_captcha(page):
        log.warning(
            f"  [{tag}] Challenge/empty markets detected "
            f"(url={_safe_text(url)}) — solve CF in the browser if prompted"
        )
        await wait_out_captcha(page, label=tag)
        url = page.url or ""

    if _is_challenge_url(url):
        # Challenge may clear into a blank shell — try back to the event
        try:
            await page.go_back(wait_until="domcontentloaded")
            await human_pause(0.7, 1.2)
            url = page.url or ""
        except Exception:
            pass

    if _is_challenge_url(url) or "/sports" not in (url or "").lower():
        log.warning(
            f"  [{tag}] Still not on an event page after challenge "
            f"(url={_safe_text(url)}) — soft sports recovery"
        )
        await ensure_rainbet_sportsbook(page, force_home=True)
        await wait_out_captcha(page, label=tag)
        return False

    log.warning(f"  [{tag}] Reloading event page once to recover markets")
    try:
        await page.reload(wait_until="domcontentloaded")
    except Exception as e:
        log.warning(f"  [{tag}] Event reload failed: {e}")
        return False
    await human_pause(0.8, 1.4)
    await wait_out_captcha(page, label=tag)
    ok = await wait_for_outcome_plates(page, timeout=10.0)
    if ok:
        log.info(f"  [{tag}] Markets recovered after event reload")
    else:
        log.warning(f"  [{tag}] Markets still empty after event reload")
        await _log_market_dom_diagnostic(page)
    return ok


async def _expand_visible_show_more(page, alert: PODAlert | None = None) -> int:
    """Click BetBy 'Show all/more' only on truncated markets we actually need."""
    await wait_for_preloader_gone(page, timeout=3.0)
    clicked = 0
    try:
        buttons = await _query_all(page, '[data-editor-id="marketCardShowMoreBtn"]')
    except Exception:
        buttons = []
    page_url = ""
    try:
        page_url = page.url or ""
    except Exception:
        page_url = ""
    for btn in buttons[:8]:
        if alert is not None:
            try:
                title = await btn.evaluate(
                    """(el) => {
                      const w = el.closest(
                        '[data-editor-id="marketCard"], [data-editor-id="tableMarketWrapper"]'
                      );
                      if (!w) return '';
                      const t = w.querySelector(
                        '[data-editor-id="simpleMarketTitle"], '
                        + '[data-editor-id="tableMarketTitle"], '
                        + '[data-editor-id="marketTitle"], '
                        + '[data-editor-id="marketCardHeader"]'
                      );
                      return (t?.innerText || '').replace(/\\s+/g, ' ').trim().split('\\n')[0];
                    }"""
                )
            except Exception:
                title = ""
            if not title or not rainbet_market_title_matches(
                alert, str(title), url=page_url
            ):
                continue
        try:
            await human_click(btn)
            clicked += 1
            await human_pause(0.2, 0.4)
        except Exception:
            continue
    return clicked


def _match_alert_in_inventory(
    inv: dict,
    alert: PODAlert,
) -> tuple[str, float, str, str] | None:
    """
    Match alert against a scraped market inventory (no extra DOM walks).
    Returns (psiId, odds, plate_name, market_title) or None.

    Spread/handicap: match by line first; if multiple plates share the line,
    disambiguate home/away via colIndex (home=left=even). A unique line hits
    even without a team string on the plate.
    """
    scrape_candidates: list[tuple[str, float, str, str, int | None]] = []
    page_url = str(inv.get("url") or "")
    for market in inv.get("markets") or []:
        title = str(market.get("title") or "")
        if not rainbet_market_title_matches(alert, title, url=page_url):
            continue
        title_l = title.lower()
        if title and (
            _is_combo_market_title(title)
            or any(
                j in title_l
                for j in (
                    "booking", "bookings", "corner", "corners",
                    "both teams", "to score", "card",
                )
            )
        ):
            continue
        plates = list(market.get("plates") or [])
        # Pre-count how many plates share each handicap line in this box
        line_counts: dict[float, int] = {}
        if alert.market == "spread":
            for plate in plates:
                lv, _ = _parse_rainbet_handicap_plate(str(plate.get("name") or ""))
                if lv is not None:
                    key = round(lv, 6)
                    line_counts[key] = line_counts.get(key, 0) + 1

        for idx, plate in enumerate(plates):
            name = str(plate.get("name") or "")
            full_text = str(plate.get("text") or name)
            odds = plate.get("odds")
            if odds is None:
                continue
            try:
                odds_f = float(odds)
            except (TypeError, ValueError):
                continue
            col_raw = plate.get("colIndex", idx)
            try:
                col_index = int(col_raw) if col_raw is not None else idx
            except (TypeError, ValueError):
                col_index = idx

            unique_line = False
            if alert.market == "spread" and alert.line not in (None, ""):
                want = _parse_float(str(alert.line))
                if want is not None:
                    unique_line = line_counts.get(round(want, 6), 0) == 1

            # Unique line in the box → ignore column side (still require line match)
            use_col = None if unique_line else col_index
            if _plate_matches_alert(name, full_text, alert, col_index=use_col):
                scrape_candidates.append(
                    (
                        str(plate.get("psiId") or ""),
                        odds_f,
                        name,
                        title or "?",
                        col_index,
                    )
                )

    if not scrape_candidates:
        return None

    # Prefer side-correct columns when several candidates remain
    side = _alert_spread_side(alert) if alert.market == "spread" else None
    if side and len(scrape_candidates) > 1:
        sided = [
            c for c in scrape_candidates
            if _plate_side_from_col(c[4]) == side
        ]
        if sided:
            scrape_candidates = sided

    trimmed = [(c[0], c[1], c[2], c[3]) for c in scrape_candidates]

    if _is_zero_line(alert.line) and len(trimmed) > 1:
        best = max(trimmed, key=lambda c: c[1])
    elif len(trimmed) > 1:
        best = min(
            trimmed,
            key=lambda c: abs(c[1] - (alert.new_odds or 0)),
        )
    else:
        best = trimmed[0]
    return best


def _sample_plates_for_alert(inv: dict, alert: PODAlert, limit: int = 8) -> list[str]:
    """Plate name samples from market boxes that matched the alert title."""
    samples: list[str] = []
    page_url = str(inv.get("url") or "")
    for market in inv.get("markets") or []:
        title = str(market.get("title") or "")
        if not rainbet_market_title_matches(alert, title, url=page_url):
            continue
        for plate in market.get("plates") or []:
            name = str(plate.get("name") or plate.get("text") or "").strip()
            if name:
                samples.append(f"{name} @ {plate.get('odds')}")
            if len(samples) >= limit:
                return samples
    return samples


async def _ensure_market_expanded(wrapper) -> None:
    """Expand a collapsed Rainbet/Duel market section if needed."""
    try:
        plates = await wrapper.query_selector_all(
            '[data-editor-id="tableOutcomePlate"], [data-editor-id="outcomePlate"]'
        )
        if plates:
            # Duel "Show all" for truncated total/handicap grids
            more = await wrapper.query_selector(
                '[data-editor-id="marketCardShowMoreBtn"]'
            )
            if more:
                try:
                    await human_click(more)
                    await human_pause(0.25, 0.45)
                except Exception:
                    pass
            return
        btn = await wrapper.query_selector('[data-editor-id="collapseMarketButton"]')
        if btn:
            await human_click(btn)
            await human_pause(0.3, 0.55)
    except Exception:
        pass


def _expected_outcome_label(alert: PODAlert) -> str:
    if alert.market == "total":
        return _total_outcome_label(alert.selection, alert.line) or alert.selection
    if alert.market == "spread":
        return _handicap_outcome_label(alert.selection, alert.line) or alert.selection
    return alert.selection


async def _find_rainbet_outcome_plate_once(page, alert: PODAlert, *, log_lookup: bool = True):
    """Single pass: inventory scrape, then Playwright walk. No recovery."""
    expected = _expected_outcome_label(alert)
    page_url = ""
    try:
        page_url = page.url or ""
    except Exception:
        page_url = ""
    if log_lookup:
        market_patterns = rainbet_market_patterns(alert, url=page_url)
        log.info(
            f"  [{book_tag()}] Looking for market [{' | '.join(market_patterns[:2]) or '?'}]"
            f" outcome '{expected}'"
        )

    inv = await _scrape_market_inventory(page)
    url = str(inv.get("url") or page.url or "")
    if _is_challenge_url(url) or _inventory_markets_blank(inv):
        return None, None, inv, "empty"

    seen_titles = [t for t in (inv.get("titles") or []) if t]
    matched_titles = [
        str(m.get("title") or "")
        for m in (inv.get("markets") or [])
        if rainbet_market_title_matches(alert, str(m.get("title") or ""), url=url)
    ]
    matched_titles = [t for t in matched_titles if t]

    hit = _match_alert_in_inventory(inv, alert)
    if hit:
        psi_id, odds, name, market_title = hit
        if matched_titles:
            log.info(
                f"  [{book_tag()}] Using market box: "
                f"{', '.join(dict.fromkeys(matched_titles))}"
            )
        plate = await _locate_psi_plate(page, psi_id) if psi_id else None
        if plate:
            log.info(
                f"  [{book_tag()}] Matched plate '{name}' @ {odds:.3f} ({market_title})"
            )
            return plate, name, inv, "ok"
        log.warning(
            f"  [{book_tag()}] Matched '{name}' via scrape but could not locate DOM node"
        )

    # Inventory already enumerated plates for the right market — matching is
    # in-memory. Do NOT burn 10–35s on a Playwright walk / expand hang.
    inv_plates = sum(len(m.get("plates") or []) for m in (inv.get("markets") or []))
    if matched_titles or inv_plates > 0:
        status = "no_selection" if matched_titles else "no_market"
        return None, None, inv, status

    # Fallback: Playwright ElementHandle walk (only when inventory was empty)
    wrappers = await _get_market_wrappers(page)
    scoped_plates: list[tuple] = []
    pw_matched: list[str] = []

    for wrapper in wrappers:
        title = await _read_market_title(wrapper)
        if title and title not in seen_titles:
            seen_titles.append(title)
        if not rainbet_market_title_matches(alert, title or "", url=url):
            continue
        if title:
            pw_matched.append(title)
        await _ensure_market_expanded(wrapper)
        try:
            await wrapper.scroll_into_view_if_needed()
        except Exception:
            pass
        for sel in (
            '[data-editor-id="tableOutcomePlate"]',
            '[data-editor-id="outcomePlate"]',
        ):
            for plate in await wrapper.query_selector_all(sel):
                scoped_plates.append((plate, title or "?"))

    if pw_matched and not hit:
        log.info(f"  [{book_tag()}] Using market box: {', '.join(dict.fromkeys(pw_matched))}")

    if not scoped_plates:
        status = "no_market"
        if matched_titles or pw_matched:
            status = "no_selection"
        return None, None, inv, status

    candidates: list[tuple] = []
    for col_i, (plate, market_title) in enumerate(scoped_plates):
        title_l = (market_title or "").lower()
        if market_title and market_title != "?" and (
            _is_combo_market_title(market_title)
            or any(
                j in title_l
                for j in (
                    "booking", "bookings", "corner", "corners",
                    "both teams", "to score", "card",
                )
            )
        ):
            continue
        try:
            name, full_text, odds = await _read_plate_name_and_odds(plate)
        except Exception:
            continue
        if odds is None:
            continue
        if _plate_matches_alert(name, full_text, alert, col_index=col_i):
            candidates.append((plate, name, odds, market_title))

    if not candidates:
        return None, None, inv, "no_selection"

    if _is_zero_line(alert.line) and len(candidates) > 1:
        for _, name, odds, market_title in candidates:
            log.info(
                f"  [{book_tag()}] Candidate '{name}' in '{market_title}' @ {odds:.3f}"
            )
        best = max(candidates, key=lambda c: c[2])
        plate, name, odds, market_title = best
        log.info(
            f"  [{book_tag()}] Best of Handicap/DNB: '{name}' @ {odds:.3f} ({market_title})"
        )
        return plate, name, inv, "ok"

    if len(candidates) > 1:
        best = min(candidates, key=lambda c: abs(c[2] - (alert.new_odds or 0)))
    else:
        best = candidates[0]

    plate, name, odds, market_title = best
    log.info(f"  [{book_tag()}] Matched plate '{name}' @ {odds:.3f} ({market_title})")
    return plate, name, inv, "ok"


async def find_rainbet_outcome_plate(page, alert: PODAlert):
    """
    Find exact outcome plate inside the correct market section.
    Happy path: one scrape. On miss/empty only: expand Show-more and/or one
    event-page reload (CF), then a single retry.
    """
    await wait_for_preloader_gone(page, timeout=PRELOADER_WAIT_TIMEOUT)
    expected = _expected_outcome_label(alert)
    plate, name, inv, status = await _find_rainbet_outcome_plate_once(page, alert)
    if plate:
        return plate, name

    # Miss / empty — recover once, then one retry (does not run on happy path)
    recovered = False
    had_plate_samples = bool(_sample_plates_for_alert(inv or {}, alert))
    if status == "empty" or _is_challenge_url(page.url) or _inventory_markets_blank(inv):
        recovered = await recover_event_page_once(page)
    else:
        await wait_for_preloader_gone(page, timeout=3.0)
        expanded = await _expand_visible_show_more(page, alert)
        if expanded:
            log.info(f"  [{book_tag()}] Expanded {expanded} truncated market(s), retrying")
        if status == "no_selection":
            samples = _sample_plates_for_alert(inv or {}, alert)
            if samples:
                log.info(
                    f"  [{book_tag()}] Plates seen: "
                    + "; ".join(_safe_text(s) for s in samples[:6])
                )
            # Retry once after Show-more — keep it fast when plates already scraped
            recovered = True
        elif expanded:
            recovered = True

    if recovered:
        await wait_for_preloader_gone(page, timeout=3.0)
        plate, name, inv, status = await _find_rainbet_outcome_plate_once(
            page, alert, log_lookup=False
        )
        if plate:
            return plate, name
        # If we already had an enumerated plate list, do not hang on further recovery
        if had_plate_samples and status == "no_selection":
            pass

    if status == "no_market":
        page_url = str((inv or {}).get("url") or "")
        if _alert_uses_ot_moneyline(alert, page_url):
            log.warning(
                f"  [{book_tag()}] No matching market box "
                f"({alert.period or 'Match'} {alert.market}) — "
                f"refusing regulation 1x2 for OT/2-way moneyline"
            )
        else:
            log.warning(
                f"  [{book_tag()}] No matching market box "
                f"({alert.period or 'Match'} {alert.market}) — refusing unscoped fallback"
            )
    else:
        log.warning(
            f"  [{book_tag()}] Selection not found: "
            f"{alert.period} {alert.market} {alert.selection} {alert.line or ''} "
            f"(expected '{expected}')".strip()
        )
    seen = [t for t in ((inv or {}).get("titles") or []) if t]
    if seen:
        log.info(f"  [{book_tag()}] Markets on page: {', '.join(seen[:12])}")
    elif status == "empty":
        await _log_market_dom_diagnostic(page)
    else:
        samples = _sample_plates_for_alert(inv or {}, alert)
        if samples:
            log.info(
                f"  [{book_tag()}] Plates seen: "
                + "; ".join(_safe_text(s) for s in samples[:6])
            )
    return None, None


async def wait_for_outcome_plates(page, timeout: float = 12.0) -> bool:
    """
    Wait for real event-page markets — ignore search-result eventCard plates.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        inv = await _scrape_market_inventory(page)
        if inv.get("searchPath"):
            await close_rainbet_search(page)
            await asyncio.sleep(0.35)
            continue
        # Real event markets: titled sections or non-card plates
        if int(inv.get("marketCount") or 0) > 0 and int(inv.get("pagePlates") or 0) > 0:
            return True
        if int(inv.get("wrappers") or 0) > 0 and int(inv.get("pagePlates") or 0) > 0:
            return True
        await asyncio.sleep(0.45)
    inv = await _scrape_market_inventory(page)
    log.info(
        f"  [{book_tag()}] Market wait timed out: markets={inv.get('marketCount')} "
        f"pagePlates={inv.get('pagePlates')} cardPlates={inv.get('eventCardPlates')} "
        f"url={_safe_text(inv.get('url') or page.url)}"
    )
    return False


async def read_plate_odds(plate_element) -> float | None:
    _, _, odds = await _read_plate_name_and_odds(plate_element)
    return odds


async def close_betslip(page) -> None:
    try:
        for _ in range(10):
            removed = False
            for btn in await _query_all(page, 'div[data-editor-id="betslipSelectionRemoveButton"]'):
                try:
                    await human_click(btn, timeout=2000)
                    await human_pause(0.15, 0.35)
                    removed = True
                except Exception:
                    pass
            if not removed:
                break
    except Exception:
        pass


# ── POD Quick Look (alt lines) ─────────────────────────────────────────────────

QUICK_LOOK_EXTRACT_JS = """
() => {
  const dialog = document.querySelector(
    'div[role="dialog"][data-slot="dialog-content"]'
  );
  if (!dialog) return { ok: false, reason: 'no_dialog', rows: [] };

  const center = dialog.querySelector('.ag-center-cols-container');
  if (!center) return { ok: false, reason: 'no_grid', rows: [] };

  const raw = [];
  for (const row of center.querySelectorAll('[role="row"][row-id]')) {
    const rowId = row.getAttribute('row-id') || '';
    if (!rowId) continue;
    const top = parseFloat(row.style.top || '0') || 0;
    const marketEl = row.querySelector('[col-id="market"]');
    const outcomeEl = row.querySelector('[col-id="outcome"]');
    const nvpEl = row.querySelector('[col-id="current.nvp"]');
    const pEl = row.querySelector('[col-id="current.p"]');
    const market = (marketEl?.innerText || '').trim();
    const outcome = (outcomeEl?.innerText || '').trim();
    const nvp = (nvpEl?.innerText || '').trim();
    const price = (pEl?.innerText || '').trim();
    raw.push({ rowId, top, market, outcome, nvp, price });
  }
  raw.sort((a, b) => a.top - b.top);

  const out = [];
  const seen = new Set();
  let lastMarket = '';
  for (const r of raw) {
    if (seen.has(r.rowId)) continue;
    seen.add(r.rowId);

    // Section headers: market text spans full width, no outcome/NVP
    if (/^(match|1st half|2nd half)$/i.test(r.market) && !r.outcome) {
      lastMarket = '';
      continue;
    }
    let market = r.market;
    if (market) lastMarket = market;
    else market = lastMarket;

    if (!r.outcome || !r.nvp) continue;
    out.push({
      rowId: r.rowId,
      market,
      outcome: r.outcome,
      nvp: r.nvp,
      price: r.price,
    });
  }
  return { ok: true, rows: out };
}
"""


@dataclass
class QuickLookAlt:
    market: str
    outcome: str
    nvp: float
    price: float | None = None
    period: str = "Match"  # Match | 1st Half
    row_id: str = ""


def _period_from_quick_look_row_id(row_id: str) -> str:
    # ...-Moneyline-Team-0  (full match)  /  ...-1  (1st half)
    m = re.search(r"-(\d+)$", row_id or "")
    if m and m.group(1) == "1":
        return "1st Half"
    return "Match"


def _infer_quick_look_market(market: str, outcome: str, row_id: str = "") -> str:
    """Fill market when POD leaves the market cell blank (row-span / Draw rows)."""
    if market:
        return market
    rid = row_id or ""
    if re.search(r"moneyline|[-_]draw(?:-|$)|[-_]home(?:-|$)|[-_]away(?:-|$)", rid, re.I):
        return "Moneyline"
    if re.search(r"spread|handicap", rid, re.I):
        return "Spread"
    if re.search(r"total", rid, re.I):
        return "Total"
    out = (outcome or "").strip().lower()
    if out in ("draw", "home", "away", "x", "1", "2"):
        return "Moneyline"
    if _parse_quick_look_spread(outcome):
        return "Spread"
    if _parse_quick_look_total(outcome):
        return "Total"
    return ""


def _teams_fuzzy_equal(a: str, b: str) -> bool:
    if not a or not b:
        return False
    return team_match_score(a, b) >= 1 or team_match_score(b, a) >= 1


def _alert_spread_side(alert: PODAlert) -> str | None:
    """Return 'H' or 'A' for the alert's selection side."""
    if not alert.selection:
        return None
    if _teams_fuzzy_equal(alert.selection, alert.home_team or ""):
        return "H"
    if _teams_fuzzy_equal(alert.selection, alert.away_team or ""):
        return "A"
    sel = (alert.selection or "").strip().lower()
    if sel in ("home", "1", "h"):
        return "H"
    if sel in ("away", "2", "a"):
        return "A"
    return None


def _parse_quick_look_spread(outcome: str) -> tuple[str, float] | None:
    m = re.match(r"^([HA])\s*([+-]?\d+(?:\.\d+)?)$", (outcome or "").strip(), re.I)
    if not m:
        return None
    return m.group(1).upper(), float(m.group(2))


def _parse_quick_look_total(outcome: str) -> tuple[str, float] | None:
    m = re.match(r"^([OU])\s*([0-9]+(?:\.\d+)?)$", (outcome or "").strip(), re.I)
    if not m:
        return None
    side = "over" if m.group(1).upper() == "O" else "under"
    return side, float(m.group(2))


def alt_line_to_alert(base: PODAlert, alt: QuickLookAlt) -> PODAlert | None:
    """Map a quick-look row onto a synthetic PODAlert for Rainbet matching."""
    market_l = (alt.market or "").lower()
    period = alt.period
    price = alt.price

    def _finish(**kwargs) -> PODAlert:
        return replace(
            base,
            period=period,
            no_vig_odds=alt.nvp,
            pinned_odds=price,
            new_odds=price if price is not None else base.new_odds,
            alert_type="ALT",
            **kwargs,
        )

    if "spread" in market_l or "handicap" in market_l or _parse_quick_look_spread(alt.outcome):
        parsed = _parse_quick_look_spread(alt.outcome)
        if not parsed:
            return None
        side, line = parsed
        selection = base.home_team if side == "H" else base.away_team
        if not selection:
            return None
        return _finish(
            market="spread",
            selection=selection,
            line=f"{line:+g}" if line != 0 else "0",
        )

    if "total" in market_l or _parse_quick_look_total(alt.outcome):
        parsed = _parse_quick_look_total(alt.outcome)
        if not parsed:
            return None
        ou, line = parsed
        return _finish(
            market="total",
            selection=ou,
            line=str(line),
        )

    # Moneyline — including Draw rows with empty market cell / row-id ...--Draw-0
    out = (alt.outcome or "").strip()
    out_l = out.lower()
    if (
        "moneyline" in market_l
        or "match odds" in market_l
        or "1x2" in market_l
        or out_l in ("draw", "x", "home", "away", "1", "2")
    ):
        if out_l in ("draw", "x"):
            selection = "Draw"
        elif out_l in ("home", "1") and base.home_team:
            selection = base.home_team
        elif out_l in ("away", "2") and base.away_team:
            selection = base.away_team
        else:
            selection = out
        return _finish(
            market="moneyline",
            selection=selection,
            line=None,
        )

    return None


def rank_nearby_alts(alert: PODAlert, alts: list[QuickLookAlt]) -> list[QuickLookAlt]:
    """
    Same market + same side only. Lines within ALT_LINE_MAX_DIST of the exact line.
    Rank by QuickLook attractiveness first (higher NVP = longer price / more room for
    soft-book EV), then closer distance. Exact line is skipped (already tried).
    """
    target_line = _parse_float(alert.line) if alert.line else None
    alert_period = _period_scope(alert.period)
    market = (alert.market or "").lower()

    scored: list[tuple[float, float, QuickLookAlt]] = []
    for alt in alts:
        if _period_scope(alt.period) != alert_period:
            continue

        ml = (alt.market or "").lower()
        if market == "spread":
            # Handicap alerts → handicap/spread rows only (never totals / ML)
            if "total" in ml or "moneyline" in ml or "1x2" in ml:
                continue
            if (
                ml
                and "spread" not in ml
                and "handicap" not in ml
                and not _parse_quick_look_spread(alt.outcome)
            ):
                continue
            parsed = _parse_quick_look_spread(alt.outcome)
            if not parsed:
                continue
            side, line = parsed
            want_side = _alert_spread_side(alert)
            if want_side and side != want_side:
                continue
            if target_line is not None and abs(line - target_line) < 1e-9:
                continue  # exact line already tried
            dist = abs(line - target_line) if target_line is not None else 0.0
            if dist > ALT_LINE_MAX_DIST + 1e-9:
                continue
            # Prefer higher NVP (more EV headroom once soft odds are read)
            scored.append((-float(alt.nvp or 0.0), dist, alt))

        elif market == "total":
            # Total alerts → totals only (never handicap / ML)
            if "spread" in ml or "handicap" in ml or "moneyline" in ml or "1x2" in ml:
                continue
            if ml and "total" not in ml and not _parse_quick_look_total(alt.outcome):
                continue
            parsed = _parse_quick_look_total(alt.outcome)
            if not parsed:
                continue
            ou, line = parsed
            sel = (alert.selection or "").lower()
            want_ou = (
                "over"
                if "over" in sel or sel == "o"
                else "under"
                if "under" in sel or sel == "u"
                else None
            )
            if want_ou and ou != want_ou:
                continue
            if target_line is not None and abs(line - target_line) < 1e-9:
                continue
            dist = abs(line - target_line) if target_line is not None else 0.0
            if dist > ALT_LINE_MAX_DIST + 1e-9:
                continue
            scored.append((-float(alt.nvp or 0.0), dist, alt))

        elif market == "moneyline":
            # Exact ML should already be on Rainbet; don't wander to other ML sides
            continue

    scored.sort(key=lambda t: (t[0], t[1]))
    return [a for _, _, a in scored]


async def close_quick_look(page) -> None:
    try:
        close_btn = await page.query_selector('button[data-slot="dialog-close"]')
        if close_btn:
            await close_btn.click()
            await asyncio.sleep(0.25)
            return
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.25)
    except Exception:
        pass


async def _find_pod_match_row(page, alert: PODAlert):
    """Find a visible POD terminal row (or match cell) for this event."""
    home = alert.home_team or ""
    away = alert.away_team or ""
    selectors = [
        '.ag-center-cols-container [col-id="match"]',
        '.ag-pinned-left-cols-container [col-id="starredEvent"]',
        '.ag-pinned-left-cols-container [col-id="match"]',
        '.ag-row[row-id]',
    ]
    for sel in selectors:
        els = await page.query_selector_all(sel)
        for el in els[:120]:
            try:
                text = await el.inner_text()
            except Exception:
                continue
            if text and teams_match_text(home, away, text):
                return el
    return None


async def _pod_row_id(cell_or_row) -> str:
    """AG Grid row-id for a cell/row — pinned and center containers share it."""
    try:
        return await cell_or_row.evaluate(
            """(el) => {
                const row = el.closest('[role="row"][row-id]') || el;
                return row.getAttribute('row-id') || '';
            }"""
        )
    except Exception:
        return ""


async def _find_pod_row_action_button(page, alert: PODAlert, selectors: list[str]):
    """
    Hover THIS event's AG Grid row and return an action button matching selectors.
    Pinned-left buttons (bell / +) stay hidden until hover.
    """
    row = await _find_pod_match_row(page, alert)
    if not row:
        return None, ""

    try:
        await row.scroll_into_view_if_needed(timeout=2_000)
        await asyncio.sleep(0.15)
    except Exception:
        pass

    row_id = await _pod_row_id(row)
    if not row_id:
        return None, ""

    escaped = row_id.replace('"', '\\"')
    row_scopes = [
        f'.ag-pinned-left-cols-container [role="row"][row-id="{escaped}"]',
        f'[role="row"][row-id="{escaped}"]',
    ]

    for sel in row_scopes:
        scope = await page.query_selector(sel)
        if not scope:
            continue
        for attempt in range(3):
            try:
                await scope.hover(timeout=1_500)
            except Exception:
                pass
            await asyncio.sleep(0.2)
            btn = None
            for bsel in selectors:
                btn = await scope.query_selector(bsel)
                if btn:
                    break
            if btn:
                try:
                    if await btn.is_visible():
                        return btn, row_id
                except Exception:
                    return btn, row_id
            if attempt == 0:
                try:
                    await scope.click(timeout=1_500)
                except Exception:
                    pass
    return None, row_id


async def _find_pod_mute_button(page, alert: PODAlert):
    """
    Find the bell (Mute this event) for THIS event only.
    The bell lives in the pinned-left column, so match by AG Grid row-id.
    """
    return await _find_pod_row_action_button(
        page,
        alert,
        [
            'button[title="Mute this event"]',
            'button[data-slot="popover-trigger"]:has(svg.lucide-bell)',
        ],
    )


async def mute_pod_event(page, alert: PODAlert) -> bool:
    """Mute an event on POD after betting it, so it stops re-alerting."""
    home = alert.home_team or ""
    away = alert.away_team or ""
    if not home or not away:
        return False

    try:
        btn, row_id = await _find_pod_mute_button(page, alert)
        if not btn:
            log.warning(f"  [Mute] Bell button not found for {home} vs {away}")
            return False

        await btn.click(timeout=3_000)
        await asyncio.sleep(0.4)

        popover = None
        for _ in range(10):
            popover = await page.query_selector(
                '[data-slot="popover-content"], [role="dialog"][data-state="open"]'
            )
            if popover:
                break
            await asyncio.sleep(0.2)

        if not popover:
            # With "Don't ask again" enabled POD mutes instantly, no confirm step
            log.info(f"  [Mute] Muted {home} vs {away} (no confirm popover)")
            return True

        try:
            prompt = re.sub(r"\s+", " ", (await popover.inner_text()).strip())
        except Exception:
            prompt = ""

        # Popover says "Mute future alerts for <home> vs <away>?" — verify it's ours
        if prompt and not teams_match_text(home, away, prompt):
            log.warning(
                f"  [Mute] Popover is for a different event, cancelling "
                f"(wanted {home} vs {away}, popover: {_safe_text(prompt)[:120]})"
            )
            cancel = await popover.query_selector('button:has-text("Cancel")')
            if cancel:
                await cancel.click(timeout=2_000)
            await _close_pod_popover(page)
            return False

        # Checking this makes later mutes a single click (no popover at all)
        if POD_MUTE_DONT_ASK_AGAIN:
            try:
                box = await popover.query_selector(
                    'button[role="checkbox"][id^="mute-confirm-checkbox"]'
                )
                if box and (await box.get_attribute("aria-checked")) != "true":
                    await box.click(timeout=2_000)
                    await asyncio.sleep(0.15)
            except Exception:
                pass

        confirm = None
        for sel in (
            'button:has-text("Mute event")',
            'button:has-text("Mute this event")',
            'button:has-text("Confirm")',
        ):
            confirm = await popover.query_selector(sel)
            if confirm:
                break

        if not confirm:
            log.warning(
                f"  [Mute] 'Mute event' button not found for {home} vs {away} "
                f"— popover: {_safe_text(prompt)[:200]}"
            )
            await _close_pod_popover(page)
            return False

        await confirm.click(timeout=3_000)
        await asyncio.sleep(0.3)
        log.info(f"  [Mute] Muted {home} vs {away}")
        return True
    except Exception as e:
        log.warning(f"  [Mute] Failed for {home} vs {away}: {e}")
        await _close_pod_popover(page)
        return False


def _pod_log_period_labels(alert: PODAlert) -> list[str]:
    scope, n = _period_scope(alert.period)
    if scope == "half" and n == 1:
        return ["1st Half", "First Half", "1st half"]
    if scope == "half" and n == 2:
        return ["2nd Half", "Second Half", "2nd half"]
    if scope == "set" and n:
        ordinal = {1: "1st", 2: "2nd", 3: "3rd", 4: "4th", 5: "5th"}.get(n, f"{n}th")
        return [f"{ordinal} Set", f"{ordinal} set"]
    if scope == "quarter" and n:
        ordinal = {1: "1st", 2: "2nd", 3: "3rd", 4: "4th"}.get(n, f"{n}th")
        return [f"{ordinal} Quarter", f"Q{n}"]
    return ["Match", "Game", "Full Time"]


def _pod_log_market_labels(alert: PODAlert) -> list[str]:
    market = (alert.market or "").lower()
    if market == "total":
        return ["Totals", "Total"]
    if market == "spread":
        return ["Spreads", "Spread", "Handicap"]
    return ["Moneyline (3-way)", "Moneyline (2-way)", "Moneyline", "Winner"]


def _pod_clean_team_label(name: str) -> str:
    name = re.sub(
        r"\s*\((?:games|points|sets|legs|maps|rounds)\)\s*",
        " ",
        name or "",
        flags=re.I,
    )
    return re.sub(r"\s+", " ", name).strip()


def _pod_log_selection_labels(alert: PODAlert) -> list[str]:
    line = _format_line_for_display(alert.line)
    raw_sel = (alert.selection or "").strip()
    sel = _pod_clean_team_label(raw_sel) or raw_sel
    if alert.market == "total":
        side = sel.lower()
        if side.startswith("over"):
            return [f"Over {line}", f"over {line}", f"O {line}"]
        if side.startswith("under"):
            return [f"Under {line}", f"under {line}", f"U {line}"]
        return [f"{sel} {line}".strip()]
    if alert.market == "spread" and line:
        signed = line if str(line).startswith("-") else f"+{line}"
        bare = line.lstrip("+")
        # POD's Log-bet list is "{line} {team}" e.g. "-1 Samorin", "+3.5 Player (Games)"
        out = [
            f"{signed} {sel}",
            f"{signed} {raw_sel}".strip(),
            f"{line} {sel}",
            f"{bare} {sel}",
            f"{sel} {line}",
            f"{sel} {signed}",
            f"({signed}) {sel}",
            f"({line}) {sel}",
            f"{sel} {bare}",
        ]
        side = _alert_spread_side(alert)
        if side == "H":
            out.extend(
                [
                    f"{signed} Home",
                    f"Home {signed}",
                    f"Home {line}",
                    f"1 {signed}",
                    f"({signed}) Home",
                ]
            )
        elif side == "A":
            out.extend(
                [
                    f"{signed} Away",
                    f"Away {signed}",
                    f"Away {line}",
                    f"2 {signed}",
                    f"({signed}) Away",
                ]
            )
        if _is_zero_line(alert.line):
            out.extend(
                [sel, raw_sel, f"(0) {sel}", f"0 {sel}", f"{sel} 0", "Draw No Bet", "DNB"]
            )
        return [x for x in out if x]
    if raw_sel.lower() in ("draw", "x", "tie") or sel.lower() in ("draw", "x", "tie"):
        return ["Draw", "X", "Tie"]
    out = [raw_sel]
    if sel and sel != raw_sel:
        out.append(sel)
    return out


def _pod_norm_option_text(option_text: str) -> str:
    t = (option_text or "").replace("_", " ").replace(",", ".")
    return re.sub(r"\s+", " ", t).strip()


_POD_LINE_TOKEN = r"[+-]?\d+(?:\.\d+)?"


def _pod_extract_option_line(opt: str) -> str | None:
    """POD lists spreads as '-1 Samorin' / '+3.5 Player (Games)' / 'Team -1.5'."""
    opt = _pod_norm_option_text(opt)
    if not opt:
        return None
    m = re.match(rf"^({_POD_LINE_TOKEN})(?:\s|$)", opt)
    if m:
        return m.group(1)
    m = re.search(rf"\(({_POD_LINE_TOKEN})\)", opt)
    if m:
        return m.group(1)
    m = re.search(rf"(?:^|\s)({_POD_LINE_TOKEN})$", opt)
    if m:
        return m.group(1)
    return None


def _pod_option_matches(option_text: str, candidates: list[str], alert: PODAlert | None = None) -> bool:
    """
    Match a POD Log-bet dropdown option to the bet we actually placed.
    Line markets must match the exact line — never let '-1' steal '-1.25'
    or 'Over 2.5' steal 'Over 2.75' via substring.
    """
    opt = _pod_norm_option_text(option_text)
    if not opt:
        return False
    opt_l = opt.lower()

    if alert and alert.market == "total":
        m = re.match(r"^\s*(over|under|o|u)\s+([+-]?\d+(?:\.\d+)?)\b", opt_l)
        if not m:
            return False
        side, line = m.group(1), m.group(2)
        if side == "o":
            side = "over"
        elif side == "u":
            side = "under"
        sel = (alert.selection or "").strip().lower()
        if not sel.startswith(side):
            return False
        return _lines_equal(alert.line, line)

    if alert and alert.market == "spread":
        extracted = _pod_extract_option_line(opt)
        if extracted is None:
            # Never treat "-0.75 Leixoes" as a 0-line just because the team matches
            if _is_zero_line(alert.line) and not re.search(r"\d", opt):
                if _selection_matches_team_plate(alert.selection, opt):
                    return True
            for cand in candidates:
                want = _pod_norm_option_text(cand).lower()
                if want and opt_l == want:
                    return True
            return False
        if not _lines_equal(alert.line, extracted):
            return False
        team_bit = re.sub(r"\([^)]*\)", " ", opt)
        team_bit = re.sub(rf"{_POD_LINE_TOKEN}", " ", team_bit)
        team_bit = re.sub(r"\s+", " ", team_bit).strip()
        if _selection_matches_team_plate(alert.selection, team_bit):
            return True
        side = _alert_spread_side(alert)
        tb = team_bit.lower()
        if side == "H" and tb in ("home", "1", "h", "home team"):
            return True
        if side == "A" and tb in ("away", "2", "a", "away team"):
            return True
        for cand in candidates:
            want = _pod_norm_option_text(cand).lower()
            if want and opt_l == want:
                return True
        return False

    if alert and (alert.market or "").lower() in ("moneyline", "ml", "winner"):
        if _selection_matches_team_plate(alert.selection, opt):
            return True
        sel_l = (alert.selection or "").strip().lower()
        if sel_l in ("draw", "x", "tie") and opt_l in ("draw", "x", "tie"):
            return True

    for cand in candidates:
        want = _pod_norm_option_text(cand).lower()
        if want and opt_l == want:
            return True
    return False


POD_SELECT_OPTION_SEL = (
    '[role="listbox"] [role="option"], '
    '[data-radix-select-viewport] [role="option"], '
    '[role="option"], [data-radix-collection-item], [cmdk-item]'
)


async def _list_open_select_options(page) -> list[dict]:
    try:
        rows = await page.evaluate(
            """() => {
              const nodes = [...document.querySelectorAll(
                '[role="listbox"] [role="option"], [data-radix-select-viewport] [role="option"], [role="option"], [data-radix-collection-item], [cmdk-item]'
              )];
              return nodes.map((el, i) => ({
                i,
                text: (el.innerText || el.textContent || '').replace(/\\s+/g, ' ').trim(),
                value: el.getAttribute('data-value') || el.getAttribute('value') || '',
                disabled: el.getAttribute('aria-disabled') === 'true'
                  || el.hasAttribute('disabled'),
              }));
            }"""
        )
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


async def _js_click_open_option(page, text: str) -> bool:
    try:
        return bool(
            await page.evaluate(
                """(text) => {
                  const nodes = [...document.querySelectorAll(
                    '[role="listbox"] [role="option"], [data-radix-select-viewport] [role="option"], [role="option"], [data-radix-collection-item], [cmdk-item]'
                  )];
                  const want = (text || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                  const el = nodes.find((n) =>
                    (n.innerText || n.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase() === want
                  );
                  if (!el) return false;
                  el.scrollIntoView({ block: 'nearest' });
                  el.click();
                  return true;
                }""",
                text,
            )
        )
    except Exception:
        return False


async def _scroll_open_listbox(page) -> bool:
    try:
        return bool(
            await page.evaluate(
                """() => {
                  const box = document.querySelector(
                    '[role="listbox"], [data-radix-select-viewport], [data-radix-select-content]'
                  );
                  if (!box) return false;
                  const before = box.scrollTop;
                  const step = Math.max(80, box.clientHeight * 0.8);
                  box.scrollTop = Math.min(box.scrollHeight, box.scrollTop + step);
                  return box.scrollTop > before + 1;
                }"""
            )
        )
    except Exception:
        return False


async def _click_visible_option(page, candidates: list[str], alert: PODAlert | None = None) -> bool:
    deadline = time.monotonic() + 6.0
    seen: set[str] = set()
    while time.monotonic() < deadline:
        options = await _list_open_select_options(page)
        for opt in options:
            if opt.get("disabled"):
                continue
            text = str(opt.get("text") or "").strip()
            value = str(opt.get("value") or "").strip()
            key = text.lower() or value.lower()
            if not key or key in seen:
                continue
            seen.add(key)
            hit = _pod_option_matches(text, candidates, alert)
            if not hit and value:
                hit = _pod_option_matches(value.replace("_", " "), candidates, alert)
            if not hit:
                continue
            if text and await _js_click_open_option(page, text):
                await asyncio.sleep(0.2)
                return True
        handles = await page.query_selector_all(POD_SELECT_OPTION_SEL)
        for handle in handles:
            try:
                if not await handle.is_visible():
                    continue
                text = (await handle.inner_text() or "").strip()
            except Exception:
                continue
            if not text or text.lower() in seen:
                continue
            if _pod_option_matches(text, candidates, alert):
                try:
                    await handle.click(timeout=2_500, force=True)
                    await asyncio.sleep(0.2)
                    return True
                except Exception:
                    try:
                        await handle.evaluate("n => n.click()")
                        await asyncio.sleep(0.2)
                        return True
                    except Exception:
                        continue
        if not await _scroll_open_listbox(page):
            await asyncio.sleep(0.15)
        else:
            await asyncio.sleep(0.08)
    return False


async def _combobox_is_open(combobox) -> bool:
    try:
        if (await combobox.get_attribute("aria-expanded")) == "true":
            return True
        if (await combobox.get_attribute("data-state")) == "open":
            return True
    except Exception:
        pass
    return False


async def _activate_pod_combobox(combobox) -> bool:
    try:
        await combobox.scroll_into_view_if_needed(timeout=2_000)
    except Exception:
        pass
    if await _combobox_is_open(combobox):
        return True
    for kwargs in ({}, {"force": True}):
        try:
            await combobox.click(timeout=3_500, **kwargs)
            await asyncio.sleep(0.25)
            if await _combobox_is_open(combobox):
                return True
        except Exception:
            continue
    try:
        await combobox.evaluate("n => n.click()")
        await asyncio.sleep(0.25)
        return await _combobox_is_open(combobox)
    except Exception:
        return False


async def _native_select_options(native) -> list[dict]:
    try:
        rows = await native.evaluate(
            """sel => [...sel.options].map(o => ({
              value: o.value || '',
              label: (o.text || o.label || '').trim(),
            }))"""
        )
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


async def _set_pod_combobox(page, combobox, candidates: list[str], alert: PODAlert | None = None) -> bool:
    if not combobox or not candidates:
        return False
    try:
        current = _pod_norm_option_text(await combobox.inner_text() or "")
    except Exception:
        current = ""
    if current and _pod_option_matches(current, candidates, alert):
        return True

    native = None
    try:
        native = await combobox.evaluate_handle(
            """(el) => {
              const item = el.closest('[data-slot="form-item"]') || el.parentElement;
              return item ? item.querySelector('select') : null;
            }"""
        )
        native = native.as_element() if native else None
    except Exception:
        native = None
    if native:
        native_opts = await _native_select_options(native)
        for opt in native_opts:
            label = str(opt.get("label") or "")
            value = str(opt.get("value") or "")
            if not (
                _pod_option_matches(label, candidates, alert)
                or (
                    value
                    and _pod_option_matches(
                        value.replace("_", " "), candidates, alert
                    )
                )
            ):
                continue
            try:
                await native.select_option(value=value)
            except Exception:
                try:
                    await native.select_option(label=label)
                except Exception:
                    continue
            await asyncio.sleep(0.15)
            try:
                current = _pod_norm_option_text(await combobox.inner_text() or "")
            except Exception:
                current = ""
            if current and _pod_option_matches(current, candidates, alert):
                return True
            break

    opened = await _activate_pod_combobox(combobox)
    if not opened:
        listed = await _list_open_select_options(page)
        if not listed:
            return False
    if await _click_visible_option(page, candidates, alert):
        for _ in range(8):
            await asyncio.sleep(0.12)
            try:
                current = _pod_norm_option_text(await combobox.inner_text() or "")
            except Exception:
                current = ""
            if current and _pod_option_matches(current, candidates, alert):
                return True
        return False
    listed = await _list_open_select_options(page)
    if listed:
        sample = ", ".join(
            (str(o.get("text") or o.get("value") or ""))[:48] for o in listed[:10]
        )
        log.info(f"  [LogBet] No matching option in dropdown: {sample}")
    try:
        if await _combobox_is_open(combobox):
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.12)
    except Exception:
        pass
    return False


async def _log_bet_combos(dialog) -> list:
    combos: list = []
    try:
        items = await dialog.query_selector_all('[data-slot="form-item"]')
    except Exception:
        return combos
    for item in items:
        combo = await item.query_selector('button[role="combobox"]')
        if combo:
            combos.append(combo)
    return combos


async def _pod_click(el, *, timeout: float = 3000) -> bool:
    """Click a POD control without dying on Radix/dialog 'element is not stable'."""
    if el is None:
        return False
    try:
        await el.click(timeout=timeout)
        return True
    except Exception:
        pass
    try:
        await el.click(timeout=min(timeout, 2000), force=True)
        return True
    except Exception:
        pass
    try:
        await el.evaluate("n => n.click()")
        return True
    except Exception:
        return False


async def _fill_pod_log_input(inp, value: str) -> None:
    """Set a Log-bet text field. Never wait on a stable click — the dialog animates."""
    try:
        await inp.evaluate(
            """(el, v) => {
              el.focus();
              const proto = window.HTMLInputElement.prototype;
              const desc = Object.getOwnPropertyDescriptor(proto, 'value');
              if (desc && desc.set) desc.set.call(el, v);
              else el.value = v;
              el.dispatchEvent(new InputEvent('input', {
                bubbles: true, composed: true, data: v, inputType: 'insertFromPaste'
              }));
              el.dispatchEvent(new Event('input', { bubbles: true }));
              el.dispatchEvent(new Event('change', { bubbles: true }));
            }""",
            value,
        )
        return
    except Exception:
        pass
    try:
        await inp.fill(value, force=True, timeout=2_000)
    except Exception:
        await inp.evaluate(
            """(el, v) => { el.value = v; }""",
            value,
        )


async def _fill_log_bet_odds_stake(page, odds_str: str, stake_str: str) -> bool:
    try:
        return bool(
            await page.evaluate(
                """([odds, stake]) => {
                  const dlg = [...document.querySelectorAll(
                    '[data-slot="dialog-content"], [role="dialog"]'
                  )].find((el) => /log bet/i.test(el.innerText || ''));
                  if (!dlg) return false;
                  const setVal = (el, v) => {
                    if (!el) return false;
                    el.focus();
                    const proto = window.HTMLInputElement.prototype;
                    const desc = Object.getOwnPropertyDescriptor(proto, 'value');
                    if (desc && desc.set) desc.set.call(el, v);
                    else el.value = v;
                    el.dispatchEvent(new InputEvent('input', {
                      bubbles: true, composed: true, data: v, inputType: 'insertFromPaste'
                    }));
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                    el.blur();
                    return true;
                  };
                  const inputs = [...dlg.querySelectorAll('input')].filter((el) => {
                    const t = (el.type || 'text').toLowerCase();
                    return t === 'text' || t === 'number' || t === ''
                      || el.getAttribute('inputmode') === 'decimal';
                  });
                  const oddsEl = dlg.querySelector('input[name="odds"]')
                    || inputs.find((i) => (i.name || '').toLowerCase() === 'odds')
                    || inputs[0];
                  const stakeEl = dlg.querySelector('input[name="stake"]')
                    || inputs.find((i) => (i.name || '').toLowerCase() === 'stake')
                    || inputs.find((i) => i !== oddsEl);
                  return setVal(oddsEl, odds) && setVal(stakeEl, stake);
                }""",
                [odds_str, stake_str],
            )
        )
    except Exception:
        return False


async def _submit_log_bet_dialog(page) -> bool:
    for _ in range(16):
        try:
            ready = await page.evaluate(
                """() => {
                  const dlg = [...document.querySelectorAll(
                    '[data-slot="dialog-content"], [role="dialog"]'
                  )].find((el) => /log bet/i.test(el.innerText || ''));
                  if (!dlg) return false;
                  const btn = dlg.querySelector('button[type="submit"]')
                    || [...dlg.querySelectorAll('button')].find(
                      (b) => /log bet/i.test((b.innerText || '').trim())
                    );
                  return !!(btn && !btn.disabled);
                }"""
            )
        except Exception:
            ready = False
        if ready:
            break
        await asyncio.sleep(0.12)
    try:
        clicked = await page.evaluate(
            """() => {
              const dlg = [...document.querySelectorAll(
                '[data-slot="dialog-content"], [role="dialog"]'
              )].find((el) => /log bet/i.test(el.innerText || ''));
              if (!dlg) return false;
              const btn = dlg.querySelector('button[type="submit"]')
                || [...dlg.querySelectorAll('button')].find(
                  (b) => /log bet/i.test((b.innerText || '').trim())
                );
              if (!btn) return false;
              btn.click();
              return true;
            }"""
        )
        return bool(clicked)
    except Exception:
        return False


async def _wait_log_bet_dialog(page):
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        dlg = await page.query_selector(
            '[data-slot="dialog-content"]:has(h2), [role="dialog"][data-state="open"]'
        )
        if dlg:
            try:
                title = (await dlg.inner_text() or "")[:80].lower()
            except Exception:
                title = ""
            if "log bet" in title:
                return dlg
        await asyncio.sleep(0.15)
    return None


async def log_pod_bet(
    page,
    alert: PODAlert,
    *,
    odds: float,
    stake: float,
) -> bool:
    """Open POD's green + Log bet dialog, fill odds/stake, submit.

    `alert` must be the selection that was actually placed (quick-look alt
    line included), not the original drop if we switched handicap/total.
    """
    home = alert.home_team or ""
    away = alert.away_team or ""
    if not home or not away:
        return False

    src = "quick-look alt" if alert.alert_type == "ALT" else "drop"
    log.info(
        f"  [LogBet] {src}: {alert.period or 'Match'} {alert.market} "
        f"{alert.selection} {alert.line or ''}".strip()
    )

    try:
        await close_quick_look(page)
        btn, _row_id = await _find_pod_row_action_button(
            page,
            alert,
            [
                'button[data-slot="dialog-trigger"][class*="border-green-400"]',
                'button[data-slot="dialog-trigger"]',
            ],
        )
        if not btn:
            log.warning(f"  [LogBet] + button not found for {home} vs {away}")
            return False

        if not await _pod_click(btn, timeout=3_000):
            log.warning(f"  [LogBet] + button click failed for {home} vs {away}")
            return False
        dialog = await _wait_log_bet_dialog(page)
        if not dialog:
            log.warning(f"  [LogBet] Dialog did not open for {home} vs {away}")
            return False

        combos = await _log_bet_combos(dialog)

        # 0 event, 1 period, 2 market, 3 selection (tags is later / optional)
        if combos:
            try:
                event_txt = await combos[0].inner_text()
            except Exception:
                event_txt = ""
            if event_txt and not teams_match_text(home, away, event_txt):
                if not await _set_pod_combobox(
                    page, combos[0], [f"{home} vs {away}", home, away]
                ):
                    log.warning(
                        f"  [LogBet] Event dropdown is not this match "
                        f"({_safe_text(event_txt)[:80]})"
                    )
                    await _close_pod_popover(page)
                    return False

        combos = await _log_bet_combos(dialog)
        if len(combos) >= 2:
            if not await _set_pod_combobox(
                page, combos[1], _pod_log_period_labels(alert)
            ):
                log.warning(
                    f"  [LogBet] Could not set period '{alert.period or 'Match'}'"
                )
        combos = await _log_bet_combos(dialog)
        if len(combos) >= 3:
            if not await _set_pod_combobox(
                page, combos[2], _pod_log_market_labels(alert)
            ):
                log.warning(f"  [LogBet] Could not set market '{alert.market}'")
                await _close_pod_popover(page)
                return False
            await asyncio.sleep(0.4)
        combos = await _log_bet_combos(dialog)
        if len(combos) >= 4:
            if not await _set_pod_combobox(
                page,
                combos[3],
                _pod_log_selection_labels(alert),
                alert,
            ):
                log.warning(
                    f"  [LogBet] Could not set selection "
                    f"'{alert.selection} {alert.line or ''}'".strip()
                )
                await _close_pod_popover(page)
                return False
            try:
                picked = _pod_norm_option_text(await combos[3].inner_text() or "")
            except Exception:
                picked = ""
            if picked:
                log.info(f"  [LogBet] Dialog selection → {picked}")

        odds_str = f"{odds:.3f}".rstrip("0").rstrip(".")
        if "." not in odds_str:
            odds_str = f"{odds:.2f}"
        stake_str = f"{stake:.2f}"
        await asyncio.sleep(0.2)
        if not await _fill_log_bet_odds_stake(page, odds_str, stake_str):
            dialog = await _wait_log_bet_dialog(page) or dialog
            odds_inp = await dialog.query_selector('input[name="odds"]')
            stake_inp = await dialog.query_selector(
                'input[name="stake"], input[inputmode="decimal"]'
            )
            if not odds_inp or not stake_inp:
                log.warning("  [LogBet] Odds/stake inputs not found")
                await _close_pod_popover(page)
                return False
            await _fill_pod_log_input(odds_inp, odds_str)
            await _fill_pod_log_input(stake_inp, stake_str)
        await asyncio.sleep(0.2)

        if not await _submit_log_bet_dialog(page):
            dialog = await _wait_log_bet_dialog(page) or dialog
            submit = await dialog.query_selector('button[type="submit"]')
            if not submit:
                submit = await dialog.query_selector('button:has-text("Log bet")')
            if not submit or not await _pod_click(submit, timeout=3_000):
                log.warning("  [LogBet] Submit button not found or not clickable")
                await _close_pod_popover(page)
                return False

        # Wait for the dialog to close
        for _ in range(15):
            still = await page.query_selector(
                '[data-slot="dialog-content"]:has(h2), [role="dialog"][data-state="open"]'
            )
            if not still:
                break
            try:
                t = (await still.inner_text() or "").lower()
            except Exception:
                break
            if "log bet" not in t:
                break
            await asyncio.sleep(0.15)
        else:
            await _close_pod_popover(page)

        log.info(
            f"  [LogBet] Logged {home} vs {away} | {alert.market} "
            f"{alert.selection} {alert.line or ''} @ {odds_str} EUR{stake_str}".strip()
        )
        return True
    except Exception as e:
        log.warning(f"  [LogBet] Failed for {home} vs {away}: {e}")
        await _close_pod_popover(page)
        return False


async def _close_pod_popover(page) -> None:
    try:
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.2)
    except Exception:
        pass


async def _reveal_quick_look_button(page, row_or_cell) -> object | None:
    """
    Magnifying glass only appears after hovering/clicking the match row.
    Click/hover the team/event text, then wait for the sky button.
    """
    # Prefer clicking team/event text inside the cell (reveals action buttons)
    click_targets = []
    for sel in ("p", "span", "div.relative", "div.cursor-pointer"):
        try:
            found = await row_or_cell.query_selector_all(sel)
            click_targets.extend(found[:3])
        except Exception:
            pass
    click_targets.append(row_or_cell)

    for target in click_targets:
        try:
            await target.hover(timeout=1_500)
        except Exception:
            pass
        try:
            await target.click(timeout=1_500)
        except Exception:
            continue
        await asyncio.sleep(0.25)

        # Button may appear on the row, the match cell, or nearby pinned cell
        for scope in (row_or_cell, page):
            btn = await scope.query_selector('button[class*="border-sky-400"]')
            if not btn:
                # Magnifying-glass SVG path often present even if class differs
                btn = await scope.query_selector(
                    'button[data-slot="tooltip-trigger"]:has(svg)'
                )
            if btn:
                try:
                    if await btn.is_visible():
                        return btn
                except Exception:
                    return btn
    return None


async def open_quick_look_for_match(page, alert: PODAlert) -> bool:
    """Hover/click the POD match row to reveal, then click the magnifying glass."""
    await close_quick_look(page)
    home = alert.home_team or ""
    away = alert.away_team or ""
    if not home or not away:
        return False

    try:
        row = await _find_pod_match_row(page, alert)
        if not row:
            log.warning(
                f"  [QuickLook] Match row not visible on POD terminal "
                f"({home} vs {away})"
            )
            return False

        # Scroll into view so hover/click works under AG Grid virtualization
        try:
            await row.scroll_into_view_if_needed(timeout=2_000)
            await asyncio.sleep(0.15)
        except Exception:
            pass

        btn = await _reveal_quick_look_button(page, row)
        if not btn:
            # One more pass: click the row itself then poll briefly
            try:
                await row.click(timeout=1_500)
            except Exception:
                pass
            for _ in range(8):
                await asyncio.sleep(0.2)
                btn = await page.query_selector('button[class*="border-sky-400"]')
                if btn and await btn.is_visible():
                    break
                btn = None

        if not btn:
            log.warning("  [QuickLook] Magnifying-glass button not found after row click")
            return False

        await btn.click()
        await page.wait_for_selector(
            'div[role="dialog"][data-slot="dialog-content"]',
            timeout=5_000,
        )
        await asyncio.sleep(0.4)
        log.info(f"  [QuickLook] Opened for {home} vs {away}")
        return True
    except Exception as e:
        log.warning(f"  [QuickLook] Failed to open: {e}")
        await close_quick_look(page)
        return False


async def scrape_quick_look_alts(page) -> list[QuickLookAlt]:
    """Scroll AG Grid in the dialog and collect market/outcome/NVP rows."""
    dialog = await page.query_selector('div[role="dialog"][data-slot="dialog-content"]')
    if not dialog:
        return []

    collected: dict[str, QuickLookAlt] = {}
    viewport = await dialog.query_selector(".ag-center-cols-viewport")

    for _ in range(25):
        data = await page.evaluate(QUICK_LOOK_EXTRACT_JS)
        for r in data.get("rows") or []:
            rid = r.get("rowId") or f"{r.get('market')}|{r.get('outcome')}|{r.get('nvp')}"
            nvp = _parse_float(str(r.get("nvp") or ""))
            if nvp is None:
                continue
            market = (r.get("market") or "").strip()
            outcome = (r.get("outcome") or "").strip()
            if not outcome:
                continue
            market = _infer_quick_look_market(market, outcome, rid)
            # Header rows have market spanning full width and no numeric nvp style — skip empty markets with section names
            if re.match(r"^(Match|1st Half|2nd Half)$", market, re.I) and not _parse_quick_look_spread(outcome) and not _parse_quick_look_total(outcome):
                continue
            if rid in collected:
                # Fill market if this scroll pass has the spanning market label
                if market and not collected[rid].market:
                    collected[rid].market = market
                continue
            collected[rid] = QuickLookAlt(
                market=market,
                outcome=outcome,
                nvp=nvp,
                price=_parse_float(str(r.get("price") or "")),
                period=_period_from_quick_look_row_id(rid),
                row_id=rid,
            )

        if not viewport:
            break
        try:
            moved = await viewport.evaluate(
                """el => {
                    const before = el.scrollTop;
                    el.scrollTop = Math.min(el.scrollTop + el.clientHeight * 0.85, el.scrollHeight);
                    return el.scrollTop > before + 2;
                }"""
            )
            if not moved:
                break
            await asyncio.sleep(0.15)
        except Exception:
            break

    # Propagate market labels down when row-span left market empty on child rows
    last_market = ""
    ordered = list(collected.values())
    # Sort by row visual order roughly via row_id suffix
    for alt in ordered:
        if alt.market:
            last_market = alt.market
        elif last_market:
            alt.market = last_market

    # Better: re-sort by parsing row positions from data — keep as list
    alts = list(collected.values())
    log.info(f"  [QuickLook] Scraped {len(alts)} alt line(s)")
    return alts


async def fetch_quick_look_alts(page, alert: PODAlert) -> list[QuickLookAlt]:
    opened = await open_quick_look_for_match(page, alert)
    if not opened:
        return []
    try:
        return await scrape_quick_look_alts(page)
    finally:
        await close_quick_look(page)


async def try_alt_lines_on_rainbet(
    page,
    alert: PODAlert,
    alts: list[QuickLookAlt],
) -> tuple[float, float, PODAlert] | None:
    """
    Exact line missing: use already-scraped QuickLook alts.
    One inventory scrape (optional Show-more), match all nearby alts in memory,
    pick best soft-book EV. No serial per-alt page opens.
    """
    candidates = rank_nearby_alts(alert, alts)
    if not candidates:
        log.info("  [Alt] No nearby alt lines in quick look")
        return None

    tag = book_tag()
    # Only consider the top NVP-ranked alts (keeps matching cheap & focused)
    shortlist = candidates[: max(ALT_ATTEMPT_MAX * 3, 6)]
    log.info(
        f"  [Alt] Exact line missing — NVP-ranking {len(candidates)} nearby "
        f"QuickLook alt(s), matching top {len(shortlist)} on one {tag} scrape"
    )

    # Expand truncated handicap grids once, then a single inventory scrape
    await _expand_visible_show_more(page, alert)
    inv = await _scrape_market_inventory(page)
    if _is_challenge_url(page.url) or _inventory_markets_blank(inv):
        if await recover_event_page_once(page):
            await _expand_visible_show_more(page, alert)
            inv = await _scrape_market_inventory(page)
        if _inventory_markets_blank(inv):
            log.info(f"  [Alt] {tag} markets empty after recovery — abort alts")
            return None

    found: list[tuple[float, float, PODAlert, str]] = []
    checked = 0
    for alt in shortlist:
        if checked >= max(ALT_ATTEMPT_MAX * 2, 4) and found:
            # Already have hits from the best-NVP alts — stop early
            break
        try:
            alt_alert = alt_line_to_alert(alert, alt)
            if not alt_alert or alt_alert.no_vig_odds is None:
                continue
            checked += 1

            # Skip alts that cannot clear MIN_EV even with optimistic soft odds
            optimistic = alt.price if alt.price and alt.price > 1.01 else None
            if optimistic is not None:
                opt_ev = compute_ev_pct(optimistic * 1.15, alt_alert.no_vig_odds)
                if opt_ev < MIN_EV_PCT - 8.0:
                    log.info(
                        f"  [Alt] Skip low-EV proxy: {alt.market} {alt.outcome} "
                        f"(NVP {alt.nvp}, opt EV {opt_ev:+.1f}%)"
                    )
                    continue

            hit = _match_alert_in_inventory(inv, alt_alert)
            if not hit:
                log.info(
                    f"  [Alt] {tag} miss: {alt.market} {alt.outcome} (NVP {alt.nvp})"
                )
                continue

            _psi, soft_odds, label, _title = hit
            ev_pct = compute_ev_pct(soft_odds, alt_alert.no_vig_odds)
            log.info(
                f"  [Alt] Candidate '{_safe_text(label)}' @ {soft_odds:.3f} "
                f"| NVP {alt_alert.no_vig_odds:.3f} | EV {ev_pct:+.1f}%"
            )
            found.append((ev_pct, soft_odds, alt_alert, label))
            if len(found) >= ALT_ATTEMPT_MAX:
                break
        except Exception as e:
            log.warning(f"  [Alt] Error on {alt.market} {alt.outcome}: {e}")
            continue

    if not found:
        log.info(f"  [Alt] No nearby alts available on {tag}")
        return None

    found.sort(key=lambda t: t[0], reverse=True)
    ev_pct, soft_odds, alt_alert, label = found[0]
    if len(found) > 1:
        summary = ", ".join(f"{ev:+.1f}%" for ev, *_ in found[:5])
        log.info(f"  [Alt] Best of {len(found)}: '{label}' EV {ev_pct:+.1f}% (all: {summary})")
    else:
        log.info(f"  [Alt] Only match: '{label}' EV {ev_pct:+.1f}%")
    return ev_pct, soft_odds, alt_alert


async def evaluate_alert_on_rainbet(
    page,
    alert: PODAlert,
    *,
    already_on_event: bool = False,
    pod_page=None,
    pod_lock: asyncio.Lock | None = None,
    quick_alts_cache: dict | None = None,
) -> tuple[float, float, PODAlert] | None:
    """Find selection on Rainbet; fall back to POD quick-look alt lines if needed.

    Returns (ev_pct, soft_odds, matched_alert).
    """
    if not alert.home_team or not alert.away_team:
        log.warning(f"  [{book_tag()}] Missing team names on alert")
        return None

    if not already_on_event:
        ok = await _with_timeout(
            navigate_rainbet_event(page, alert),
            RAINBET_OP_TIMEOUT,
            "event search",
        )
        if not ok:
            await close_rainbet_search(page)
            await ensure_rainbet_sportsbook(page)
            return None
        if not await wait_for_outcome_plates(page):
            log.warning(
                f"  [{book_tag()}] Event page loaded but no outcome plates found yet"
            )
            await _log_market_dom_diagnostic(page)
            # One recovery pass (CF / hollow DOM) — skip if still blank
            if not await recover_event_page_once(page):
                await ensure_rainbet_sportsbook(page)
                return None
            if not await wait_for_outcome_plates(page, timeout=8.0):
                await ensure_rainbet_sportsbook(page)
                return None

    opened_sport = sport_from_href(page.url)
    pod_sport = alert_canonical_sport(alert)
    if not sports_agree(pod_sport, opened_sport):
        log.warning(
            f"  [{book_tag()}] Skip market lookup — sport mismatch "
            f"(POD {pod_sport or '?'} vs Rainbet {opened_sport or '?'}) "
            f"url={_safe_text(page.url)}"
        )
        return None

    await wait_for_preloader_gone(page, timeout=PRELOADER_WAIT_TIMEOUT)
    plate, plate_text = await _with_timeout(
        find_rainbet_outcome_plate(page, alert),
        OUTCOME_LOOKUP_TIMEOUT,
        "outcome lookup",
    ) or (None, None)

    if plate:
        soft_odds = await read_plate_odds(plate)
        if soft_odds is None or alert.no_vig_odds is None:
            return None
        ev_pct = compute_ev_pct(soft_odds, alert.no_vig_odds)
        log.info(
            f"  [{book_tag()}] Found '{_safe_text(plate_text)}' @ {soft_odds:.3f} | EV {ev_pct:+.1f}%"
        )
        return ev_pct, soft_odds, alert

    # Exact line not on Rainbet — try quick look alts
    if not pod_page:
        return None

    mk = alert.match_key
    alts: list[QuickLookAlt] = []
    # Only reuse a successful scrape — empty/failed opens must not poison the cache
    if quick_alts_cache is not None and quick_alts_cache.get(mk):
        alts = quick_alts_cache[mk]
    else:
        lock = pod_lock or asyncio.Lock()
        async with lock:
            alts = await fetch_quick_look_alts(pod_page, alert)
        if quick_alts_cache is not None and alts:
            quick_alts_cache[mk] = alts

    if not alts:
        return None

    return await try_alt_lines_on_rainbet(page, alert, alts)


def _bcgame_btc_stake(fiat_usd: float) -> float:
    """Fixed BTC size for a dollar stake. Not recomputed if the price moves."""
    return round(float(fiat_usd) / BCGAME_BTC_USD, 8)


def _format_stake_amount(stake: float) -> str:
    if abs(stake) >= 1 and abs(stake - round(stake)) < 1e-6:
        return str(int(round(stake)))
    text = f"{stake:.8f}".rstrip("0").rstrip(".")
    return text or "0"


def _stake_amounts_match(current: float | None, wanted: float) -> bool:
    if current is None:
        return False
    if abs(wanted) >= 1:
        return abs(current - wanted) < 0.01
    return abs(current - wanted) <= max(1e-8, abs(wanted) * 0.005)


def _parse_stake_value(raw: str) -> float | None:
    cleaned = re.sub(r"[^0-9.]", "", raw or "")
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


async def _betslip_target(page):
    """Betslip may live in an iframe — find the scope that holds it."""
    for frame in page.frames:
        try:
            if await frame.query_selector('div[data-editor-id="betslipContent"]'):
                return frame
        except Exception:
            continue
    return page


async def _read_slip_odds(target) -> float | None:
    try:
        el = await target.query_selector('div[data-editor-id="betslipSelectionOdd"] div')
        if not el:
            return None
        return _parse_float(await el.inner_text())
    except Exception:
        return None


async def _set_betslip_stake(target, stake: float) -> bool:
    """Type the stake and verify it stuck (Rainbet resets to a minimum sometimes)."""
    stake_str = _format_stake_amount(stake)
    last_seen = None
    for attempt in range(6):
        stake_input = await target.query_selector('label[data-editor-id="betslipStakeInput"] input')
        if not stake_input:
            await human_pause(0.25, 0.45)
            continue
        try:
            await stake_input.wait_for_element_state("editable", timeout=2_000)
        except Exception:
            pass

        try:
            await human_click(stake_input)
            await stake_input.press("Control+A")
            await stake_input.press("Backspace")
        except Exception:
            pass

        typed = False
        try:
            await stake_input.fill(stake_str)
            typed = True
        except Exception:
            try:
                await stake_input.type(stake_str, delay=random.randint(25, 55))
                typed = True
            except Exception:
                typed = False

        if not typed:
            try:
                await stake_input.evaluate(
                    """(el, v) => {
                        el.focus();
                        const setter = Object.getOwnPropertyDescriptor(
                            window.HTMLInputElement.prototype, 'value'
                        ).set;
                        setter.call(el, v);
                        el.dispatchEvent(new InputEvent('input', { bubbles: true, data: v }));
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    }""",
                    stake_str,
                )
            except Exception:
                pass

        await human_pause(0.35, 0.65)

        try:
            current = _parse_stake_value(await stake_input.input_value())
        except Exception:
            current = _parse_stake_value(await stake_input.get_attribute("value") or "")
        last_seen = current
        if _stake_amounts_match(current, float(stake)):
            return True

        try:
            await stake_input.evaluate(
                """(el, v) => {
                    el.focus();
                    const setter = Object.getOwnPropertyDescriptor(
                        window.HTMLInputElement.prototype, 'value'
                    ).set;
                    setter.call(el, v);
                    el.dispatchEvent(new InputEvent('input', { bubbles: true, data: v }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }""",
                stake_str,
            )
        except Exception:
            pass
        await human_pause(0.3, 0.55)

        try:
            current = _parse_stake_value(await stake_input.input_value())
        except Exception:
            current = _parse_stake_value(await stake_input.get_attribute("value") or "")
        last_seen = current
        if _stake_amounts_match(current, float(stake)):
            return True

        log.info(
            f"  [Bet] Stake retry {attempt + 1}/6 "
            f"(wanted {stake}, field={last_seen})"
        )

    log.error(f"  [Bet] Stake did not set to {stake} (field={last_seen}), aborting")
    return False


async def _wait_for_bet_confirmation(target, timeout_ms: int = 6_000) -> bool:
    try:
        await target.wait_for_selector(
            'div[data-editor-id="betslipReceipt"], '
            'div[data-editor-id="betslipContent"]:has-text("Placed"), '
            'div[data-editor-id="betslipContent"]:has-text("Accepted"), '
            'div[data-editor-id="betslipContent"]:has-text("Success")',
            timeout=timeout_ms,
        )
        return True
    except PlaywrightTimeout:
        # Empty slip also means the bet went through
        try:
            remaining = await target.query_selector_all(
                'div[data-editor-id="betslipSelectionRemoveButton"]'
            )
            return not remaining
        except Exception:
            return False


async def place_rainbet_bet(page, plate_element, expected_odds: float, stake: float) -> bool:
    """Click outcome plate, set stake, confirm odds, place bet."""
    try:
        if get_book().domain == "bc.game":
            fiat = stake
            stake = _bcgame_btc_stake(fiat)
            log.info(
                f"  [Bet] BC.Game slip {stake:.8f} BTC "
                f"(fixed ${fiat:.0f} at ${BCGAME_BTC_USD:,.2f}/BTC)"
            )
        await wait_for_preloader_gone(page, timeout=PRELOADER_WAIT_TIMEOUT)
        await close_betslip(page)
        await human_pause(0.25, 0.5)

        plate_odds = await read_plate_odds(plate_element)
        if plate_odds is not None and abs(plate_odds - expected_odds) > ODDS_DRIFT_TOL:
            log.warning(
                f"  [Bet] Board odds drifted {expected_odds} -> {plate_odds}, aborting"
            )
            return False

        await human_click(plate_element, timeout=5000)

        target = await _betslip_target(page)
        try:
            await target.wait_for_selector(
                'div[data-editor-id="betslipContent"]', timeout=6_000
            )
        except PlaywrightTimeout:
            log.warning("  [Bet] Betslip did not open, aborting")
            return False

        closed = await target.query_selector(
            'div[data-editor-id="betslipContent"]:has-text("CLOSED")'
        )
        if closed:
            log.warning("  [Bet] Selection suspended/CLOSED, aborting")
            await close_betslip(page)
            return False

        slip_odds = await _read_slip_odds(target)
        if slip_odds is not None and abs(slip_odds - expected_odds) > ODDS_DRIFT_TOL:
            log.warning(f"  [Bet] Slip odds drifted {expected_odds} -> {slip_odds}, aborting")
            await close_betslip(page)
            return False
        if slip_odds is not None:
            log.info(f"  [Bet] Slip odds confirmed: {slip_odds:.3f}")

        if not await _set_betslip_stake(target, stake):
            await close_betslip(page)
            return False

        # Odds can move while the stake is being typed
        slip_odds = await _read_slip_odds(target)
        if slip_odds is not None and abs(slip_odds - expected_odds) > ODDS_DRIFT_TOL:
            log.warning(f"  [Bet] Late odds drift {expected_odds} -> {slip_odds}, aborting")
            await close_betslip(page)
            return False

        place_btn = await target.query_selector(
            'button[data-editor-id="betslipPlaceBetButton"]'
        )
        if not place_btn:
            log.error("  [Bet] Place Bet button not found, aborting")
            await close_betslip(page)
            return False

        await human_click(place_btn)
        await human_pause(0.5, 0.9)

        if await _wait_for_bet_confirmation(target):
            log.info(
                f"  [Bet] BET PLACED @ {slip_odds or expected_odds:.3f} for "
                f"{_format_stake_amount(stake)} "
                f"{'BTC' if get_book().domain == 'bc.game' else 'EUR'}"
            )
            await close_betslip(page)
            return True

        log.warning("  [Bet] No confirmation seen — treating as NOT placed")
        await close_betslip(page)
        return False
    except Exception as e:
        log.error(f"  [Bet] Error: {e}")
        await close_betslip(page)
        return False


class SeenAlertStore:
    def __init__(self, path: str = SEEN_ALERTS_FILE):
        self.path = path
        self._ids: set[str] = set()
        self._known_drop_keys: set[str] = set()
        self._load()

    @staticmethod
    def drop_key(alert: PODAlert) -> str:
        """Unique state for a drop — changes when odds move on the same row."""
        if alert.row_id and alert.new_odds is not None:
            old = alert.old_odds if alert.old_odds is not None else ""
            return f"{alert.row_id}|{alert.new_odds}|{old}"
        return alert.alert_id

    def clear(self) -> None:
        self._ids.clear()
        self._known_drop_keys.clear()

    def seed_alerts(self, alerts: list[PODAlert]) -> None:
        """Mark everything currently on screen as already seen (startup backlog)."""
        for alert in alerts:
            self.acknowledge(alert)

    def _load(self) -> None:
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                self._ids = set(data.get("alert_ids", []))
                self._known_drop_keys = set(data.get("drop_keys", []))
            elif isinstance(data, list):
                self._ids = set(data)
        except FileNotFoundError:
            self._ids = set()
            self._known_drop_keys = set()
        except Exception as e:
            log.warning(f"[Store] Could not load {self.path}: {e}")

    def save(self) -> None:
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "alert_ids": sorted(self._ids),
                        "drop_keys": sorted(self._known_drop_keys),
                    },
                    f,
                    indent=2,
                )
        except Exception as e:
            log.warning(f"[Store] Could not save {self.path}: {e}")

    def is_actionable(self, alert: PODAlert) -> tuple[bool, str]:
        if not alert.is_parsed:
            return False, "unparsed"
        key = self.drop_key(alert)
        if key in self._known_drop_keys:
            return False, "unchanged"
        return True, "new-drop"

    def acknowledge(self, alert: PODAlert) -> None:
        self._ids.add(alert.alert_id)
        self._known_drop_keys.add(self.drop_key(alert))


async def dump_debug_html(page, label: str = "pod_terminal") -> None:
    global _html_dumped
    if not DEBUG_DUMP_HTML or _html_dumped:
        return
    try:
        ts = datetime.now().strftime("%H%M%S")
        html = await page.content()
        main_file = f"debug_{label}_main_{ts}.html"
        with open(main_file, "w", encoding="utf-8") as f:
            f.write(html)
        log.info(f"  [DEBUG] Saved {main_file} ({len(html)} bytes)")

        for i, frame in enumerate(page.frames):
            if frame == page.main_frame:
                continue
            try:
                frame_html = await frame.content()
                if len(frame_html) > 500:
                    frame_file = f"debug_{label}_frame{i}_{ts}.html"
                    with open(frame_file, "w", encoding="utf-8") as f:
                        f.write(frame_html)
                    log.info(f"  [DEBUG] Saved {frame_file} ({len(frame_html)} bytes)")
            except Exception:
                pass

        _html_dumped = True
    except Exception as e:
        log.error(f"  [DEBUG] HTML dump error: {e}")


AG_GRID_EXTRACT_JS = """
() => {
  const byRowId = {};

  for (const row of document.querySelectorAll('.ag-row[row-id]')) {
    const rowId = row.getAttribute('row-id');
    if (!rowId) continue;
    if (!byRowId[rowId]) {
      byRowId[rowId] = { rowId, cells: {} };
    }

    for (const cell of row.querySelectorAll('[role="gridcell"][col-id]')) {
      const colId = cell.getAttribute('col-id');
      const text = (cell.innerText || '').trim();
      if (colId && text) {
        byRowId[rowId].cells[colId] = text;
      }
    }
  }

  return Object.values(byRowId)
    .filter((row) => row.cells.match && row.cells.rowOutcome && row.cells.noVigPrice)
    .map((row) => ({
      rowId: row.rowId,
      cells: row.cells,
      rowText: Object.values(row.cells).join(' | '),
    }));
}
"""


async def wait_for_pod_grid(page, timeout_ms: int = 60_000) -> bool:
    try:
        await page.wait_for_selector(".ag-row[row-id]", timeout=timeout_ms)
        await asyncio.sleep(0.4)
        return True
    except Exception:
        return False


async def _extract_ag_grid_rows(page) -> list[dict[str, Any]]:
    """Merge AG Grid row fragments by row-id (pinned + center columns)."""
    try:
        return await page.evaluate(AG_GRID_EXTRACT_JS)
    except Exception as e:
        log.error(f"[POD] AG Grid evaluate error: {e}")
        return []


_scrape_warned = False


async def scrape_pod_alerts(page, quiet: bool = False, grid_timeout_ms: int = 15_000) -> list[PODAlert]:
    """Scrape visible drop alerts from the POD terminal AG Grid."""
    global _scrape_warned
    alerts: list[PODAlert] = []
    seen_ids: set[str] = set()

    await dump_debug_html(page)
    await wait_for_pod_grid(page, timeout_ms=grid_timeout_ms)

    for target in [page, *page.frames]:
        rows = await _extract_ag_grid_rows(target)
        if not rows:
            continue

        for row in rows:
            cells = row.get("cells") or {}
            row_id = row.get("rowId") or ""
            row_text = row.get("rowText") or ""
            alert = _build_alert_from_col_ids(cells, row_id, row_text)
            if not alert or alert.alert_id in seen_ids:
                continue
            seen_ids.add(alert.alert_id)
            alerts.append(alert)

        if alerts:
            break

    if not alerts and not quiet:
        if not _scrape_warned:
            log.warning(
                "[POD] No alert rows parsed — open Alerts [Dropping odds] and wait for the table"
            )
            _scrape_warned = True

    return alerts


async def ensure_on_terminal(page) -> bool:
    url = page.url or ""
    if "/terminal" in url:
        return True
    try:
        await page.goto(POD_TERMINAL_URL, wait_until="domcontentloaded")
        await asyncio.sleep(1.5)
        return "/terminal" in (page.url or "")
    except Exception as e:
        log.error(f"[POD] Navigation error: {e}")
        return False


async def _probe_pod_health(page) -> dict[str, Any]:
    """Snapshot whether the POD terminal grid/session still looks live."""
    js = """() => {
      const url = location.href || '';
      const text = (document.body?.innerText || '').replace(/\\s+/g, ' ').toLowerCase();
      const rowEls = document.querySelectorAll('.ag-row[row-id]');
      const rowIds = [...rowEls].map((r) => r.getAttribute('row-id') || '').filter(Boolean);
      const uniq = [...new Set(rowIds)].sort();
      const hasPassword = !!document.querySelector('input[type="password"]');
      const loginWords = /\\b(log in|sign in|login|sign-in)\\b/.test(text.slice(0, 2500));
      const expired = /session (has )?expired|please log in|please sign in/.test(text.slice(0, 2500));
      return {
        url,
        rowCount: uniq.length,
        rowSig: uniq.slice(0, 40).join('|'),
        looksLogin: (hasPassword || loginWords || expired) && uniq.length === 0,
        onTerminal: url.toLowerCase().includes('/terminal'),
      };
    }"""
    try:
        info = await page.evaluate(js)
        return info if isinstance(info, dict) else {}
    except Exception as e:
        return {"error": str(e), "rowCount": 0, "rowSig": "", "looksLogin": False, "onTerminal": False}


async def keepalive_pod_terminal(page) -> None:
    """Light pointer activity so POD doesn't idle-disconnect the live grid.

    Do not wheel-scroll: AG Grid virtualizes rows, and scrolling changes which
    row-ids are in the DOM, which used to reset the stale-grid watchdog.
    """
    try:
        box = await page.evaluate(
            """() => {
              const el = document.querySelector('.ag-root, .ag-body-viewport, [class*="ag-theme"]');
              if (!el) return null;
              const r = el.getBoundingClientRect();
              if (r.width < 40 || r.height < 40) return null;
              return { x: r.left + Math.min(120, r.width * 0.25), y: r.top + 40 };
            }"""
        )
        if isinstance(box, dict) and box.get("x") is not None:
            await page.mouse.move(float(box["x"]), float(box["y"]))
            await human_pause(0.05, 0.12)
            await page.mouse.move(float(box["x"]) + 12, float(box["y"]) + 8)
    except Exception:
        pass


async def recover_pod_terminal(page, *, reason: str) -> bool:
    """Hard-reload the POD terminal to reconnect a frozen/logged-out grid."""
    log.warning(f"[POD] Terminal looks dead ({reason}) — hard-reloading")
    await _close_pod_popover(page)
    try:
        await close_quick_look(page)
    except Exception:
        pass
    try:
        await _hard_reload_page(page, POD_TERMINAL_URL)
    except Exception as e:
        log.warning(f"[POD] Terminal hard-reload failed: {e}")
        try:
            await page.goto(POD_TERMINAL_URL, wait_until="domcontentloaded")
        except Exception as e2:
            log.warning(f"[POD] Terminal navigation failed: {e2}")
            return False
    if "/terminal" not in (page.url or "").lower():
        try:
            await page.goto(POD_TERMINAL_URL, wait_until="domcontentloaded")
        except Exception as e:
            log.warning(f"[POD] Terminal navigation failed: {e}")
            return False
    await human_pause(0.8, 1.4)
    ok = await wait_for_pod_grid(page, timeout_ms=20_000)
    if ok:
        log.info("[POD] Terminal grid recovered")
        return True
    health = await _probe_pod_health(page)
    if health.get("looksLogin"):
        log.warning("[POD] Login wall after reload — sign in on the POD tab")
    else:
        log.warning("[POD] Grid still missing after reload")
    return ok


async def poll_terminal_once(page, store: SeenAlertStore) -> list[PODAlert]:
    if not await ensure_on_terminal(page):
        log.warning("[POD] Not on terminal page — waiting for login/navigation")
        return []

    alerts = await scrape_pod_alerts(
        page, quiet=True, grid_timeout_ms=POD_QUIET_GRID_TIMEOUT_MS
    )
    actionable: list[PODAlert] = []
    skipped: dict[str, int] = {}

    for alert in alerts:
        # Stale POD rows with moving odds must not clog the queue overnight
        if not alert.is_fresh:
            store.acknowledge(alert)
            skipped["stale"] = skipped.get("stale", 0) + 1
            continue
        ok, reason = store.is_actionable(alert)
        if not ok:
            skipped[reason] = skipped.get(reason, 0) + 1
            continue
        actionable.append(alert)

    if skipped and not actionable:
        parts = ", ".join(f"{k}={v}" for k, v in sorted(skipped.items()))
        log.debug(f"[POD] No new drops ({parts})")
        return []

    if not actionable:
        return []

    actionable.sort(key=lambda a: a.sort_age)
    if len(actionable) > 1:
        log.info(f"[POD] {len(actionable)} new drop(s) queued")
    else:
        log.info(f"[POD] New drop ({actionable[0].age_label()})")
    return actionable


class AlertCoordinator:
    """
    Greyhound-style coordinator:
    - POD enqueues alerts
    - Rainbet workers claim a match and process all markets for it on one page
    - Different matches can run on parallel sessions
    """

    def __init__(self):
        self._lock = asyncio.Lock()
        self._pod_ui_lock = asyncio.Lock()  # serialize quick-look on shared POD page
        self._pending: list[PODAlert] = []
        self._pending_ids: set[str] = set()
        self._match_owner: dict[str, str] = {}
        self._session_match: dict[str, str] = {}
        self._wake = asyncio.Event()
        self._refresh_seq = 0
        self._session_refresh_at: dict[str, int] = {}
        self._refresh_reason = ""

    @property
    def pod_ui_lock(self) -> asyncio.Lock:
        return self._pod_ui_lock

    def request_sportsbook_hard_refresh(self, reason: str) -> None:
        """Ask idle workers to hard-reload their Rainbet/Duel tabs."""
        self._refresh_seq += 1
        self._refresh_reason = reason
        self._wake.set()
        log.warning(f"[POD] Sportsbook hard-refresh queued ({reason})")

    def pending_hard_refresh(self, session_name: str) -> str | None:
        if self._session_refresh_at.get(session_name, 0) < self._refresh_seq:
            return self._refresh_reason or "stale POD feed"
        return None

    def mark_hard_refresh_done(self, session_name: str) -> None:
        self._session_refresh_at[session_name] = self._refresh_seq

    def _purge_stale_locked(self) -> int:
        """Drop queued alerts whose POD-reported drop has gone stale while waiting."""
        if not self._pending:
            return 0
        keep: list[PODAlert] = []
        dropped = 0
        for alert in self._pending:
            if alert.is_fresh:
                keep.append(alert)
                continue
            self._pending_ids.discard(
                f"{alert.alert_id}|{SeenAlertStore.drop_key(alert)}"
            )
            dropped += 1
        if dropped:
            self._pending = keep
        return dropped

    async def enqueue(self, alerts: list[PODAlert]) -> int:
        added = 0
        async with self._lock:
            stale = self._purge_stale_locked()
            for alert in alerts:
                if not alert.is_fresh:
                    stale += 1
                    continue
                key = f"{alert.alert_id}|{SeenAlertStore.drop_key(alert)}"
                if key in self._pending_ids:
                    continue
                self._pending.append(alert)
                self._pending_ids.add(key)
                added += 1
            if added:
                self._wake.set()
        if stale:
            log.info(
                f"[Queue] Dropped {stale} stale alert(s) "
                f"(older than {get_max_alert_age_sec():.0f}s per POD)"
            )
        return added

    async def wait(self, timeout: float = 0.5) -> None:
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return
        self._wake.clear()

    async def claim_batch(self, session_name: str) -> list[PODAlert]:
        """Claim all pending alerts for one match, preferring current session match."""
        async with self._lock:
            self._purge_stale_locked()
            if not self._pending:
                return []

            current = self._session_match.get(session_name)
            if current:
                batch = [a for a in self._pending if a.match_key == current]
                if batch:
                    keep = [a for a in self._pending if a.match_key != current]
                    self._pending = keep
                    for a in batch:
                        self._pending_ids.discard(
                            f"{a.alert_id}|{SeenAlertStore.drop_key(a)}"
                        )
                    return batch
                # Current match drained — free ownership
                if self._match_owner.get(current) == session_name:
                    del self._match_owner[current]
                self._session_match.pop(session_name, None)

            # Newest first among pending match groups
            by_match: dict[str, list[PODAlert]] = {}
            for alert in self._pending:
                by_match.setdefault(alert.match_key, []).append(alert)

            ordered_keys = sorted(
                by_match.keys(),
                key=lambda mk: min(a.sort_age for a in by_match[mk]),
            )
            for mk in ordered_keys:
                owner = self._match_owner.get(mk)
                if owner is not None and owner != session_name:
                    continue
                batch = by_match[mk]
                self._pending = [a for a in self._pending if a.match_key != mk]
                for a in batch:
                    self._pending_ids.discard(
                        f"{a.alert_id}|{SeenAlertStore.drop_key(a)}"
                    )
                self._match_owner[mk] = session_name
                self._session_match[session_name] = mk
                return batch
            return []

    async def drain_same_match(self, session_name: str) -> list[PODAlert]:
        async with self._lock:
            self._purge_stale_locked()
            mk = self._session_match.get(session_name)
            if not mk:
                return []
            batch = [a for a in self._pending if a.match_key == mk]
            if not batch:
                return []
            self._pending = [a for a in self._pending if a.match_key != mk]
            for a in batch:
                self._pending_ids.discard(f"{a.alert_id}|{SeenAlertStore.drop_key(a)}")
            return batch

    async def release_session(self, session_name: str) -> None:
        async with self._lock:
            mk = self._session_match.pop(session_name, None)
            if mk and self._match_owner.get(mk) == session_name:
                del self._match_owner[mk]

    async def requeue(
        self,
        alerts: list[PODAlert],
        session_name: str,
        *,
        reason: str = "",
    ) -> int:
        """Put claimed alerts back after a failed open — was the 'glitch' cause."""
        await self.release_session(session_name)
        to_add: list[PODAlert] = []
        for alert in alerts:
            retries = int(alert.raw_fields.get("search_retries", 0)) + 1
            alert.raw_fields["search_retries"] = retries
            if retries > RAINBET_SEARCH_MAX_RETRIES:
                log.warning(
                    f"[Queue] Giving up after {retries} search tries: "
                    f"{alert.home_team} vs {alert.away_team}"
                )
                continue
            to_add.append(alert)
        if not to_add:
            return 0
        n = await self.enqueue(to_add)
        log.info(
            f"[Queue] Re-queued {n} alert(s)"
            f"{f' ({reason})' if reason else ''} — retry "
            f"{to_add[0].raw_fields.get('search_retries')}/{RAINBET_SEARCH_MAX_RETRIES}"
        )
        return n

    async def pending_count(self) -> int:
        async with self._lock:
            return len(self._pending)


def bet_key(alert: PODAlert) -> str:
    """Identity of a bettable selection — POD re-alerts the same market repeatedly."""
    return "|".join(
        [
            alert.match_key,
            (alert.period or "").lower(),
            (alert.market or "").lower(),
            normalise_team_name(alert.selection),
            _normalise_line_value(alert.line) or "",
        ]
    )


_placed_bets: set[str] = set()
_placed_bets_lock = asyncio.Lock()


async def claim_bet_slot(alert: PODAlert) -> bool:
    """Reserve this selection so parallel sessions can't double-bet it."""
    key = bet_key(alert)
    async with _placed_bets_lock:
        if key in _placed_bets:
            return False
        _placed_bets.add(key)
        return True


async def release_bet_slot(alert: PODAlert) -> None:
    async with _placed_bets_lock:
        _placed_bets.discard(bet_key(alert))


async def process_alert_on_page(
    page,
    alert: PODAlert,
    *,
    session_name: str,
    already_on_event: bool,
    dry_run: bool,
    stake: float,
    pod_page=None,
    pod_lock: asyncio.Lock | None = None,
    quick_alts_cache: dict | None = None,
) -> None:
    if is_ignored_special_market(alert):
        log.info(
            f"[{session_name}] Skipping special market "
            f"(corners/bookings/cards): {alert.home_team} vs {alert.away_team}"
        )
        return

    if not alert.is_fresh:
        log.info(
            f"[{session_name}] Stale drop ({alert.age_label()}) — skip "
            f"{alert.home_team} vs {alert.away_team}"
        )
        return

    log.info(f"[{session_name}] ({alert.age_label()}) {alert.summary()}")

    result = await evaluate_alert_on_rainbet(
        page,
        alert,
        already_on_event=already_on_event,
        pod_page=pod_page,
        pod_lock=pod_lock,
        quick_alts_cache=quick_alts_cache,
    )
    if result is None:
        return

    ev_pct, soft_odds, matched = result
    alert.ev_pct = ev_pct
    alert.soft_odds = soft_odds
    if matched.alert_type == "ALT":
        log.info(
            f"  [{session_name}] Using alt line: {matched.market} "
            f"{matched.selection} {matched.line or ''} NVP={matched.no_vig_odds}"
        )

    if ev_pct < MIN_EV_PCT:
        log.info(
            f"  [{session_name}] EV {ev_pct:+.1f}% below {MIN_EV_PCT}% threshold — skip"
        )
        return

    if ev_pct >= MAX_PLAUSIBLE_EV_PCT:
        log.warning(
            f"  [{session_name}] Absurd EV {ev_pct:+.1f}% "
            f"(>= {MAX_PLAUSIBLE_EV_PCT:.0f}%) — skip (likely wrong event/market)"
        )
        return

    log.info(f"  [{session_name}] VALUE EV {ev_pct:+.1f}% >= {MIN_EV_PCT}%")
    if dry_run:
        log.info(f"  [{session_name}] Dry-run — bet not placed (use --bet)")
        return

    if not await claim_bet_slot(matched):
        log.info(
            f"  [{session_name}] Already bet this selection "
            f"({matched.market} {matched.selection} {matched.line or ''}) — skip"
        )
        return

    plate, plate_text = await find_rainbet_outcome_plate(page, matched)
    if not plate:
        log.warning(f"  [{session_name}] Selection vanished before betting — skip")
        await release_bet_slot(matched)
        return

    placed = await place_rainbet_bet(page, plate, soft_odds, stake)
    if not placed:
        # Not placed — allow a retry on the next alert for this selection
        await release_bet_slot(matched)
        return

    log.info(
        f"  [{session_name}] BET: {_safe_text(plate_text or matched.selection)} "
        f"@ {soft_odds:.3f} | EV {ev_pct:+.1f}% | "
        + (
            f"BTC{_bcgame_btc_stake(stake):.8f}"
            if get_book().domain == "bc.game"
            else f"EUR{stake:.2f}"
        )
    )

    # Track the bet on POD, then mute so it stops re-alerting
    if pod_page is not None:
        lock = pod_lock or asyncio.Lock()
        async with lock:
            await log_pod_bet(pod_page, matched, odds=soft_odds, stake=stake)
            await mute_pod_event(pod_page, alert)


async def rainbet_session_loop(
    page,
    coordinator: AlertCoordinator,
    session_name: str,
    dry_run: bool = True,
    stake: float = FLAT_STAKE,
    start_delay: float = 0.0,
    pod_page=None,
) -> None:
    if start_delay > 0:
        log.info(f"[Init] {session_name} waiting {start_delay:.1f}s before start")
        await asyncio.sleep(start_delay)

    log.info(f"[{session_name}] {book_tag()} worker ready")
    last_health_check = time.monotonic()
    last_keepalive = time.monotonic()
    while True:
        try:
            await wait_out_captcha(page, label=session_name)

            refresh_reason = coordinator.pending_hard_refresh(session_name)
            if refresh_reason:
                if await detect_rainbet_captcha(page):
                    await wait_out_captcha(page, label=session_name)
                else:
                    coordinator.mark_hard_refresh_done(session_name)
                    log.warning(
                        f"[{session_name}] Hard-refreshing sportsbook ({refresh_reason})"
                    )
                    await recover_rainbet_page(
                        page, label=session_name, force=True, hard=True
                    )
                    last_keepalive = time.monotonic()
                    last_health_check = time.monotonic()
                    continue

            batch = await coordinator.claim_batch(session_name)
            if not batch:
                # Idle is exactly when Rainbet drops the widget — heal before
                # the next alert arrives instead of failing it
                now = time.monotonic()
                if now - last_health_check >= IDLE_HEALTH_CHECK_SEC:
                    last_health_check = now
                    if await detect_rainbet_session_expired(page):
                        await recover_rainbet_session_expired(page, label=session_name)
                        last_keepalive = time.monotonic()
                    elif await detect_rainbet_dead_page(page):
                        await recover_rainbet_page(page, label=session_name)
                        last_keepalive = time.monotonic()
                    elif now - last_keepalive >= KEEPALIVE_IDLE_SEC:
                        await keepalive_sportsbook(page, label=session_name)
                        last_keepalive = time.monotonic()
                await coordinator.wait(0.6 + random.uniform(0.0, 0.5))
                continue

            await ensure_rainbet_alive(page, label=session_name)
            last_keepalive = time.monotonic()

            mk = batch[0].match_key
            log.info(
                f"[{session_name}] Claimed {len(batch)} alert(s) for "
                f"{batch[0].home_team} vs {batch[0].away_team}"
            )

            # Navigate once for the match
            opened = await _with_timeout(
                navigate_rainbet_event(page, batch[0]),
                RAINBET_OP_TIMEOUT,
                f"{session_name} event search",
            )
            if not opened:
                log.warning(f"[{session_name}] Could not open event — re-queueing")
                await close_rainbet_search(page)
                if await detect_rainbet_captcha(page):
                    await wait_out_captcha(page, label=session_name)
                elif await detect_rainbet_session_expired(page):
                    await recover_rainbet_session_expired(page, label=session_name)
                elif await detect_rainbet_dead_page(page):
                    await recover_rainbet_page(page, label=session_name)
                else:
                    await soft_return_sportsbook(page, allow_goto=True)
                await coordinator.requeue(batch, session_name, reason="search/open failed")
                await human_pause(1.8, 3.2)
                continue

            if not await wait_for_outcome_plates(page):
                log.warning(
                    f"[{session_name}] Event opened but no market boxes — skipping match"
                )
                await _log_market_dom_diagnostic(page)
                await soft_return_sportsbook(page, allow_goto=True)
                await coordinator.release_session(session_name)
                await human_pause(0.8, 1.5)
                continue

            on_event = True
            quick_alts_cache: dict = {}
            while batch:
                for alert in batch:
                    try:
                        await process_alert_on_page(
                            page,
                            alert,
                            session_name=session_name,
                            already_on_event=on_event,
                            dry_run=dry_run,
                            stake=stake,
                            pod_page=pod_page,
                            pod_lock=coordinator.pod_ui_lock,
                            quick_alts_cache=quick_alts_cache,
                        )
                    except Exception as e:
                        log.error(f"[{session_name}] Alert error: {e}")
                    on_event = True
                    await human_pause(*RAINBET_ALERT_PAUSE)
                batch = await coordinator.drain_same_match(session_name)
                if batch:
                    log.info(
                        f"[{session_name}] +{len(batch)} more on same match — staying"
                    )

            await coordinator.release_session(session_name)
            await close_rainbet_search(page)
            # Soft leave — do NOT force-reload after every match (captcha bait)
            await soft_return_sportsbook(page, allow_goto=False)
            await human_pause(*RAINBET_MATCH_IDLE_PAUSE)
            log.info(f"[{session_name}] Done with match, idle")
        except Exception as e:
            log.error(f"[{session_name}] Worker error: {e}")
            await coordinator.release_session(session_name)
            try:
                if await detect_rainbet_captcha(page):
                    await wait_out_captcha(page, label=session_name)
                else:
                    await soft_return_sportsbook(page, allow_goto=True)
            except Exception:
                pass
            await human_pause(1.2, 2.5)


async def pod_poll_loop(
    pod_page,
    store: SeenAlertStore,
    coordinator: AlertCoordinator | None,
    poll_interval: float,
) -> None:
    log.info("[POD] Terminal polling started")
    last_heartbeat = time.monotonic()
    last_keepalive = time.monotonic()
    last_new_drop = time.monotonic()
    last_row_sig = ""
    last_row_change = time.monotonic()
    empty_polls = 0
    last_login_log = 0.0

    while True:
        try:
            health = await _probe_pod_health(pod_page)
            rows = int(health.get("rowCount") or 0)
            sig = str(health.get("rowSig") or "")
            now = time.monotonic()
            if sig and sig != last_row_sig:
                last_row_sig = sig
                last_row_change = now
            empty_polls = empty_polls + 1 if rows == 0 else 0

            pod_lock = coordinator.pod_ui_lock if coordinator is not None else None

            if health.get("looksLogin"):
                if now - last_login_log >= 120:
                    log.warning("[POD] Login/session wall detected — sign in on the POD tab")
                    last_login_log = now
            elif not health.get("onTerminal"):
                log.warning("[POD] Left the terminal page — navigating back")
                await ensure_on_terminal(pod_page)
            elif empty_polls >= POD_EMPTY_GRID_POLLS:
                if pod_lock:
                    async with pod_lock:
                        await recover_pod_terminal(pod_page, reason="no grid rows")
                else:
                    await recover_pod_terminal(pod_page, reason="no grid rows")
                empty_polls = 0
                last_row_sig = ""
                last_row_change = time.monotonic()
                last_new_drop = time.monotonic()
            elif rows > 0 and now - last_new_drop >= POD_STALE_DROP_SEC:
                reason = (
                    f"no new drop for {POD_STALE_DROP_SEC / 60:.0f}m "
                    f"({rows} row(s) still on grid)"
                )
                if pod_lock:
                    async with pod_lock:
                        await recover_pod_terminal(pod_page, reason=reason)
                else:
                    await recover_pod_terminal(pod_page, reason=reason)
                if coordinator is not None:
                    coordinator.request_sportsbook_hard_refresh(reason)
                empty_polls = 0
                last_row_sig = ""
                last_row_change = time.monotonic()
                last_new_drop = time.monotonic()
                last_keepalive = time.monotonic()
            elif rows > 0 and now - last_row_change >= POD_STALE_GRID_SEC:
                reason = f"grid unchanged for {POD_STALE_GRID_SEC / 60:.0f}m"
                if pod_lock:
                    async with pod_lock:
                        await recover_pod_terminal(pod_page, reason=reason)
                else:
                    await recover_pod_terminal(pod_page, reason=reason)
                if coordinator is not None:
                    coordinator.request_sportsbook_hard_refresh(reason)
                last_row_sig = ""
                last_row_change = time.monotonic()
                last_new_drop = time.monotonic()
                last_keepalive = time.monotonic()
            elif now - last_keepalive >= POD_KEEPALIVE_SEC:
                await keepalive_pod_terminal(pod_page)
                last_keepalive = now

            if now - last_heartbeat >= POD_HEARTBEAT_SEC:
                ago_min = (now - last_new_drop) / 60.0
                log.info(
                    f"[POD] Heartbeat: {rows} grid row(s), last new drop {ago_min:.0f}m ago"
                )
                last_heartbeat = now

            new_alerts = await poll_terminal_once(pod_page, store)
            if new_alerts:
                last_new_drop = time.monotonic()
                last_row_change = last_new_drop
                for alert in new_alerts:
                    store.acknowledge(alert)
                store.save()
                if coordinator is not None:
                    n = await coordinator.enqueue(new_alerts)
                    if n:
                        pending = await coordinator.pending_count()
                        log.info(f"[POD] Enqueued {n} — queue depth {pending}")
                else:
                    for alert in new_alerts:
                        log.info(f"[PARSED] ({alert.age_label()}) {alert.summary()}")
        except Exception as e:
            log.error(f"[POD] Poll error: {e}")
        await asyncio.sleep(poll_interval)


async def main(
    proxy_server: str | None = None,
    proxy_user: str | None = None,
    proxy_pass: str | None = None,
    poll_interval: float = POLL_INTERVAL,
    reset_seen: bool = False,
    probe_only: bool = False,
    dry_run: bool = True,
    match_rainbet: bool = True,
    stake: float = FLAT_STAKE,
    auto_start: bool = False,
    sessions: int = MAX_CONCURRENT_SESSIONS,
):
    proxy_config = None
    if proxy_server:
        proxy_config = {"server": proxy_server}
        if proxy_user:
            proxy_config["username"] = proxy_user
        if proxy_pass:
            proxy_config["password"] = proxy_pass
        log.info(f"[Init] Using proxy server: {proxy_server}")

    session_count = max(1, min(sessions, 4))

    log.info("[Init] Starting Playwright…")
    async with async_playwright() as p:
        log.info("[Init] Launching Chromium (no output until the window opens)…")
        browser = await p.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-first-run",
                "--window-size=1400,900",
                "--window-position=80,80",
            ],
            proxy=proxy_config,
        )
        log.info("[Init] Chromium launched")
        context = await browser.new_context(viewport={"width": 1380, "height": 860})
        pod_page = await context.new_page()
        rainbet_pages = []
        for i in range(session_count if match_rainbet else 1):
            page = await context.new_page()
            rainbet_pages.append(page)

        await pod_page.goto(POD_TERMINAL_URL, wait_until="domcontentloaded")
        await human_pause(0.4, 0.8)
        book = get_book()
        # Open every soft-book tab first - never block S2 behind S1 captcha waits
        for i, rb in enumerate(rainbet_pages, start=1):
            try:
                await rb.goto(book.sports_url, wait_until="domcontentloaded")
            except Exception as e:
                log.warning(f"[Init] S{i} goto failed: {e}")
            await human_pause(0.5, 1.0)
            log.info(f"[Init] Opened {book.name} session tab S{i}/{session_count} ({rb.url})")

        log.info("=" * 62)
        log.info("POD BETTING BOT")
        log.info(f"  POD:     {POD_TERMINAL_URL}")
        log.info(f"  {book.name}: {book.sports_url}  ({session_count} parallel tabs)")
        log.info(f"  Log into POD + every {book.name} tab, then open Alerts [Dropping odds]")
        stake_label = (
            f"${stake:.0f} = {_bcgame_btc_stake(stake):.8f} BTC"
            if book.domain == "bc.game"
            else f"EUR{stake:.0f}"
        )
        log.info(
            f"  Max age: {get_max_alert_age_sec():.0f}s | "
            f"Min EV: {MIN_EV_PCT:.1f}% | Stake: {stake_label} | "
            f"Mode: {'BET' if not dry_run else 'DRY-RUN'} | "
            f"{book.name}: {'ON' if match_rainbet else 'OFF'}"
        )
        log.info("=" * 62)
        if auto_start:
            log.info(f"[Init] Auto-start — log into POD terminal + {book.name} now (up to 120s)...")
            for attempt in range(1, 121):
                if await wait_for_pod_grid(pod_page, timeout_ms=1_000):
                    log.info(f"[Init] POD grid detected after {attempt}s")
                    break
                if attempt % 15 == 0:
                    log.info(f"[Init] Still waiting for POD grid... ({attempt}s)")
                await asyncio.sleep(1.0)
            else:
                log.warning("[Init] POD grid not detected — continuing anyway")
        else:
            input(f"Press ENTER when POD terminal + {book.name} are ready → ")

        if not auto_start:
            await wait_for_pod_grid(pod_page)

        if probe_only:
            alerts = await scrape_pod_alerts(pod_page)
            log.info(f"[POD] Probe found {len(alerts)} parsed row(s)")
            for i, alert in enumerate(alerts[:10], start=1):
                log.info(f"  {i}. {alert.summary()}")
            return

        store = SeenAlertStore()
        if reset_seen:
            store.clear()
            store.save()
            log.info("[POD] Cleared seen-alert cache (visible rows will be processed)")

        seed_alerts: list[PODAlert] = []
        for attempt in range(1, 31):
            seed_alerts = await scrape_pod_alerts(pod_page, grid_timeout_ms=15_000)
            if seed_alerts:
                break
            log.info(f"[POD] Waiting for alert table... ({attempt}/30)")
            await asyncio.sleep(1.0)

        if reset_seen:
            log.info(f"[POD] Found {len(seed_alerts)} visible row(s) — test mode, not suppressing")
        else:
            store.seed_alerts(seed_alerts)
            store.save()
            log.info(f"[POD] Seeded {len(seed_alerts)} existing terminal rows as seen")
        if seed_alerts:
            log.info(f"[POD] Sample parsed row: {seed_alerts[0].summary()}")

        log.info("=" * 60)
        mode = "match + dry-run" if dry_run else "match + BET"
        log.info(
            f"POD BOT — Running ({mode}) with {session_count} {get_book().name} session(s)"
        )
        log.info("=" * 60)

        if not match_rainbet:
            await pod_poll_loop(pod_page, store, None, poll_interval)
            return

        coordinator = AlertCoordinator()
        tasks = [
            asyncio.create_task(pod_poll_loop(pod_page, store, coordinator, poll_interval))
        ]
        for i, rb_page in enumerate(rainbet_pages):
            name = f"S{i + 1}"
            tasks.append(
                asyncio.create_task(
                    rainbet_session_loop(
                        rb_page,
                        coordinator,
                        name,
                        dry_run=dry_run,
                        stake=stake,
                        start_delay=i * SESSION_START_STAGGER,
                        pod_page=pod_page,
                    )
                )
            )
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        args = parse_args()
        if args.duel:
            set_book(BOOK_DUEL)
        elif args.bcgame:
            set_book(BOOK_BCGAME)
        if args.debug_html:
            DEBUG_DUMP_HTML = True
        POLL_INTERVAL = args.poll
        MIN_EV_PCT = args.min_ev
        FLAT_STAKE = args.stake
        set_max_alert_age_sec(args.max_alert_age)
        asyncio.run(
            main(
                proxy_server=args.proxy_server,
                proxy_user=args.proxy_user,
                proxy_pass=args.proxy_pass,
                poll_interval=args.poll,
                reset_seen=args.reset_seen,
                probe_only=args.probe,
                dry_run=not args.bet,
                match_rainbet=not args.no_rainbet,
                stake=args.stake,
                auto_start=args.auto_start,
                sessions=args.sessions,
            )
        )
    except KeyboardInterrupt:
        log.info("\nBot stopped.")
