"""
Bet Analysis Script
- Parses betting_log.txt for placed bets with midpoint odds
- Scrapes Duel.com bet history to get actual outcomes
- Plots actual profit vs expected profit with 95% confidence interval
"""

import vpn_asyncio  # noqa: F401  — Proton blocks 127.0.0.1 socketpair
import argparse
import asyncio
import re
import json
import os
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from statistics import NormalDist
import numpy as np
import matplotlib.pyplot as plt
from playwright.async_api import async_playwright

# ── Configuration ────────────────────────────────────────────────────────────────
LOG_FILE = Path(__file__).parent / "betting_log.txt"
BETS_CACHE_FILE = Path(__file__).parent / "bets_cache.json"
CHROME_DATA_DIR = r"C:\Users\anzeg\AppData\Local\Google\Chrome\User Data"

BRITISH_TRACKS = {
    "dunstall",
    "harlow",
    "hove",
    "doncaster",
    "nottingham",
    "towcester",
    "yarmouth",
    "romford",
    "monmore",
    "sunderland",
    "valley",
    "newcastle",
}

CONFIDENCE_LEVELS = list(range(50, 100, 5))


def parse_args():
    parser = argparse.ArgumentParser(description="Greyhound Betting Analysis")
    parser.add_argument(
        "--rainbet",
        action="store_true",
        help="Scrape Rainbet bet history instead of Duel",
    )
    return parser.parse_args()


def normalize_track_name(track_name: str) -> str:
    """Normalize track names before region classification."""
    value = (track_name or "").strip().lower()
    value = re.sub(r'^\d{2}:\d{2}\s+', '', value)
    value = re.sub(r'\s*\([a-z]{2}\)\s*$', '', value)
    value = re.sub(r'\s+', ' ', value)
    return value.strip()


def classify_region(track_name: str) -> str:
    """Classify track into British or Australian using user-provided track list."""
    if normalize_track_name(track_name) in BRITISH_TRACKS:
        return "British"
    return "Australian"


# ── Parse betting log for placed bets ────────────────────────────────────────────
def parse_betting_log() -> list[dict]:
    """
    Parse betting_log.txt to extract all placed bets with their midpoint odds.
    Also tracks the active Sharp race to attach track and region to each bet.
    Returns list of: {timestamp, dog_name, duel_odds, midpoint, stake, edge, track_name, region}
    """
    bets = []
    
    with open(LOG_FILE, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
    
    # Pattern for VALUE line: ** VALUE -> 4. Goldcash Prince @ 1.7  (mid=1.620)
    value_pattern = re.compile(
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+\*\* VALUE -> (.+?) @ ([\d.]+)\s+\(mid=([\d.]+)\)'
    )
    
    # Pattern for BET PLACED line: OK BET PLACED: 2. Ballyhimikin Lee @ 3.7 for EUR1.0
    placed_pattern = re.compile(
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+OK BET PLACED: (.+?) @ ([\d.]+) for EUR([\d.]+)'
    )

    # Pattern for navigation context with track name
    sharp_nav_pattern = re.compile(
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+\[Sharp\]\s+OK Navigated to \d{2}:\d{2}\s+(.+?)(?:\s+\(([A-Z]{2})\))?\s*$'
    )
    nav_switch_pattern = re.compile(
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+\[Nav\]\s+Switching to:\s+\d{2}:\d{2}\s+(.+?)\s+\(([A-Z]{2})\)'
    )
    monitoring_pattern = re.compile(
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+MONITORING:\s+\d{2}:\d{2}\s+(.+?)\s+\(([A-Z]{2})\)'
    )
    
    # Track the last VALUE found to match with BET PLACED
    last_value = None
    current_track = ""
    current_country = ""
    
    for line in lines:
        # Track current race context from navigation/monitoring lines
        nav_match = sharp_nav_pattern.search(line)
        if not nav_match:
            nav_match = nav_switch_pattern.search(line)
        if not nav_match:
            nav_match = monitoring_pattern.search(line)

        if nav_match:
            current_track = nav_match.group(2).strip()
            current_country = (nav_match.group(3) or "").strip().upper()
            continue

        # Check for VALUE line
        value_match = value_pattern.search(line)
        if value_match:
            track_name = current_track
            last_value = {
                'timestamp': value_match.group(1),
                'dog_name': value_match.group(2).strip(),
                'duel_odds': float(value_match.group(3)),
                'midpoint': float(value_match.group(4)),
                'track_name': track_name,
                'track_norm': normalize_track_name(track_name),
                'region': classify_region(track_name),
                'country_code': current_country,
            }
            continue
        
        # Check for BET PLACED line
        placed_match = placed_pattern.search(line)
        if placed_match and last_value:
            timestamp = placed_match.group(1)
            dog_name = placed_match.group(2).strip()
            duel_odds = float(placed_match.group(3))
            stake = float(placed_match.group(4))
            
            # Use the last VALUE's midpoint (same dog name should match)
            # Sometimes names might differ slightly, so we also check timestamp proximity
            bet = {
                'timestamp': timestamp,
                'dog_name': dog_name,
                'duel_odds': duel_odds,
                'midpoint': last_value['midpoint'],
                'stake': stake,
                'edge': (duel_odds / last_value['midpoint'] - 1) * 100,  # Edge %
                'track_name': last_value.get('track_name', ''),
                'track_norm': last_value.get('track_norm', ''),
                'region': last_value.get('region', classify_region('')),
                'country_code': last_value.get('country_code', ''),
            }
            bets.append(bet)
    
    return bets


# ── Scrape Duel bet history ──────────────────────────────────────────────────────
async def scrape_duel_history(page, start_date: datetime = None) -> list[dict]:
    """
    Scrape bet history from Duel.com.
    Returns list of: {timestamp, dog_name, odds, stake, won, payout}
    If start_date is provided, only returns bets from that date onwards.
    """
    history = []
    
    try:
        # Find the right frame/page that contains the bets
        target = page
        for frame in page.frames:
            test_cards = await frame.query_selector_all('div[data-editor-id="bet"]')
            if test_cards:
                target = frame
                print(f"[Scraper] Found bets in frame: {frame.url[:50]}...")
                break
        
        # User should already be on the bet history page
        # Click "Show more" button repeatedly to load all bets
        print("[Scraper] Loading bets (clicking 'Show more')...")
        
        for i in range(100):  # Max 100 clicks
            # Check if oldest visible bet is before start_date - if so, stop loading
            if start_date:
                bet_cards = await target.query_selector_all('div[data-editor-id="bet"]')
                if bet_cards:
                    oldest_date = None
                    # Check the last (oldest) bet card
                    last_card = bet_cards[-1]
                    time_spans = await last_card.query_selector_all('span')
                    for span in time_spans:
                        text = (await span.inner_text()).strip()
                        if re.match(r'[A-Z][a-z]+ \d+, \d{4}, \d+:\d+', text):
                            oldest_date = parse_duel_date(text)
                            if oldest_date and oldest_date < start_date:
                                print(f"[Scraper] Reached bets before {start_date.strftime('%Y-%m-%d')}, stopping")
                                break
                    else:
                        pass  # Continue if no date found
                    # Check if we should break outer loop
                    if oldest_date and oldest_date < start_date:
                        break
            
            # Try to find show more button
            show_more = await target.query_selector('div[data-editor-id="pillButton"]:has-text("Show more")')
            if not show_more:
                show_more = await page.query_selector('div[data-editor-id="pillButton"]:has-text("Show more")')
            if not show_more:
                break  # No more button
            
            try:
                await show_more.scroll_into_view_if_needed()
                await show_more.click()
                await asyncio.sleep(1.5)
                
                if (i + 1) % 10 == 0:
                    count = len(await target.query_selector_all('div[data-editor-id="bet"]'))
                    print(f"[Scraper] Loaded {count} bets so far...")
            except Exception:
                break  # Button disappeared or errored
        
        # Parse each bet card
        bet_cards = await target.query_selector_all('div[data-editor-id="bet"]')
        
        if not bet_cards:
            for frame in page.frames:
                bet_cards = await frame.query_selector_all('div[data-editor-id="bet"]')
                if bet_cards:
                    break
        
        if not bet_cards:
            bet_cards = await page.query_selector_all('div[data-editor-id="bet"]')
        
        print(f"[Scraper] Found {len(bet_cards)} bet cards, parsing...")
        
        parsed = 0
        skipped = 0
        skip_reasons = Counter()
        for i, card in enumerate(bet_cards):
            try:
                bet, reason = await parse_bet_card_fast(card)
                if bet:
                    # Filter by start date if provided
                    if start_date:
                        bet_date = parse_duel_date(bet['timestamp'])
                        if bet_date and bet_date < start_date:
                            skipped += 1
                            skip_reasons["before_start_date"] += 1
                            continue  # Skip bets before start date
                    history.append(bet)
                    parsed += 1
                else:
                    skipped += 1
                    skip_reasons[reason or "unknown"] += 1
                
                # Progress every 50 cards
                if (i + 1) % 50 == 0:
                    print(f"[Scraper] Parsed {i+1}/{len(bet_cards)} cards ({parsed} valid, {skipped} skipped)")
                    
            except Exception as e:
                skipped += 1
                skip_reasons["exception"] += 1
                continue
        
        print(f"[Scraper] Done: {parsed} valid bets, {skipped} skipped")
        if skip_reasons:
            reason_text = ", ".join(f"{k}={v}" for k, v in skip_reasons.most_common(6))
            print(f"[Scraper] Skip reasons: {reason_text}")
        
    except Exception as e:
        print(f"[Scraper] Error scraping history: {e}")
    
    return history


def parse_duel_date(date_str: str) -> datetime | None:
    """Parse Duel date format like 'Apr 3, 2026, 17:07' to datetime."""
    try:
        # Handle formats like "Apr 3, 2026, 17:07" or "Apr 3, 17:09"
        formats = [
            "%b %d, %Y, %H:%M",
            "%b %d, %H:%M",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return None
    except Exception:
        return None


async def parse_bet_card_fast(card) -> dict | None:
    """Parse a single bet card using innerHTML for speed (single DOM call)."""
    try:
        # Get all text content in one call
        html = await card.inner_html()
        text = await card.inner_text()
        
        # Check for Won/Lost status
        if 'betsWonStatus' in html:
            won = True
        elif 'betsLostStatus' in html:
            won = False
        else:
            return None, "pending_or_open"
        
        # Extract timestamp (Apr 8, 2026, 09:26)
        timestamp_match = re.search(r'([A-Z][a-z]+ \d+, \d{4}, \d+:\d+)', text)
        timestamp = timestamp_match.group(1) if timestamp_match else None
        
        # Extract dog name (Winner XYZ)
        winner_match = re.search(r'Winner\s+([A-Za-z][A-Za-z\s\'\-]+?)(?:\n|Total Odds)', text)
        dog_name = winner_match.group(1).strip() if winner_match else None
        
        # Extract odds (Total Odds followed by number)
        odds_match = re.search(r'Total Odds\s*\n?\s*([\d.]+)', text)
        odds = float(odds_match.group(1)) if odds_match else None
        
        # Extract stake (Stake followed by number)
        stake_match = re.search(r'Stake\s*\n?\s*([\d.]+)', text)
        stake = float(stake_match.group(1)) if stake_match else None
        
        missing = []
        if not timestamp:
            missing.append("timestamp")
        if not dog_name:
            missing.append("dog_name")
        if odds is None:
            missing.append("odds")
        if stake is None:
            missing.append("stake")
        if missing:
            return None, f"missing_{'_'.join(missing)}"
        
        payout = odds * stake if won else 0
        
        return {
            'timestamp': timestamp,
            'dog_name': dog_name,
            'odds': odds,
            'stake': stake,
            'won': won,
            'payout': payout
        }, None
        
    except Exception as e:
        return None, f"parse_exception_{type(e).__name__}"


# ── Match bets from log to Duel history ──────────────────────────────────────────
def match_bets(log_bets: list[dict], duel_history: list[dict]) -> tuple[list[dict], dict]:
    """
    Match bets from betting_log.txt to Duel history to get outcomes.
    Returns list with matched bets including 'won' status.
    """
    matched = []
    used_duel_indices = set()
    unmatched = 0
    
    for log_idx, log_bet in enumerate(log_bets):
        # Normalize dog name for matching
        log_name = normalize_name(log_bet['dog_name'])
        log_odds = log_bet['duel_odds']
        log_time = parse_log_datetime(log_bet['timestamp'])
        
        # Find matching bet in Duel history
        best_score = None
        best_duel_idx = None
        best_match = None
        for duel_idx, duel_bet in enumerate(duel_history):
            if duel_idx in used_duel_indices:
                continue

            duel_name = normalize_name(duel_bet['dog_name'])
            odds_diff = abs(log_odds - duel_bet['odds'])
            if odds_diff > 1.0:
                continue
            
            if not names_match(log_name, duel_name):
                continue

            time_penalty = 0
            duel_time = parse_duel_date(duel_bet['timestamp'])
            if log_time and duel_time:
                time_penalty = abs((duel_time - log_time).total_seconds()) / 3600.0

            score = odds_diff + (time_penalty * 0.02)
            if best_score is None or score < best_score:
                best_score = score
                best_match = duel_bet
                best_duel_idx = duel_idx
        
        if best_match:
            used_duel_indices.add(best_duel_idx)
            matched.append({
                **log_bet,
                'won': best_match['won'],
                'payout': best_match['payout'],
                'actual_profit': best_match['payout'] - log_bet['stake'] if best_match['won'] else -log_bet['stake']
            })
        else:
            unmatched += 1
    
    diagnostics = {
        "total_log_bets": len(log_bets),
        "duel_bets": len(duel_history),
        "matched": len(matched),
        "unmatched": unmatched,
        "match_rate_pct": (len(matched) / len(log_bets) * 100) if log_bets else 0.0,
    }
    return matched, diagnostics


def normalize_name(name: str) -> str:
    """Normalize dog name for matching."""
    # Remove leading number and period (e.g., "4. Goldcash Prince" -> "Goldcash Prince")
    name = re.sub(r'^\d+\.\s*', '', name)
    return name.lower().strip()


def parse_log_datetime(date_str: str) -> datetime | None:
    """Parse log timestamp format."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def names_match(name1: str, name2: str) -> bool:
    """Check if two dog names match (fuzzy)."""
    # Simple contains check for now
    return name1 in name2 or name2 in name1 or name1 == name2


def bet_cache_key(bet: dict) -> tuple:
    """Stable key for de-duplicating matched bets in cache."""
    return (
        normalize_name(str(bet.get('dog_name', ''))),
        str(bet.get('timestamp', '')),
        round(float(bet.get('duel_odds', 0.0)), 3),
        round(float(bet.get('stake', 0.0)), 3),
    )


def merge_cached_and_new(cached: list[dict], new_bets: list[dict]) -> tuple[list[dict], int]:
    """Merge new matched bets into cached matched bets (dedupe by stable key)."""
    merged = {}
    for b in cached or []:
        merged[bet_cache_key(b)] = b
    before = len(merged)
    for b in new_bets or []:
        merged[bet_cache_key(b)] = b
    after = len(merged)

    def sort_key(b: dict):
        ts = parse_log_datetime(str(b.get('timestamp', '')))
        return (ts is None, ts or datetime.max)

    merged_list = sorted(merged.values(), key=sort_key)
    return merged_list, max(0, after - before)


def enrich_bets_with_region(bets: list[dict], log_bets: list[dict]) -> list[dict]:
    """Fill track/region fields on cached bets by matching them to parsed log bets."""
    log_by_key = {bet_cache_key(b): b for b in log_bets}
    enriched = []

    for bet in bets or []:
        updated = dict(bet)
        source = log_by_key.get(bet_cache_key(updated))

        if source:
            for key in ('track_name', 'track_norm', 'region', 'country_code'):
                if not updated.get(key):
                    updated[key] = source.get(key)

        if not updated.get('track_norm'):
            updated['track_norm'] = normalize_track_name(str(updated.get('track_name', '')))
        if not updated.get('region'):
            updated['region'] = classify_region(str(updated.get('track_name', '')))

        enriched.append(updated)

    return enriched


def split_bets_by_region(bets: list[dict]) -> dict[str, list[dict]]:
    """Split bets into British and Australian groups."""
    split = {
        'British': [],
        'Australian': [],
    }

    for bet in bets or []:
        region = bet.get('region') or classify_region(str(bet.get('track_name', '')))
        if region not in split:
            region = 'Australian'

        normalized_bet = dict(bet)
        normalized_bet['region'] = region
        split[region].append(normalized_bet)

    return split


# ── Calculate statistics ─────────────────────────────────────────────────────────
def calculate_statistics(bets: list[dict]) -> dict:
    """Calculate expected and actual profit statistics. Only uses matched bets."""
    # Filter to only matched bets (those with won != None)
    matched_bets = [b for b in bets if b.get('won') is not None]
    
    total_stake = 0
    total_expected_profit = 0
    total_actual_profit = 0
    cumulative_expected = []
    cumulative_actual = []
    
    # For variance calculation
    expected_profits = []
    
    for bet in matched_bets:
        stake = bet['stake']
        midpoint = bet['midpoint']
        duel_odds = bet['duel_odds']
        
        # True probability (assuming midpoint is fair)
        p_win = 1 / midpoint
        
        # Expected profit for this bet
        ev = p_win * (duel_odds - 1) * stake - (1 - p_win) * stake
        expected_profits.append({
            'stake': stake,
            'p_win': p_win,
            'odds': duel_odds,
            'ev': ev
        })
        
        total_stake += stake
        total_expected_profit += ev
        
        total_actual_profit += bet['actual_profit']
        
        cumulative_expected.append(total_expected_profit)
        cumulative_actual.append(total_actual_profit)
    
    return {
        'total_bets': len(matched_bets),
        'total_stake': total_stake,
        'expected_profit': total_expected_profit,
        'actual_profit': total_actual_profit,
        'expected_yield': (total_expected_profit / total_stake * 100) if total_stake > 0 else 0,
        'actual_yield': (total_actual_profit / total_stake * 100) if total_stake > 0 else 0,
        'cumulative_expected': cumulative_expected,
        'cumulative_actual': cumulative_actual,
        'bet_details': expected_profits,
        'matched_bets': matched_bets
    }


def z_value_for_interval(level_pct: float) -> float:
    """Convert central interval level (e.g. 95) to a standard normal z-score."""
    p = max(0.001, min(99.999, float(level_pct))) / 100.0
    tail = (1.0 - p) / 2.0
    return NormalDist().inv_cdf(1.0 - tail)


def calculate_confidence_bands(
    bets: list[dict],
    levels: list[int] | None = None,
) -> dict[int, dict[str, list[float]]]:
    """
    Calculate cumulative confidence bands for multiple central intervals.
    Example levels: 50, 55, ..., 95.
    """
    matched_bets = [b for b in bets if b.get('won') is not None]
    if not matched_bets:
        return {}

    levels = levels or CONFIDENCE_LEVELS
    clean_levels = sorted({int(v) for v in levels if 0 < int(v) < 100}, reverse=True)
    if not clean_levels:
        return {}

    cumulative_var = 0.0
    cumulative_ev = 0.0
    cumulative_evs = []
    cumulative_stds = []

    for bet in matched_bets:
        stake = bet['stake']
        midpoint = bet['midpoint']
        duel_odds = bet['duel_odds']

        p = 1 / midpoint
        win_amount = (duel_odds - 1) * stake
        lose_amount = -stake

        ev_single = p * win_amount + (1 - p) * lose_amount
        var_single = p * (1 - p) * (win_amount - lose_amount) ** 2

        cumulative_ev += ev_single
        cumulative_var += var_single

        cumulative_evs.append(cumulative_ev)
        cumulative_stds.append(float(np.sqrt(cumulative_var)))

    bands: dict[int, dict[str, list[float]]] = {}
    for level in clean_levels:
        z = z_value_for_interval(level)
        upper = [ev + z * std for ev, std in zip(cumulative_evs, cumulative_stds)]
        lower = [ev - z * std for ev, std in zip(cumulative_evs, cumulative_stds)]
        bands[level] = {
            'upper': upper,
            'lower': lower,
        }

    return bands


# ── Plotting ─────────────────────────────────────────────────────────────────────
def plot_results(
    stats: dict,
    bets: list[dict],
    confidence_bands: dict[int, dict[str, list[float]]],
    output_filename: str = "bet_analysis.png",
    title: str = "Betting Performance Analysis",
):
    """Create the profit chart with layered confidence interval bands."""
    if not stats['total_bets']:
        print(f"[Plot] Skipping {output_filename} (no matched bets)")
        return

    plt.style.use('dark_background')
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    x = list(range(1, len(stats['cumulative_expected']) + 1))

    # Plot layered confidence interval bands (50% .. 95%)
    if confidence_bands:
        levels = sorted(confidence_bands.keys(), reverse=True)
        min_level = min(levels)
        max_level = max(levels)
        max_alpha = 0.22
        min_alpha = 0.04
        scale = max(1, len(levels) - 1)
        cmap = plt.get_cmap('turbo')
        color_positions = np.linspace(0.12, 0.92, len(levels))
        band_colors = [cmap(pos) for pos in color_positions]

        for idx, level in enumerate(levels):
            band = confidence_bands[level]
            alpha = min_alpha + (idx / scale) * (max_alpha - min_alpha)
            label = f"{min_level}% to {max_level}% interval bands" if idx == 0 else None
            ax.fill_between(
                x,
                band['lower'],
                band['upper'],
                alpha=alpha,
                color=band_colors[idx],
                label=label,
            )
    
    # Plot expected profit (CLV)
    ax.plot(x, stats['cumulative_expected'], color='cyan', linewidth=1.5, label='Expected Profit (CLV)')
    
    # Plot actual profit
    ax.plot(x, stats['cumulative_actual'], color='#00FF00', linewidth=2, label='Actual Profit')
    
    # Styling
    ax.set_xlabel('Bet Number', fontsize=12, color='white')
    ax.set_ylabel('Cumulative Profit ($)', fontsize=12, color='white')
    ax.set_title(title, fontsize=16, color='white', fontweight='bold')
    
    # Add summary stats as text
    summary = (
        f"Total Bets: {stats['total_bets']}\n"
        f"Total Stake: ${stats['total_stake']:.2f}\n"
        f"Actual Profit: ${stats['actual_profit']:.2f}\n"
        f"Expected Profit: ${stats['expected_profit']:.2f}\n"
        f"Actual Yield: {stats['actual_yield']:.2f}%\n"
        f"Expected Yield: {stats['expected_yield']:.2f}%"
    )
    
    # Add text box
    props = dict(boxstyle='round', facecolor='#1a1a1a', alpha=0.9, edgecolor='#333')
    ax.text(0.02, 0.98, summary, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', bbox=props, color='white', family='monospace')
    
    # Headline stats
    actual_profit = stats['actual_profit']
    expected_yield = stats['expected_yield']
    
    fig.text(0.15, 0.95, f"${actual_profit:,.2f}", fontsize=28, color='white', 
             fontweight='bold', transform=fig.transFigure)
    fig.text(0.75, 0.95, f"{expected_yield:.2f}% YIELD", fontsize=16, color='cyan',
             fontweight='bold', transform=fig.transFigure)
    
    ax.legend(loc='lower right', fontsize=10)
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_facecolor('#0a0a0a')
    fig.patch.set_facecolor('#0a0a0a')
    
    # Zero line
    ax.axhline(y=0, color='white', linestyle='-', alpha=0.3, linewidth=0.5)
    
    plt.tight_layout()
    plt.subplots_adjust(top=0.9)
    
    # Save and show
    output_path = Path(__file__).parent / output_filename
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='#0a0a0a')
    print(f"[Plot] Saved to {output_path}")
    
    plt.show()


# ── Save/Load cache ──────────────────────────────────────────────────────────────
def save_cache(bets: list[dict]):
    """Save matched bets to cache file."""
    with open(BETS_CACHE_FILE, 'w') as f:
        json.dump(bets, f, indent=2, default=str)
    print(f"[Cache] Saved {len(bets)} bets to {BETS_CACHE_FILE}")


def load_cache() -> list[dict] | None:
    """Load bets from cache if exists."""
    if BETS_CACHE_FILE.exists():
        with open(BETS_CACHE_FILE, 'r') as f:
            return json.load(f)
    return None


# ── Main ─────────────────────────────────────────────────────────────────────────
async def main(use_rainbet: bool = False):
    print("=" * 60)
    print("  Greyhound Betting Analysis")
    print("=" * 60)
    
    # Step 1: Parse betting log
    print("\n[1] Parsing betting_log.txt...")
    log_bets = parse_betting_log()
    print(f"    Found {len(log_bets)} placed bets in log")
    
    if not log_bets:
        print("    No bets found in log!")
        return
    
    # Step 2: Check cache mode
    cached = load_cache()
    mode = "full"
    
    if cached and len(cached) > 0:
        print(f"\n[2] Found {len(cached)} cached bets")
        print("    Choose mode:")
        print("      [1] Use cache only (no scraping)")
        print("      [2] Incremental update (cache + scrape only newer period)")
        print("      [3] Full rescrape and rebuild cache")
        mode_input = input("    Mode (1/2/3, default 2): ").strip() or "2"
        mode = {"1": "cache", "2": "incremental", "3": "full"}.get(mode_input, "incremental")
    else:
        mode = "full"
    
    if mode == "cache":
        matched_bets = cached
    else:
        # Ask for start date
        print("\n[2] Date filter setup")
        default_date = ""
        if mode == "incremental" and cached:
            latest_cached = None
            for b in cached:
                ts = parse_log_datetime(str(b.get("timestamp", "")))
                if ts and (latest_cached is None or ts > latest_cached):
                    latest_cached = ts
            if latest_cached:
                default_date = latest_cached.strftime("%Y-%m-%d")
                print(f"    Suggested start date from cache: {default_date}")

        prompt = "    From what date? (YYYY-MM-DD or press ENTER for all): "
        if default_date:
            prompt = f"    From what date? (YYYY-MM-DD, ENTER for {default_date}): "
        date_input = input(prompt).strip()
        if not date_input and default_date:
            date_input = default_date

        start_date = None
        if date_input:
            try:
                start_date = datetime.strptime(date_input, "%Y-%m-%d")
                print(f"    Will filter bets from {start_date.strftime('%Y-%m-%d')} onwards")
            except ValueError:
                print("    Invalid date format, scraping all bets")
        
        site_label = "Rainbet" if use_rainbet else "Duel"
        bet_history_url = (
            "https://rainbet.com/sportsbook/bets"
            if use_rainbet
            else "https://duel.com/sports?bt-path=/bets"
        )
        captcha_steps = 2 if use_rainbet else 1

        print("\n[3] Launching browser...")
        print(f"    A new browser window will open. Please log into {site_label}.")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=False,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-first-run',
                    '--window-size=1400,900',
                ],
            )
            
            context = await browser.new_context(
                viewport={'width': 1380, 'height': 800},
            )
            
            page = await context.new_page()
            await page.goto(bet_history_url, wait_until='domcontentloaded')

            for step in range(1, captcha_steps + 1):
                input(f"\n    [Step {step}] Press ENTER to pass the anti-robot check...")
                await asyncio.sleep(1)

            input(f"\n    [Step {captcha_steps + 1}] Now log into {site_label} and navigate to bet history.")
            input("             Press ENTER when you're on the bet history page...")

            print(f"\n    Scraping {site_label} bet history...")
            duel_history = await scrape_duel_history(page, start_date)
            print(f"    Found {len(duel_history)} bets in {site_label} history")
            
            # Match bets
            print("\n[4] Matching bets...")
            new_matched_bets, match_diag = match_bets(log_bets, duel_history)
            print(
                f"    Matched {match_diag['matched']}/{match_diag['total_log_bets']} bets "
                f"({match_diag['match_rate_pct']:.1f}%); unmatched={match_diag['unmatched']}"
            )

            if mode == "incremental" and cached:
                matched_bets, newly_added = merge_cached_and_new(cached, new_matched_bets)
                print(f"    Incremental merge: +{newly_added} new bets, total cached now {len(matched_bets)}")
            else:
                matched_bets = new_matched_bets
            
            # Save cache (full or merged incremental)
            save_cache(matched_bets)
            
            await browser.close()

    # Ensure region fields are available for both fresh and cached paths
    matched_bets = enrich_bets_with_region(matched_bets, log_bets)
    save_cache(matched_bets)

    # Step 3: Calculate overall statistics and regenerate main chart
    print("\n[3] Calculating overall statistics...")
    overall_stats = calculate_statistics(matched_bets)

    print(f"\n{'='*60}")
    print("  RESULTS - ALL RACES")
    print(f"{'='*60}")
    print(f"  Total Bets:      {overall_stats['total_bets']}")
    print(f"  Total Stake:     ${overall_stats['total_stake']:.2f}")
    print(f"  Actual Profit:   ${overall_stats['actual_profit']:.2f}")
    print(f"  Expected Profit: ${overall_stats['expected_profit']:.2f}")
    print(f"  Actual Yield:    {overall_stats['actual_yield']:.2f}%")
    print(f"  Expected Yield:  {overall_stats['expected_yield']:.2f}%")
    print(f"{'='*60}")

    overall_confidence_bands = calculate_confidence_bands(matched_bets)
    print("\n[4] Generating main plot (all races)...")
    plot_results(
        overall_stats,
        matched_bets,
        overall_confidence_bands,
        output_filename="bet_analysis.png",
        title="Betting Performance Analysis (All Races)",
    )
    
    # Step 5: Calculate statistics by region
    print("\n[5] Calculating statistics by region...")
    bets_by_region = split_bets_by_region(matched_bets)

    for region_name, output_filename in [
        ("British", "bet_analysis_british.png"),
        ("Australian", "bet_analysis_australian.png"),
    ]:
        region_bets = bets_by_region[region_name]
        stats = calculate_statistics(region_bets)

        print(f"\n{'='*60}")
        print(f"  RESULTS - {region_name.upper()} RACES")
        print(f"{'='*60}")
        print(f"  Total Bets:      {stats['total_bets']}")
        print(f"  Total Stake:     ${stats['total_stake']:.2f}")
        print(f"  Actual Profit:   ${stats['actual_profit']:.2f}")
        print(f"  Expected Profit: ${stats['expected_profit']:.2f}")
        print(f"  Actual Yield:    {stats['actual_yield']:.2f}%")
        print(f"  Expected Yield:  {stats['expected_yield']:.2f}%")
        print(f"{'='*60}")

        confidence_bands = calculate_confidence_bands(region_bets)
        print(f"\n[6] Generating plot for {region_name} races...")
        plot_results(
            stats,
            region_bets,
            confidence_bands,
            output_filename=output_filename,
            title=f"Betting Performance Analysis ({region_name} Races)",
        )


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args.rainbet))
