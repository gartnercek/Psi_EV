"""
Horse Racing Betting Bot — Full Auto Navigation
────────────────────────────────────────────────
Automatically finds matching horse races on Sharpexchange + Duel.com,
navigates both sites, computes no-vig fair odds from Sharp back prices,
and places bets on Duel when value is found.

Setup:
    pip install playwright
    playwright install chromium
    python horse_bot.py

On startup:
    - Navigate Sharp to the Horse Racing section (left panel)
    - Duel will be handled automatically from there
    - Press ENTER and the bot runs fully automatically
"""

import vpn_asyncio  # noqa: F401  — Proton blocks 127.0.0.1 socketpair
import argparse
import asyncio
import logging
import math
import os
import random
import re
from datetime import datetime, timedelta
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ── Configuration ───────────────────────────────────────────────────────────────
POLL_INTERVAL          = 5     # seconds between odds checks during a race
NAV_CHECK_INTERVAL     = 20    # seconds between race-finding attempts
RACE_WINDOW_MINUTES    = 15.1   # start monitoring a race this many minutes before start
LATE_WINDOW_MINUTES    = float(os.getenv('HORSE_LATE_WINDOW_MINUTES', '20'))  # monitoring window after scheduled start; does NOT imply in-play
PRIME_WINDOW_MIN       = 1     # prime betting window starts at this many minutes
PRIME_WINDOW_MAX       = 5     # prime betting window ends at this many minutes
FLAT_STAKE             = 3.0   # euros per bet
MIN_VALUE_EDGE_PCT     = 3.0   # minimum perceived value edge required before betting
MAX_CONCURRENT_SESSIONS = int(os.getenv('HORSE_MAX_CONCURRENT_SESSIONS', '3'))  # parallel Sharp+Duel session pairs
SESSION_START_STAGGER  = 4.0   # seconds between starting each session loop
POLL_JITTER_MAX        = 1.2   # random sleep jitter to avoid robotic cadence
ODDS_DRIFT_TOL         = 0.05  # abort bet if slip odds differ more than this
DEBUG_DUMP_HTML        = os.getenv('HORSE_DEBUG_DUMP_HTML', '0').strip().lower() in ('1', 'true', 'yes', 'on')
NO_VIG_METHOD          = os.getenv('HORSE_NOVIG_METHOD', 'power').strip().lower()
MIN_SHARP_MATCHED_EUR  = float(os.getenv('HORSE_MIN_SHARP_MATCHED_EUR', '5000'))
LOW_MATCHED_COOLDOWN_SECONDS = float(os.getenv('HORSE_LOW_MATCHED_COOLDOWN_SECONDS', '90'))
LOW_MATCHED_WAIT_SECONDS = float(os.getenv('HORSE_LOW_MATCHED_WAIT_SECONDS', '60'))
LOW_MATCHED_RETRY_INTERVAL = float(os.getenv('HORSE_LOW_MATCHED_RETRY_INTERVAL', '5'))
IN_PLAY_BLOCK_SECONDS  = float(os.getenv('HORSE_IN_PLAY_BLOCK_SECONDS', '1800'))
NODE_HEAP_MB            = int(os.getenv('HORSE_NODE_HEAP_MB', '6144'))
BLOCK_RESOURCE_TYPES    = {
    v.strip().lower()
    for v in os.getenv('HORSE_BLOCK_RESOURCE_TYPES', 'image,media').split(',')
    if v.strip()
}

_node_opts = os.environ.get('NODE_OPTIONS', '')
if '--max-old-space-size' not in _node_opts:
    extra = f"--max-old-space-size={NODE_HEAP_MB}"
    os.environ['NODE_OPTIONS'] = f"{_node_opts} {extra}".strip()

# Duel horse paths can vary by deployment.
DUEL_SPORT_PATHS = ['/horse-racing', '/horse']

SHIN_MAX_ITERATIONS = 1000
SHIN_CONVERGENCE_THRESHOLD = 1e-12

# Duel class names — update if site deploys break scraping
DUEL_NAME_CLASS = 'bt311'
DUEL_ODDS_CLASS = 'bt329'

SHARP_ROW_SELECTORS = [
    'div.runnerRow',
    'div[class*="runnerRow"]',
    'li[class*="runnerRow"]',
    'li[class*="runner"]',
    'tr[class*="runner"]',
    'tr[data-auto*="runner"]',
    '[data-auto*="runnerRow"]',
]
SHARP_NAME_SELECTORS = [
    'span[data-auto="runner_name"]',
    '[data-auto="runner_name"]',
    'span[class*="runnerName"]',
    'div[class*="runnerName"]',
    '[data-auto*="runnerName"]',
    '[data-auto*="selectionName"]',
    'span[class*="selectionName"]',
    'div[class*="selectionName"]',
]
SHARP_BACK_SELECTORS = [
    'button.biab_back-0 span.biab_bet-odds',
    'button[class*="back"] span.biab_bet-odds',
    'button[class*="back"] span[class*="odds"]',
    '[data-auto*="back"] span[class*="odds"]',
    '[data-auto*="back"] span',
    '[class*="back"] [class*="odds"]',
    '[data-auto*="price"]',
    'button[class*="back"]',
    '[data-auto*="back"]',
]
RUNNER_PREFIX_RE = re.compile(r'^\s*(\d{1,2})\s*[\.\-\)]?\s*(.+?)\s*$')

# ── Logging ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(message)s',
    handlers=[
        logging.FileHandler('horse_betting_log.txt'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger()

# Track if we've already dumped HTML this session (only dump once)
_html_dumped = {'sharp': False, 'duel': False}


async def dump_debug_html(page, site_name: str):
    """Dump all frame HTML to files for debugging."""
    if not DEBUG_DUMP_HTML or _html_dumped.get(site_name):
        return
    
    try:
        timestamp = datetime.now().strftime('%H%M%S')
        
        # Dump main page
        main_html = await page.content()
        main_file = f'debug_{site_name}_main_{timestamp}.html'
        with open(main_file, 'w', encoding='utf-8') as f:
            f.write(main_html)
        log.info(f"  [DEBUG] Saved {main_file} ({len(main_html)} bytes)")
        
        # Dump each frame
        for i, frame in enumerate(page.frames):
            if frame == page.main_frame:
                continue
            try:
                frame_html = await frame.content()
                if len(frame_html) > 500:  # Skip tiny frames
                    frame_file = f'debug_{site_name}_frame{i}_{timestamp}.html'
                    with open(frame_file, 'w', encoding='utf-8') as f:
                        f.write(frame_html)
                    log.info(f"  [DEBUG] Saved {frame_file} ({len(frame_html)} bytes)")
            except Exception as e:
                log.info(f"  [DEBUG] Frame {i} not accessible: {e}")
        
        _html_dumped[site_name] = True
        log.info(f"  [DEBUG] HTML dump complete for {site_name}")
        
    except Exception as e:
        log.error(f"  [DEBUG] HTML dump error: {e}")


def parse_args():
    parser = argparse.ArgumentParser(description="Horse Racing Betting Bot")
    parser.add_argument(
        "--proxy",
        "--vpn",
        dest="proxy_server",
        help="Proxy server URL, e.g. http://host:port or socks5://host:port",
    )
    parser.add_argument("--proxy-user", dest="proxy_user", help="Proxy username (optional)")
    parser.add_argument("--proxy-pass", dest="proxy_pass", help="Proxy password (optional)")
    return parser.parse_args()


# ── Track name normaliser ────────────────────────────────────────────────────────
def normalise_track(name: str) -> str:
    """
    Makes track names comparable across sites.
    '18:12 Sheffield (GB)' → 'sheffield'
    'Sheffield 2'          → 'sheffield'
    'The Valley'           → 'valley'
    'Valley (GB)'          → 'valley'
    'Dunstall Park (G)'    → 'dunstall park'
    """
    name = re.sub(r'^\d{2}:\d{2}\s+', '', name)       # remove time prefix
    name = re.sub(r'\s*\([A-Z]{1,3}\)\s*$', '', name)  # remove (G), (GB), (AU) etc.
    name = re.sub(r'\s+\d+$', '', name)                 # remove trailing number
    name = re.sub(r'\s+[A-Z]$', '', name)               # remove trailing single letter (G, etc.)
    name = re.sub(r'^the\s+', '', name, flags=re.IGNORECASE)  # remove leading "The "
    return name.strip().lower()


def track_match_key(name: str) -> str:
    """Build a looser key for cross-site track matching (e.g. Mountaineer vs Mountaineer Park)."""
    key = normalise_track(name)
    key = re.sub(r'\b(?:park|racecourse|racetrack|race\s*track|raceway)$', '', key).strip()
    key = re.sub(r'\s+', ' ', key)
    return key


def normalise_dog_name(name: str) -> str:
    """
    Normalise dog names for comparison.
    '1. Droppys Typical' → 'droppys typical'
    '2. Ballyhimikin Lee' → 'ballyhimikin lee'
    """
    # Remove leading number and dot
    name = re.sub(r'^\d+\.\s*', '', name)
    return name.strip().lower()


def dogs_match(sharp_name: str, duel_name: str) -> bool:
    """Check if two dog names are the same (fuzzy match)."""
    s = normalise_dog_name(sharp_name)
    d = normalise_dog_name(duel_name)
    # Exact match or one contains the other (handles truncation)
    return s == d or s in d or d in s


def _extract_runner_number_and_name(name_text: str) -> tuple[str | None, str]:
    text = re.sub(r'\s+', ' ', (name_text or '')).strip()
    if not text:
        return None, ''

    match = RUNNER_PREFIX_RE.match(text)
    if not match:
        return None, text

    num = match.group(1)
    horse_name = match.group(2).strip()
    return num, f"{num}. {horse_name}" if horse_name else num


def _runner_key(num: str | None, name: str) -> str | None:
    """Use numeric cloth number when available; otherwise key by normalized horse name."""
    if num:
        return str(num)

    norm = normalise_dog_name(name or '')
    if not norm:
        return None
    return f"name:{norm}"


def _extract_decimal_odds(odds_text: str) -> float | None:
    if not odds_text:
        return None

    text = odds_text.replace(',', '.')

    # Remove visible currency/liquidity amounts so they are not mistaken for odds.
    text = re.sub(r'[€$£]\s*\d+(?:\.\d+)?', ' ', text)

    values = []
    for m in re.finditer(r'\d+(?:\.\d+)?', text):
        try:
            value = float(m.group(0))
        except ValueError:
            continue
        if value > 1.0:
            values.append(value)

    if not values:
        return None

    decimal_values = [v for v in values if not float(v).is_integer()]
    if decimal_values:
        return decimal_values[0]

    return values[0]


def _extract_runner_name_from_row_text(row_text: str) -> str:
    lines = [re.sub(r'\s+', ' ', l).strip() for l in (row_text or '').split('\n')]
    lines = [l for l in lines if l]

    for candidate in lines:
        if re.match(r'^\d{1,2}\s*[\.\-\)]\s+\S', candidate):
            return candidate
        if re.match(r'^\d{1,2}\s+\S', candidate):
            return candidate

    for idx, line in enumerate(lines):
        if not re.fullmatch(r'\d{1,2}', line):
            continue
        for follow in lines[idx + 1: idx + 5]:
            if re.search(r'[A-Za-z]', follow) and not re.fullmatch(r'\d+(?:\.\d+)?', follow):
                return f"{line}. {follow}"

    # Final fallback for layouts that show plain horse names without cloth numbers.
    for candidate in lines:
        c = candidate.lower()
        if not re.search(r'[a-z]', c):
            continue
        if re.match(r'^\d+(?:\.\d+)?$', c):
            continue
        if '€' in c or '$' in c or '£' in c:
            continue
        return candidate

    return ''


async def _extract_back_from_row(row) -> float | None:
    for back_selector in SHARP_BACK_SELECTORS:
        try:
            back_el = await row.query_selector(back_selector)
        except Exception:
            back_el = None
        if not back_el:
            continue

        try:
            back_text = (await back_el.inner_text()).strip()
        except Exception:
            back_text = ''

        back = _extract_decimal_odds(back_text)
        if back is not None:
            return back

    # Fallback for DOM variants where classes differ but odds are still visible.
    generic_selectors = [
        'button',
        '[role="button"]',
        'span[class*="odds"]',
        '[data-auto*="odds"]',
        '[data-auto*="price"]',
    ]

    candidates = []
    for selector in generic_selectors:
        try:
            candidates.extend(await row.query_selector_all(selector))
        except Exception:
            continue

    best = None
    best_score = -10**9
    seen = set()
    for el in candidates:
        try:
            txt = (await el.inner_text()).strip()
        except Exception:
            continue

        if not txt:
            continue

        odds = _extract_decimal_odds(txt)
        if odds is None:
            continue

        key = (txt, f"{odds:.3f}")
        if key in seen:
            continue
        seen.add(key)

        cls = ((await el.get_attribute('class')) or '').lower()
        data_auto = ((await el.get_attribute('data-auto')) or '').lower()
        attrs = f"{cls} {data_auto}"

        score = 0
        if 'lay' in attrs:
            score -= 20
        if 'back' in attrs:
            score += 30
        if 'odds' in attrs or 'price' in attrs:
            score += 10
        if attrs.strip() == '':
            score -= 3

        # Slightly prefer smaller prices in tie situations to avoid accidental totals/stakes.
        score -= int(min(odds, 50))

        if score > best_score:
            best_score = score
            best = odds

    return best


async def _extract_back_prices_from_target(target) -> list[float]:
    """Extract Sharp back prices in DOM order from back elements."""
    button_selectors = [
        'button.biab_back-0',
        'button.biab_back-cell.biab_back-0',
        'button.biab_bet-back.biab_back-0',
        'button.biab_back-cell',
        'button[class*="back"][class*="-0"]',
        'button[class*="back"]',
    ]

    for selector in button_selectors:
        try:
            buttons = await target.query_selector_all(selector)
        except Exception:
            buttons = []

        if not buttons:
            continue

        prices = []
        seen = set()
        for button in buttons:
            try:
                odds_el = await button.query_selector('span.biab_bet-odds, span[class*="betOdds"], span[class*="odds"]')
                if odds_el:
                    txt = (await odds_el.inner_text()).strip()
                else:
                    txt = (await button.inner_text()).strip()
            except Exception:
                continue

            odd = _extract_decimal_odds(txt)
            if odd is None:
                continue

            cell_id = (await button.get_attribute('data-cell-id')) or ''
            key = cell_id or f"{len(prices)}:{odd:.3f}"
            if key in seen:
                continue
            seen.add(key)

            prices.append(odd)

        if prices:
            return prices

    span_selectors = [
        'div.biab_bet-content span.biab_bet-odds',
        'button.biab_back-0 span.biab_bet-odds',
        '.biab_back-cell .biab_bet-odds',
        'button[class*="back"] span.biab_bet-odds',
        'span.biab_bet-odds',
        'span[class*="betOdds"]',
    ]

    for selector in span_selectors:
        try:
            spans = await target.query_selector_all(selector)
        except Exception:
            spans = []

        if not spans:
            continue

        prices = []
        for span in spans:
            try:
                txt = (await span.inner_text()).strip()
            except Exception:
                continue

            odd = _extract_decimal_odds(txt)
            if odd is None:
                continue

            try:
                context = await span.evaluate(
                    """el => {
                        const back = el.closest('button[class*="back"], .biab_back-cell, [class*="back-cell"], [data-auto*="back"]');
                        const lay = el.closest('button[class*="lay"], .biab_lay-cell, [class*="lay-cell"], [data-auto*="lay"]');
                        const ref = back || lay || el.closest('button,[role="button"],div');
                        const cls = ref && ref.className ? String(ref.className).toLowerCase() : '';
                        return { hasBack: !!back, hasLay: !!lay, classes: cls };
                    }"""
                )
            except Exception:
                context = {'hasBack': False, 'hasLay': False, 'classes': ''}

            container_classes = context.get('classes', '')
            has_back = bool(context.get('hasBack'))
            has_lay = bool(context.get('hasLay'))

            if has_lay and not has_back:
                continue

            if not has_back and 'back' not in container_classes and selector not in (
                'span.biab_bet-odds',
                'span[class*="betOdds"]',
                'div.biab_bet-content span.biab_bet-odds',
            ):
                continue

            prices.append(odd)

        if prices:
            return prices

    # Last resort: traverse open shadow roots and collect back-button prices.
    try:
        shadow_prices = await target.evaluate("""() => {
            const results = [];
            const seen = new Set();
            const roots = [document];

            while (roots.length) {
                const root = roots.pop();
                if (!root || typeof root.querySelectorAll !== 'function') continue;

                const all = root.querySelectorAll('*');
                for (const node of all) {
                    if (node && node.shadowRoot) roots.push(node.shadowRoot);
                }

                const buttons = root.querySelectorAll('button');
                for (const btn of buttons) {
                    const cls = (btn.className || '').toLowerCase();
                    if (!cls.includes('back')) continue;
                    if (cls.includes('lay') && !cls.includes('back')) continue;

                    const oddsEl = btn.querySelector('span.biab_bet-odds, span[class*="betOdds"], span[class*="odds"]');
                    let txt = (oddsEl ? oddsEl.textContent : btn.textContent || '').replace(',', '.');
                    txt = txt.replace(/[€$£]\\s*\\d+(?:\\.\\d+)?/g, ' ');
                    const matches = txt.match(/\\d+(?:\\.\\d+)?/g) || [];

                    let val = null;
                    for (const m of matches) {
                        const v = parseFloat(m);
                        if (Number.isFinite(v) && v > 1) {
                            val = v;
                            if (m.includes('.')) break;
                        }
                    }
                    if (val === null) continue;

                    const id = btn.getAttribute('data-cell-id') || `${val.toFixed(3)}:${results.length}`;
                    if (seen.has(id)) continue;
                    seen.add(id);
                    results.push(val);
                }
            }

            return results;
        }""")
    except Exception:
        shadow_prices = []

    if shadow_prices:
        return [float(v) for v in shadow_prices if isinstance(v, (int, float)) and float(v) > 1.0]

    return []


def _get_sharp_targets(page, preferred_target):
    targets = [preferred_target]
    if preferred_target is not page:
        targets.append(page)

    for frame in page.frames:
        if frame not in targets:
            targets.append(frame)

    return targets


async def _get_best_sharp_rows(target) -> list:
    best_rows = []
    for selector in SHARP_ROW_SELECTORS:
        try:
            rows = await target.query_selector_all(selector)
        except Exception:
            continue

        if len(rows) > len(best_rows):
            best_rows = rows

    return best_rows


async def _get_primary_sharp_market_target(page, frame):
    """Pick the target that most likely contains the currently open market rows."""
    best_target = None
    best_count = -1

    for target in _get_sharp_targets(page, frame):
        rows = await _get_best_sharp_rows(target)
        count = len(rows)
        if count > best_count:
            best_count = count
            best_target = target

    return best_target or frame


async def _element_is_visible(el) -> bool:
    try:
        return bool(await el.evaluate("""node => {
            if (!node) return false;
            const style = window.getComputedStyle(node);
            if (!style) return false;
            if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity || '1') === 0) {
                return false;
            }
            const rect = node.getBoundingClientRect();
            return rect.width > 0 && rect.height > 0;
        }"""))
    except Exception:
        return False


async def _block_heavy_resources(route, request):
    if request.resource_type in BLOCK_RESOURCE_TYPES:
        await route.abort()
    else:
        await route.continue_()


def _parse_money_amount(text: str) -> float | None:
    """Parse strings like 'EUR 8,580' into a float value."""
    if not text:
        return None

    cleaned = text.replace('\xa0', ' ').strip()
    m = re.search(r'(\d[\d\.,\s]*)', cleaned)
    if not m:
        return None

    raw = re.sub(r'\s+', '', m.group(1))
    if not raw:
        return None

    if ',' in raw and '.' in raw:
        if raw.rfind(',') > raw.rfind('.'):
            raw = raw.replace('.', '').replace(',', '.')
        else:
            raw = raw.replace(',', '')
    elif ',' in raw:
        parts = raw.split(',')
        if len(parts) == 2 and len(parts[1]) in (1, 2):
            raw = f"{parts[0]}.{parts[1]}"
        else:
            raw = ''.join(parts)
    elif '.' in raw:
        parts = raw.split('.')
        if len(parts) == 2 and len(parts[1]) in (1, 2):
            raw = f"{parts[0]}.{parts[1]}"
        elif len(parts) > 2:
            raw = ''.join(parts)

    try:
        value = float(raw)
        return value if value >= 0 else None
    except ValueError:
        return None


async def get_sharp_market_state(page) -> dict:
    """Read market-level state from Sharp.

    in_play is set only from explicit market-status UI markers, never from race time.
    """
    state = {
        'matched_eur': None,
        'matched_text': '',
        'in_play': False,
        'in_play_text': '',
        'in_play_source': '',
    }

    matched_selectors = [
        'span[class*="matchedTotal"]',
        'div[class*="marketDetailInfo"][class*="matched"]',
        'div[class*="matched"]',
    ]
    in_play_selectors = [
        'div[class*="_inPlay_"] .biab_in-play-active:has-text("In-Play")',
        'div[class*="_inPlay_"] .biab_in-play-active:has-text("In Play")',
        'div[class*="_inPlay_"] span.biab_in-play-icon.biab_event-inplay.biab_active[title*="In-Play"]',
        'div[class*="_inPlay_"] span.biab_in-play-icon.biab_event-inplay.biab_active[title*="In Play"]',
        'span.biab_in-play-icon.biab_event-inplay.biab_active[title*="In-Play"]',
        'span.biab_in-play-icon.biab_event-inplay.biab_active[title*="In Play"]',
    ]

    try:
        frame = await get_sharp_frame(page)
        market_target = await _get_primary_sharp_market_target(page, frame)
        matched_values = []

        for target in (market_target,):
            matched_contexts = []
            for selector in matched_selectors:
                try:
                    elements = await target.query_selector_all(selector)
                except Exception:
                    elements = []

                for el in elements[:10]:
                    try:
                        txt = (await el.inner_text()).strip()
                    except Exception:
                        continue

                    lowered = txt.lower()
                    if 'matched' not in lowered and 'eur' not in lowered:
                        continue

                    amount = _parse_money_amount(txt)
                    if amount is None:
                        continue

                    matched_values.append(amount)
                    if not state['matched_text']:
                        state['matched_text'] = txt

                    # Scope in-play probing to the market detail area around matched info.
                    try:
                        ctx_handle = await el.evaluate_handle(
                            """node => {
                                const primary = node.closest(
                                    'div[class*="marketDetailInfo"], div[class*="market-tv"], div[class*="marketHeader"], div[class*="market-detail"], section, article'
                                );
                                return primary || node.parentElement || node;
                            }"""
                        )
                        ctx_el = ctx_handle.as_element()
                    except Exception:
                        ctx_el = None

                    if ctx_el:
                        matched_contexts.append(ctx_el)
                        try:
                            parent_handle = await ctx_el.evaluate_handle('node => node.parentElement')
                            parent_el = parent_handle.as_element()
                        except Exception:
                            parent_el = None
                        if parent_el:
                            matched_contexts.append(parent_el)

            scopes = matched_contexts[:8] if matched_contexts else [target]
            for scope in scopes:
                for selector in in_play_selectors:
                    try:
                        in_play_elements = await scope.query_selector_all(selector)
                    except Exception:
                        in_play_elements = []

                    for in_play_el in in_play_elements[:8]:
                        if not await _element_is_visible(in_play_el):
                            continue

                        txt = ''
                        title = ''
                        try:
                            txt = (await in_play_el.inner_text()).strip()
                        except Exception:
                            txt = ''

                        try:
                            title = ((await in_play_el.get_attribute('title')) or '').strip()
                        except Exception:
                            title = ''

                        lowered = f"{txt} {title}".lower()
                        if 'in play' not in lowered and 'in-play' not in lowered:
                            continue

                        state['in_play'] = True
                        state['in_play_text'] = txt or title
                        state['in_play_source'] = selector
                        break

                    if state['in_play']:
                        break

                if state['in_play']:
                    break

            if state['in_play'] and matched_values:
                break

        if matched_values:
            state['matched_eur'] = max(matched_values)

    except Exception as e:
        log.debug(f"[Sharp] market-state probe error: {e}")

    return state


async def wait_for_sharp_liquidity(sharp_page, race_label: str, min_matched: float = MIN_SHARP_MATCHED_EUR) -> tuple[str, dict]:
    """Wait briefly for Sharp matched liquidity to reach the minimum threshold.

    Returns (status, state) where status is one of: ready, in_play, timeout.
    """
    deadline = datetime.now() + timedelta(seconds=max(1.0, LOW_MATCHED_WAIT_SECONDS))
    last_state = {
        'matched_eur': None,
        'matched_text': '',
        'in_play': False,
        'in_play_text': '',
        'in_play_source': '',
    }

    while True:
        last_state = await get_sharp_market_state(sharp_page)
        matched_eur = last_state.get('matched_eur')

        if matched_eur is not None and matched_eur >= min_matched:
            return 'ready', last_state

        if last_state.get('in_play'):
            return 'in_play', last_state

        remaining = (deadline - datetime.now()).total_seconds()
        if remaining <= 0:
            return 'timeout', last_state

        pretty = 'unknown' if matched_eur is None else f"EUR {matched_eur:,.0f}"
        log.info(
            f"  [{race_label}] Waiting for Sharp liquidity... matched={pretty}, "
            f"target=EUR {min_matched:,.0f}, remaining={remaining:.0f}s"
        )
        await asyncio.sleep(min(LOW_MATCHED_RETRY_INTERVAL, max(0.5, remaining)))


def parse_race_time(time_str: str) -> datetime:
    """
    Convert 'HH:MM' to today's datetime (or tomorrow if time is earlier than now,
    indicating a race after midnight).
    """
    now = datetime.now()
    t = datetime.strptime(time_str, '%H:%M').replace(
        year=now.year, month=now.month, day=now.day
    )
    if t < now - timedelta(hours=1):   # race is post-midnight, add a day
        t += timedelta(days=1)
    return t


def minutes_until(race_dt: datetime) -> float:
    return (race_dt - datetime.now()).total_seconds() / 60


def _normalise_probabilities(values: list[float]) -> list[float]:
    total = sum(v for v in values if v > 0)
    if total <= 0:
        return [1.0 / len(values)] * len(values) if values else []
    return [max(0.0, v) / total for v in values]


def implied_probabilities_power(odds: list[float]) -> tuple[list[float], dict]:
    """
    Power method: find exponent k such that sum((1/odds_i)^k)=1,
    then use resulting probabilities as no-vig probabilities.
    """
    inv = [1.0 / o for o in odds if o > 1.0]
    if not inv:
        return [], {'method': 'power', 'k': None}

    if len(inv) == 1:
        return [1.0], {'method': 'power', 'k': 1.0}

    if sum(inv) <= 1.0:
        probs = _normalise_probabilities(inv)
        return probs, {'method': 'power', 'k': 1.0}

    def f(exp: float) -> float:
        return sum(p ** exp for p in inv) - 1.0

    lo = 1.0
    hi = 1.0
    while f(hi) > 0 and hi < 256.0:
        hi *= 2.0

    for _ in range(80):
        mid = (lo + hi) / 2.0
        if f(mid) > 0:
            lo = mid
        else:
            hi = mid

    k = (lo + hi) / 2.0
    probs = _normalise_probabilities([p ** k for p in inv])
    return probs, {'method': 'power', 'k': k}


def implied_probabilities_shin(odds: list[float]) -> tuple[list[float], dict]:
    """
    Shin method using the iterative procedure from Jullien & Salanie (1994).
    Formula aligned with the open-source shin package implementation.
    """
    inv = [1.0 / o for o in odds if o > 1.0]
    n = len(inv)
    if n == 0:
        return [], {'method': 'shin', 'z': 0.0, 'iterations': 0, 'delta': 0.0}
    if n == 1:
        return [1.0], {'method': 'shin', 'z': 0.0, 'iterations': 0, 'delta': 0.0}

    s_inv = sum(inv)
    if s_inv <= 0:
        probs = [1.0 / n] * n
        return probs, {'method': 'shin', 'z': 0.0, 'iterations': 0, 'delta': 0.0}

    if n == 2:
        diff = inv[0] - inv[1]
        denom = s_inv * (diff ** 2 - 1.0)
        if abs(denom) < 1e-12:
            z = 0.0
        else:
            z = ((s_inv - 1.0) * (diff ** 2 - s_inv)) / denom
        z = max(0.0, min(0.999999999, z))
        delta = 0.0
        iterations = 0
    else:
        z = 0.0
        delta = float('inf')
        iterations = 0
        while delta > SHIN_CONVERGENCE_THRESHOLD and iterations < SHIN_MAX_ITERATIONS:
            z0 = z
            total = 0.0
            for io in inv:
                inner = z * z + 4.0 * (1.0 - z) * (io ** 2) / s_inv
                total += math.sqrt(max(0.0, inner))
            z = (total - 2.0) / (n - 2)
            z = max(0.0, min(0.999999999, z))
            delta = abs(z - z0)
            iterations += 1

    if z >= 1.0:
        probs = _normalise_probabilities(inv)
        return probs, {'method': 'shin', 'z': z, 'iterations': iterations, 'delta': delta}

    probs = []
    for io in inv:
        inner = z * z + 4.0 * (1.0 - z) * (io ** 2) / s_inv
        p = (math.sqrt(max(0.0, inner)) - z) / (2.0 * (1.0 - z))
        probs.append(max(0.0, p))

    probs = _normalise_probabilities(probs)
    return probs, {'method': 'shin', 'z': z, 'iterations': iterations, 'delta': delta}


def fair_odds_by_runner(sharp_runners: dict, method: str = NO_VIG_METHOD) -> tuple[dict[str, float], dict]:
    """Build no-vig fair odds mapping keyed by runner number."""
    if not sharp_runners:
        return {}, {'method': method, 'count': 0}

    ordered = sorted(
        sharp_runners.items(),
        key=lambda kv: int(kv[0]) if str(kv[0]).isdigit() else 999,
    )

    odds = []
    keys = []
    for num, runner in ordered:
        back = float(runner.get('back', 0.0) or 0.0)
        if back > 1.0:
            odds.append(back)
            keys.append(str(num))

    if len(odds) < 2:
        return {}, {'method': method, 'count': len(odds)}

    selected_method = method if method in ('power', 'shin') else 'power'
    if selected_method == 'shin':
        probs, details = implied_probabilities_shin(odds)
    else:
        probs, details = implied_probabilities_power(odds)

    fair = {}
    for num, p in zip(keys, probs):
        if p > 0:
            fair[num] = 1.0 / p

    details['count'] = len(fair)
    return fair, details


# ── Sharpexchange: re-navigate to horse section ─────────────────────────────────
async def recover_sharp_horse_section(page) -> bool:
    """Click Horse Racing links when Sharp navigates away from horse markets."""
    try:
        target_frame = page
        for frame in page.frames:
            # Check if this frame has the sports list
            sports = await frame.query_selector_all('li[data-navigation-type="EVENT_TYPE"]')
            if sports:
                target_frame = frame
                break

        # Find any Horse Racing Today card first.
        all_links = await target_frame.query_selector_all('a.biab_href')
        for link in all_links:
            text = (await link.inner_text()).lower()
            if 'horse' in text and 'today' in text:
                await link.click()
                await asyncio.sleep(2)
                log.info("[Sharp] Re-navigated to Horse Racing section (today card)")
                return True

        # Fall back to any Horse Racing link.
        for link in all_links:
            text = (await link.inner_text()).lower()
            if 'horse' in text:
                await link.click()
                await asyncio.sleep(2)
                log.info("[Sharp] Re-navigated to Horse Racing section")
                return True

        log.warning("[Sharp] Could not find Horse Racing link")
        return False
    except Exception as e:
        log.error(f"[Sharp] Recovery error: {e}")
        return False


# ── Sharpexchange: read race list ────────────────────────────────────────────────
async def get_sharp_race_list(page) -> list:
    """
    Returns list of dicts:
    { time_str, track_raw, track_norm, race_dt, sport_id, frame }
    """
    races = []
    try:
        # Check if races are in an iframe
        frames = page.frames
        target_frame = page
        
        for frame in frames:
            items_in_frame = await frame.query_selector_all('li[data-navigation-type="RACE"]')
            if items_in_frame:
                target_frame = frame
                break
        
        items = await target_frame.query_selector_all('li[data-navigation-type="RACE"]')
        
        for item in items:
            link = await item.query_selector('a[data-sport-id]')
            if not link:
                continue
            
            sport_id = await link.get_attribute('data-sport-id')
            text = (await link.inner_text()).strip()
            
            if not re.search(r'\d{2}:\d{2}', text):
                text_div = await item.query_selector('div[class*="leftContainer"] > div')
                if text_div:
                    text = (await text_div.inner_text()).strip()
            
            if not re.search(r'\d{2}:\d{2}', text):
                text = (await item.inner_text()).strip()
            
            m = re.match(r'(\d{2}:\d{2})\s+(.+)', text)
            if not m:
                m = re.search(r'(\d{2}:\d{2})\s+(.+?)(?:\n|$)', text)
            
            if not m:
                continue

            time_str, track_raw = m.groups()
            races.append({
                'time_str':   time_str,
                'track_raw':  track_raw.strip(),
                'track_norm': normalise_track(track_raw),
                'track_key':  track_match_key(track_raw),
                'race_dt':    parse_race_time(time_str),
                'sport_id':   sport_id,
                'frame':      target_frame,  # Store the frame reference
            })
    except Exception as e:
        log.error(f"[Sharp] race list error: {e}")
    return races


# ── Sharpexchange: navigate to a race ───────────────────────────────────────────
async def navigate_sharp(page, race: dict) -> bool:
    """Click the race in the left panel - it auto-navigates to Win market."""
    try:
        # Find the frame with races
        target_frame = page
        for frame in page.frames:
            items = await frame.query_selector_all('li[data-navigation-type="RACE"]')
            if items:
                target_frame = frame
                break
        
        link = await target_frame.query_selector(f'a[data-sport-id="{race["sport_id"]}"]')
        if not link:
            log.error(f"[Sharp] Race link not found for {race['sport_id']}")
            return False

        await link.click()
        await asyncio.sleep(2)
        
        log.info(f"[Sharp] OK Navigated to {race['time_str']} {race['track_raw']}")
        return True
    except Exception as e:
        log.error(f"[Sharp] Navigation error: {e}")
        return False


# ── Duel: read Today tab races ───────────────────────────────────────────────────
async def click_duel_today_tab(page) -> bool:
    """Click the Today tab to show race grid. Returns True if clicked."""
    try:
        pill_buttons = await page.query_selector_all('div[data-editor-id="pillButton"]')
        for btn in pill_buttons:
            text = (await btn.inner_text()).strip()
            if 'Today' in text:
                await btn.click()
                await asyncio.sleep(1.5)
                log.info("[Duel] Clicked Today tab")
                return True
        log.warning("[Duel] Could not find Today tab button")
        return False
    except Exception as e:
        log.error(f"[Duel] Today tab click error: {e}")
        return False


def parse_duel_race_href(href: str) -> tuple[str | None, str | None]:
    """Extract (country, track_slug) from Duel race href for configured sport paths."""
    try:
        parts = [p for p in href.split('/') if p]
        for marker in ('horse-racing', 'horse'):
            if marker in parts:
                idx = parts.index(marker)
                if len(parts) > idx + 2:
                    return parts[idx + 1], parts[idx + 2]
        return None, None
    except Exception:
        return None, None


async def get_duel_today_races(page) -> list:
    """
    Scrape race links from Today tab (assumes Today tab is already showing).
    Returns list of: { track_raw, track_norm, mins_away, race_link, time_str, country, href }
    """
    races = []
    try:
        # Find all horse race links on the page.
        selectors = [f'a[href*="{path}/"]' for path in DUEL_SPORT_PATHS]
        all_links = await page.query_selector_all(', '.join(selectors))
        
        for link in all_links:
            try:
                href = await link.get_attribute('href')
                if not href:
                    continue
                
                country, track_slug = parse_duel_race_href(href)
                if not country or not track_slug:
                    continue
                
                # Get link text (the countdown time)
                link_text = (await link.inner_text()).strip()
                
                # Only process countdown format (M:SS or MM:SS)
                if not re.match(r'^\d{1,2}:\d{2}$', link_text):
                    continue
                
                # Parse countdown
                m, s = link_text.split(':')
                mins_away = int(m) + int(s) / 60
                
                # Convert track slug to readable name
                track_raw = track_slug.replace('-', ' ').title()
                # Add country info for disambiguation
                country_display = country.replace('-', ' ').title()
                
                races.append({
                    'track_raw': track_raw,
                    'track_norm': normalise_track(track_raw),
                    'track_key': track_match_key(track_raw),
                    'mins_away': mins_away,
                    'time_str': link_text,
                    'race_link': link,
                    'country': country_display,
                    'href': href,
                })
            except Exception:
                continue
        
        if races:
            sample = ", ".join(f"{r['track_raw']}({r['time_str']})" for r in races[:5])
            if len(races) > 5:
                sample += f"...+{len(races)-5} more"
            log.info(f"[Duel] Found {len(races)} races: {sample}")
                
    except Exception as e:
        log.error(f"[Duel] Today races error: {e}")
    
    return races


# ── Duel: navigate to a race ─────────────────────────────────────────────────────
async def navigate_duel(page, race: dict) -> bool:
    """Navigate to a Duel race by clicking Today tab then finding fresh link."""
    try:
        track_slug = None
        href = race.get('href')
        
        # Extract track slug from known horse path formats.
        if href:
            _, track_slug = parse_duel_race_href(href)
        
        if not track_slug:
            log.warning(f"[Duel] No track slug for {race.get('track_raw', 'unknown')}")
            return False
        
        # ALWAYS click Today tab first to ensure we're on the race list page
        await click_duel_today_tab(page)
        await asyncio.sleep(0.5)
        
        # Find ANY link for this track with a countdown time (fresh from current page)
        # Check both main page and frames
        all_links = []
        
        # Try main page
        for marker in DUEL_SPORT_PATHS:
            main_links = await page.query_selector_all(f'a[href*="{marker}/"][href*="/{track_slug}/"]')
            all_links.extend(main_links)
        
        # Try frames
        for frame in page.frames:
            try:
                for marker in DUEL_SPORT_PATHS:
                    frame_links = await frame.query_selector_all(f'a[href*="{marker}/"][href*="/{track_slug}/"]')
                    all_links.extend(frame_links)
            except Exception:
                pass
        
        # Find one with countdown format (M:SS or MM:SS)
        link = None
        for l in all_links:
            try:
                text = (await l.inner_text()).strip()
                if re.match(r'^\d{1,2}:\d{2}$', text):
                    link = l
                    break
            except Exception:
                pass
        
        if not link:
            log.warning(f"[Duel] No countdown link found for {track_slug}")
            return False
        
        # Scroll and click
        await link.scroll_into_view_if_needed()
        await asyncio.sleep(0.3)
        await link.click()
        await asyncio.sleep(2.5)
        
        # Verify navigation succeeded (odds plates visible)
        plates = await page.query_selector_all('div[data-editor-id="tableOutcomePlate"]')
        if not plates:
            for frame in page.frames:
                plates = await frame.query_selector_all('div[data-editor-id="tableOutcomePlate"]')
                if plates:
                    break
        
        if plates:
            mins_info = f" ({race['mins_away']:.1f}min)" if race.get('mins_away') else ""
            log.info(f"[Duel] OK Navigated to {race['track_raw']} {race.get('time_str', '')}{mins_info}")
            return True
        
        log.warning(f"[Duel] Click succeeded but no odds plates visible for {race['track_raw']}")
        return False
        
    except Exception as e:
        log.error(f"[Duel] Navigation error: {e}")
        return False


# ── Find matching race ───────────────────────────────────────────────────────────
def find_best_duel_race(sharp_race: dict, duel_races: list):
    """
    Find the Duel race that best matches a Sharp race by track name AND time.
    Returns the matching race or None.
    """
    sharp_mins = minutes_until(sharp_race['race_dt'])
    sharp_track = sharp_race['track_norm']
    sharp_key = sharp_race.get('track_key') or track_match_key(sharp_race.get('track_raw', sharp_track))
    
    # Prefer suffix-stripped key match (Mountaineer vs Mountaineer Park),
    # then fall back to strict normalized-name match.
    track_matches = [
        r for r in duel_races
        if (r.get('track_key') or track_match_key(r.get('track_raw', r['track_norm']))) == sharp_key
    ]
    if not track_matches:
        track_matches = [r for r in duel_races if r['track_norm'] == sharp_track]
    
    if not track_matches:
        return None
    
    # Find closest time match
    best_race = None
    best_diff = float('inf')
    
    for race in track_matches:
        if race.get('mins_away') is not None:
            diff = abs(race['mins_away'] - sharp_mins)
            if diff < best_diff:
                best_diff = diff
                best_race = race
    
    # Accept if within 2 min tolerance
    if best_race and best_diff < 2.0:
        return best_race
    
    return None


def find_match(sharp_races: list, duel_races: list):
    """
    Find the soonest Sharp race that also appears in Duel's Today grid.
    Returns (sharp_race, duel_race) or (None, None).
    """
    candidates = []
    for race in sharp_races:
        mins = minutes_until(race['race_dt'])
        if -LATE_WINDOW_MINUTES <= mins <= RACE_WINDOW_MINUTES:
            duel_race = find_best_duel_race(race, duel_races)
            if duel_race:
                candidates.append((mins, race, duel_race))

    if not candidates:
        return None, None

    candidates.sort(key=lambda x: x[0])   # soonest first
    _, sharp_race, duel_race = candidates[0]
    return sharp_race, duel_race


def find_all_matches(sharp_races: list, duel_races: list) -> list:
    """
    Find ALL Sharp races within the window that also appear in Duel's Today grid.
    Returns list of (sharp_race, duel_race, minutes_away) sorted by time.
    """
    matches = []
    for race in sharp_races:
        mins = minutes_until(race['race_dt'])
        if -LATE_WINDOW_MINUTES <= mins <= RACE_WINDOW_MINUTES:
            duel_race = find_best_duel_race(race, duel_races)
            if duel_race:
                matches.append((race, duel_race, mins))

    matches.sort(key=lambda x: x[2])   # soonest first
    return matches


class SessionCoordinator:
    """Coordinates race ownership and shared bet tracking across session loops."""

    def __init__(self):
        self._lock = asyncio.Lock()
        self.bet_action_lock = asyncio.Lock()
        self.race_owner: dict[str, str] = {}
        self.session_race: dict[str, str] = {}
        self.bets_by_race: dict[str, set[str]] = {}
        self.blocked_races: dict[str, dict] = {}

    async def assign_race(self, session_name: str, prioritized_matches: list):
        """Assign one available race to a session, preferring existing assignment."""
        async with self._lock:
            available_ids = {m[0]['sport_id'] for m in prioritized_matches}
            current_race_id = self.session_race.get(session_name)

            if current_race_id and current_race_id not in available_ids:
                if self.race_owner.get(current_race_id) == session_name:
                    del self.race_owner[current_race_id]
                self.session_race.pop(session_name, None)
                current_race_id = None

            if current_race_id:
                for match in prioritized_matches:
                    if match[0]['sport_id'] == current_race_id:
                        return match

            for match in prioritized_matches:
                race_id = match[0]['sport_id']
                owner = self.race_owner.get(race_id)
                if owner is None or owner == session_name:
                    self.race_owner[race_id] = session_name
                    self.session_race[session_name] = race_id
                    return match

            return None

    async def ensure_bets_set(self, race_id: str) -> set:
        async with self._lock:
            if race_id not in self.bets_by_race:
                self.bets_by_race[race_id] = set()
            return self.bets_by_race[race_id]

    async def release_session_race(self, session_name: str, race_id: str | None = None):
        async with self._lock:
            rid = race_id if race_id is not None else self.session_race.get(session_name)
            if rid and self.race_owner.get(rid) == session_name:
                del self.race_owner[rid]
            if session_name in self.session_race and self.session_race.get(session_name) == rid:
                del self.session_race[session_name]

    async def clear_race(self, race_id: str):
        async with self._lock:
            owner = self.race_owner.pop(race_id, None)
            if owner and self.session_race.get(owner) == race_id:
                del self.session_race[owner]
            self.bets_by_race.pop(race_id, None)

    async def block_race(self, race_id: str, seconds: float, reason: str):
        """Temporarily block a race from assignment across all sessions."""
        block_until = datetime.now() + timedelta(seconds=max(1.0, seconds))
        async with self._lock:
            existing = self.blocked_races.get(race_id)
            if existing and isinstance(existing.get('until'), datetime) and existing['until'] > block_until:
                block_until = existing['until']

            self.blocked_races[race_id] = {
                'until': block_until,
                'reason': reason,
            }

            owner = self.race_owner.pop(race_id, None)
            if owner and self.session_race.get(owner) == race_id:
                del self.session_race[owner]

    async def is_race_blocked(self, race_id: str) -> tuple[bool, str | None, float]:
        async with self._lock:
            entry = self.blocked_races.get(race_id)
            if not entry:
                return False, None, 0.0

            until = entry.get('until')
            reason = str(entry.get('reason', 'blocked'))
            if not isinstance(until, datetime):
                self.blocked_races.pop(race_id, None)
                return False, None, 0.0

            remaining = (until - datetime.now()).total_seconds()
            if remaining <= 0:
                self.blocked_races.pop(race_id, None)
                return False, None, 0.0

            return True, reason, remaining


# ── Helper to find the Sharp iframe ──────────────────────────────────────────────
async def get_sharp_frame(page):
    """Find the frame containing the race data."""
    for frame in page.frames:
        items = await frame.query_selector_all('li[data-navigation-type="RACE"]')
        if items:
            return frame
    return page


# ── Sharpexchange: odds scraper ──────────────────────────────────────────────────
async def get_sharp_odds(page) -> dict:
    runners = {}
    rows_seen = 0
    name_hits = 0
    back_hits = 0
    max_fallback_prices = 0
    parsed_runner_nums = 0
    parsed_runner_keys = 0
    try:
        frame = await get_sharp_frame(page)
        
        # Debug dump on first successful scrape
        await dump_debug_html(page, 'sharp')

        runner_meta: dict[str, dict] = {}
        runner_order: list[str] = []

        for target in _get_sharp_targets(page, frame):
            rows = await _get_best_sharp_rows(target)
            if not rows:
                continue

            for row in rows:
                rows_seen += 1
                try:
                    raw_name = ''
                    row_text = ''
                    for name_selector in SHARP_NAME_SELECTORS:
                        name_el = await row.query_selector(name_selector)
                        if not name_el:
                            continue

                        text = (await name_el.inner_text()).strip()
                        if text:
                            raw_name = text
                            break

                    if not raw_name:
                        row_text = (await row.inner_text()).strip()
                        raw_name = _extract_runner_name_from_row_text(row_text)

                    if not row_text:
                        row_text = (await row.inner_text()).strip()

                    if not raw_name:
                        continue

                    name_hits += 1
                    num, name = _extract_runner_number_and_name(raw_name)
                    key = _runner_key(num, name)
                    if not key:
                        continue

                    if num:
                        parsed_runner_nums += 1
                    else:
                        parsed_runner_keys += 1

                    if key not in runner_meta:
                        lowered = row_text.lower()
                        is_non_runner = bool(
                            re.search(r'\b(scr(?:atched)?|withdrawn|non[-\s]?runner|reserve|void)\b', lowered)
                        )
                        runner_meta[key] = {
                            'name': name,
                            'back': None,
                            'non_runner': is_non_runner,
                        }
                        runner_order.append(key)

                    back = await _extract_back_from_row(row)
                    if back is not None and runner_meta[key]['back'] is None:
                        runner_meta[key]['back'] = back
                        back_hits += 1
                except Exception:
                    continue

        # Some Sharp layouts split runner rows and odds cells. If row-level extraction
        # fails, map by the back-button DOM order as a pragmatic fallback.
        if runner_meta and back_hits == 0:
            for target in _get_sharp_targets(page, frame):
                by_index = await _extract_back_prices_from_target(target)
                max_fallback_prices = max(max_fallback_prices, len(by_index))
                if not by_index:
                    continue

                preferred = [
                    key for key in runner_order
                    if runner_meta[key]['back'] is None and not runner_meta[key].get('non_runner', False)
                ]
                if not preferred:
                    preferred = [key for key in runner_order if runner_meta[key]['back'] is None]

                if not preferred:
                    continue

                # If there are significantly fewer prices than candidate runners,
                # this target is likely not the right market container.
                if len(by_index) + 2 < len(preferred):
                    continue

                map_count = min(len(by_index), len(preferred))
                if map_count <= 0:
                    continue

                for idx in range(map_count):
                    key = preferred[idx]
                    if runner_meta[key]['back'] is None:
                        runner_meta[key]['back'] = by_index[idx]
                        back_hits += 1

                if back_hits > 0:
                    log.info(
                        f"[Sharp] Fallback mapped {back_hits}/{len(preferred)} back odds "
                        f"(prices={len(by_index)}, runners={len(runner_order)})"
                    )
                    break

        for key in runner_order:
            info = runner_meta[key]
            if info.get('back') is None:
                continue
            runners[str(key)] = {
                'name': info['name'],
                'back': float(info['back']),
            }

        if not runners:
            log.warning(
                f"[Sharp] Parsed 0 runners (rows_seen={rows_seen}, name_hits={name_hits}, parsed_nums={parsed_runner_nums}, parsed_name_keys={parsed_runner_keys}, back_hits={back_hits}). "
                f"fallback_prices_max={max_fallback_prices}. "
                "Set HORSE_DEBUG_DUMP_HTML=1 and share debug_sharp_*.html if this persists."
            )
    except Exception as e:
        log.error(f"[Sharp] odds error: {e}")
    return runners


# ── Duel: odds scraper ───────────────────────────────────────────────────────────
async def get_duel_odds(page) -> dict:
    runners = {}
    try:
        # Check all frames for Duel content
        target = page
        for frame in page.frames:
            plates = await frame.query_selector_all('div[data-editor-id="tableOutcomePlate"]')
            if plates:
                target = frame
                break
        
        # Debug dump on first successful scrape
        await dump_debug_html(page, 'duel')
        
        # Get all plates (odds buttons) - there are 3 per runner (Win, Place, Show)
        plates = await target.query_selector_all('div[data-editor-id="tableOutcomePlate"]')
        
        if not plates:
            return runners
        
        # Find runner names by looking at the parent structure of each odds plate
        # Each runner row contains: name div + 3 odds plates (Win, Place, Show)
        # We process Win plates only (indices 0, 3, 6, 9...)
        win_plate_index = 0
        for i in range(0, len(plates), 3):
            win_plate = plates[i]
            
            # Get odds value
            odds_span = await win_plate.query_selector('span')
            odds_value = None
            if odds_span:
                odds_text = (await odds_span.inner_text()).strip()
                if odds_text and odds_text != '-':
                    try:
                        odds_value = float(odds_text)
                    except ValueError:
                        pass
            
            if odds_value is None:
                win_plate_index += 1
                continue
            
            # Find the runner name by going up to the row container and looking for name
            # The name is a sibling div in the same row as the plates
            try:
                # Go up to find the row that contains this plate
                row = await win_plate.evaluate_handle('''el => {
                    let parent = el.parentElement;
                    // Go up until we find a row-level container (has multiple plate children)
                    for (let i = 0; i < 10 && parent; i++) {
                        parent = parent.parentElement;
                        if (!parent) break;
                        // Check if this is a runner row (contains name pattern)
                        let text = parent.innerText;
                        if (text && /^\\d+\\.\\s+\\w/.test(text.split('\\n')[0])) {
                            return parent;
                        }
                    }
                    return null;
                }''')
                
                if row:
                    row_text = await row.evaluate('el => el.innerText')
                    # First line should be runner name like "1. Dog Name"
                    lines = row_text.strip().split('\n')
                    name_text = None
                    for line in lines:
                        line = line.strip()
                        if line and line[0].isdigit() and '. ' in line[:4] and len(line) < 40:
                            name_text = line
                            break
                    
                    if name_text:
                        runner_num = name_text.split('.')[0].strip()
                        runners[runner_num] = {
                            'name': name_text,
                            'odds': odds_value,
                            'plate_element': win_plate
                        }
            except Exception:
                pass
            
            win_plate_index += 1
        
        # Debug: log what we found
        if runners:
            sorted_nums = sorted(runners.keys(), key=lambda x: int(x) if x.isdigit() else 999)
            names_str = ", ".join(f"{n}:{runners[n]['name'][:15]}" for n in sorted_nums[:3])
            odds_str = ", ".join(f"idx{n}={runners[n]['odds']}" for n in sorted_nums[:3])
            log.info(f"  [Duel debug] Names: {names_str}... | Odds: {odds_str}...")
            
    except Exception as e:
        log.error(f"[Duel] odds error: {e}")
    return runners


# ── Detect if race is still live ─────────────────────────────────────────────────
async def race_still_live(sharp_page) -> bool:
    """Returns True if Sharp still shows runners with valid odds."""
    try:
        frame = await get_sharp_frame(sharp_page)
        for target in _get_sharp_targets(sharp_page, frame):
            rows = await _get_best_sharp_rows(target)
            for row in rows:
                back = await _extract_back_from_row(row)
                if back is not None:
                    return True
        return False
    except Exception:
        return False


# ── Bet placement ────────────────────────────────────────────────────────────────
async def close_betslip(page):
    """Close ALL selections from the bet slip (handles multiple stuck bets)."""
    try:
        # Keep removing selections until none left (max 10 iterations for safety)
        for _ in range(10):
            removed = False
            
            # Try main page first
            btns = await page.query_selector_all('div[data-editor-id="betslipSelectionRemoveButton"]')
            for btn in btns:
                try:
                    await btn.click()
                    await asyncio.sleep(0.3)
                    removed = True
                except Exception:
                    pass
            
            # Try all frames
            for frame in page.frames:
                btns = await frame.query_selector_all('div[data-editor-id="betslipSelectionRemoveButton"]')
                for btn in btns:
                    try:
                        await btn.click()
                        await asyncio.sleep(0.3)
                        removed = True
                    except Exception:
                        pass
            
            if not removed:
                break  # No more selections to remove
                
    except Exception:
        pass


async def place_bet(duel_page, runner: dict, expected_odds: float, stake: float) -> bool:
    try:
        # Use the stored plate element directly - this is the exact Win odds button
        plate_element = runner.get('plate_element')
        
        if not plate_element:
            log.error(f"  X No plate element stored for {runner['name']}")
            return False
        
        # Find the target frame for bet slip operations
        target = duel_page
        for frame in duel_page.frames:
            plates = await frame.query_selector_all('div[data-editor-id="tableOutcomePlate"]')
            if plates:
                target = frame
                break

        # Clear any stuck selections from betslip BEFORE clicking new odds
        await close_betslip(duel_page)
        await asyncio.sleep(0.3)

        await plate_element.click()
        log.info(f"  -> Clicked odds for {runner['name']}")

        await target.wait_for_selector(
            'div[data-editor-id="betslipContent"]', timeout=5000
        )

        # Check if selection is suspended/closed
        closed_el = await target.query_selector('div[data-editor-id="betslipContent"]:has-text("CLOSED")')
        if closed_el:
            log.warning(f"  X Selection is CLOSED/suspended, aborting")
            await close_betslip(duel_page)
            return False

        # Verify odds haven't drifted
        slip_odds_el = await target.query_selector(
            'div[data-editor-id="betslipSelectionOdd"] div'
        )
        if slip_odds_el:
            try:
                slip_odds = float((await slip_odds_el.inner_text()).strip())
                drift = abs(slip_odds - expected_odds)
                if drift > ODDS_DRIFT_TOL:
                    log.warning(f"  X Odds drifted {expected_odds} -> {slip_odds}, aborting")
                    await close_betslip(duel_page)
                    return False
                log.info(f"  -> Slip odds confirmed: {slip_odds}")
            except ValueError:
                pass

        # Fill stake
        stake_input = await target.query_selector(
            'label[data-editor-id="betslipStakeInput"] input'
        )
        if not stake_input:
            log.error("  X Stake input not found")
            await close_betslip(target)
            return False

        # Fill stake (click 3 times to select all, then type)
        await stake_input.click(click_count=3)
        await stake_input.fill(str(int(stake) if stake == int(stake) else stake))

        # Place bet
        place_btn = await target.query_selector(
            'button[data-editor-id="betslipPlaceBetButton"]'
        )
        if not place_btn:
            log.error("  X Place Bet button not found")
            await close_betslip(target)
            return False

        await place_btn.click()
        log.info(f"  OK BET PLACED: {runner['name']} @ {expected_odds} for EUR{stake}")
        return True

    except PlaywrightTimeout:
        log.error("  X Timeout waiting for bet slip")
        await close_betslip(duel_page)
        return False
    except Exception as e:
        log.error(f"  X Bet error: {e}")
        await close_betslip(duel_page)
        return False


# ── Single poll iteration for one race ────────────────────────────────────────────
async def poll_once(
    sharp_page,
    duel_page,
    race_id: str,
    race_label: str,
    placed_bets: set,
    coordinator: SessionCoordinator,
) -> bool:
    """
    Poll odds once for a race, place bets if value found.
    Returns True if race is still live, False if race ended.
    """
    market_state = await get_sharp_market_state(sharp_page)
    matched_eur = market_state.get('matched_eur')

    if matched_eur is not None and matched_eur < MIN_SHARP_MATCHED_EUR:
        wait_status, wait_state = await wait_for_sharp_liquidity(sharp_page, race_label)
        if wait_status == 'ready':
            market_state = wait_state
            matched_eur = market_state.get('matched_eur')
        elif wait_status == 'in_play':
            detail = wait_state.get('in_play_text') or wait_state.get('in_play_source') or 'marker-detected'
            log.info(f"  [{race_label}] Sharp market is IN PLAY ({detail}) -> stopping monitoring")
            await coordinator.block_race(race_id, IN_PLAY_BLOCK_SECONDS, 'in-play')
            return False
        else:
            pretty = 'unknown' if matched_eur is None else f"EUR {matched_eur:,.0f}"
            log.info(
                f"  [{race_label}] Sharp matched EUR {pretty} stayed below minimum "
                f"EUR {MIN_SHARP_MATCHED_EUR:,.0f} after waiting -> skipping"
            )
            await coordinator.block_race(race_id, LOW_MATCHED_COOLDOWN_SECONDS, 'low-matched')
            return False

    if market_state.get('in_play'):
        detail = market_state.get('in_play_text') or market_state.get('in_play_source') or 'marker-detected'
        log.info(f"  [{race_label}] Sharp market is IN PLAY ({detail}) -> stopping monitoring")
        await coordinator.block_race(race_id, IN_PLAY_BLOCK_SECONDS, 'in-play')
        return False

    if not await race_still_live(sharp_page):
        log.info(f"  Race '{race_label}' is no longer live")
        return False

    sharp_runners = await get_sharp_odds(sharp_page)
    duel_runners  = await get_duel_odds(duel_page)

    if not sharp_runners or not duel_runners:
        log.info(f"  [{race_label}] Waiting for odds... (Sharp: {len(sharp_runners)}, Duel: {len(duel_runners)})")
        return True

    fair_by_num, novig_details = fair_odds_by_runner(sharp_runners, NO_VIG_METHOD)
    if not fair_by_num:
        log.info(f"  [{race_label}] Waiting for full Sharp horse market to price no-vig odds...")
        return True

    if novig_details.get('method') == 'power':
        k = novig_details.get('k')
        if k is not None:
            log.info(f"  [NoVig] method=power k={k:.5f} runners={novig_details.get('count', 0)}")
    elif novig_details.get('method') == 'shin':
        z = novig_details.get('z', 0.0)
        iters = novig_details.get('iterations', 0)
        log.info(f"  [NoVig] method=shin z={z:.5f} iterations={iters} runners={novig_details.get('count', 0)}")

    # Build lookup by normalized runner name for Duel runners.
    duel_by_name = {}
    for _, duel in duel_runners.items():
        norm_name = normalise_dog_name(duel['name'])
        duel_by_name[norm_name] = duel

    matched_count = 0
    for num, sharp in sharp_runners.items():
        fair_odds = fair_by_num.get(str(num))
        if fair_odds is None or fair_odds <= 1.0:
            continue

        # Find matching Duel runner by name.
        duel = None
        for duel_runner in duel_by_name.values():
            if dogs_match(sharp['name'], duel_runner['name']):
                duel = duel_runner
                break

        if not duel:
            continue

        matched_count += 1
        sharp_back = float(sharp['back'])
        duel_odds = float(duel['odds'])
        edge_pct = round((duel_odds / fair_odds - 1) * 100, 1)

        log.info(
            f"  [{num}] {sharp['name']:28s}  "
            f"sharp_back={sharp_back:.3f}  fair={fair_odds:.3f}  "
            f"|  duel={duel_odds:.3f}  edge={edge_pct:+.1f}%"
        )

        # Only bet once per runner per race, and only at/above min value threshold.
        if edge_pct >= MIN_VALUE_EDGE_PCT and num not in placed_bets:
            async with coordinator.bet_action_lock:
                # Re-check after waiting for lock to avoid duplicate bets.
                if num in placed_bets:
                    continue

                log.info(
                    f"  ** VALUE -> {sharp['name']} @ {duel_odds:.3f}  "
                    f"(fair={fair_odds:.3f}) edge={edge_pct:+.1f}%"
                )
                success = await place_bet(duel_page, duel, duel_odds, FLAT_STAKE)
                if success:
                    placed_bets.add(num)
                    await asyncio.sleep(2)

    # Warn if no runners matched - likely wrong race on Duel.
    if matched_count == 0 and sharp_runners and duel_runners:
        sharp_sample = list(sharp_runners.values())[0]['name']
        duel_sample = list(duel_runners.values())[0]['name']
        log.warning(f"  X RACE MISMATCH! Sharp: {sharp_sample} vs Duel: {duel_sample}")
        return True  # Keep trying, navigation might fix it


    return True


# ── Main navigation loop with parallel session assignment ───────────────────────
async def navigation_loop(
    sharp_page,
    duel_page,
    coordinator: SessionCoordinator,
    session_name: str,
    start_delay: float = 0.0,
):
    current_race_id = None
    duel_races = []
    need_duel_race_refresh = True

    if start_delay > 0:
        log.info(f"[Init] {session_name} waiting {start_delay:.1f}s before starting")
        await asyncio.sleep(start_delay)

    while True:
        sharp_races = await get_sharp_race_list(sharp_page)
        if not sharp_races:
            log.warning("[Nav] No races found on Sharp — attempting recovery...")
            recovered = await recover_sharp_horse_section(sharp_page)
            if recovered:
                await asyncio.sleep(2)
                sharp_races = await get_sharp_race_list(sharp_page)
            if not sharp_races:
                await coordinator.release_session_race(session_name, current_race_id)
                current_race_id = None
                log.warning("[Nav] Still no races after recovery — waiting...")
                await asyncio.sleep(NAV_CHECK_INTERVAL + random.uniform(0.5, POLL_JITTER_MAX))
                need_duel_race_refresh = True
                continue

        if need_duel_race_refresh:
            await click_duel_today_tab(duel_page)
            duel_races = await get_duel_today_races(duel_page)
            need_duel_race_refresh = False
            if not duel_races:
                await coordinator.release_session_race(session_name, current_race_id)
                current_race_id = None
                log.warning("[Nav] No races found in Duel Today tab")
                await asyncio.sleep(NAV_CHECK_INTERVAL + random.uniform(0.5, POLL_JITTER_MAX))
                need_duel_race_refresh = True
                continue

        all_matches = find_all_matches(sharp_races, duel_races)
        if not all_matches:
            upcoming = sorted(
                [r for r in sharp_races if minutes_until(r['race_dt']) > 0],
                key=lambda r: r['race_dt']
            )
            if upcoming:
                next_r = upcoming[0]
                mins = minutes_until(next_r['race_dt'])
                log.info(
                    f"[Nav] No match in {RACE_WINDOW_MINUTES}min window. "
                    f"Next: {next_r['time_str']} {next_r['track_raw']} in {mins:.1f} min"
                )
            else:
                log.info("[Nav] No upcoming races found")

            await coordinator.release_session_race(session_name, current_race_id)
            current_race_id = None
            await asyncio.sleep(NAV_CHECK_INTERVAL + random.uniform(0.5, POLL_JITTER_MAX))
            need_duel_race_refresh = True
            continue

        prime_races = [(r, c, m) for r, c, m in all_matches if PRIME_WINDOW_MIN <= m <= PRIME_WINDOW_MAX]
        other_races = [(r, c, m) for r, c, m in all_matches if m < PRIME_WINDOW_MIN or m > PRIME_WINDOW_MAX]
        prioritized = prime_races + other_races

        prioritized_unblocked = []
        blocked_samples = []
        for race, duel_race, mins in prioritized:
            blocked, reason, remaining = await coordinator.is_race_blocked(race['sport_id'])
            if blocked:
                blocked_samples.append(f"{race['time_str']} {race['track_raw']} ({reason},{remaining:.0f}s)")
                continue
            prioritized_unblocked.append((race, duel_race, mins))

        def race_str(r, m):
            prime_mark = "*" if PRIME_WINDOW_MIN <= m <= PRIME_WINDOW_MAX else ""
            return f"{r['time_str']} {r['track_raw']}{prime_mark}"

        race_list = ", ".join(race_str(r, m) for r, _, m in all_matches)
        log.info(
            f"[Nav] {len(all_matches)} races in window "
            f"(-{LATE_WINDOW_MINUTES:.1f}..+{RACE_WINDOW_MINUTES:.1f}min, * = prime 2-5min): {race_list}"
        )

        if blocked_samples:
            sample = ", ".join(blocked_samples[:3])
            if len(blocked_samples) > 3:
                sample += f"...+{len(blocked_samples)-3} more"
            log.info(f"[Nav] Temporarily blocked Sharp markets: {sample}")

        if not prioritized_unblocked:
            await coordinator.release_session_race(session_name, current_race_id)
            current_race_id = None
            await asyncio.sleep(max(2.0, POLL_INTERVAL + random.uniform(0.5, POLL_JITTER_MAX)))
            continue

        target_match = await coordinator.assign_race(session_name, prioritized_unblocked)
        if not target_match:
            log.info(f"[Nav] {session_name} idle — races currently monitored by another session")
            current_race_id = None
            need_duel_race_refresh = True
            await asyncio.sleep(max(2.0, POLL_INTERVAL + random.uniform(0.5, POLL_JITTER_MAX)))
            continue

        target_race, target_card, _ = target_match
        race_id = target_race['sport_id']
        race_label = f"{target_race['time_str']} {target_race['track_raw']}"

        if race_id != current_race_id:
            mins = minutes_until(target_race['race_dt'])
            log.info(f"[Session] {session_name} assigned race {race_label}")
            log.info(f"\n[Nav] Switching to: {race_label} ({mins:.1f} min away)")

            sharp_ok = await navigate_sharp(sharp_page, target_race)
            duel_ok = await navigate_duel(duel_page, target_card)

            if not (sharp_ok and duel_ok):
                log.error("[Nav] Navigation failed — retrying")
                await coordinator.release_session_race(session_name, race_id)
                current_race_id = None
                need_duel_race_refresh = True
                await asyncio.sleep(5 + random.uniform(0.5, POLL_JITTER_MAX))
                continue

            wait_status, market_state = await wait_for_sharp_liquidity(sharp_page, race_label)
            if wait_status == 'ready':
                pass
            elif wait_status == 'in_play':
                detail = market_state.get('in_play_text') or market_state.get('in_play_source') or 'marker-detected'
                log.info(f"[Nav] Skipping {race_label}: Sharp market already IN PLAY ({detail})")
                await coordinator.block_race(race_id, IN_PLAY_BLOCK_SECONDS, 'in-play')
                current_race_id = None
                await asyncio.sleep(max(1.0, random.uniform(0.5, POLL_JITTER_MAX)))
                continue
            else:
                matched_eur = market_state.get('matched_eur')
                pretty = 'unknown' if matched_eur is None else f"EUR {matched_eur:,.0f}"
                log.info(
                    f"[Nav] Skipping {race_label}: Sharp matched {pretty} stayed below "
                    f"EUR {MIN_SHARP_MATCHED_EUR:,.0f} after waiting"
                )
                await coordinator.block_race(race_id, LOW_MATCHED_COOLDOWN_SECONDS, 'low-matched')
                current_race_id = None
                await asyncio.sleep(max(1.0, random.uniform(0.5, POLL_JITTER_MAX)))
                continue

            current_race_id = race_id
            log.info(f"\n{'='*60}")
            log.info(f"  MONITORING: {race_label}")
            log.info(f"{'='*60}")

        placed_bets = await coordinator.ensure_bets_set(race_id)
        still_live = await poll_once(sharp_page, duel_page, race_id, race_label, placed_bets, coordinator)

        if not still_live:
            await coordinator.clear_race(race_id)
            current_race_id = None
            need_duel_race_refresh = True
            log.info(f"[Nav] {race_label} ended — looking for next race")
            continue

        sleep_for = max(1.5, POLL_INTERVAL + random.uniform(0.3, POLL_JITTER_MAX))
        log.info(f"  {'-'*48} sleeping {sleep_for:.1f}s")
        await asyncio.sleep(sleep_for)


# ── Entry point ──────────────────────────────────────────────────────────────────
async def main(proxy_server=None, proxy_user=None, proxy_pass=None):
    async with async_playwright() as p:
        proxy_config = None
        if proxy_server:
            proxy_config = {"server": proxy_server}
            if proxy_user:
                proxy_config["username"] = proxy_user
            if proxy_pass:
                proxy_config["password"] = proxy_pass
            log.info(f"[Init] Using proxy server: {proxy_server}")

        browser = await p.chromium.launch(
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-first-run',
                '--window-size=1280,800',
                '--window-position=100,100',
            ],
            proxy=proxy_config,
        )
        
        context = await browser.new_context(
            viewport={'width': 1260, 'height': 700},
        )

        if BLOCK_RESOURCE_TYPES:
            log.info(
                f"[Init] Blocking resource types: {', '.join(sorted(BLOCK_RESOURCE_TYPES))}"
            )
            await context.route('**/*', _block_heavy_resources)

        sessions = []
        for i in range(MAX_CONCURRENT_SESSIONS):
            sharp_page = await context.new_page()
            duel_page = await context.new_page()
            sessions.append((sharp_page, duel_page))

        for i, (sharp_page, duel_page) in enumerate(sessions, start=1):
            await sharp_page.goto('https://sharpexch.com', wait_until='domcontentloaded')
            await asyncio.sleep(0.6)
            await duel_page.goto(f'https://duel.com/sports?bt-path={DUEL_SPORT_PATHS[0]}', wait_until='domcontentloaded')
            await asyncio.sleep(0.6)
            log.info(f"[Init] Opened Sharp+Duel session pair {i}/{MAX_CONCURRENT_SESSIONS}")

        print("""
╔══════════════════════════════════════════════════════════════╗
    ║  HORSE BOT                                                   ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║  Multiple Sharp+Duel tab pairs opened. Please:               ║
    ║  1. Log into Sharp and ensure Horse Racing is visible         ║
║  2. Log into Duel manually                                   ║
║  3. Keep both sites logged in (shared profile handles tabs)  ║
║                                                              ║
    ║  Pricing mode: Power/Shin no-vig from Sharp back odds.       ║
    ║  Set HORSE_NOVIG_METHOD=shin or HORSE_NOVIG_METHOD=power.    ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
""")
        input("Press ENTER when Sharp + Duel are ready in the opened tabs → ")

        log.info("=" * 60)
        log.info(f"HORSE BOT — Running with {MAX_CONCURRENT_SESSIONS} parallel sessions")
        log.info("=" * 60)

        coordinator = SessionCoordinator()
        tasks = []
        for i, (sharp_page, duel_page) in enumerate(sessions, start=1):
            session_name = f"S{i}"
            start_delay = (i - 1) * SESSION_START_STAGGER
            tasks.append(
                asyncio.create_task(
                    navigation_loop(
                        sharp_page,
                        duel_page,
                        coordinator,
                        session_name,
                        start_delay=start_delay,
                    )
                )
            )

        await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        args = parse_args()
        asyncio.run(main(args.proxy_server, args.proxy_user, args.proxy_pass))
    except KeyboardInterrupt:
        log.info("\nBot stopped.")