"""
Greyhound Betting Bot — Full Auto Navigation
─────────────────────────────────────────────
Automatically finds matching races on Sharpexchange + Duel.com (or Rainbet),
navigates both sites, monitors odds, and places bets when value is found.

Setup:
    pip install playwright
    playwright install chromium
    python greyhound_bot.py

"""
# Imported first so asyncio.run works while Proton VPN is connected.
import vpn_asyncio  # noqa: F401

_DOC_CONTINUE = """
On startup:
    - Navigate Sharp to the Greyhound Racing section (left panel)
    - Duel/Rainbet will be handled automatically from there
    - Press ENTER and the bot runs fully automatically
"""

import vpn_asyncio  # noqa: F401  — Proton blocks 127.0.0.1 socketpair
import argparse
import asyncio
import logging
import os
import random
import re
from datetime import datetime, timedelta
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ── Configuration ───────────────────────────────────────────────────────────────
POLL_INTERVAL          = 5     # seconds between odds checks during a race
NAV_CHECK_INTERVAL     = 20    # seconds between race-finding attempts
RACE_WINDOW_MINUTES    = 8.1     # start monitoring a race this many minutes before start
PRIME_WINDOW_MIN       = 1     # prime betting window starts at this many minutes
PRIME_WINDOW_MAX       = 5     # prime betting window ends at this many minutes
FLAT_STAKE             = 15.0   # dollars on BC.Game, euros on Rainbet/Duel
# Coinbase BTC-USD spot on 2026-10-04. Frozen — BC.Game slips are in BTC.
BCGAME_BTC_USD         = 84803.695
MIN_VALUE_EDGE_PCT     = 3.0   # minimum perceived value edge required before betting
MAX_CONCURRENT_SESSIONS = 2    # parallel Sharp+Duel session pairs
SESSION_START_STAGGER  = 4.0   # seconds between starting each session loop
POLL_JITTER_MAX        = 1.2   # random sleep jitter to avoid robotic cadence
ODDS_DRIFT_TOL         = 0.05  # abort bet if slip odds differ more than this
DEBUG_DUMP_HTML        = False # dump iframe HTML to files for analysis

# Duel class names — update if site deploys break scraping
DUEL_NAME_CLASS = 'bt311'
DUEL_ODDS_CLASS = 'bt329'
DUEL_BASE_URL = "https://duel.com"
DUEL_LABEL = "Duel"
DUEL_RACE_LIST_URL = f"{DUEL_BASE_URL}/sports?bt-path=/greyhound-58"
ASSUME_BET_PLACED_ON_BLANK = True
SHARP_TODAYS_CARD_URL = "https://sharpxch.com/player/sport/tc-4339/"

# ── Logging ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(message)s',
    handlers=[
        logging.FileHandler('betting_log.txt'),
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


def _greyhound_path_index(parts: list[str]) -> int | None:
    """Index of the greyhound sport segment in a BetBy href.

    Rainbet/Duel use /greyhound/country/track. BC.Game may use /greyhound-58/.
    """
    for token in ("greyhound", "greyhound-58"):
        if token in parts:
            return parts.index(token)
    return None


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


def parse_args():
    parser = argparse.ArgumentParser(description="Greyhound Betting Bot")
    parser.add_argument(
        "--proxy",
        "--vpn",
        dest="proxy_server",
        help="Proxy server URL, e.g. http://host:port or socks5://host:port",
    )
    parser.add_argument("--proxy-user", dest="proxy_user", help="Proxy username (optional)")
    parser.add_argument("--proxy-pass", dest="proxy_pass", help="Proxy password (optional)")
    site_group = parser.add_mutually_exclusive_group()
    site_group.add_argument(
        "--rainbet",
        action="store_true",
        help="Use rainbet.com for the Duel tab",
    )
    site_group.add_argument(
        "--duel",
        action="store_true",
        help="Use duel.com for the Duel tab (default)",
    )
    site_group.add_argument(
        "--bc",
        "--bcgame",
        dest="bcgame",
        action="store_true",
        help="Use bc.game sports (BetBy) for the soft-book tab",
    )
    parser.add_argument(
        "--stake",
        type=float,
        default=FLAT_STAKE,
        help=f"Stake per bet in euros (default {FLAT_STAKE:g})",
    )
    return parser.parse_args()


# ── Sharpexchange: re-navigate to greyhound section ─────────────────────────────
async def _sharp_is_404(page) -> bool:
    try:
        title_el = await page.query_selector('h3.title')
        if not title_el:
            return False
        text = (await title_el.inner_text()).strip().upper()
        return '404' in text and 'PAGE NOT FOUND' in text
    except Exception:
        return False


async def _sharp_has_races(page) -> bool:
    try:
        for frame in page.frames:
            items = await frame.query_selector_all('li[data-navigation-type="RACE"]')
            if items:
                return True
        items = await page.query_selector_all('li[data-navigation-type="RACE"]')
        return bool(items)
    except Exception:
        return False


async def recover_sharp_greyhound_section(page) -> bool:
    """Click 'Greyhound Racing -Today's Card' when Sharp logged out or navigated away."""
    try:
        if await _sharp_is_404(page):
            log.warning("[Sharp] 404 detected — loading Today's Card URL")
            await page.goto(SHARP_TODAYS_CARD_URL, wait_until='domcontentloaded')
            await asyncio.sleep(2)
            if await _sharp_has_races(page):
                log.info("[Sharp] Today's Card loaded via direct URL")
                return True

        target_frame = page
        for frame in page.frames:
            # Check if this frame has the sports list
            sports = await frame.query_selector_all('li[data-navigation-type="EVENT_TYPE"]')
            if sports:
                target_frame = frame
                break
        
        # First try "Today's Card" (more specific)
        todays_card = await target_frame.query_selector('a[data-sport-id="4339"]:has-text("Today")')
        if not todays_card:
            # Fall back to main Greyhound Racing link
            todays_card = await target_frame.query_selector('a[data-sport-id="4339"]')
        
        if todays_card:
            await todays_card.click()
            await asyncio.sleep(2)
            log.info("[Sharp] Re-navigated to Greyhound Racing section")
            return True
        
        # Alternative: search by text content
        all_links = await target_frame.query_selector_all('a.biab_href')
        for link in all_links:
            text = await link.inner_text()
            if 'greyhound' in text.lower() and 'today' in text.lower():
                await link.click()
                await asyncio.sleep(2)
                log.info("[Sharp] Re-navigated to Greyhound Racing section (via text)")
                return True
        
        log.warning("[Sharp] Could not find Greyhound Racing link")
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
        await ensure_duel_page_ready(page, "before Today tab", require_race_content=True)
        pill_buttons = await page.query_selector_all('div[data-editor-id="pillButton"]')
        for btn in pill_buttons:
            text = (await btn.inner_text()).strip()
            if 'Today' in text:
                await btn.click()
                await asyncio.sleep(1.5)
                log.info("[Duel] Clicked Today tab")
                return True
        log.warning("[Duel] Could not find Today tab button")
        if DUEL_RACE_LIST_URL:
            log.warning(f"[Duel] Resetting {DUEL_LABEL} page to race list URL")
            await page.goto(DUEL_RACE_LIST_URL, wait_until='domcontentloaded')
            await asyncio.sleep(1.5)
            pill_buttons = await page.query_selector_all('div[data-editor-id="pillButton"]')
            for btn in pill_buttons:
                text = (await btn.inner_text()).strip()
                if 'Today' in text:
                    await btn.click()
                    await asyncio.sleep(1.5)
                    log.info("[Duel] Clicked Today tab")
                    return True
        return False
    except Exception as e:
        log.error(f"[Duel] Today tab click error: {e}")
        return False


async def get_duel_today_races(page) -> list:
    """
    Scrape race links from Today tab (assumes Today tab is already showing).
    Returns list of: { track_raw, track_norm, mins_away, race_link, time_str, country, href }
    """
    try:
        for attempt in range(2):
            races = []
            # Find ALL greyhound race links on the page
            all_links = await page.query_selector_all('a[href*="/greyhound"]')
            
            for link in all_links:
                try:
                    href = await link.get_attribute('href')
                    if not href:
                        continue
                    
                    # Parse href to get country and track
                    # Accepts both /greyhound/... and /sportsbook/greyhound/... formats
                    parts = href.split('/')
                    greyhound_idx = _greyhound_path_index(parts)
                    if greyhound_idx is None or greyhound_idx + 2 >= len(parts):
                        continue
                    country = parts[greyhound_idx + 1]  # e.g., "united-kingdom", "australia"
                    track_slug = parts[greyhound_idx + 2]  # e.g., "hove-2", "the-meadows"
                    
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
                        'mins_away': mins_away,
                        'time_str': link_text,
                        'race_link': link,
                        'country': country_display,
                        'href': href,
                    })
                except Exception:
                    continue

            if races or attempt == 1:
                if races:
                    sample = ", ".join(f"{r['track_raw']}({r['time_str']})" for r in races[:5])
                    if len(races) > 5:
                        sample += f"...+{len(races)-5} more"
                    log.info(f"[Duel] Found {len(races)} races: {sample}")
                return races

            await ensure_duel_page_ready(page, "no races found", require_race_content=True)

    except Exception as e:
        log.error(f"[Duel] Today races error: {e}")
    
    return []


# ── Duel: navigate to a race ─────────────────────────────────────────────────────
async def navigate_duel(page, race: dict) -> bool:
    """Navigate to a Duel race by clicking Today tab then finding fresh link."""
    try:
        track_slug = None
        href = race.get('href')
        
        # Extract track slug (e.g., "kinsley-2", "newcastle-2")
        if href:
            href_parts = href.split('/')
            greyhound_idx = _greyhound_path_index(href_parts)
            if greyhound_idx is not None and greyhound_idx + 2 < len(href_parts):
                track_slug = href_parts[greyhound_idx + 2]
            else:
                track_slug = None
        
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
        main_links = await page.query_selector_all(f'a[href*="/greyhound"][href*="/{track_slug}/"]')
        all_links.extend(main_links)
        
        # Try frames
        for frame in page.frames:
            try:
                frame_links = await frame.query_selector_all(f'a[href*="/greyhound"][href*="/{track_slug}/"]')
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
    
    # Filter to races matching track name
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
        if 0 <= mins <= RACE_WINDOW_MINUTES:
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
        if 0 <= mins <= RACE_WINDOW_MINUTES:
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
    try:
        frame = await get_sharp_frame(page)
        
        # Debug dump on first successful scrape
        await dump_debug_html(page, 'sharp')
        
        rows = await frame.query_selector_all('div.runnerRow')
        for row in rows:
            name_el = await row.query_selector('span[data-auto="runner_name"]')
            back_el = await row.query_selector('button.biab_back-0 span.biab_bet-odds')
            lay_el  = await row.query_selector('button.biab_lay-0 span.biab_bet-odds')

            if not (name_el and back_el and lay_el):
                continue

            name     = (await name_el.inner_text()).strip()
            back_txt = (await back_el.inner_text()).strip()
            lay_txt  = (await lay_el.inner_text()).strip()

            if not back_txt or not lay_txt:
                continue

            back = float(back_txt)
            lay  = float(lay_txt)
            num  = name.split('.')[0].strip()

            runners[num] = {
                'name':     name,
                'back':     back,
                'lay':      lay,
                'midpoint': round((back + lay) / 2, 4)
            }
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
        rows = await frame.query_selector_all('div.runnerRow')
        for row in rows:
            back_el = await row.query_selector('button.biab_back-0 span.biab_bet-odds')
            if back_el:
                txt = (await back_el.inner_text()).strip()
                if txt and txt not in ('', '-'):
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


async def _read_plate_odds(plate_element) -> float | None:
    """Read the current odds from a Duel odds plate element."""
    try:
        odds_span = await plate_element.query_selector('span')
        txt = (await odds_span.inner_text()).strip() if odds_span else (await plate_element.inner_text()).strip()
        return float(txt)
    except Exception:
        return None


async def _read_slip_odds(target) -> float | None:
    try:
        slip_odds_el = await target.query_selector(
            'div[data-editor-id="betslipSelectionOdd"] div'
        )
        if not slip_odds_el:
            return None
        return float((await slip_odds_el.inner_text()).strip())
    except Exception:
        return None


async def _page_has_selector(page, selector: str) -> bool:
    try:
        if await page.query_selector(selector):
            return True
    except Exception:
        pass
    for frame in page.frames:
        try:
            if await frame.query_selector(selector):
                return True
        except Exception:
            pass
    return False


async def _duel_page_has_content(page, require_race_content: bool = False) -> bool:
    race_selectors = [
        'div[data-editor-id="tableOutcomePlate"]',
        'a[href*="/greyhound"]',
        'div[data-editor-id="betslipContent"]',
    ]
    base_selectors = [
        'div[data-editor-id="pillButton"]',
    ]

    selectors = race_selectors if require_race_content else (race_selectors + base_selectors)
    for sel in selectors:
        if await _page_has_selector(page, sel):
            return True
    return False


async def ensure_duel_page_ready(page, reason: str = "", require_race_content: bool = False) -> bool:
    if await _duel_page_has_content(page, require_race_content=require_race_content):
        return True

    msg = f"[Duel] Page looks blank"
    if reason:
        msg += f" ({reason})"
    log.warning(f"{msg} — reloading race list")

    if DUEL_RACE_LIST_URL:
        await page.goto(DUEL_RACE_LIST_URL, wait_until='domcontentloaded')
        await asyncio.sleep(1.5)
    return await _duel_page_has_content(page, require_race_content=require_race_content)


def _parse_stake_value(raw: str) -> float | None:
    try:
        cleaned = re.sub(r'[^0-9.]', '', raw or '')
        return float(cleaned) if cleaned else None
    except Exception:
        return None


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


async def _set_betslip_stake(target, stake: float) -> bool:
    stake_str = _format_stake_amount(stake)
    for _ in range(3):
        stake_input = await target.query_selector(
            'label[data-editor-id="betslipStakeInput"] input'
        )
        if not stake_input:
            return False

        try:
            await stake_input.wait_for_element_state('editable', timeout=2000)
        except Exception:
            pass

        await stake_input.click()
        await stake_input.press('Control+A')
        await stake_input.type(stake_str, delay=30)
        await stake_input.press('Tab')
        await asyncio.sleep(0.2)

        try:
            current_raw = await stake_input.input_value()
        except Exception:
            current_raw = await stake_input.get_attribute('value')

        current = _parse_stake_value(current_raw or '')
        if _stake_amounts_match(current, float(stake)):
            return True

        await stake_input.evaluate(
            "(el, v) => { el.value = v; el.dispatchEvent(new Event('input', { bubbles: true })); el.dispatchEvent(new Event('change', { bubbles: true })); }",
            stake_str,
        )
        await asyncio.sleep(0.2)

        try:
            current_raw = await stake_input.input_value()
        except Exception:
            current_raw = await stake_input.get_attribute('value')

        current = _parse_stake_value(current_raw or '')
        if _stake_amounts_match(current, float(stake)):
            return True

    return False


async def _wait_for_bet_confirmation(target, timeout_ms: int = 5000) -> bool:
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
        try:
            remove_btns = await target.query_selector_all(
                'div[data-editor-id="betslipSelectionRemoveButton"]'
            )
            if not remove_btns:
                return True
        except Exception:
            pass
        return False


async def place_bet(duel_page, runner: dict, expected_odds: float, stake: float) -> bool:
    try:
        if not await ensure_duel_page_ready(duel_page, "before placing bet", require_race_content=True):
            log.warning("  X Duel page still blank before bet — aborting")
            return False

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

        # Verify the board odds on the plate before clicking
        plate_odds = await _read_plate_odds(plate_element)
        if plate_odds is not None:
            drift = abs(plate_odds - expected_odds)
            if drift > ODDS_DRIFT_TOL:
                log.warning(f"  X Odds drifted on board {expected_odds} -> {plate_odds}, aborting")
                await close_betslip(duel_page)
                return False

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
        slip_odds = await _read_slip_odds(target)
        if slip_odds is not None:
            drift = abs(slip_odds - expected_odds)
            if drift > ODDS_DRIFT_TOL:
                log.warning(f"  X Odds drifted {expected_odds} -> {slip_odds}, aborting")
                await close_betslip(duel_page)
                return False
            log.info(f"  -> Slip odds confirmed: {slip_odds}")

        # Fill stake. BC.Game's slip is denominated in the deposited crypto.
        slip_stake = stake
        if "bc.game" in (DUEL_BASE_URL or ""):
            slip_stake = _bcgame_btc_stake(stake)
            log.info(
                f"  -> BC.Game slip {slip_stake:.8f} BTC "
                f"(fixed ${stake:.0f} at ${BCGAME_BTC_USD:,.2f}/BTC)"
            )
        ok = await _set_betslip_stake(target, slip_stake)
        if not ok:
            log.error(f"  X Stake did not set to {slip_stake}")
            await close_betslip(target)
            return False

        # Re-check slip odds right before placing
        slip_odds = await _read_slip_odds(target)
        if slip_odds is not None:
            drift = abs(slip_odds - expected_odds)
            if drift > ODDS_DRIFT_TOL:
                log.warning(f"  X Odds drifted late {expected_odds} -> {slip_odds}, aborting")
                await close_betslip(duel_page)
                return False

        # Place bet
        place_btn = await target.query_selector(
            'button[data-editor-id="betslipPlaceBetButton"]'
        )
        if not place_btn:
            log.error("  X Place Bet button not found")
            await close_betslip(target)
            return False

        await place_btn.click()
        await asyncio.sleep(0.5)

        confirmed = await _wait_for_bet_confirmation(target)
        if confirmed:
            log.info(
                f"  OK BET PLACED: {runner['name']} @ {expected_odds} for "
                f"{_format_stake_amount(slip_stake)} "
                f"{'BTC' if 'bc.game' in (DUEL_BASE_URL or '') else 'EUR'}"
            )
            return True

        if not await _duel_page_has_content(duel_page, require_race_content=True):
            log.warning("  ? Bet confirmation missing and page blanked")
            await ensure_duel_page_ready(duel_page, "after bet")
            if ASSUME_BET_PLACED_ON_BLANK:
                log.warning("  ? Assuming bet placed to avoid duplicates")
                return True

        log.warning("  X Bet not confirmed")
        return False

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
    race_label: str,
    placed_bets: set,
    coordinator: SessionCoordinator,
    duel_card: dict | None = None,
) -> bool:
    """
    Poll odds once for a race, place bets if value found.
    Returns True if race is still live, False if race ended.
    """
    if not await race_still_live(sharp_page):
        log.info(f"  Race '{race_label}' is no longer live")
        return False

    sharp_runners = await get_sharp_odds(sharp_page)
    duel_runners  = await get_duel_odds(duel_page)

    if not sharp_runners or not duel_runners:
        if not duel_runners:
            has_plates = await _page_has_selector(duel_page, 'div[data-editor-id="tableOutcomePlate"]')
            if not has_plates:
                log.warning("  [Duel] No odds plates detected — reloading race list")
                await ensure_duel_page_ready(duel_page, "no odds plates", require_race_content=True)
                if duel_card is not None:
                    await navigate_duel(duel_page, duel_card)
        log.info(f"  [{race_label}] Waiting for odds... (Sharp: {len(sharp_runners)}, Duel: {len(duel_runners)})")
        return True

    # Build lookup by normalized dog name for Duel runners
    duel_by_name = {}
    for num, duel in duel_runners.items():
        norm_name = normalise_dog_name(duel['name'])
        duel_by_name[norm_name] = duel

    matched_count = 0
    for num, sharp in sharp_runners.items():
        sharp_norm = normalise_dog_name(sharp['name'])
        
        # Find matching Duel runner by name
        duel = None
        for duel_norm, duel_runner in duel_by_name.items():
            if dogs_match(sharp['name'], duel_runner['name']):
                duel = duel_runner
                break
        
        if not duel:
            # No matching dog on Duel - skip
            continue
        
        matched_count += 1
        midpoint  = sharp['midpoint']
        duel_odds = duel['odds']
        edge_pct  = round((duel_odds / midpoint - 1) * 100, 1)

        log.info(
            f"  [{num}] {sharp['name']:28s}  "
            f"back={sharp['back']}  lay={sharp['lay']}  "
            f"mid={midpoint:.3f}  |  duel={duel_odds}  edge={edge_pct:+.1f}%"
        )

        # Only bet once per runner per race, and only at/above min value threshold.
        if edge_pct >= MIN_VALUE_EDGE_PCT and num not in placed_bets:
            async with coordinator.bet_action_lock:
                # Re-check after waiting for lock to avoid duplicate bets.
                if num in placed_bets:
                    continue

                log.info(
                    f"  ** VALUE -> {sharp['name']} @ {duel_odds}  "
                    f"(mid={midpoint:.3f}) edge={edge_pct:+.1f}%"
                )
                success = await place_bet(duel_page, duel, duel_odds, FLAT_STAKE)
                if success:
                    placed_bets.add(num)
                    await asyncio.sleep(2)
    
    # Warn if no dogs matched - likely wrong race on Duel
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
            recovered = await recover_sharp_greyhound_section(sharp_page)
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

        def race_str(r, m):
            prime_mark = "*" if PRIME_WINDOW_MIN <= m <= PRIME_WINDOW_MAX else ""
            return f"{r['time_str']} {r['track_raw']}{prime_mark}"

        race_list = ", ".join(race_str(r, m) for r, _, m in all_matches)
        log.info(f"[Nav] {len(all_matches)} races in window (* = prime 2-5min): {race_list}")

        target_match = await coordinator.assign_race(session_name, prioritized)
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

            current_race_id = race_id
            log.info(f"\n{'='*60}")
            log.info(f"  MONITORING: {race_label}")
            log.info(f"{'='*60}")

        placed_bets = await coordinator.ensure_bets_set(race_id)
        still_live = await poll_once(sharp_page, duel_page, race_label, placed_bets, coordinator, target_card)

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
async def main(proxy_server=None, proxy_user=None, proxy_pass=None, duel_base_url="https://duel.com", duel_label="Duel"):
    async with async_playwright() as p:
        global DUEL_BASE_URL, DUEL_LABEL, DUEL_RACE_LIST_URL
        DUEL_BASE_URL = duel_base_url
        DUEL_LABEL = duel_label
        if "rainbet.com" in duel_base_url:
            DUEL_RACE_LIST_URL = f"{duel_base_url}/sportsbook/greyhound-58?timeRange=TODAY"
        elif "bc.game" in duel_base_url:
            DUEL_RACE_LIST_URL = f"{duel_base_url}/sports/greyhound-58?timeRange=TODAY"
        else:
            DUEL_RACE_LIST_URL = f"{duel_base_url}/sports?bt-path=/greyhound-58"

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

        sessions = []
        for i in range(MAX_CONCURRENT_SESSIONS):
            sharp_page = await context.new_page()
            duel_page = await context.new_page()
            sessions.append((sharp_page, duel_page))

        duel_url = DUEL_RACE_LIST_URL
        for i, (sharp_page, duel_page) in enumerate(sessions, start=1):
            await sharp_page.goto('https://sharpexch.com', wait_until='domcontentloaded')
            await asyncio.sleep(0.6)
            await duel_page.goto(duel_url, wait_until='domcontentloaded')
            await asyncio.sleep(0.6)
            log.info(f"[Init] Opened Sharp+{duel_label} session pair {i}/{MAX_CONCURRENT_SESSIONS}")

        print(f"""
╔══════════════════════════════════════════════════════════════╗
║  GREYHOUND BOT                                               ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
    ║  Multiple Sharp+{duel_label} tab pairs opened. Please:               ║
║  1. Log into Sharp and ensure Greyhound Racing is visible    ║
    ║  2. Log into {duel_label} manually                                   ║
║  3. Keep both sites logged in (shared profile handles tabs)  ║
║                                                              ║
    ║  For {duel_label} CAPTCHA: If it keeps failing, the bot can still    ║
║  work with just Sharp - it will show value opportunities     ║
    ║  but won't auto-bet on {duel_label}.                                 ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
""")
        input(f"Press ENTER when Sharp + {duel_label} are ready in the opened tabs → ")

        log.info("=" * 60)
        log.info(f"GREYHOUND BOT — Running with {MAX_CONCURRENT_SESSIONS} parallel sessions")
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
        FLAT_STAKE = args.stake
        if args.rainbet:
            duel_base_url = "https://rainbet.com"
            duel_label = "Rainbet"
        elif args.bcgame:
            duel_base_url = "https://bc.game"
            duel_label = "BC.Game"
        else:
            duel_base_url = "https://duel.com"
            duel_label = "Duel"
        asyncio.run(main(args.proxy_server, args.proxy_user, args.proxy_pass, duel_base_url, duel_label))
    except KeyboardInterrupt:
        log.info("\nBot stopped.")