"""
DamaDam Bot - Message Sender v2.2 (Fixed)
Flow: Run → Pick Nick → Scrape Profile → Go to Posts → Pick Post → 
      Post Msg → Send with CSRF → Refresh & Verify → Update Sheet → Next Nick
"""

import time
import json
import os
import sys
import re
import pickle
import csv
import argparse
from datetime import datetime, timedelta, timezone
import gspread
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from gspread.exceptions import WorksheetNotFound

# Option-A refactor imports
from browser import setup_browser as setup_browser_mod, login as login_mod
from modes.inbox_mode import run_inbox_mode as run_inbox_mode_mod
from modes.activity_mode import run_activity_mode as run_activity_mode_mod

# ============================================================================
# CONFIGURATION
# ============================================================================

VERSION = "2.3.0"

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

LOGIN_EMAIL = os.environ.get("DD_LOGIN_EMAIL", "0utLawZ")
LOGIN_PASS = os.environ.get("DD_LOGIN_PASS", "asdasd")
LOGIN_URL = "https://damadam.pk/login/"
HOME_URL = "https://damadam.pk/"
BASE_URL = "https://damadam.pk"
COOKIE_FILE = os.environ.get("COOKIE_FILE", "damadam_cookies.pkl")
SHEET_ID = os.environ.get("DD_SHEET_ID", "1xph0dra5-wPcgMXKubQD7A2CokObpst7o2rWbDA10t8")
CREDENTIALS_FILE = os.environ.get("CREDENTIALS_FILE", "credentials.json")
SHEET_FONT = os.environ.get("SHEET_FONT", "Asimovian")
DD_MODE = os.environ.get("DD_MODE", "Msg")

# ============================================================================
# HELPERS
# ============================================================================

def get_pkt_time():
    """Get current time in Pakistan timezone"""
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5)

def log_msg(m):
    """Print timestamped message"""
    print(f"[{get_pkt_time().strftime('%H:%M:%S')}] {m}")
    sys.stdout.flush()

def get_sheet(sheet_name="MessageList"):
    """Connect to Google Sheet"""
    if not os.path.exists(CREDENTIALS_FILE):
        log_msg(f"❌ {CREDENTIALS_FILE} not found!")
        sys.exit(1)
    
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
    client = gspread.authorize(creds)
    workbook = client.open_by_key(SHEET_ID)
    try:
        return workbook.worksheet(sheet_name)
    except WorksheetNotFound:
        log_msg(f"⚠️ Sheet '{sheet_name}' not found, using first sheet")
        return workbook.sheet1

def get_or_create_sheet(sheet_name):
    """Get or create a worksheet"""
    if not os.path.exists(CREDENTIALS_FILE):
        log_msg(f"❌ {CREDENTIALS_FILE} not found!")
        sys.exit(1)
    
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
    client = gspread.authorize(creds)
    workbook = client.open_by_key(SHEET_ID)
    try:
        return workbook.worksheet(sheet_name)
    except WorksheetNotFound:
        log_msg(f"📄 Creating new sheet: {sheet_name}")
        return workbook.add_worksheet(title=sheet_name, rows=1000, cols=26)

ALIGN_MAP = {"L": "LEFT", "C": "CENTER", "R": "RIGHT"}
WRAP_MAP = {"WRAP": "WRAP", "CLIP": "CLIP", "OVERFLOW": "OVERFLOW"}

PROFILES_COLUMN_SPECS = {
    "widths": [2, 150, 80, 2, 80, 70, 140, 40, 40, 40, 70, 40, 60, 40, 2, 10, 40, 80, 150, 2, 70],
    "alignments": ["L", "L", "C", "L", "C", "C", "L", "C", "C", "C", "C", "C", "C", "C", "L", "L", "C", "L", "L", "L", "C"],
    "wrap": ["CLIP"] * 21
}

RUNLIST_COLUMN_SPECS = {
    "widths": [80, 120, 260, 120, 80, 80, 320, 100, 260, 260, 160],
    "alignments": ["C", "L", "L", "L", "C", "C", "L", "C", "L", "L", "C"],
    "wrap": ["CLIP"] * 11
}

CHECKLIST_COLUMN_SPECS = {
    "widths": [200, 200, 200, 200],
    "alignments": ["L", "L", "L", "L"],
    "wrap": ["CLIP"] * 4
}

def index_to_column_letter(index: int) -> str:
    """Convert zero-based column index to letter"""
    result = ""
    index += 1
    while index > 0:
        index -= 1
        result = chr(ord('A') + (index % 26)) + result
        index //= 26
    return result

def apply_column_styles(sheet, specs):
    max_idx = len(specs["widths"]) - 1
    last_letter = index_to_column_letter(max_idx)
    body_text = {"fontFamily": SHEET_FONT, "fontSize": 9, "bold": False}
    header_text = {"fontFamily": SHEET_FONT, "fontSize": 10, "bold": False}

    try:
        sheet.format(f"A1:{last_letter}1", {
            "textFormat": header_text,
            "horizontalAlignment": "CENTER",
            "wrapStrategy": "WRAP"
        })
    except Exception as e:
        log_msg(f"⚠️ Header formatting skipped for {sheet.title}: {e}")

    for idx, width in enumerate(specs["widths"]):
        letter = index_to_column_letter(idx)
        align = ALIGN_MAP.get(specs.get("alignments", [])[idx], "LEFT") if idx < len(specs.get("alignments", [])) else "LEFT"
        wrap_strategy = WRAP_MAP.get(specs.get("wrap", [])[idx], "WRAP") if idx < len(specs.get("wrap", [])) else "WRAP"
        try:
            sheet.set_column_width(idx + 1, width)
        except Exception:
            pass
        try:
            sheet.format(f"{letter}:{letter}", {
                "textFormat": body_text,
                "horizontalAlignment": align,
                "wrapStrategy": wrap_strategy
            })
        except Exception:
            continue

    try:
        sheet.freeze(rows=1)
    except Exception:
        pass

def retry_gspread_call(action, *args, retries=4, delay=1, **kwargs):
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return action(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt == retries:
                raise
            log_msg(f"⚠️ GSheets write failed (attempt {attempt}/{retries}): {str(exc)[:60]}")
            time.sleep(delay)
            delay *= 2
    if last_exc:
        raise last_exc

def batch_update_with_retry(sheet, body):
    return retry_gspread_call(sheet.spreadsheet.batch_update, body)

def insert_row_with_retry(sheet, values, index, **kwargs):
    params = {"value_input_option": "USER_ENTERED"}
    params.update(kwargs)
    return retry_gspread_call(sheet.insert_row, values, index, **params)

def update_cell_with_retry(sheet, row, col, value, **kwargs):
    params = {}
    params.update(kwargs)
    return retry_gspread_call(sheet.update_cell, row, col, value, **params)

def apply_alternating_banding(sheet, total_columns, start_row=1):
    # Color-based banding removed as per updated requirements.
    return

def apply_sheet_formatting(runlist_sheet, profiles_sheet, checklist_sheet):
    """Format RunList, ProfilesData and CheckList as requested"""
    apply_column_styles(profiles_sheet, PROFILES_COLUMN_SPECS)
    apply_alternating_banding(profiles_sheet, len(PROFILES_COLUMN_SPECS["widths"]))

    apply_column_styles(runlist_sheet, RUNLIST_COLUMN_SPECS)
    apply_alternating_banding(runlist_sheet, len(RUNLIST_COLUMN_SPECS["widths"]))

    if checklist_sheet:
        apply_column_styles(checklist_sheet, CHECKLIST_COLUMN_SPECS)
        apply_alternating_banding(checklist_sheet, len(CHECKLIST_COLUMN_SPECS["widths"]))

# ============================================================================
# BROWSER & AUTHENTICATION
# ============================================================================

def setup_browser():
    """Setup headless Chrome browser"""
    try:
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option('excludeSwitches', ['enable-automation'])
        opts.add_experimental_option('useAutomationExtension', False)
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        driver = webdriver.Chrome(options=opts)
        driver.set_page_load_timeout(30)
        driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        return driver
    except Exception as e:
        log_msg(f"❌ Browser error: {e}")
        return None

def save_cookies(driver):
    """Save cookies to file"""
    try:
        with open(COOKIE_FILE, 'wb') as f:
            pickle.dump(driver.get_cookies(), f)
        log_msg("✅ Cookies saved")
    except Exception as e:
        log_msg(f"⚠️ Cookie save failed: {e}")

def load_cookies(driver):
    """Load cookies from file"""
    try:
        if not os.path.exists(COOKIE_FILE):
            return False
        driver.get(HOME_URL)
        time.sleep(2)
        with open(COOKIE_FILE, 'rb') as f:
            cookies = pickle.load(f)
        for c in cookies:
            try:
                driver.add_cookie(c)
            except:
                pass
        driver.refresh()
        time.sleep(3)
        log_msg("✅ Cookies loaded")
        return True
    except Exception as e:
        log_msg(f"⚠️ Cookie load failed: {e}")
        return False

def login(driver) -> bool:
    """Login to DamaDam"""
    try:
        driver.get(HOME_URL)
        time.sleep(2)
        
        if load_cookies(driver):
            if 'login' not in driver.current_url.lower():
                log_msg("✅ Already logged in via cookies")
                return True
        
        driver.get(LOGIN_URL)
        time.sleep(3)
        
        try:
            nick = WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#nick, input[name='nick']"))
            )
            pw = driver.find_element(By.CSS_SELECTOR, "#pass, input[name='pass']")
            btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit'], form button")
            
            nick.clear()
            nick.send_keys(LOGIN_EMAIL)
            time.sleep(0.5)
            pw.clear()
            pw.send_keys(LOGIN_PASS)
            time.sleep(0.5)
            btn.click()
            time.sleep(4)
            
            if 'login' not in driver.current_url.lower():
                save_cookies(driver)
                log_msg("✅ Login successful")
                return True
            else:
                log_msg("❌ Login failed")
                return False
        except Exception as e:
            log_msg(f"❌ Login error: {e}")
            return False
    except Exception as e:
        log_msg(f"❌ Login process error: {e}")
        return False

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def clean_text(v: str) -> str:
    """Clean text data"""
    if not v:
        return ""
    v = str(v).strip().replace('\xa0', ' ')
    bad = {
        "No city",
        "Not set",
        "[No Posts]",
        "N/A",
        "no city",
        "not set",
        "[no posts]",
        "n/a",
        "[No Post URL]",
        "[Error]",
        "none",
        "null",
        "no age",
        "no set"
    }
    return "" if v in bad else re.sub(r"\s+", " ", v)

def convert_relative_date_to_absolute(text: str) -> str:
    """Convert relative dates to absolute"""
    if not text:
        return ""
    t = text.lower().strip()
    t = (
        t.replace("mins", "minutes")
        .replace("min", "minute")
        .replace("secs", "seconds")
        .replace("sec", "second")
        .replace("hrs", "hours")
        .replace("hr", "hour")
    )
    if any(keyword in t for keyword in {"just now", "abhi"}):
        return get_pkt_time().strftime("%d-%b-%y")
    m = re.search(r"(\d+)\s*(second|minute|hour|day|week|month|year)s?\s*ago", t)
    if not m:
        return text
    amt = int(m.group(1))
    unit = m.group(2)
    s_map = {"second": 1, "minute": 60, "hour": 3600, "day": 86400, "week": 604800, "month": 2592000, "year": 31536000}
    if unit in s_map:
        dt = get_pkt_time() - timedelta(seconds=amt * s_map[unit])
        return dt.strftime("%d-%b-%y")
    return text

def to_absolute_url(href: str) -> str:
    """Ensure href is converted to absolute URL"""
    if not href:
        return ""
    href = href.strip()
    if href.startswith("/"):
        return f"{BASE_URL}{href}"
    if not href.startswith("http"):
        return f"{BASE_URL}/{href}"
    return href

def extract_text_comment_url(href: str) -> str:
    match = re.search(r"/comments/text/(\d+)/", href or "")
    if match:
        return to_absolute_url(f"/comments/text/{match.group(1)}/").rstrip("/")
    return to_absolute_url(href or "")

def extract_image_comment_url(href: str) -> str:
    match = re.search(r"/comments/image/(\d+)/", href or "")
    if match:
        return to_absolute_url(f"/content/{match.group(1)}/g/")
    return to_absolute_url(href or "")

def parse_post_timestamp(text: str) -> str:
    return convert_relative_date_to_absolute(text)

def get_friend_status(driver) -> str:
    try:
        page_source = (driver.page_source or "").lower()
        if 'action="/follow/remove/"' in page_source or 'unfollow.svg' in page_source:
            return "Yes"
        if 'follow.svg' in page_source and 'unfollow' not in page_source:
            return "No"
        return ""
    except Exception:
        return ""

def scrape_recent_post(driver, nickname: str) -> dict:
    """Fetch the latest post link and timestamp for the nickname"""
    post_url = f"{BASE_URL}/profile/public/{nickname}"
    try:
        driver.get(post_url)
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "article.mbl"))
            )
        except TimeoutException:
            return {"LPOST": "", "LDATE-TIME": ""}

        post_element = driver.find_element(By.CSS_SELECTOR, "article.mbl")
        post_data = {"LPOST": "", "LDATE-TIME": ""}

        url_selectors = [
            ("a[href*='/content/']", lambda href: to_absolute_url(href)),
            ("a[href*='/comments/text/']", extract_text_comment_url),
            ("a[href*='/comments/image/']", extract_image_comment_url),
        ]

        for selector, formatter in url_selectors:
            try:
                link = post_element.find_element(By.CSS_SELECTOR, selector)
                href = link.get_attribute("href")
                if href:
                    formatted = formatter(href)
                    if formatted:
                        post_data["LPOST"] = formatted
                        break
            except Exception:
                continue

        time_selectors = [
            "span[itemprop='datePublished']",
            "time[itemprop='datePublished']",
            "span.cxs.cgy",
            "time",
        ]

        for selector in time_selectors:
            try:
                time_elem = post_element.find_element(By.CSS_SELECTOR, selector)
                raw_text = time_elem.text.strip()
                if raw_text:
                    post_data["LDATE-TIME"] = parse_post_timestamp(raw_text)
                    break
            except Exception:
                continue

        return post_data
    except Exception:
        return {"LPOST": "", "LDATE-TIME": ""}

def load_tags_mapping(checklist_sheet):
    """Build nickname to tags mapping from CheckList sheet"""
    mapping = {}
    if not checklist_sheet:
        return mapping
    try:
        all_values = checklist_sheet.get_all_values()
    except Exception as exc:
        log_msg(f"⚠️ Failed to read CheckList: {str(exc)[:60]}")
        return mapping

    if not all_values or len(all_values) < 2:
        return mapping

    headers = all_values[0]
    for col_idx, header in enumerate(headers):
        tag_name = clean_text(header)
        if not tag_name:
            continue
        for row in all_values[1:]:
            if col_idx >= len(row):
                continue
            nickname = row[col_idx].strip()
            if not nickname:
                continue
            key = nickname.lower()
            if key in mapping:
                if tag_name not in mapping[key]:
                    mapping[key] += f", {tag_name}"
            else:
                mapping[key] = tag_name

    log_msg(f"🏷️ Loaded {len(mapping)} tags from CheckList")
    return mapping

def extract_nickname_from_user_url(user_url: str) -> str:
    if not user_url:
        return ""
    u = user_url.strip()
    m = re.search(r"/users/([^/]+)/?", u)
    if m:
        return m.group(1)
    return ""

def apply_message_template(template: str, *, name: str = "", nickname: str = "", city: str = "", posts: str = "", followers: str = "") -> str:
    if not template:
        return ""
    rendered = str(template)
    replacements = {
        "{name}": name or "",
        "{{name}}": name or "",
        "{nick}": nickname or "",
        "{{nick}}": nickname or "",
        "{city}": city or "",
        "{{city}}": city or "",
        "{posts}": posts or "",
        "{{posts}}": posts or "",
        "{followers}": followers or "",
        "{{followers}}": followers or "",
    }
    for k, v in replacements.items():
        rendered = rendered.replace(k, v)
        rendered = rendered.replace(k.upper(), v)
        rendered = rendered.replace(k.title(), v)
    return rendered


def write_message_list_csv_export(rows: list[dict], output_path: str) -> None:
    if not rows:
        return
    fieldnames = [
        "MODE",
        "NAME",
        "NICK/URL",
        "CITY",
        "POSTS",
        "FOLLOWRS",
        "MESSAGE",
        "STATUS",
        "NOTES",
        "RESULT URL",
        "DATE TIME DONE",
        "ROW",
    ]
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    needs_header = (not os.path.exists(output_path)) or os.path.getsize(output_path) == 0
    with open(output_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if needs_header:
            w.writeheader()
        for r in rows:
            w.writerow(r)

def ensure_simple_sheet_headers(sheet, headers: list[str]) -> None:
    try:
        existing = sheet.get_all_values()
    except Exception:
        existing = []
    if not existing:
        insert_row_with_retry(sheet, headers, 1)
        return
    if not existing[0] or len(existing[0]) < len(headers):
        sheet.clear()
        insert_row_with_retry(sheet, headers, 1)

def save_html_snapshot(prefix: str, html: str) -> str:
    os.makedirs("exports", exist_ok=True)
    filename = f"{prefix}_{get_pkt_time().strftime('%Y%m%d_%H%M%S')}.html"
    path = os.path.join("exports", filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html or "")
    return path

def run_inbox_mode(driver):
    inbox_url = f"{BASE_URL}/inbox/"
    driver.get(inbox_url)
    time.sleep(3)
    html = driver.page_source or ""
    saved_path = save_html_snapshot("inbox", html)

    inbox_sheet = get_or_create_sheet("Inbox")
    ensure_simple_sheet_headers(inbox_sheet, ["DATETIME", "URL", "SAVED_FILE", "STATUS"])
    insert_row_with_retry(
        inbox_sheet,
        [get_pkt_time().strftime("%d-%b-%y %I:%M %p"), inbox_url, saved_path, "Captured"],
        2,
    )
    log_msg(f"📥 Inbox captured -> {saved_path}")

def run_activity_mode(driver):
    activity_url = f"{BASE_URL}/activity/"
    driver.get(activity_url)
    time.sleep(3)
    html = driver.page_source or ""
    saved_path = save_html_snapshot("activity", html)

    activity_sheet = get_or_create_sheet("Activity")
    ensure_simple_sheet_headers(activity_sheet, ["DATETIME", "URL", "SAVED_FILE", "STATUS"])
    insert_row_with_retry(
        activity_sheet,
        [get_pkt_time().strftime("%d-%b-%y %I:%M %p"), activity_url, saved_path, "Captured"],
        2,
    )
    log_msg(f"📌 Activity captured -> {saved_path}")

# ============================================================================
# MAIN PROCESS
# ============================================================================

def run_msg_mode(driver):
    """Msg mode process"""
    print("\n" + "="*70)
    print(f"🎯 DamaDam Message Bot v{VERSION} - Fixed Flow")
    print("="*70)
    
    # CONNECT TO SHEETS
    log_msg("📊 Connecting to Google Sheets...")
    runlist_sheet = get_sheet("MessageList")
    profiles_sheet = get_or_create_sheet("ProfilesData")
    checklist_sheet = get_or_create_sheet("CheckList")
    log_msg("✅ Sheets connected\n")

    apply_sheet_formatting(runlist_sheet, profiles_sheet, checklist_sheet)
    tags_mapping = load_tags_mapping(checklist_sheet)
        
    # Initialize ProfilesData headers if needed
    if len(profiles_sheet.get_all_values()) <= 1:
        headers = ["IMAGE", "NICK NAME", "TAGS", "LAST POST", "LAST POST TIME", "FRIEND", "CITY",
                  "GENDER", "MARRIED", "AGE", "JOINED", "FOLLOWERS", "STATUS", "POSTS", 
                  "PROFILE LINK", "INTRO", "SOURCE", "DATETIME SCRAP", "POST MSG", "POST LINK"]
        profiles_sheet.insert_row(headers, 1)
        log_msg("📄 ProfilesData headers created\n")

    # Initialize MessageList headers if needed
    if len(runlist_sheet.get_all_values()) <= 1:
        headers = [
            "MODE",
            "NAME",
            "NICK/URL",
            "CITY",
            "POSTS",
            "FOLLOWRS",
            "MESSAGE",
            "STATUS",
            "NOTES",
            "RESULT URL",
            "DATE TIME DONE",
        ]
        runlist_sheet.insert_row(headers, 1)

    # Ensure CheckList headers are present
    checklist_headers = ["List 1🎌", "List 2💓", "List 3🔖", "List 4🐱‍🏍"]
    current = checklist_sheet.get_all_values()
    if not current or not current[0] or any(not c.strip() for c in current[0][:len(checklist_headers)]):
        checklist_sheet.clear()
        insert_row_with_retry(checklist_sheet, checklist_headers, 1)
        
    # GET PENDING TARGETS
    runlist_rows = runlist_sheet.get_all_values()
    pending_targets = []

    for i in range(1, len(runlist_rows)):
        row = runlist_rows[i]
        mode = row[0].strip() if len(row) > 0 else ""
        name = row[1].strip() if len(row) > 1 else ""
        nick_or_url = row[2].strip() if len(row) > 2 else ""
        city = row[3].strip() if len(row) > 3 else ""
        posts = row[4].strip() if len(row) > 4 else ""
        followers = row[5].strip() if len(row) > 5 else ""
        message_template = row[6].strip() if len(row) > 6 else ""
        status = row[7].strip() if len(row) > 7 else ""

        if not status or status.lower() != "pending":
            continue

        mode_upper = mode.upper()
        nickname = ""
        profile_url = ""

        if mode_upper == "NICK":
            nickname = nick_or_url
            profile_url = f"{BASE_URL}/users/{nickname}/"
        elif mode_upper == "URL":
            profile_url = nick_or_url
            nickname = extract_nickname_from_user_url(profile_url)
        else:
            pending_targets.append({
                'row': i + 1,
                'mode': mode,
                'nickname': "",
                'profile_url': "",
                'name': name,
                'city': city,
                'posts': posts,
                'followers': followers,
                'message_template': message_template,
                'invalid_mode': True,
            })
            continue

        pending_targets.append({
            'row': i + 1,
            'mode': mode_upper,
            'nickname': nickname,
            'profile_url': profile_url,
            'name': name,
            'city': city,
            'posts': posts,
            'followers': followers,
            'message_template': message_template,
            'invalid_mode': False,
        })
        
    if not pending_targets:
        log_msg("⚠️ No pending targets found")
        return
        
    log_msg(f"📋 Found {len(pending_targets)} pending targets\n")
    log_msg("="*70)
        
    # PROCESS EACH TARGET
    success_count = 0
    failed_count = 0

    export_rows: list[dict] = []
    export_path = os.path.join(os.getcwd(), "folderExport", "msg.csv")
        
    try:
        for idx, target in enumerate(pending_targets, 1):
            if should_exit:
                log_msg("🛑 Stop requested (Ctrl+C). Exiting after current completed targets...")
                break

            nickname = target.get('nickname', '')
            profile_url = target.get('profile_url', '')
            name = target.get('name', '')
            city = target.get('city', '')
            posts = target.get('posts', '')
            followers = target.get('followers', '')
            message = apply_message_template(
                target.get('message_template', ''),
                name=name,
                nickname=nickname,
                city=city,
                posts=posts,
                followers=followers,
            )
            runlist_row = target['row']
                
            print("\n" + "-"*70)
            log_msg(f"[{idx}/{len(pending_targets)}] 👤 Processing: {nickname}")
            print("-"*70)
                
            export_record = {
                "MODE": target.get('mode', ''),
                "NAME": name,
                "NICK/URL": profile_url if (target.get('mode', '').upper() == 'URL') else nickname,
                "CITY": city,
                "POSTS": posts,
                "FOLLOWRS": followers,
                "MESSAGE": message,
                "STATUS": "",
                "NOTES": "",
                "RESULT URL": "",
                "DATE TIME DONE": "",
                "ROW": runlist_row,
            }

            try:
                if target.get('invalid_mode'):
                    log_msg(f"  ❌ Invalid MODE: {target.get('mode', '')}")
                    update_cell_with_retry(runlist_sheet, runlist_row, 8, "Error")
                    update_cell_with_retry(runlist_sheet, runlist_row, 9, "Invalid MODE (use NICK or URL)")
                    update_cell_with_retry(runlist_sheet, runlist_row, 10, "")
                    update_cell_with_retry(runlist_sheet, runlist_row, 11, get_pkt_time().strftime("%d-%b-%y %I:%M %p"))
                    export_record["STATUS"] = "Error"
                    export_record["NOTES"] = "Invalid MODE (use NICK or URL)"
                    export_record["DATE TIME DONE"] = get_pkt_time().strftime("%d-%b-%y %I:%M %p")
                    failed_count += 1
                    export_rows.append(export_record)
                    continue

                if not nickname:
                    log_msg(f"  ❌ Missing nickname")
                    update_cell_with_retry(runlist_sheet, runlist_row, 8, "Error")
                    update_cell_with_retry(runlist_sheet, runlist_row, 9, "Invalid NICK/URL (nickname not found)")
                    update_cell_with_retry(runlist_sheet, runlist_row, 10, "")
                    update_cell_with_retry(runlist_sheet, runlist_row, 11, get_pkt_time().strftime("%d-%b-%y %I:%M %p"))
                    export_record["STATUS"] = "Error"
                    export_record["NOTES"] = "Invalid NICK/URL (nickname not found)"
                    export_record["DATE TIME DONE"] = get_pkt_time().strftime("%d-%b-%y %I:%M %p")
                    failed_count += 1
                    export_rows.append(export_record)
                    continue

                # STEP 1: Scrape Profile
                profile_data = scrape_profile(driver, nickname, profile_url=profile_url)
                if not profile_data:
                    log_msg(f"  ❌ Failed to scrape profile")
                    update_cell_with_retry(runlist_sheet, runlist_row, 8, "Error")
                    update_cell_with_retry(runlist_sheet, runlist_row, 9, "Profile scrape failed")
                    update_cell_with_retry(runlist_sheet, runlist_row, 10, "")
                    update_cell_with_retry(runlist_sheet, runlist_row, 11, get_pkt_time().strftime("%d-%b-%y %I:%M %p"))
                    export_record["STATUS"] = "Error"
                    export_record["NOTES"] = "Profile scrape failed"
                    export_record["DATE TIME DONE"] = get_pkt_time().strftime("%d-%b-%y %I:%M %p")
                    failed_count += 1
                    export_rows.append(export_record)
                    continue

                # Check if suspended
                if profile_data.get('STATUS') == 'Suspended':
                    log_msg(f"  ⚠️ Account suspended")
                    update_cell_with_retry(runlist_sheet, runlist_row, 8, "Error")
                    update_cell_with_retry(runlist_sheet, runlist_row, 9, "Account suspended")
                    update_cell_with_retry(runlist_sheet, runlist_row, 10, "")
                    update_cell_with_retry(runlist_sheet, runlist_row, 11, get_pkt_time().strftime("%d-%b-%y %I:%M %p"))
                    export_record["STATUS"] = "Error"
                    export_record["NOTES"] = "Account suspended"
                    export_record["DATE TIME DONE"] = get_pkt_time().strftime("%d-%b-%y %I:%M %p")
                    failed_count += 1
                    export_rows.append(export_record)
                    continue

                # Check post count
                post_count = int(profile_data.get('POSTS', '0'))
                if post_count == 0:
                    log_msg(f"  ⚠️ No posts available")
                    update_cell_with_retry(runlist_sheet, runlist_row, 8, "Error")
                    update_cell_with_retry(runlist_sheet, runlist_row, 9, "No posts")
                    update_cell_with_retry(runlist_sheet, runlist_row, 10, "")
                    update_cell_with_retry(runlist_sheet, runlist_row, 11, get_pkt_time().strftime("%d-%b-%y %I:%M %p"))
                    export_record["STATUS"] = "Error"
                    export_record["NOTES"] = "No posts"
                    export_record["DATE TIME DONE"] = get_pkt_time().strftime("%d-%b-%y %I:%M %p")
                    failed_count += 1
                    export_rows.append(export_record)
                    continue

                # STEP 2: Find Open Post
                post_url = find_first_open_post(driver, nickname)
                if not post_url:
                    log_msg(f"  ❌ No open posts found")
                    update_cell_with_retry(runlist_sheet, runlist_row, 8, "Error")
                    update_cell_with_retry(runlist_sheet, runlist_row, 9, "No open posts")
                    update_cell_with_retry(runlist_sheet, runlist_row, 10, "")
                    update_cell_with_retry(runlist_sheet, runlist_row, 11, get_pkt_time().strftime("%d-%b-%y %I:%M %p"))
                    export_record["STATUS"] = "Error"
                    export_record["NOTES"] = "No open posts"
                    export_record["DATE TIME DONE"] = get_pkt_time().strftime("%d-%b-%y %I:%M %p")
                    failed_count += 1
                    export_rows.append(export_record)
                    continue

                # STEP 3: Send Message & Verify
                result = send_and_verify_message(driver, post_url, message)

                # STEP 4: Update Sheets
                # Save to ProfilesData
                write_profile_to_sheet(profiles_sheet, 2, profile_data, tags_mapping)

                # Update message details
                update_cell_with_retry(profiles_sheet, 2, 19, result['msg'])  # POST MSG
                update_cell_with_retry(profiles_sheet, 2, 20, result['link'])  # POST LINK

                # Update RunList based on result
                if "Posted" in result['status']:
                    log_msg(f"  ✅ SUCCESS!")
                    update_cell_with_retry(runlist_sheet, runlist_row, 8, "Done")
                    update_cell_with_retry(runlist_sheet, runlist_row, 9, f"Posted @ {get_pkt_time().strftime('%I:%M %p')}")
                    update_cell_with_retry(runlist_sheet, runlist_row, 10, result.get('link', ''))
                    update_cell_with_retry(runlist_sheet, runlist_row, 11, get_pkt_time().strftime("%d-%b-%y %I:%M %p"))
                    export_record["STATUS"] = "Done"
                    export_record["NOTES"] = f"Posted @ {get_pkt_time().strftime('%I:%M %p')}"
                    export_record["RESULT URL"] = result.get('link', '')
                    export_record["DATE TIME DONE"] = get_pkt_time().strftime("%d-%b-%y %I:%M %p")
                    success_count += 1
                elif "verification" in result['status'].lower():
                    log_msg(f"  ⚠️ Needs manual verification")
                    update_cell_with_retry(runlist_sheet, runlist_row, 8, "Done")
                    update_cell_with_retry(runlist_sheet, runlist_row, 9, f"Check manually @ {get_pkt_time().strftime('%I:%M %p')}")
                    update_cell_with_retry(runlist_sheet, runlist_row, 10, result.get('link', ''))
                    update_cell_with_retry(runlist_sheet, runlist_row, 11, get_pkt_time().strftime("%d-%b-%y %I:%M %p"))
                    export_record["STATUS"] = "Done"
                    export_record["NOTES"] = f"Check manually @ {get_pkt_time().strftime('%I:%M %p')}"
                    export_record["RESULT URL"] = result.get('link', '')
                    export_record["DATE TIME DONE"] = get_pkt_time().strftime("%d-%b-%y %I:%M %p")
                    success_count += 1
                else:
                    log_msg(f"  ❌ FAILED: {result['status']}")
                    update_cell_with_retry(runlist_sheet, runlist_row, 8, "Error")
                    update_cell_with_retry(runlist_sheet, runlist_row, 9, result['status'])
                    update_cell_with_retry(runlist_sheet, runlist_row, 10, result.get('link', ''))
                    update_cell_with_retry(runlist_sheet, runlist_row, 11, get_pkt_time().strftime("%d-%b-%y %I:%M %p"))
                    export_record["STATUS"] = "Error"
                    export_record["NOTES"] = result['status']
                    export_record["RESULT URL"] = result.get('link', '')
                    export_record["DATE TIME DONE"] = get_pkt_time().strftime("%d-%b-%y %I:%M %p")
                    failed_count += 1

                export_rows.append(export_record)
                time.sleep(2)

            except Exception as e:
                error_msg = f"Error: {str(e)[:40]}"
                log_msg(f"  ❌ {error_msg}")
                update_cell_with_retry(runlist_sheet, runlist_row, 8, "Error")
                update_cell_with_retry(runlist_sheet, runlist_row, 9, error_msg)
                update_cell_with_retry(runlist_sheet, runlist_row, 10, "")
                update_cell_with_retry(runlist_sheet, runlist_row, 11, get_pkt_time().strftime("%d-%b-%y %I:%M %p"))
                export_record["STATUS"] = "Error"
                export_record["NOTES"] = error_msg
                export_record["DATE TIME DONE"] = get_pkt_time().strftime("%d-%b-%y %I:%M %p")
                failed_count += 1
                export_rows.append(export_record)
    except KeyboardInterrupt:
        signal_handler(None, None)
    finally:
        write_message_list_csv_export(export_rows, export_path)
        log_msg(f"📄 CSV export saved: {export_path}")
    
    # SUMMARY
    print("\n" + "="*70)
    log_msg("📊 RUN COMPLETE!")
    log_msg(f"   ✅ Success: {success_count}/{len(pending_targets)}")
    log_msg(f"   ❌ Failed: {failed_count}/{len(pending_targets)}")
    print("="*70 + "\n")
        
def main():
    """Main bot process"""
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument(
        "--mode",
        default=None,
        help="Run mode: msg | inbox | activity (overrides DD_MODE env)",
    )
    args, _unknown = parser.parse_known_args()

    effective_mode = (args.mode or DD_MODE or "Msg").strip().lower()

    print("\n" + "="*70)
    print(f"🎯 DamaDam Message Bot v{VERSION} - Mode: {effective_mode.title()}")
    print("="*70)

    if not os.path.exists(CREDENTIALS_FILE):
        log_msg(f"❌ {CREDENTIALS_FILE} not found!")
        log_msg(f"💡 Please create {CREDENTIALS_FILE} with your Google credentials")
        return

    driver = setup_browser_mod()
    if not driver:
        log_msg("❌ Browser setup failed")
        return

    try:
        log_msg("🔐 Logging in...")
        if not login_mod(driver):
            log_msg("❌ Login failed")
            return

        mode = effective_mode
        if mode == "msg":
            run_msg_mode(driver)
        elif mode == "inbox":
            run_inbox_mode_mod(driver)
        elif mode in {"activity", "activityy"}:
            run_activity_mode_mod(driver)
        else:
            log_msg(f"❌ Unknown mode: {effective_mode} (use msg/inbox/activity)")
    finally:
        driver.quit()
        log_msg("🔒 Browser closed")

# Global flag to control the main loop
should_exit = False

def signal_handler(sig, frame):
    global should_exit
    log_msg("\n\n🛑 Received shutdown signal. Finishing current operation and exiting gracefully...")
    should_exit = True

if __name__ == "__main__":
    import signal
    # Register the signal handler
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        main()
    except KeyboardInterrupt:
        signal_handler(None, None)
    except Exception as e:
        log_msg(f"❌ An error occurred: {str(e)}")
        sys.exit(1)
