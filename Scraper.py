"""
DamaDam Bot - Message Sender v1.1 (Enhanced)
Flow: Run → Pick Nick → Scrape Profile → Go to Posts → Pick Post → 
      Post Msg → Send with CSRF → Refresh & Verify → Update Sheet → Next Nick
"""

import time
import json
import os
import sys
import re
import pickle
import threading
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
import subprocess
from rich.console import Console
from rich.progress import Progress

console = Console()

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

# ============================================================================
# CONFIGURATION
# ============================================================================

VERSION = "1.1"

# Thread safety lock
sheet_lock = threading.Lock()

LOGIN_EMAIL = os.environ.get("DD_LOGIN_EMAIL", "0utLawZ")
LOGIN_PASS = os.environ.get("DD_LOGIN_PASS", "asdasd")
LOGIN_URL = "https://damadam.pk/login/"
HOME_URL = "https://damadam.pk/"
BASE_URL = "https://damadam.pk"
COOKIE_FILE = os.environ.get("COOKIE_FILE", "damadam_cookies.pkl")
SHEET_ID = os.environ.get("DD_SHEET_ID", "1xph0dra5-wPcgMXKubQD7A2CokObpst7o2rWbDA10t8")
CREDENTIALS_FILE = os.environ.get("CREDENTIALS_FILE", "credentials.json")

# ============================================================================
# HELPERS
# ============================================================================

def get_pkt_time():
    """Get current time in Pakistan timezone"""
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5)

def log_msg(m):
    """Print timestamped message"""
    console.print(f"[{get_pkt_time().strftime('%H:%M:%S')}] {m}")

def clean_url(url: str) -> str:
    """Clean URL by removing reply fragments and trailing slashes"""
    if not url:
        return url
    
    # Remove /#/reply patterns (keep the post ID)
    url = re.sub(r'/\d+/#reply$', '', url)
    url = re.sub(r'/#reply$', '', url)
    
    # Remove trailing slash
    url = url.rstrip('/')
    
    return url

def process_template_message(message: str, profile_data: dict) -> str:
    """Process template message with profile data placeholders"""
    if not message:
        return ""
    
    # Extract profile data
    name = profile_data.get('NICK NAME', profile_data.get('NAME', 'Unknown'))
    city = clean_text(profile_data.get('CITY', 'Unknown'))
    posts = profile_data.get('POSTS', '0')
    followers = profile_data.get('FOLLOWERS', '0')
    
    # Replace placeholders
    processed = message.replace('{{name}}', name)
    processed = processed.replace('{{city}}', city)
    processed = processed.replace('{{posts}}', posts)
    processed = processed.replace('{{followers}}', followers)
    
    return processed

# DO NOT MODIFY - Sheet structure and column mapping
# Changing this will break data mapping and cause sheet update failures
def get_or_create_msglist_sheet():
    """Get or create MsgList sheet with proper structure"""
    if not os.path.exists(CREDENTIALS_FILE):
        log_msg(f"❌ {CREDENTIALS_FILE} not found!")
        sys.exit(1)
    
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
    client = gspread.authorize(creds)
    workbook = client.open_by_key(SHEET_ID)
    
    try:
        sheet = workbook.worksheet("MsgList")
        # Check if headers exist and are correct
        existing_headers = sheet.row_values(1)
        expected_headers = ["MODE", "NAME", "NICK/URL", "CITY", "POSTS", "FOLLOWERS", "MESSAGE", "STATUS", "NOTES", "RESULT URL"]
        
        if existing_headers != expected_headers:
            log_msg("📄 Updating MsgList headers...")
            sheet.clear()
            sheet.insert_row(expected_headers, 1)
            log_msg("✅ MsgList headers updated")
        return sheet
    except WorksheetNotFound:
        log_msg("📄 Creating new MsgList sheet...")
        sheet = workbook.add_worksheet(title="MsgList", rows=1000, cols=10)
        headers = ["MODE", "NAME", "NICK/URL", "CITY", "POSTS", "FOLLOWERS", "MESSAGE", "STATUS", "NOTES", "RESULT URL"]
        sheet.insert_row(headers, 1)
        log_msg("✅ MsgList sheet created")
        return sheet


ALIGN_MAP = {"L": "LEFT", "C": "CENTER", "R": "RIGHT"}
WRAP_MAP = {"WRAP": "WRAP", "CLIP": "CLIP", "OVERFLOW": "OVERFLOW"}

PROFILES_COLUMN_SPECS = {
    "widths": [2, 150, 80, 2, 80, 70, 140, 40, 40, 40, 70, 40, 60, 40, 2, 10, 40, 80, 150, 2, 70],
    "alignments": ["L", "L", "C", "L", "C", "C", "L", "C", "C", "C", "C", "C", "C", "C", "L", "L", "C", "L", "L", "L", "C"],
    "wrap": ["CLIP"] * 21
}

RUNLIST_COLUMN_SPECS = {
    "widths": [200, 140, 200, 100, 100, 300],
    "alignments": ["L", "C", "L", "C", "C", "L"],
    "wrap": ["CLIP"] * 6
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
    body_text = {"fontFamily": "Asimovian", "fontSize": 9, "bold": False}
    header_text = {"fontFamily": "Asimovian", "fontSize": 10, "bold": False}

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

def _navigate_with_retry(driver, url: str, *, retries: int = 2, delay: float = 2.0) -> bool:
    for attempt in range(1, retries + 1):
        try:
            driver.get(url)
            return True
        except (TimeoutException, WebDriverException) as exc:
            log_msg(
                f"⚠️ Navigation failed ({attempt}/{retries}) to {url}: {str(exc)[:80]}"
            )
            try:
                driver.execute_script("window.stop();")
            except Exception:
                pass
            if attempt == retries:
                return False
            time.sleep(delay * attempt)
    return False


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
        opts.add_argument("--disable-software-rasterizer")
        opts.page_load_strategy = "eager"
        driver = webdriver.Chrome(options=opts)
        driver.set_page_load_timeout(45)
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
        if not _navigate_with_retry(driver, HOME_URL):
            return False
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

# ============================================================================
# CRITICAL FUNCTIONS - DO NOT MODIFY WITHOUT EXPLICIT APPROVAL
# ============================================================================

# DO NOT MODIFY - Core authentication logic
# Changing this will break login/session management and cause bot to fail authentication
def login(driver) -> bool:
    """Login to DamaDam"""
    try:
        if not _navigate_with_retry(driver, HOME_URL):
            return False
        time.sleep(2)

        if load_cookies(driver):
            # Simple verification: check if we're not on login/signup pages
            current_url = driver.current_url.lower()
            if 'login' not in current_url and 'signup' not in current_url:
                log_msg("✅ Already logged in via cookies")
                return True
            else:
                log_msg("⚠️ Cookies expired, need fresh login")

        if not _navigate_with_retry(driver, LOGIN_URL):
            log_msg("❌ Login page load failed")
            return False
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
            
            # Simple verification: check if we're not on login page anymore
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
        log_msg(f"  🔍 Scraping profile: {nickname}")
        driver.get(post_url)
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "article.mbl"))
        )
        
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

# ============================================================================
# PROFILE SCRAPING
# ============================================================================

# DO NOT MODIFY - Profile data extraction logic
# Changing this will break template processing and data mapping, causing message failures
def scrape_profile(driver, nickname: str) -> dict | None:
    """Scrape full profile details from user page"""
    url = f"{BASE_URL}/users/{nickname}/"
    try:
        log_msg(f"  🔍 Scraping profile: {nickname}")
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1.cxl.clb.lsp")))
        
        now = get_pkt_time()
        data = {
            "IMAGE": "",
            "NICK NAME": nickname,
            "TAGS": "",
            "LAST POST": "",
            "LAST POST TIME": "",
            "FRIEND": "",
            "CITY": "",
            "GENDER": "",
            "MARRIED": "",
            "AGE": "",
            "JOINED": "",
            "FOLLOWERS": "",
            "STATUS": "Unknown",
            "POSTS": "0",
            "PROFILE LINK": url.rstrip('/'),
            "INTRO": "",
            "SOURCE": "Target",
            "DATETIME SCRAP": now.strftime("%d-%b-%y %I:%M %p")
        }
        
        page_source = driver.page_source
        data['FRIEND'] = get_friend_status(driver)
        
        # Check status
        if 'account suspended' in page_source.lower():
            data['STATUS'] = "Suspended"
            return data
        elif 'background:tomato' in page_source.lower() or 'style="background:tomato"' in page_source.lower():
            data['STATUS'] = "Unverified"
        else:
            try:
                driver.find_element(By.CSS_SELECTOR, "div[style*='tomato']")
                data['STATUS'] = "Unverified"
            except:
                data['STATUS'] = "Verified"
        
        # Get intro
        for sel in ["span.cl.sp.lsp.nos", "span.cl", ".ow span.nos"]:
            try:
                intro = driver.find_element(By.CSS_SELECTOR, sel)
                if intro.text.strip():
                    data['INTRO'] = clean_text(intro.text)
                    break
            except:
                pass
        
        # Get profile fields
        fields = {'City:': 'CITY', 'Gender:': 'GENDER', 'Married:': 'MARRIED', 'Age:': 'AGE', 'Joined:': 'JOINED'}
        for label, key in fields.items():
            try:
                elem = driver.find_element(By.XPATH, f"//b[contains(text(), '{label}')]/following-sibling::span[1]")
                value = elem.text.strip()
                if not value:
                    continue
                if key == 'JOINED':
                    data[key] = convert_relative_date_to_absolute(value)
                elif key == 'GENDER':
                    low = value.lower()
                    data[key] = "💃" if low == 'female' else "🕺" if low == 'male' else value
                elif key == 'MARRIED':
                    low = value.lower()
                    if low in {'yes', 'married'}:
                        data[key] = "💖"
                    elif low in {'no', 'single', 'unmarried'}:
                        data[key] = "💔"
                    else:
                        data[key] = value
                else:
                    data[key] = clean_text(value)
            except:
                continue
        
        # Get followers
        for sel in ["span.cl.sp.clb", ".cl.sp.clb"]:
            try:
                followers = driver.find_element(By.CSS_SELECTOR, sel)
                match = re.search(r'(\d+)', followers.text)
                if match:
                    data['FOLLOWERS'] = match.group(1)
                    break
            except:
                pass
        
        # Get post count
        for sel in ["a[href*='/profile/public/'] button div:first-child", "a[href*='/profile/public/'] button div"]:
            try:
                posts = driver.find_element(By.CSS_SELECTOR, sel)
                match = re.search(r'(\d+)', posts.text)
                if match:
                    data['POSTS'] = match.group(1)
                    break
            except:
                pass
        
        # Get avatar image
        for sel in ["img[src*='avatar-imgs']", "img[src*='avatar']", "div[style*='whitesmoke'] img[src*='cloudfront.net']"]:
            try:
                img = driver.find_element(By.CSS_SELECTOR, sel)
                src = img.get_attribute('src')
                if src and ('avatar' in src or 'cloudfront.net' in src):
                    data['IMAGE'] = src.replace('/thumbnail/', '/')
                    break
            except:
                pass
        
        post_data = scrape_recent_post(driver, nickname)
        if post_data.get('LPOST'):
            data['LAST POST'] = post_data['LPOST']
        if post_data.get('LDATE-TIME'):
            data['LAST POST TIME'] = post_data['LDATE-TIME']
        
        log_msg(f"  ✅ Profile: {data['GENDER']}, {data['CITY']}, Posts: {data['POSTS']}")
        return data
    except TimeoutException:
        log_msg(f"  ⚠️ Timeout scraping {nickname}")
        return None
    except Exception as e:
        log_msg(f"  ❌ Error scraping {nickname}: {str(e)[:60]}")
        return None

# ============================================================================
# POST FINDING & MESSAGE SENDING
# ============================================================================

def find_first_open_post(driver, nickname: str) -> str | None:
    """Find first post with open comments"""
    url = f"{BASE_URL}/profile/public/{nickname}/"
    try:
        log_msg(f"  📄 Opening posts page...")
        driver.get(url)
        time.sleep(3)
        
        # Find all posts
        posts = driver.find_elements(By.CSS_SELECTOR, "article.mbl")
        log_msg(f"  📊 Found {len(posts)} posts")
        
        for idx, post in enumerate(posts, 1):
            try:
                # Look for REPLIES button
                reply_btn = post.find_element(By.XPATH, ".//a[button[@itemprop='discussionUrl']]")
                post_link = reply_btn.get_attribute("href")
                
                if post_link:
                    if not post_link.startswith('http'):
                        post_link = f"{BASE_URL}{post_link}"
                    log_msg(f"  ✓ Found open post #{idx}: {post_link}")
                    return post_link
            except:
                continue
        
        log_msg(f"  ⚠️ No open posts found")
        return None
    except Exception as e:
        log_msg(f"  ❌ Error finding posts: {str(e)[:60]}")
        return None

# DO NOT MODIFY - Core message sending and verification logic
# Changing this will break the entire messaging system and cause posting failures
def send_and_verify_message(driver, post_url: str, message: str) -> dict:
    """Send message to post and verify it was posted"""
    try:
        log_msg(f"  📝 Opening post: {post_url}")
        driver.get(post_url)
        time.sleep(3)
        
        # Check if we're on the right page
        if "damadam.pk" not in driver.current_url.lower():
            log_msg(f"  ⚠️ Redirected away from damadam.pk")
            return {"status": "Redirected", "link": driver.current_url, "msg": ""}
        
        # Check for "FOLLOW TO REPLY"
        page_source = driver.page_source
        
        if "FOLLOW TO REPLY" in page_source.upper():
            log_msg(f"  ⚠️ Need to follow user first")
            return {"status": "Not Following", "link": post_url, "msg": ""}
        
        # Try to click reply buttons to reveal forms
        try:
            # Look for reply/reaction buttons that might reveal comment forms
            reply_buttons = driver.find_elements(By.CSS_SELECTOR, "button[itemprop='discussionUrl'], a[button[@itemprop='discussionUrl']], .reply-btn, [onclick*='reply'], [onclick*='comment']")
            log_msg(f"  🔍 Found {len(reply_buttons)} reply buttons")
            
            for i, btn in enumerate(reply_buttons):
                try:
                    if btn.is_displayed() and btn.is_enabled():
                        log_msg(f"  🖱️ Clicking reply button {i+1}")
                        driver.execute_script("arguments[0].click();", btn)
                        time.sleep(1)
                except:
                    continue
                    
            # Also try looking for any buttons/links that might be reply-related
            all_buttons = driver.find_elements(By.TAG_NAME, "button")
            all_links = driver.find_elements(By.TAG_NAME, "a")
            log_msg(f"  🔍 Page has {len(all_buttons)} buttons and {len(all_links)} links")
            
            # Look for any element with text containing 'reply', 'comment', 'respond'
            interactive_elements = driver.find_elements(By.CSS_SELECTOR, "button, a, [onclick], [data-action]")
            reply_related = []
            for elem in interactive_elements:
                try:
                    text = elem.text.lower()
                    onclick = elem.get_attribute("onclick") or ""
                    if any(word in text for word in ['reply', 'comment', 'respond', 'jawab']) or any(word in onclick.lower() for word in ['reply', 'comment', 'respond']):
                        reply_related.append(elem.text[:30])
                except:
                    continue
            if reply_related:
                log_msg(f"  🎯 Found reply-related elements: {reply_related[:5]}")
        except:
            pass
        
        time.sleep(2)  # Wait for any dynamic forms to load
        
        # Find the main reply form
        try:
            # Debug: Check what forms are available
            all_forms = driver.find_elements(By.TAG_NAME, "form")
            log_msg(f"  🔍 Found {len(all_forms)} total forms")
            for i, form in enumerate(all_forms):
                try:
                    action = form.get_attribute("action") or "no-action"
                    displayed = form.is_displayed()
                    log_msg(f"     Form {i+1}: action='{action}', visible={displayed}")
                except:
                    log_msg(f"     Form {i+1}: error checking")
            
            # Look for form with action="/direct-response/send/"
            # There might be multiple forms, find the visible one (not display:none)
            forms = driver.find_elements(By.CSS_SELECTOR, "form[action*='direct-response/send']")
            log_msg(f"  🔍 Found {len(forms)} direct-response forms")
            
            form = None
            for i, f in enumerate(forms):
                try:
                    displayed = f.is_displayed()
                    log_msg(f"     Direct form {i+1}: visible={displayed}")
                except:
                    log_msg(f"     Direct form {i+1}: error checking visibility")
                    
                # Skip hidden forms (template form has style="display:none")
                if not f.is_displayed():
                    continue
                # Check if form has the main reply textarea
                try:
                    f.find_element(By.CSS_SELECTOR, "textarea[name='direct_response']")
                    form = f
                    break
                except:
                    continue
            
            if not form:
                log_msg(f"  ❌ No visible reply form found")
                return {"status": "Comments closed", "link": post_url, "msg": ""}
            
            # Get CSRF token
            csrf_input = form.find_element(By.NAME, "csrfmiddlewaretoken")
            csrf_token = csrf_input.get_attribute("value")
            log_msg(f"  🔐 Got CSRF token: {csrf_token[:20]}...")
            
            # Get hidden fields
            hidden_fields = {}
            for hidden in form.find_elements(By.CSS_SELECTOR, "input[type='hidden']"):
                name = hidden.get_attribute("name")
                value = hidden.get_attribute("value")
                if name and value:
                    hidden_fields[name] = value
            
            log_msg(f"  📋 Hidden fields: {len(hidden_fields)} found")
            
            # Find textarea - try multiple selectors
            textarea = None
            textarea_selectors = [
                "textarea[name='direct_response']",
                "textarea#id_direct_response",
                "textarea.inp"
            ]
            
            for selector in textarea_selectors:
                try:
                    textarea = form.find_element(By.CSS_SELECTOR, selector)
                    if textarea:
                        break
                except:
                    continue
            
            if not textarea:
                log_msg(f"  ❌ Textarea not found in form")
                return {"status": "Textarea not found", "link": post_url, "msg": ""}
            
            # Clear and type message
            textarea.clear()
            time.sleep(0.5)
            
            # Limit message to 350 chars
            if len(message) > 350:
                message = message[:350]
            
            textarea.send_keys(message)
            log_msg(f"  ✍️ Typed message: '{message}' ({len(message)} chars)")
            time.sleep(1)
            
            # Find send button
            send_btn = form.find_element(By.CSS_SELECTOR, "button[type='submit']")
            
            # Scroll to button
            driver.execute_script("arguments[0].scrollIntoView(true);", send_btn)
            time.sleep(0.5)
            
            # Click send
            log_msg(f"  🚀 Clicking send button...")
            try:
                send_btn.click()
            except:
                # Fallback to JavaScript click
                driver.execute_script("arguments[0].click();", send_btn)
            
            log_msg(f"  ⏳ Waiting 3 seconds for post to process...")
            time.sleep(3)
            
            # Refresh page to see new message
            log_msg(f"  🔄 Refreshing page to verify...")
            driver.get(post_url)
            time.sleep(2)
            
            # Check if message appears
            fresh_page = driver.page_source
            
            # Multiple verification methods
            verifications = {
                "username_href": f'href="/users/{LOGIN_EMAIL}/"' in fresh_page,
                "username_bold": f'<b>{LOGIN_EMAIL}</b>' in fresh_page,
                "message_in_bdi": f'<bdi>{message}</bdi>' in fresh_page,
                "recent_time": any(x in fresh_page.lower() for x in ['sec ago', 'secs ago', 'seconds ago']),
                "simple_username": LOGIN_EMAIL in fresh_page,
                "simple_message": message in fresh_page
            }
            
            log_msg(f"  🔍 Verification checks:")
            for check_name, result in verifications.items():
                log_msg(f"     {check_name}: {'✓' if result else '✗'}")
            
            # If any verification passes
            if any(verifications.values()):
                log_msg(f"  ✅ Message verified!")
                log_msg(f"  🔗 Success URL: {post_url}")
                return {"status": "✅ Posted", "link": post_url, "msg": message}
            else:
                log_msg(f"  ⚠️ Message sent but not verified")
                return {"status": "⚠️ Pending verification", "link": post_url, "msg": message}
                
        except NoSuchElementException as e:
            log_msg(f"  ❌ Form element not found: {str(e)[:60]}")
            return {"status": "Form not found", "link": post_url, "msg": ""}
            
    except Exception as e:
        log_msg(f"  ❌ Error: {str(e)[:100]}")
        return {"status": f"Error: {str(e)[:30]}", "link": post_url, "msg": ""}

# ============================================================================
# MAIN PROCESS
# ============================================================================

def write_profile_to_sheet(sheet, row_num, profile_data, tags_mapping=None):
    """Write normalized profile data to ProfilesData sheet"""
    tags_mapping = tags_mapping or {}
    cleaned = dict(profile_data)

    nickname = (cleaned.get("NICK NAME") or "").strip()
    nickname_key = nickname.lower()

    if not cleaned.get("DATETIME SCRAP"):
        cleaned["DATETIME SCRAP"] = get_pkt_time().strftime("%d-%b-%y %I:%M %p")

    if cleaned.get("LAST POST TIME"):
        cleaned["LAST POST TIME"] = convert_relative_date_to_absolute(cleaned["LAST POST TIME"])

    if nickname_key and not cleaned.get("TAGS") and nickname_key in tags_mapping:
        cleaned["TAGS"] = tags_mapping[nickname_key]

    columns = [
        "IMAGE", "NICK NAME", "TAGS", "LAST POST", "LAST POST TIME", "FRIEND", "CITY",
        "GENDER", "MARRIED", "AGE", "JOINED", "FOLLOWERS", "STATUS", "POSTS",
        "PROFILE LINK", "INTRO", "SOURCE", "DATETIME SCRAP", "POST MSG", "POST LINK"
    ]

    raw_passthrough = {"IMAGE", "LAST POST", "PROFILE LINK", "POST LINK"}

    row_values = []
    for col_name in columns:
        value = cleaned.get(col_name, "")
        if col_name in raw_passthrough:
            row_values.append(value or "")
        else:
            row_values.append(clean_text(value))

    insert_row_with_retry(sheet, row_values, row_num)

# DO NOT MODIFY - Main orchestration and MODE logic
# Changing this will break the entire bot flow and targeting system
def main():
    """Main bot process"""
    console.print("\n" + "="*70)
    console.print(f" [bold green]DamaDam Message Bot v{VERSION} - Enhanced[/bold green]")
    console.print("="*70)
    
    # Check credentials
    if not os.path.exists(CREDENTIALS_FILE):
        log_msg(f" {CREDENTIALS_FILE} not found!")
        log_msg(f" Please create {CREDENTIALS_FILE} with your Google credentials")
        return
    
    driver = setup_browser()
    if not driver:
        log_msg("❌ Browser setup failed")
        return
    
    try:
        # LOGIN
        console.print("[blue]🔐 Logging in...[/blue]")
        if not login(driver):
            log_msg("❌ Login failed")
            return
        
        # CONNECT TO SHEETS
        console.print("[blue]📊 Connecting to Google Sheets...[/blue]")
        msglist_sheet = get_or_create_msglist_sheet()
        log_msg("✅ MsgList connected\n")
        
        # GET PENDING TARGETS
        msglist_rows = msglist_sheet.get_all_values()
        pending_targets = []
        
        for i in range(1, len(msglist_rows)):
            row = msglist_rows[i]
            if len(row) > 7:  # Ensure we have enough columns
                mode = row[0].strip().lower() if len(row) > 0 else ""
                name = row[1].strip() if len(row) > 1 else ""
                nick_or_url = row[2].strip() if len(row) > 2 else ""
                city = row[3].strip() if len(row) > 3 else ""
                posts = row[4].strip() if len(row) > 4 else ""
                followers = row[5].strip() if len(row) > 5 else ""
                message = row[6].strip() if len(row) > 6 else ""
                status = row[7].strip().lower() if len(row) > 7 else ""
                
                if nick_or_url and status == "pending":
                    pending_targets.append({
                        'row': i + 1,
                        'mode': mode,
                        'name': name,
                        'nick_or_url': nick_or_url,
                        'city': city,
                        'posts': posts,
                        'followers': followers,
                        'message': message
                    })
        
        if not pending_targets:
            log_msg("⚠️ No pending targets found")
            return
        
        console.print(f"[magenta]📋 Found {len(pending_targets)} pending targets[/magenta]\n")
        console.print("="*70)
        
        # PROCESS EACH TARGET
        success_count = 0
        failed_count = 0
        
        for idx, target in enumerate(pending_targets, 1):
            mode = target['mode']
            name = target['name']
            nick_or_url = target['nick_or_url']
            city = target['city']
            posts = target['posts']
            followers = target['followers']
            message = target['message']
            msglist_row = target['row']
            
            console.print("\n" + "-"*70)
            log_msg(f"[{idx}/{len(pending_targets)}] 👤 Processing: {name}")
            console.print("-"*70)
            
            try:
                post_url = None
                profile_data = {}
                
                # STEP 1: Handle based on MODE
                if mode == "url":
                    # Direct URL mode - use the URL directly
                    post_url = clean_url(nick_or_url)
                    log_msg(f"  🌐 Using direct URL: {post_url}")
                    # Create minimal profile data for template processing
                    profile_data = {
                        'CITY': city,
                        'POSTS': posts,
                        'FOLLOWERS': followers,
                        'STATUS': 'URL Mode'
                    }
                else:
                    # Nick mode - scrape profile first
                    log_msg(f"  🔍 Scraping profile: {nick_or_url}")
                    profile_data = scrape_profile(driver, nick_or_url)
                    if not profile_data:
                        log_msg(f"  ❌ Failed to scrape profile")
                        with sheet_lock:
                            update_cell_with_retry(msglist_sheet, msglist_row, 8, "Failed")
                            update_cell_with_retry(msglist_sheet, msglist_row, 9, "Profile scrape failed")
                        failed_count += 1
                        continue

                    # Write scraped fields back to MsgList so the sheet stays in sync
                    # Only fill when existing cells are empty (avoids overwriting manual values)
                    scraped_city = clean_text(profile_data.get("CITY", ""))
                    scraped_posts = clean_text(profile_data.get("POSTS", ""))
                    scraped_followers = clean_text(profile_data.get("FOLLOWERS", ""))
                    with sheet_lock:
                        if not city and scraped_city:
                            update_cell_with_retry(msglist_sheet, msglist_row, 4, scraped_city)
                            city = scraped_city
                        if not posts and scraped_posts:
                            update_cell_with_retry(msglist_sheet, msglist_row, 5, scraped_posts)
                            posts = scraped_posts
                        if not followers and scraped_followers:
                            update_cell_with_retry(msglist_sheet, msglist_row, 6, scraped_followers)
                            followers = scraped_followers
                    
                    # Check if suspended
                    if profile_data.get('STATUS') == 'Suspended':
                        log_msg(f"  ⚠️ Account suspended")
                        with sheet_lock:
                            update_cell_with_retry(msglist_sheet, msglist_row, 8, "Skipped")
                            update_cell_with_retry(msglist_sheet, msglist_row, 9, "Account suspended")
                        failed_count += 1
                        continue
                    
                    # Check post count
                    post_count = int(profile_data.get('POSTS', '0'))
                    if post_count == 0:
                        log_msg(f"  ⚠️ No posts available")
                        with sheet_lock:
                            update_cell_with_retry(msglist_sheet, msglist_row, 8, "Skipped")
                            update_cell_with_retry(msglist_sheet, msglist_row, 9, "No posts")
                        failed_count += 1
                        continue
                    
                    # STEP 2: Find Open Post
                    post_url = find_first_open_post(driver, nick_or_url)
                    if not post_url:
                        log_msg(f"  ❌ No open posts found")
                        with sheet_lock:
                            update_cell_with_retry(msglist_sheet, msglist_row, 8, "Failed")
                            update_cell_with_retry(msglist_sheet, msglist_row, 9, "No open posts")
                        failed_count += 1
                        continue
                
                # STEP 3: Process template message
                processed_message = process_template_message(message, profile_data)
                log_msg(f"  💬 Processed message: '{processed_message}'")
                
                # STEP 4: Send Message & Verify
                result = send_and_verify_message(driver, post_url, processed_message)
                
                # STEP 5: Update MsgList based on result
                with sheet_lock:
                    if "Posted" in result['status']:
                        log_msg(f"  ✅ SUCCESS!")
                        clean_result_url = clean_url(result['link'])
                        log_msg(f"  🔗 Success URL: {clean_result_url}")
                        update_cell_with_retry(msglist_sheet, msglist_row, 8, "Done")
                        update_cell_with_retry(msglist_sheet, msglist_row, 9, f"Posted @ {get_pkt_time().strftime('%I:%M %p')}")
                        update_cell_with_retry(msglist_sheet, msglist_row, 10, clean_result_url)  # RESULT URL
                        success_count += 1
                    elif "verification" in result['status'].lower():
                        log_msg(f"  ⚠️ Needs manual verification")
                        clean_result_url = clean_url(result['link'])
                        log_msg(f"  🔗 Check URL: {clean_result_url}")
                        update_cell_with_retry(msglist_sheet, msglist_row, 8, "Done")
                        update_cell_with_retry(msglist_sheet, msglist_row, 9, f"Check manually @ {get_pkt_time().strftime('%I:%M %p')}")
                        update_cell_with_retry(msglist_sheet, msglist_row, 10, clean_result_url)  # RESULT URL
                        success_count += 1
                    else:
                        log_msg(f"  ❌ FAILED: {result['status']}")
                        update_cell_with_retry(msglist_sheet, msglist_row, 8, "Failed")
                        update_cell_with_retry(msglist_sheet, msglist_row, 9, result['status'])
                        if result['link']:
                            clean_result_url = clean_url(result['link'])
                            update_cell_with_retry(msglist_sheet, msglist_row, 10, clean_result_url)  # RESULT URL
                        failed_count += 1
                
                time.sleep(2)
                
            except Exception as e:
                error_msg = f"Error: {str(e)[:40]}"
                log_msg(f"  ❌ {error_msg}")
                with sheet_lock:
                    update_cell_with_retry(msglist_sheet, msglist_row, 8, "Failed")
                    update_cell_with_retry(msglist_sheet, msglist_row, 9, error_msg)
                failed_count += 1
        
        # SUMMARY
        console.print("\n" + "="*70)
        log_msg("📊 RUN COMPLETE!")
        log_msg(f"   ✅ Success: {success_count}/{len(pending_targets)}")
        log_msg(f"   ❌ Failed: {failed_count}/{len(pending_targets)}")
        console.print("="*70 + "\n")
        
        # Ensure GitHub connection and push changes
        subprocess.run(['git', 'add', '.'])
        subprocess.run(['git', 'commit', '-m', 'Update from bot run'])
        subprocess.run(['git', 'push'])
        
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
