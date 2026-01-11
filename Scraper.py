"""
DamaDam Bot - Message Sender V1.1.100.2 (Enhanced)
Flow: Run → Pick Nick → Scrape Profile → Go to Posts → Pick Post → 
      Post Msg → Send with CSRF → Refresh & Verify → Update Sheet → Next Nick
"""

import time
import os
import sys
import re
import pickle
import threading
import argparse
from datetime import datetime, timedelta, timezone
import gspread
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
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

VERSION = "1.1.100.2"

DEBUG = os.environ.get("DD_DEBUG", "0").strip() == "1"
VERBOSE_FORMS = os.environ.get("DD_VERBOSE_FORMS", "0").strip() == "1"
AUTO_PUSH = os.environ.get("DD_AUTO_PUSH", "1").strip() == "1"
PROFILES_SHEET_ID = os.environ.get(
    "DD_PROFILES_SHEET_ID",
    "16t-D8dCXFvheHEpncoQ_VnXQKkrEREAup7c1ZLFXvu0",
).strip()
GSHEET_API_CALLS = 0

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
CHROMEDRIVER_PATH = os.environ.get("CHROMEDRIVER_PATH", "chromedriver.exe")

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

    url = str(url).strip()

    text_match = re.search(r"/comments/text/(\d+)/", url)
    if text_match:
        return f"{BASE_URL}/comments/text/{text_match.group(1)}"

    image_match = re.search(r"/comments/image/(\d+)/", url)
    if image_match:
        return f"{BASE_URL}/comments/image/{image_match.group(1)}"
     
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

def _bool_icon(value: bool) -> str:
    return "✅" if value else "❎"

def _looks_like_url(value: str) -> bool:
    v = (value or "").strip().lower()
    return v.startswith("http") or "damadam.pk" in v

def _normalize_profile_key(value: str) -> str:
    v = (value or "").strip().lower()
    return re.sub(r"[^a-z0-9]+", "", v)

def _pick_target_and_name(mode: str, row: list[str]) -> tuple[str, str]:
    b = (row[1].strip() if len(row) > 1 and row[1] else "")
    c = (row[2].strip() if len(row) > 2 and row[2] else "")

    mode_norm = (mode or "").strip().lower()
    b_is_url = _looks_like_url(b)
    c_is_url = _looks_like_url(c)

    if mode_norm == "url":
        if b_is_url and not c_is_url:
            return b, c
        if c_is_url and not b_is_url:
            return c, b
        return c, b

    # Nick mode: nickname should not be a URL
    if b_is_url and not c_is_url:
        return c, b
    if c_is_url and not b_is_url:
        return b, c
    return c, b

def _get_gspread_client():
    if not os.path.exists(CREDENTIALS_FILE):
        log_msg(f"❌ {CREDENTIALS_FILE} not found!")
        sys.exit(1)
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
    return gspread.authorize(creds)

def load_profiles_lookup() -> dict:
    lookup: dict = {}
    if not PROFILES_SHEET_ID:
        return lookup
    try:
        client = _get_gspread_client()
        wb = client.open_by_key(PROFILES_SHEET_ID)
        ws = None
        for title in ("Profiles", "PROFILES", "PROFILE"):
            try:
                ws = wb.worksheet(title)
                break
            except Exception:
                continue
        if ws is None:
            raise WorksheetNotFound("Profiles")
        rows = ws.get("B2:K")
    except Exception as exc:
        msg = f"⚠️ Profiles lookup unavailable: {str(exc)[:80]}"
        if DEBUG:
            log_msg(msg)
        else:
            reason = str(exc)
            hint = ""
            if "403" in reason or "PERMISSION" in reason.upper() or "insufficient" in reason.lower():
                hint = " (share Profiles sheet with service account)"
            elif "404" in reason or "not found" in reason.lower():
                hint = " (check DD_PROFILES_SHEET_ID / sheet access)"

            short_reason = (reason or "").strip().replace("\n", " ")
            if short_reason:
                log_msg(f"⚠️ Profiles lookup unavailable{hint}: {short_reason[:60]}")
            else:
                log_msg(f"⚠️ Profiles lookup unavailable{hint}")
        return lookup

    for r in rows:
        nick = (r[0] if len(r) > 0 else "").strip()
        if not nick:
            continue
        city = (r[2] if len(r) > 2 else "").strip()
        followers = (r[7] if len(r) > 7 else "").strip()
        posts = (r[9] if len(r) > 9 else "").strip()
        lookup[nick.lower()] = {"CITY": city, "FOLLOWERS": followers, "POSTS": posts}

        nick_norm = _normalize_profile_key(nick)
        if nick_norm and nick_norm not in lookup:
            lookup[nick_norm] = {"CITY": city, "FOLLOWERS": followers, "POSTS": posts}

    if DEBUG:
        log_msg(f"📋 Loaded {len(lookup)} profiles from Profiles sheet")
    elif lookup:
        log_msg(f"📋 Profiles lookup loaded: {len(lookup)}")
    return lookup

# DO NOT MODIFY - Sheet structure and column mapping
# Changing this will break data mapping and cause sheet update failures
def get_or_create_msglist_sheet():
    """Get or create MsgList sheet with proper structure"""
    client = _get_gspread_client()
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

def get_or_create_run_history_sheet():
    client = _get_gspread_client()
    workbook = client.open_by_key(SHEET_ID)

    try:
        sheet = workbook.worksheet("Run History")
    except WorksheetNotFound:
        sheet = workbook.add_worksheet(title="Run History", rows=2000, cols=12)

    try:
        existing_headers = sheet.row_values(1)
    except Exception:
        existing_headers = []

    if not existing_headers:
        headers = [
            "RUN ID",
            "RUN TS",
            "MODE",
            "TARGET",
            "NAME",
            "STATUS",
            "RESULT URL",
            "MESSAGE",
            "PROCESSED",
            "SUCCESS",
            "FAILED",
            "GSHEET API CALLS",
        ]
        retry_gspread_call(sheet.insert_row, headers, 1)

    return sheet

def retry_gspread_call(action, *args, retries=4, delay=1, **kwargs):
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            global GSHEET_API_CALLS
            GSHEET_API_CALLS += 1
            return action(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt == retries:
                raise
            if DEBUG:
                log_msg(f"⚠️ GSheets write failed (attempt {attempt}/{retries}): {str(exc)[:60]}")
            time.sleep(delay)
            delay *= 2
    if last_exc:
        raise last_exc

def update_cell_with_retry(sheet, row, col, value, **kwargs):
    params = {}
    params.update(kwargs)
    return retry_gspread_call(sheet.update_cell, row, col, value, **params)

def insert_row_with_retry(sheet, row_values, row_num):
    return retry_gspread_call(sheet.insert_row, row_values, row_num)

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
        if CHROMEDRIVER_PATH and os.path.exists(CHROMEDRIVER_PATH):
            driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=opts)
        else:
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
        if DEBUG:
            log_msg(f"  🔍 Scraping recent post: {nickname}")
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

def find_first_open_post(driver, nickname: str) -> str | None:
    """Find first post with open comments"""
    url = f"{BASE_URL}/profile/public/{nickname}/"
    try:
        max_pages = int(os.environ.get("DD_MAX_POST_PAGES", "4") or "4")
        current_url = url

        for page_idx in range(1, max_pages + 1):
            log_msg(f"  📄 Opening posts page... ({page_idx}/{max_pages})")
            driver.get(current_url)
            time.sleep(3)

            posts = driver.find_elements(By.CSS_SELECTOR, "article.mbl")
            log_msg(f"  📊 Found {len(posts)} posts")

            for idx, post in enumerate(posts, 1):
                try:
                    # Prefer explicit text/image comment links if present
                    href = ""
                    for sel in [
                        "a[href*='/comments/text/']",
                        "a[href*='/comments/image/']",
                    ]:
                        try:
                            a = post.find_element(By.CSS_SELECTOR, sel)
                            href = a.get_attribute("href") or ""
                            if href:
                                break
                        except Exception:
                            continue

                    if not href:
                        reply_btn = post.find_element(By.XPATH, ".//a[button[@itemprop='discussionUrl']]")
                        href = reply_btn.get_attribute("href") or ""

                    if not href:
                        continue

                    if not href.startswith("http"):
                        href = f"{BASE_URL}{href}"

                    post_link = clean_url(href)
                    if not _looks_like_url(post_link):
                        continue

                    log_msg(f"  ✓ Found open post #{idx}: {post_link}")
                    return post_link
                except Exception:
                    continue

            # Try pagination
            try:
                next_link = driver.find_element(By.CSS_SELECTOR, "a[rel='next']")
                next_href = next_link.get_attribute("href") or ""
                if not next_href:
                    break
                current_url = next_href
            except Exception:
                break

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
        log_msg(f"  📝 Opening Post...")
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
            if DEBUG:
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
            if DEBUG:
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
            if DEBUG and reply_related:
                log_msg(f"  🎯 Found reply-related elements: {reply_related[:5]}")
        except:
            pass
        
        time.sleep(2)  # Wait for any dynamic forms to load
        
        # Find the main reply form
        try:
            # Debug: Check what forms are available
            all_forms = driver.find_elements(By.TAG_NAME, "form")
            if DEBUG and VERBOSE_FORMS:
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
            if DEBUG and VERBOSE_FORMS:
                log_msg(f"  🔍 Found {len(forms)} direct-response forms")
            
            form = None
            for i, f in enumerate(forms):
                if DEBUG and VERBOSE_FORMS:
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
            if DEBUG and VERBOSE_FORMS:
                log_msg(f"  🔐 Got CSRF token: {csrf_token[:20]}...")
            
            # Get hidden fields
            hidden_fields = {}
            for hidden in form.find_elements(By.CSS_SELECTOR, "input[type='hidden']"):
                name = hidden.get_attribute("name")
                value = hidden.get_attribute("value")
                if name and value:
                    hidden_fields[name] = value
            
            if DEBUG and VERBOSE_FORMS:
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
            log_msg("  🔄 Refreshing Page To Verify...")
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
            
            if DEBUG:
                log_msg("  🔍 Verification Checks:")
                for check_name, result in verifications.items():
                    log_msg(f"     {check_name}: {_bool_icon(result)}")
            
            # If any verification passes
            if any(verifications.values()):
                log_msg("  ✅ Message Verified!")
                return {"status": "✅ Posted", "link": clean_url(post_url), "msg": message}
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
# PROFILE SCRAPING
# ============================================================================

# DO NOT MODIFY - Profile data extraction logic
# Changing this will break template processing and data mapping, causing message failures
def scrape_profile(driver, nickname: str) -> dict | None:
    """Scrape full profile details from user page"""
    url = f"{BASE_URL}/users/{nickname}/"
    try:
        if DEBUG:
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
                    data[key] = "🚺" if low == 'female' else "🚹" if low == 'male' else value
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
    console.print(f" [bold green]DamaDam Message Bot V{VERSION} - Enhanced[/bold green]")
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
        global GSHEET_API_CALLS
        GSHEET_API_CALLS += 1
        msglist_rows = msglist_sheet.get_all_values()
        pending_targets = []
        
        for i in range(1, len(msglist_rows)):
            row = msglist_rows[i]
            if len(row) > 7:  # Ensure we have enough columns
                mode = row[0].strip().lower() if len(row) > 0 else ""
                nick_or_url, name = _pick_target_and_name(mode, row)
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

        args = argparse.ArgumentParser(add_help=False)
        args.add_argument("--max-profiles", type=int, default=None)
        parsed = args.parse_known_args()[0]
        max_profiles = parsed.max_profiles
        if max_profiles is None:
            max_profiles = int(os.environ.get("DD_MAX_PROFILES", os.environ.get("DD_BATCH_SIZE", "0")) or "0")
        if max_profiles > 0:
            pending_targets = pending_targets[:max_profiles]

        profiles_lookup: dict = {}
        if any((t.get("mode") or "") != "url" for t in pending_targets):
            profiles_lookup = load_profiles_lookup()
        
        console.print(f"[magenta]📋 Found {len(pending_targets)} pending targets[/magenta]\n")
        console.print("="*70)
        
        # PROCESS EACH TARGET
        success_count = 0
        failed_count = 0
        run_rows: list[dict] = []
        
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
                    if not _looks_like_url(post_url):
                        raise ValueError(f"Invalid URL target: {nick_or_url}")

                    # Optional Profiles lookup for URL mode (use NAME as key)
                    name_key = (name or "").strip().lower()
                    name_key_norm = _normalize_profile_key(name_key)
                    pdata = profiles_lookup.get(name_key) or profiles_lookup.get(name_key_norm)
                    if pdata:
                        pdata_city = clean_text(pdata.get("CITY", ""))
                        pdata_posts = clean_text(pdata.get("POSTS", ""))
                        pdata_followers = clean_text(pdata.get("FOLLOWERS", ""))

                        updated_fields: list[str] = []
                        if pdata_city and clean_text(city) != pdata_city:
                            city = pdata_city
                            with sheet_lock:
                                update_cell_with_retry(msglist_sheet, msglist_row, 4, city)
                            updated_fields.append("city")
                        if pdata_posts and clean_text(posts) != pdata_posts:
                            posts = pdata_posts
                            with sheet_lock:
                                update_cell_with_retry(msglist_sheet, msglist_row, 5, posts)
                            updated_fields.append("posts")
                        if pdata_followers and clean_text(followers) != pdata_followers:
                            followers = pdata_followers
                            with sheet_lock:
                                update_cell_with_retry(msglist_sheet, msglist_row, 6, followers)
                            updated_fields.append("followers")

                        if updated_fields:
                            log_msg(f"  📌 Prefilled from Profiles: {', '.join(updated_fields)}")
                        elif DEBUG:
                            log_msg("  📌 Profiles match found (no changes)")

                    log_msg(f"  🌐 Using direct URL: {post_url}")
                    # Create minimal profile data for template processing
                    profile_data = {
                        'NAME': name or 'Unknown',
                        'NICK NAME': name or 'Unknown',
                        'CITY': city,
                        'POSTS': posts,
                        'FOLLOWERS': followers,
                        'STATUS': 'URL Mode'
                    }
                else:
                    key = (nick_or_url or "").strip().lower()
                    key_norm = _normalize_profile_key(key)
                    pdata = profiles_lookup.get(key) or profiles_lookup.get(key_norm)
                    if pdata:
                        pdata_city = clean_text(pdata.get("CITY", ""))
                        pdata_posts = clean_text(pdata.get("POSTS", ""))
                        pdata_followers = clean_text(pdata.get("FOLLOWERS", ""))

                        updated_fields: list[str] = []

                        if pdata_city and clean_text(city) != pdata_city:
                            city = pdata_city
                            with sheet_lock:
                                update_cell_with_retry(msglist_sheet, msglist_row, 4, city)
                            updated_fields.append("city")
                        if pdata_posts and clean_text(posts) != pdata_posts:
                            posts = pdata_posts
                            with sheet_lock:
                                update_cell_with_retry(msglist_sheet, msglist_row, 5, posts)
                            updated_fields.append("posts")
                        if pdata_followers and clean_text(followers) != pdata_followers:
                            followers = pdata_followers
                            with sheet_lock:
                                update_cell_with_retry(msglist_sheet, msglist_row, 6, followers)
                            updated_fields.append("followers")

                        if updated_fields:
                            log_msg(
                                f"  📌 Prefilled from Profiles: {', '.join(updated_fields)}"
                            )
                        elif DEBUG:
                            log_msg("  📌 Profiles match found (no changes)")

                    # Nick mode - scrape profile first
                    if not DEBUG:
                        log_msg(f"  🔍 Scraping profile: {nick_or_url}")
                    profile_data = scrape_profile(driver, nick_or_url)
                    if not profile_data:
                        log_msg(f"  ❌ Failed to scrape profile")
                        with sheet_lock:
                            update_cell_with_retry(msglist_sheet, msglist_row, 8, "Failed")
                            update_cell_with_retry(msglist_sheet, msglist_row, 9, "Profile scrape failed")
                        failed_count += 1
                        continue

                    # Ensure template placeholders use the best-known values (Profiles sheet may be more complete
                    # than scraped values, and scraped values may fill missing Profiles data)
                    if city:
                        profile_data["CITY"] = city
                    if posts:
                        profile_data["POSTS"] = posts
                    if followers:
                        profile_data["FOLLOWERS"] = followers

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
                        run_rows.append({
                            "run_ts": get_pkt_time().strftime("%Y-%m-%d %H:%M:%S"),
                            "mode": mode,
                            "target": nick_or_url,
                            "name": name,
                            "status": "Done",
                            "result_url": clean_result_url,
                            "message": processed_message,
                        })
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
                        run_rows.append({
                            "run_ts": get_pkt_time().strftime("%Y-%m-%d %H:%M:%S"),
                            "mode": mode,
                            "target": nick_or_url,
                            "name": name,
                            "status": result['status'],
                            "result_url": clean_url(result.get('link') or ""),
                            "message": processed_message,
                        })
                        failed_count += 1
                
                time.sleep(2)
                
            except Exception as e:
                error_msg = f"Error: {str(e)[:40]}"
                log_msg(f"  ❌ {error_msg}")
                with sheet_lock:
                    update_cell_with_retry(msglist_sheet, msglist_row, 8, "Failed")
                    update_cell_with_retry(msglist_sheet, msglist_row, 9, error_msg)
                run_rows.append({
                    "run_ts": get_pkt_time().strftime("%Y-%m-%d %H:%M:%S"),
                    "mode": mode,
                    "target": nick_or_url,
                    "name": name,
                    "status": error_msg,
                    "result_url": "",
                    "message": "",
                })
                failed_count += 1
        
        # SUMMARY
        console.print("\n" + "="*70)
        log_msg("📊 RUN COMPLETE!")
        log_msg(f"   ✅ Success: {success_count}/{len(pending_targets)}")
        log_msg(f"   ❌ Failed: {failed_count}/{len(pending_targets)}")
        console.print("="*70 + "\n")
        
        run_id = get_pkt_time().strftime("%Y-%m-%d %H:%M:%S")
        try:
            run_history_sheet = get_or_create_run_history_sheet()
            values: list[list[str]] = []
            for r in run_rows:
                values.append([
                    run_id,
                    r.get("run_ts", ""),
                    r.get("mode", ""),
                    r.get("target", ""),
                    r.get("name", ""),
                    r.get("status", ""),
                    r.get("result_url", ""),
                    r.get("message", ""),
                    str(len(pending_targets)),
                    str(success_count),
                    str(failed_count),
                    str(GSHEET_API_CALLS),
                ])

            if not values:
                values.append([
                    run_id,
                    run_id,
                    "",
                    "",
                    "",
                    "SUMMARY",
                    "",
                    "",
                    str(len(pending_targets)),
                    str(success_count),
                    str(failed_count),
                    str(GSHEET_API_CALLS),
                ])

            retry_gspread_call(
                run_history_sheet.append_rows,
                values,
                value_input_option="USER_ENTERED",
            )
        except Exception as exc:
            if DEBUG:
                log_msg(f"⚠️ Run History sheet append failed: {str(exc)[:80]}")

        if AUTO_PUSH:
            try:
                status = subprocess.run(
                    ["git", "status", "--porcelain"],
                    capture_output=True,
                    text=True,
                    check=False,
                ).stdout.strip()
                if status:
                    subprocess.run(["git", "add", "."], capture_output=True, text=True)
                    subprocess.run(
                        ["git", "commit", "-m", "Update From Bot Run"],
                        capture_output=True,
                        text=True,
                    )
                    subprocess.run(["git", "push"], capture_output=True, text=True)
            except Exception as exc:
                if DEBUG:
                    log_msg(f"⚠️ Git Auto-Push Failed: {str(exc)[:80]}")
        
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
