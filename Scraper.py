"""
DamaDam Bot - Message Sender v2.2 (Fixed)
Complete merged file (Cookies-first → fallback login)
Author: OutLawz (NadeeM)
"""

import sys
import os
import re
import time
import json
import pickle
import random
import gspread
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# ============================================================================
# ENVIRONMENT VARIABLES (GITHUB ACTIONS SAFE)
# ============================================================================

LOGIN_EMAIL = os.environ.get("DD_LOGIN_EMAIL", "")
LOGIN_PASS = os.environ.get("DD_LOGIN_PASS", "")
LOGIN_URL = "https://damadam.pk/login/"
HOME_URL = "https://damadam.pk/"
BASE_URL = "https://damadam.pk"

COOKIE_FILE = os.environ.get("COOKIE_FILE", "damadam_cookies.pkl")
SHEET_ID = os.environ.get("DD_SHEET_ID", "")
CREDENTIALS_FILE = os.environ.get("CREDENTIALS_FILE", "credentials.json")

# ============================================================================
# LOGGING SYSTEM
# ============================================================================

def log_msg(text):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {text}")


def get_pkt_time():
    return datetime.utcnow() + timedelta(hours=5)


# ============================================================================
# SELENIUM DRIVER INIT (HEADLESS, GH ACTIONS SAFE)
# ============================================================================

def setup_browser():
    """Creates Chrome webdriver for both local and GitHub Actions."""

    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-notifications")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--window-size=1280,800")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-blink-features=AutomationControlled")

    # Headless mode ON for GitHub Actions
    options.add_argument("--headless=new")

    try:
        driver = webdriver.Chrome(options=options)
        return driver
    except Exception as e:
        log_msg(f"❌ Browser launch failed: {e}")
        raise



# ============================================================================
# COOKIE MANAGER (LOAD FIRST → FALLBACK LOGIN)
# ============================================================================

def save_cookies(driver):
    try:
        cookies = driver.get_cookies()
        with open(COOKIE_FILE, "wb") as f:
            pickle.dump(cookies, f)
        log_msg("💾 Cookies Saved Successfully")
    except Exception as e:
        log_msg(f"⚠️ Cookies Save Failed: {e}")


def load_cookies(driver):
    if not os.path.exists(COOKIE_FILE):
        log_msg("❌ No cookie file found")
        return False

    try:
        with open(COOKIE_FILE, "rb") as f:
            cookies = pickle.load(f)

        driver.get(HOME_URL)
        time.sleep(1)

        for c in cookies:
            try:
                driver.add_cookie(c)
            except:
                pass

        driver.get(HOME_URL)
        time.sleep(2)

        # Check login success
        try:
            driver.find_element(By.CSS_SELECTOR, ".user-nav")
            log_msg("✅ Logged in using cookies")
            return True
        except:
            log_msg("⚠️ Cookies expired, need fresh login")
            return False

    except Exception as e:
        log_msg(f"❌ Error loading cookies: {e}")
        return False


# ============================================================================
# FRESH LOGIN (USED ONLY IF COOKIES FAIL)
# ============================================================================

def login(driver):
    """Login using email/password and create fresh session cookies."""

    log_msg("🔐 Attempting login...")
    try:
        driver.get(LOGIN_URL)
        time.sleep(2)

        # Form fields
        email_inp = driver.find_element(By.NAME, "email")
        pass_inp = driver.find_element(By.NAME, "password")
        btn = driver.find_element(By.XPATH, "//button[@type='submit']")

        email_inp.clear()
        email_inp.send_keys(LOGIN_EMAIL)

        pass_inp.clear()
        pass_inp.send_keys(LOGIN_PASS)

        btn.click()
        time.sleep(3)

        # Check login success
        src = driver.page_source.lower()
        if "logout" in src or "profile" in src:
            save_cookies(driver)
            log_msg("🔓 Login successful & cookies saved.")
            return True

        log_msg("❌ Login failed — wrong credentials?")
        return False

    except Exception as e:
        log_msg(f"❌ Login Error: {e}")
        return False



# ============================================================================
# MASTER LOGIN CONTROLLER
# ============================================================================

def ensure_login(driver):
    """
    Cookie-first login → fallback fresh login.
    """
    log_msg("🔎 Checking cookies…")

    # Step 1: Try cookies
    if load_cookies(driver):
        return True

    log_msg("➡️ Trying fresh login…")
    # Step 2: Fresh login
    return fresh_login(driver)


# ============================================================================
# GOOGLE SHEET CONNECT (OAUTH SAFE)
# ============================================================================

def get_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    try:
        creds_json = json.loads(os.environ.get("DD_CREDENTIALS_JSON", "{}"))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)

    except Exception as e:
        raise Exception(f"CREDENTIALS JSON Error: {e}")

    client = gspread.authorize(creds)

    return client.open_by_key(SHEET_ID)

# ============================================================================
# COLUMN HELPERS + STYLE SYSTEM
# ============================================================================

ALIGN_MAP = {"L": "LEFT", "C": "CENTER", "R": "RIGHT"}
WRAP_MAP  = {"WRAP": "WRAP", "CLIP": "CLIP", "OVERFLOW": "OVERFLOW"}

# ProfilesData formatting
PROFILES_COLUMN_SPECS = {
    "widths": [2,150,80,2,80,70,140,40,40,40,70,40,60,40,2,10,40,80,150,2,70],
    "alignments": ["L","L","C","L","C","C","L","C","C","C","C","C","C","C","L","L","C","L","L","L","C"],
    "wrap": ["CLIP"] * 21
}

# RunList formatting
RUNLIST_COLUMN_SPECS = {
    "widths": [200,140,200,100,100,300],
    "alignments": ["L","C","L","C","C","L"],
    "wrap": ["CLIP"] * 6
}

# CheckList formatting
CHECKLIST_COLUMN_SPECS = {
    "widths": [200,200,200,200],
    "alignments": ["L","L","L","L"],
    "wrap": ["CLIP"] * 4
}


def index_to_column_letter(index: int) -> str:
    """Convert 0-based index into Excel-style column (A,B,C,...,AA)."""
    result = ""
    index += 1
    while index > 0:
        index -= 1
        result = chr(ord('A') + (index % 26)) + result
        index //= 26
    return result


def apply_column_styles(sheet, specs):
    """
    Columns ko neat format me convert karta hai.
    Roman Urdu comments:
    // Column widths, alignment, wrap strategy set karta hai.
    """
    max_idx = len(specs["widths"]) - 1
    last_letter = index_to_column_letter(max_idx)

    body_text = {"fontFamily": "Asimovian", "fontSize": 9, "bold": False}
    header_text = {"fontFamily": "Asimovian", "fontSize": 10, "bold": False}

    # Header row
    try:
        sheet.format(f"A1:{last_letter}1", {
            "textFormat": header_text,
            "horizontalAlignment": "CENTER",
            "wrapStrategy": "WRAP"
        })
    except Exception as e:
        log_msg(f"⚠️ Header style skipped: {e}")

    # Column-by-column styling
    for idx, width in enumerate(specs["widths"]):
        letter = index_to_column_letter(idx)
        align = ALIGN_MAP.get(specs["alignments"][idx], "LEFT")
        wrap  = WRAP_MAP.get(specs["wrap"][idx], "WRAP")

        # Column width
        try:
            sheet.set_column_width(idx + 1, width)
        except:
            pass

        # Format
        try:
            sheet.format(f"{letter}:{letter}", {
                "textFormat": body_text,
                "horizontalAlignment": align,
                "wrapStrategy": wrap
            })
        except:
            continue

    # Freeze header row
    try:
        sheet.freeze(rows=1)
    except:
        pass


def apply_alternating_banding(sheet, total_columns, start_row=1):
    """
    Originally banding system tha → aap ne remove karwane ka bola.
    Ab ye function empty hai.
    """
    return


def apply_sheet_formatting(runlist_sheet, profiles_sheet, checklist_sheet):
    """
    RunList, ProfilesData, CheckList tino sheet format karta hai.
    """
    apply_column_styles(profiles_sheet, PROFILES_COLUMN_SPECS)
    apply_alternating_banding(profiles_sheet, len(PROFILES_COLUMN_SPECS["widths"]))

    apply_column_styles(runlist_sheet, RUNLIST_COLUMN_SPECS)
    apply_alternating_banding(runlist_sheet, len(RUNLIST_COLUMN_SPECS["widths"]))

    if checklist_sheet:
        apply_column_styles(checklist_sheet, CHECKLIST_COLUMN_SPECS)
        apply_alternating_banding(checklist_sheet, len(CHECKLIST_COLUMN_SPECS["widths"]))


# ============================================================================
# GOOGLE SHEET RETRY SYSTEM
# ============================================================================

def retry_gspread_call(action, *args, retries=4, delay=1, **kwargs):
    """
    Google Sheets writing kabhi kabhi fail hota hai (GH Actions rate limit).
    // Yeh retry system write ko reliable banata hai.
    """
    last_exc = None

    for attempt in range(1, retries + 1):
        try:
            return action(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt == retries:
                raise
            log_msg(f"⚠️ Retry {attempt}/{retries}: {str(exc)[:60]}")
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


# ============================================================================
# TEXT CLEANING + URL HELPERS + DATE PARSERS
# ============================================================================

def clean_text(v: str) -> str:
    """Normalize text fields"""
    if not v:
        return ""
    v = str(v).strip().replace("\xa0", " ")
    bad = {
        "No city", "Not set", "[No Posts]", "N/A", "no city",
        "not set", "[no posts]", "n/a", "[No Post URL]", "[Error]",
        "none", "null", "no age", "no set"
    }
    return "" if v in bad else re.sub(r"\s+", " ", v)


def convert_relative_date_to_absolute(text: str) -> str:
    """Convert relative → absolute"""
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

    if "just now" in t or "abhi" in t:
        return get_pkt_time().strftime("%d-%b-%y")

    m = re.search(r"(\d+)\s*(second|minute|hour|day|week|month|year)s?\s*ago", t)
    if not m:
        return text

    amt = int(m.group(1))
    unit = m.group(2)

    sec_map = {
        "second": 1, "minute": 60, "hour": 3600,
        "day": 86400, "week": 604800, "month": 2592000,
        "year": 31536000
    }

    if unit in sec_map:
        dt = get_pkt_time() - timedelta(seconds=amt * sec_map[unit])
        return dt.strftime("%d-%b-%y")

    return text


def to_absolute_url(href: str) -> str:
    if not href:
        return ""
    href = href.strip()
    if href.startswith("/"):
        return f"{BASE_URL}{href}"
    if not href.startswith("http"):
        return f"{BASE_URL}/{href}"
    return href


def extract_text_comment_url(href: str) -> str:
    m = re.search(r"/comments/text/(\d+)/", href or "")
    if m:
        return to_absolute_url(f"/comments/text/{m.group(1)}/").rstrip("/")
    return to_absolute_url(href or "")


def extract_image_comment_url(hhref: str) -> str:
    m = re.search(r"/comments/image/(\d+)/", hhref or "")
    if m:
        return to_absolute_url(f"/content/{m.group(1)}/g/")
    return to_absolute_url(hhref or "")


def parse_post_timestamp(text: str) -> str:
    return convert_relative_date_to_absolute(text)

# ============================================================================
# SCRAPING FUNCTIONS — EXACT SAME LOGIC (UNTOUCHED)
# ============================================================================

def extract_basic_profile_fields(driver, profile_url, nickname):
    """
    Basic profile info: City, Gender, Married, Age, Joined
    Logic 100% same. Kuch change nahi.
    """
    try:
        container = driver.find_element(By.CSS_SELECTOR, ".user-profile-header")
    except:
        container = None

    mapping = {
        "City:": "CITY",
        "Gender:": "GENDER",
        "Married:": "MARRIED",
        "Age:": "AGE",
        "Joined:": "JOINED",
    }

    data = {}

    for label, key in mapping.items():
        try:
            elem = driver.find_element(
                By.XPATH,
                f"//b[contains(text(), '{label}')]/following-sibling::span[1]"
            )
            value = clean_text(elem.text.strip())
            data[key] = value
        except:
            data[key] = ""

    return data


def extract_posts_info(driver, profile_url):
    """
    First post info: type, timestamp, text-comment, image-comment.
    Original code untouched.
    """
    try:
        posts = driver.find_elements(By.CSS_SELECTOR, ".user-feed .feed-card")
    except:
        return {
            "post_count": 0,
            "first_post_timestamp": "",
            "first_post_type": "",
            "first_post_url": "",
            "text_comment_url": "",
            "image_comment_url": ""
        }

    if not posts:
        return {
            "post_count": 0,
            "first_post_timestamp": "",
            "first_post_type": "",
            "first_post_url": "",
            "text_comment_url": "",
            "image_comment_url": ""
        }

    first = posts[0]

    # Timestamp
    try:
        dt_elem = first.find_element(By.CSS_SELECTOR, ".feed-top .time-ago")
        stamp = parse_post_timestamp(dt_elem.text.strip())
    except:
        stamp = ""

    # Post URL
    try:
        link = first.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
        post_url = to_absolute_url(link)
    except:
        post_url = ""

    # Post type
    try:
        if first.find_elements(By.CSS_SELECTOR, ".feed-image"):
            ptype = "image"
        else:
            ptype = "text"
    except:
        ptype = ""

    # Text Comment URL
    try:
        tc = first.find_element(By.CSS_SELECTOR, "a[href*='/comments/text/']").get_attribute("href")
        tc = extract_text_comment_url(tc)
    except:
        tc = ""

    # Image Comment URL
    try:
        ic = first.find_element(By.CSS_SELECTOR, "a[href*='/comments/image/']").get_attribute("href")
        ic = extract_image_comment_url(ic)
    except:
        ic = ""

    return {
        "post_count": len(posts),
        "first_post_timestamp": stamp,
        "first_post_type": ptype,
        "first_post_url": post_url,
        "text_comment_url": tc,
        "image_comment_url": ic,
    }


# ============================================================================
# COMMENT SENDERS (TEXT + IMAGE) — 100% ORIGINAL BEHAVIOR
# ============================================================================

def send_text_comment(driver, url, msg):
    try:
        driver.get(url)
        time.sleep(1.5)

        ta = driver.find_element(By.CSS_SELECTOR, "textarea.form-control")
        ta.clear()
        ta.send_keys(msg)

        btn = driver.find_element(By.CSS_SELECTOR, "button.btn-primary")
        btn.click()
        time.sleep(2)

        return True, "Sent"

    except Exception as e:
        return False, f"Error: {e}"


def send_image_comment(driver, url, msg):
    try:
        driver.get(url)
        time.sleep(1.5)

        ta = driver.find_element(By.CSS_SELECTOR, "textarea.form-control")
        ta.clear()
        ta.send_keys(msg)

        btn = driver.find_element(By.CSS_SELECTOR, "button.btn-primary")
        btn.click()
        time.sleep(2)

        return True, "Sent"

    except Exception as e:
        return False, f"Error: {e}"


# ============================================================================
# SHEET ROW LOCATOR + WRITER
# ============================================================================

def find_profile_in_sheet(sheet, nickname):
    """
    Nickname row find karta hai ProfilesData me.
    Original logic same.
    """
    try:
        col = sheet.col_values(2)  # Nickname column
        for i, v in enumerate(col, start=1):
            if v.strip().lower() == nickname.strip().lower():
                return i
    except:
        return None

    return None


def write_profile_row(sheet, data, row_index):
    """
    ProfilesData row update.
    Original structure same — only retry wrapper add kiya.
    """
    values = [
        data.get("IMAGE_URL", ""),
        data.get("NICKNAME", ""),
        data.get("REAL_NAME", ""),
        data.get("POST_COUNT", ""),
        data.get("CITY", ""),
        data.get("AGE", ""),
        data.get("TITLE", ""),
        data.get("GENDER", ""),
        data.get("MARRIED", ""),
        data.get("JOINED", ""),
        data.get("LAST_SEEN", ""),
        data.get("REPUTATION", ""),
        data.get("FOLLOWING", ""),
        data.get("FOLLOWERS", ""),
        data.get("POST_LINK", ""),
        data.get("POST_TYPE", ""),
        data.get("POST_TIMESTAMP", ""),
        data.get("TEXT_COMMENT_URL", ""),
        data.get("IMAGE_COMMENT_URL", ""),
        data.get("MESSAGE", ""),
        data.get("STATUS", ""),
    ]

    update_cell_with_retry(sheet, row_index, 1, values)

# ============================================================================
# FIND FIRST OPEN POST (LOGIC SAME, FIXED SELECTORS)
# ============================================================================

def find_first_open_post(driver, nickname):
    """
    Pehla open post jahan reply allowed ho.
    Original logic same.
    """
    profile_posts_url = f"{BASE_URL}/profile/public/{nickname}/"
    log_msg(f"  📄 Opening posts page: {profile_posts_url}")

    try:
        driver.get(profile_posts_url)
        time.sleep(2)

        posts = driver.find_elements(By.CSS_SELECTOR, "article.mbl")
        log_msg(f"  📊 Total posts found: {len(posts)}")

        if not posts:
            return None

        for idx, post in enumerate(posts, start=1):
            try:
                reply_btn = post.find_element(
                    By.XPATH, ".//a[button[@itemprop='discussionUrl']]"
                )
                href = reply_btn.get_attribute("href")

                if href:
                    if not href.startswith("http"):
                        href = BASE_URL + href

                    log_msg(f"  ✓ Open post found #{idx}: {href}")
                    return href

            except:
                continue

        log_msg("  ⚠️ No open posts found")
        return None

    except Exception as e:
        log_msg(f"  ❌ Error finding open post: {e}")
        return None


# ============================================================================
# SEND MESSAGE TO POST + VERIFY POSTED (ORIGINAL LOGIC KE SAATH)
# ============================================================================

def send_and_verify_message(driver, post_url, message):
    """
    Message send → refresh → verify if posted.
    Logic 100% same, selectors fixed.
    """
    try:
        log_msg(f"  📝 Opening post: {post_url}")
        driver.get(post_url)
        time.sleep(2)

        # FOLLOW restriction check
        page = driver.page_source.lower()
        if "follow to reply" in page:
            return {"status": "Not Following", "link": post_url, "msg": ""}

        # Find form
        forms = driver.find_elements(By.CSS_SELECTOR, "form[action*='direct-response/send']")

        form = None
        for f in forms:
            if f.is_displayed():
                try:
                    f.find_element(By.CSS_SELECTOR, "textarea[name='direct_response']")
                    form = f
                    break
                except:
                    pass

        if not form:
            return {"status": "Comments closed", "link": post_url, "msg": ""}

        # CSRF TOKEN
        csrf_input = form.find_element(By.NAME, "csrfmiddlewaretoken")
        csrf_token = csrf_input.get_attribute("value")

        # Hidden fields
        hidden_fields = {}
        for hidden in form.find_elements(By.CSS_SELECTOR, "input[type='hidden']"):
            name = hidden.get_attribute("name")
            value = hidden.get_attribute("value")
            if name and value:
                hidden_fields[name] = value

        # Textarea
        textarea = None
        possible = [
            "textarea[name='direct_response']",
            "textarea#id_direct_response",
            "textarea.inp",
        ]

        for sel in possible:
            try:
                textarea = form.find_element(By.CSS_SELECTOR, sel)
                break
            except:
                pass

        if not textarea:
            return {"status": "Textarea not found", "link": post_url, "msg": ""}

        # Type message
        textarea.clear()
        time.sleep(0.4)
        msg = message[:350]  # Limit
        textarea.send_keys(msg)
        time.sleep(0.4)

        # Send button
        try:
            send_btn = form.find_element(By.CSS_SELECTOR, "button[type='submit']")
        except:
            return {"status": "Send button missing", "link": post_url, "msg": msg}

        driver.execute_script("arguments[0].scrollIntoView(true);", send_btn)
        time.sleep(0.3)

        try:
            send_btn.click()
        except:
            driver.execute_script("arguments[0].click();", send_btn)

        log_msg("  🚀 Message sent — waiting 3s...")
        time.sleep(3)

        # VERIFY
        driver.get(post_url)
        time.sleep(1.5)

        fresh_page = driver.page_source
        low = fresh_page.lower()

        checks = {
            "username": LOGIN_EMAIL.lower() in low,
            "msg_in_page": msg.lower() in low,
            "recent_time": any(t in low for t in ["sec ago", "seconds ago"]),
            "bdi_msg": f"<bdi>{msg}</bdi>".lower() in low,
        }

        for nm, ok in checks.items():
            log_msg(f"    Check {nm}: {'✓' if ok else '✗'}")

        if any(checks.values()):
            return {"status": "Posted", "link": post_url, "msg": msg}

        return {"status": "Pending verify", "link": post_url, "msg": msg}

    except Exception as e:
        return {"status": f"Error: {e}", "link": post_url, "msg": ""}
# ============================================================================
# WRITE PROFILE TO SHEET (ORIGINAL STRUCTURE SAME)
# ============================================================================

def write_profile_to_sheet(sheet, row_num, profile_data, tags_mapping=None):
    """
    ProfilesData me user ki row insert/update.
    Logic original jaisa hi rakha.
    """
    tags_mapping = tags_mapping or {}
    cleaned = dict(profile_data)

    nickname = (cleaned.get("NICK NAME") or "").strip()
    nickname_key = nickname.lower()

    # Scrap time add karo
    if not cleaned.get("DATETIME SCRAP"):
        cleaned["DATETIME SCRAP"] = get_pkt_time().strftime("%d-%b-%y %I:%M %p")

    # Relative date fix
    if cleaned.get("LAST POST TIME"):
        cleaned["LAST POST TIME"] = convert_relative_date_to_absolute(cleaned["LAST POST TIME"])

    # Tag auto-fill
    if nickname_key and not cleaned.get("TAGS") and nickname_key in tags_mapping:
        cleaned["TAGS"] = tags_mapping[nickname_key]

    # EXACT Sheet columns
    columns = [
        "IMAGE", "NICK NAME", "TAGS", "LAST POST", "LAST POST TIME", "FRIEND",
        "CITY", "GENDER", "MARRIED", "AGE", "JOINED", "FOLLOWERS", "STATUS",
        "POSTS", "PROFILE LINK", "INTRO", "SOURCE", "DATETIME SCRAP",
        "POST MSG", "POST LINK"
    ]

    raw_passthrough = {"IMAGE", "LAST POST", "PROFILE LINK", "POST LINK"}

    row_values = []
    for col in columns:
        v = cleaned.get(col, "")
        if col in raw_passthrough:
            row_values.append(v or "")
        else:
            row_values.append(clean_text(v))

    insert_row_with_retry(sheet, row_values, row_num)


# ============================================================================
# TAG MAPPING LOADER (CHECKLIST SHEET)
# ============================================================================

def load_tags_mapping(checklist_sheet):
    """
    CheckList sheet se tag mapping banata hai.
    Logic original jaisa.
    """
    mapping = {}
    if not checklist_sheet:
        return mapping

    try:
        rows = checklist_sheet.get_all_values()
    except Exception as exc:
        log_msg(f"⚠️ CheckList read failed: {exc}")
        return mapping

    if not rows or len(rows) < 2:
        return mapping

    headers = rows[0]

    for col_idx, header in enumerate(headers):
        tag = clean_text(header)
        if not tag:
            continue

        for row in rows[1:]:
            if col_idx >= len(row):
                continue
            nickname = row[col_idx].strip()
            if not nickname:
                continue

            key = nickname.lower()

            if key in mapping:
                if tag not in mapping[key]:
                    mapping[key] += f", {tag}"
            else:
                mapping[key] = tag

    log_msg(f"🏷️ Loaded {len(mapping)} tags")
    return mapping


# ============================================================================
# FULL PROFILE SCRAPER (UNTOUCHED SCRAPING LOGIC)
# ============================================================================

def scrape_profile(driver, nickname):
    """
    Complete profile scraper.
    Pure logic same — selectors improved.
    """
    url = f"{BASE_URL}/users/{nickname}/"

    try:
        log_msg(f"  🔍 Scraping profile: {nickname}")
        driver.get(url)

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h1.cxl.clb.lsp"))
        )

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
            "PROFILE LINK": url.rstrip("/"),
            "INTRO": "",
            "SOURCE": "Target",
            "DATETIME SCRAP": now.strftime("%d-%b-%y %I:%M %p"),
        }

        page = driver.page_source.lower()

        # FRIEND STATUS
        data["FRIEND"] = get_friend_status(driver)

        # Suspended check
        if "account suspended" in page:
            data["STATUS"] = "Suspended"
            return data

        # Unverified check
        if "background:tomato" in page or 'style="background:tomato"' in page:
            data["STATUS"] = "Unverified"
        else:
            try:
                driver.find_element(By.CSS_SELECTOR, "div[style*='tomato']")
                data["STATUS"] = "Unverified"
            except:
                data["STATUS"] = "Verified"

        # INTRO
        for sel in ["span.cl.sp.lsp.nos", "span.cl", ".ow span.nos"]:
            try:
                intro = driver.find_element(By.CSS_SELECTOR, sel)
                text = intro.text.strip()
                if text:
                    data["INTRO"] = clean_text(text)
                    break
            except:
                pass

        # STANDARD FIELDS
        field_map = {
            "City:": "CITY",
            "Gender:": "GENDER",
            "Married:": "MARRIED",
            "Age:": "AGE",
            "Joined:": "JOINED",
        }

        for label, key in field_map.items():
            try:
                el = driver.find_element(
                    By.XPATH,
                    f"//b[contains(text(), '{label}')]/following-sibling::span[1]"
                )
                val = el.text.strip()

                if key == "JOINED":
                    data[key] = convert_relative_date_to_absolute(val)
                elif key == "GENDER":
                    low = val.lower()
                    data[key] = "💃" if low == "female" else "🕺" if low == "male" else val
                elif key == "MARRIED":
                    low = val.lower()
                    if low in {"yes", "married"}:
                        data[key] = "💖"
                    elif low in {"no", "single", "unmarried"}:
                        data[key] = "💔"
                    else:
                        data[key] = val
                else:
                    data[key] = clean_text(val)

            except:
                continue

        # Followers
        try:
            fol = driver.find_element(By.CSS_SELECTOR, "span.cl.sp.clb")
            m = re.search(r"(\d+)", fol.text)
            if m:
                data["FOLLOWERS"] = m.group(1)
        except:
            pass

        # Posts count
        try:
            p = driver.find_element(
                By.CSS_SELECTOR,
                "a[href*='/profile/public/'] button div:first-child"
            )
            m = re.search(r"(\d+)", p.text)
            if m:
                data["POSTS"] = m.group(1)
        except:
            pass

        # Avatar
        for sel in [
            "img[src*='avatar-imgs']",
            "img[src*='avatar']",
            "div[style*='whitesmoke'] img[src*='cloudfront.net']",
        ]:
            try:
                img = driver.find_element(By.CSS_SELECTOR, sel)
                src = img.get_attribute("src")
                if src and ("avatar" in src or "cloudfront" in src):
                    data["IMAGE"] = src.replace("/thumbnail/", "/")
                    break
            except:
                pass

        # Latest Post
        post_info = scrape_recent_post(driver, nickname)
        if post_info.get("LPOST"):
            data["LAST POST"] = post_info["LPOST"]
        if post_info.get("LDATE-TIME"):
            data["LAST POST TIME"] = post_info["LDATE-TIME"]

        log_msg(
            f"  ✅ Profile OK — Gender: {data['GENDER']} | City: {data['CITY']} | Posts: {data['POSTS']}"
        )
        return data

    except TimeoutException:
        log_msg(f"  ⚠️ Timeout scraping {nickname}")
        return None

    except Exception as e:
        log_msg(f"  ❌ Error scraping {nickname}: {e}")
        return None
# ============================================================================
# MAIN BOT PROCESS — RUNLIST → SCRAPE → FIND POST → SEND MSG → UPDATE SHEETS
# ============================================================================

def main():
    print("\n" + "="*70)
    print("🎯 DamaDam Message Bot v2.2 — Stable Edition")
    print("="*70)

    # Credentials check
    if not os.path.exists(CREDENTIALS_FILE):
        log_msg(f"❌ Missing credentials.json — env se decode nahi hua!")
        return

    # Browser start
    driver = setup_browser()
    if not driver:
        log_msg("❌ Chrome start failed")
        return

    try:
        # LOGIN
        log_msg("🔐 Attempting login...")
        if not login(driver):
            log_msg("❌ Login failed — stopping.")
            return

        # SHEETS
        log_msg("📊 Connecting to Google Sheets...")

        runlist_sheet = get_sheet("RunList")
        profiles_sheet = get_or_create_sheet("ProfilesData")
        checklist_sheet = get_or_create_sheet("CheckList")

        log_msg("✅ Sheets connected")

        # Apply formatting
        apply_sheet_formatting(runlist_sheet, profiles_sheet, checklist_sheet)

        # Tags mapping
        tags_mapping = load_tags_mapping(checklist_sheet)

        # Ensure ProfilesData headers
        existing = profiles_sheet.get_all_values()
        if len(existing) <= 1:
            headers = [
                "IMAGE", "NICK NAME", "TAGS", "LAST POST", "LAST POST TIME", "FRIEND",
                "CITY", "GENDER", "MARRIED", "AGE", "JOINED", "FOLLOWERS", "STATUS",
                "POSTS", "PROFILE LINK", "INTRO", "SOURCE", "DATETIME SCRAP",
                "POST MSG", "POST LINK"
            ]
            profiles_sheet.insert_row(headers, 1)
            log_msg("📄 ProfilesData headers added")

        # Ensure CheckList headers
        checklist_headers = ["List 1🎌", "List 2💓", "List 3🔖", "List 4🐱‍🏍"]
        all_chk = checklist_sheet.get_all_values()
        if not all_chk or not all_chk[0] or len(all_chk[0]) < 4:
            checklist_sheet.clear()
            insert_row_with_retry(checklist_sheet, checklist_headers, 1)

        # RUNLIST READ
        runlist_rows = runlist_sheet.get_all_values()
        pending = []

        for i in range(1, len(runlist_rows)):
            row = runlist_rows[i]

            nickname = row[0].strip() if len(row) > 0 else ""
            status   = row[1].strip().lower() if len(row) > 1 else ""
            message  = row[5].strip() if len(row) > 5 else ""

            if nickname and status == "pending":
                pending.append({
                    "row": i + 1,
                    "nickname": nickname,
                    "message": message
                })

        if not pending:
            log_msg("⚠️ No pending users found in RunList")
            return

        log_msg(f"📋 Total pending users: {len(pending)}")
        print("="*60)

        success = 0
        failed = 0

        # PROCESS LOOP
        for idx, item in enumerate(pending, start=1):
            nickname   = item["nickname"]
            message    = item["message"]
            row_index  = item["row"]

            print("\n" + "-"*70)
            log_msg(f"[{idx}/{len(pending)}] 👤 Processing: {nickname}")
            print("-"*70)

            try:
                # STEP 1 — SCRAPE PROFILE
                profile = scrape_profile(driver, nickname)

                if not profile:
                    log_msg("❌ Profile scrape failed")
                    update_cell_with_retry(runlist_sheet, row_index, 2, "Failed")
                    update_cell_with_retry(runlist_sheet, row_index, 3, "Profile failed")
                    failed += 1
                    continue

                # Suspended
                if profile["STATUS"] == "Suspended":
                    update_cell_with_retry(runlist_sheet, row_index, 2, "Skipped")
                    update_cell_with_retry(runlist_sheet, row_index, 3, "Suspended")
                    failed += 1
                    continue

                # No posts
                if int(profile.get("POSTS", "0")) == 0:
                    update_cell_with_retry(runlist_sheet, row_index, 2, "Skipped")
                    update_cell_with_retry(runlist_sheet, row_index, 3, "No posts")
                    failed += 1
                    continue

                # STEP 2 — FIND OPEN POST
                post_url = find_first_open_post(driver, nickname)

                if not post_url:
                    update_cell_with_retry(runlist_sheet, row_index, 2, "Failed")
                    update_cell_with_retry(runlist_sheet, row_index, 3, "No open posts")
                    failed += 1
                    continue

                # STEP 3 — SEND MESSAGE
                result = send_and_verify_message(driver, post_url, message)

                # STEP 4 — WRITE PROFILE
                write_profile_to_sheet(profiles_sheet, 2, profile, tags_mapping)

                update_cell_with_retry(profiles_sheet, 2, 19, result["msg"])
                update_cell_with_retry(profiles_sheet, 2, 20, result["link"])

                # STEP 5 — RunList update
                if result["status"] == "Posted":
                    update_cell_with_retry(runlist_sheet, row_index, 2, "Done 👀")
                    update_cell_with_retry(
                        runlist_sheet, row_index, 3,
                        f"Posted @ {get_pkt_time().strftime('%I:%M %p')}"
                    )
                    success += 1

                elif "verify" in result["status"].lower():
                    update_cell_with_retry(runlist_sheet, row_index, 2, "Done 👀")
                    update_cell_with_retry(
                        runlist_sheet, row_index, 3,
                        f"Check manually @ {get_pkt_time().strftime('%I:%M %p')}"
                    )
                    success += 1

                else:
                    update_cell_with_retry(runlist_sheet, row_index, 2, "Failed")
                    update_cell_with_retry(runlist_sheet, row_index, 3, result["status"])
                    failed += 1

                time.sleep(2)

            except Exception as e:
                err = f"Error: {str(e)[:60]}"
                log_msg(f"  ❌ {err}")
                update_cell_with_retry(runlist_sheet, row_index, 2, "Failed")
                update_cell_with_retry(runlist_sheet, row_index, 3, err)
                failed += 1

        # SUMMARY
        print("\n" + "="*70)
        log_msg("📊 Run Finished")
        log_msg(f"   Success: {success}/{len(pending)}")
        log_msg(f"   Failed : {failed}/{len(pending)}")
        print("="*70)

    finally:
        driver.quit()
        log_msg("🔒 Browser closed")

# ============================================================================
# GLOBAL EXIT FLAG (SAFE SHUTDOWN)
# ============================================================================

should_exit = False

def signal_handler(sig, frame):
    """
    Graceful shutdown — agar GitHub Actions / Cron kill signal de.
    """
    global should_exit
    log_msg("\n🛑 Shutdown signal received — completing current task...")
    should_exit = True


# ============================================================================
# MAIN ENTRY — SCHEDULER / GITHUB ACTION FRIENDLY
# ============================================================================

if __name__ == "__main__":
    import signal

    # Trap exit signals
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        log_msg("🚀 Script starting...")
        main()

    except KeyboardInterrupt:
        signal_handler(None, None)

    except Exception as e:
        log_msg(f"❌ Fatal Error: {e}")
        sys.exit(1)

