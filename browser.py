import os
import time
import pickle

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from config import HOME_URL, LOGIN_URL, COOKIE_FILE, DD_LOGIN_EMAIL, DD_LOGIN_PASS
from utils import log_msg


def setup_browser():
    try:
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        service = None
        local_driver = os.path.join(os.getcwd(), "chromedriver.exe")
        if os.path.exists(local_driver):
            service = Service(executable_path=local_driver)
        driver = webdriver.Chrome(service=service, options=opts) if service else webdriver.Chrome(options=opts)
        driver.set_page_load_timeout(30)
        driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        return driver
    except Exception as e:
        log_msg(f"❌ Browser error: {e}")
        return None


def save_cookies(driver):
    try:
        with open(COOKIE_FILE, "wb") as f:
            pickle.dump(driver.get_cookies(), f)
        log_msg("✅ Cookies saved")
    except Exception as e:
        log_msg(f"⚠️ Cookie save failed: {e}")


def load_cookies(driver):
    try:
        if not os.path.exists(COOKIE_FILE):
            return False
        driver.get(HOME_URL)
        time.sleep(2)
        with open(COOKIE_FILE, "rb") as f:
            cookies = pickle.load(f)
        for c in cookies:
            try:
                driver.add_cookie(c)
            except Exception:
                pass
        driver.refresh()
        time.sleep(3)
        log_msg("✅ Cookies loaded")
        return True
    except Exception as e:
        log_msg(f"⚠️ Cookie load failed: {e}")
        return False


def login(driver) -> bool:
    try:
        driver.get(HOME_URL)
        time.sleep(2)

        if load_cookies(driver):
            if "login" not in driver.current_url.lower():
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
            nick.send_keys(DD_LOGIN_EMAIL)
            time.sleep(0.5)
            pw.clear()
            pw.send_keys(DD_LOGIN_PASS)
            time.sleep(0.5)
            btn.click()
            time.sleep(4)

            if "login" not in driver.current_url.lower():
                save_cookies(driver)
                log_msg("✅ Login successful")
                return True

            log_msg("❌ Login failed")
            return False
        except Exception as e:
            log_msg(f"❌ Login error: {e}")
            return False
    except Exception as e:
        log_msg(f"❌ Login process error: {e}")
        return False
