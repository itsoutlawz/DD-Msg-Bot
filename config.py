import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

VERSION = "2.3.0"

DD_MODE = os.environ.get("DD_MODE", "Msg")

DD_LOGIN_EMAIL = os.environ.get("DD_LOGIN_EMAIL", "0utLawZ")
DD_LOGIN_PASS = os.environ.get("DD_LOGIN_PASS", "asdasd")

LOGIN_URL = "https://damadam.pk/login/"
HOME_URL = "https://damadam.pk/"
BASE_URL = "https://damadam.pk"

COOKIE_FILE = os.environ.get("COOKIE_FILE", "damadam_cookies.pkl")
DD_SHEET_ID = os.environ.get("DD_SHEET_ID", "1xph0dra5-wPcgMXKubQD7A2CokObpst7o2rWbDA10t8")
CREDENTIALS_FILE = os.environ.get("CREDENTIALS_FILE", "credentials.json")

SHEET_FONT = os.environ.get("SHEET_FONT", "Asimovian")
